//! Reader of a multi-channel data block.
//!
//!

use super::records::{RecordEntryPlan, RecordPlan};
use crate::io::reader::TdmsReader;
use crate::{error::TdmsError, io::data_types::TdmsStorageType};
use std::num::NonZeroU64;
use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

/// The multichannel interleaved reader will read from an interleaved block.
///
/// We will assume a single datatype as it is unclear if multiple types exist in the wild.
pub struct MultiChannelInterleavedReader<R: Read + Seek, T: TdmsReader<R>> {
    reader: T,
    _marker: PhantomData<R>,
    block_size: NonZeroU64,
    block_start: u64,
}

impl<R: Read + Seek, T: TdmsReader<R>> MultiChannelInterleavedReader<R, T> {
    pub fn new(reader: T, block_start: u64, block_size: NonZeroU64) -> Self {
        Self {
            reader,
            _marker: PhantomData,
            block_size,
            block_start,
        }
    }

    /// Read the data from the block for the channels specified into the output slices,
    /// using the skip amounts provided in the plan.
    ///
    /// Returns the number of values read in this block.
    ///
    /// *ASSUMPTION*: All channels have the same number of values available. The spec
    /// allows for different lengths, but all clients have I have seen do not.
    ///
    /// For interleaved data, we skip the minimum across all channels (entire rows),
    /// then read rows while discarding samples for channels that need more skipping.
    pub fn read<D: TdmsStorageType>(
        &mut self,
        mut channels: RecordPlan<D>,
    ) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;
        let total_row_count = self.block_size.get() / channels.row_size() as u64;

        // Find minimum skip (we can skip entire rows up to this point)
        let min_skip = channels.block_skips().min().unwrap_or(0);

        // Skip entire rows
        if min_skip > 0 {
            let skip_bytes = min_skip as i64 * channels.row_size() as i64;
            self.reader.move_position(skip_bytes)?;
        }

        // Calculate the remaining skip per channel
        for skip in channels.block_skips_mut() {
            *skip = skip.saturating_sub(min_skip);
        }

        // Read rows, discarding samples for channels that still need to skip
        let rows_to_process = total_row_count.saturating_sub(min_skip);

        let mut samples_read = 0;
        for row in 0..rows_to_process {
            let mut any_values_read = false;
            for read_instruction in channels.read_instructions().iter_mut() {
                match &mut read_instruction.plan {
                    RecordEntryPlan::Read { output, block_skip } => {
                        let read_value = self.reader.read_value()?;

                        // Only write if we've skipped enough for this channel
                        if row >= *block_skip
                            && let Some(value) = output.next()
                        {
                            *value = read_value;
                            any_values_read = true;
                        }
                    }
                    RecordEntryPlan::Skip(bytes) => {
                        self.reader.move_position(*bytes)?;
                    }
                };
            }
            if !any_values_read {
                break;
            }
            samples_read += 1;
        }

        Ok(samples_read)
    }

    pub fn read_from<D: TdmsStorageType>(
        &mut self,
        mut channels: RecordPlan<D>,
        samples_to_skip: u64,
    ) -> Result<usize, TdmsError> {
        for channel_skip in channels.block_skips_mut() {
            *channel_skip = samples_to_skip;
        }
        self.read(channels)
    }
}

#[cfg(test)]
mod tests {
    use crate::{io::data_types::DataType, io::reader::BigEndianReader, meta_data::RawDataMeta};

    use super::*;
    use std::io::{Cursor, Write};

    fn create_test_buffer() -> Cursor<Vec<u8>> {
        let buffer = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(buffer);
        for index in 0..100 {
            let value = index as f64;
            cursor.write(&value.to_be_bytes()).unwrap();
        }
        cursor
    }

    fn create_test_meta_data(columns: usize) -> Vec<RawDataMeta> {
        let rows = 1024 / columns;
        let mut meta_data = Vec::with_capacity(columns);
        for _ in 0..columns {
            meta_data.push(RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: rows as u64,
                total_size_bytes: None,
            });
        }
        meta_data
    }

    #[test]
    fn read_data_interleaved_single() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(2);

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output, vec![0.0, 2.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_multi() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output_1, vec![0.0, 4.0, 8.0]);
        assert_eq!(output_2, vec![2.0, 6.0, 10.0]);
    }

    #[test]
    fn read_data_interleaved_multi_different_lengths() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 2];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output_1, vec![0.0, 4.0, 8.0]);
        assert_eq!(output_2, vec![2.0, 6.0]);
    }

    #[test]
    fn read_data_interleaved_with_skip() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        // Skip first 2 rows (samples)
        let rows_read = reader.read_from(read_plan, 2).unwrap();

        // Interleaved: [0,1,2,3][4,5,6,7][8,9,10,11]...
        // After skipping 2 rows: starts at row 2 which is [8,9,10,11]
        assert_eq!(output_1, vec![8.0, 12.0, 16.0]);
        assert_eq!(output_2, vec![10.0, 14.0, 18.0]);
        assert_eq!(rows_read, 3);
    }
}
