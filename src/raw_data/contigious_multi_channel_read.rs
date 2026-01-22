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

/// The multichannel contiguous reader will read from a contiguous block.
///
/// We will assume a single datatype as it is unclear if multiple types exist in the wild.
pub struct MultiChannelContiguousReader<R: Read + Seek, T: TdmsReader<R>> {
    reader: T,
    _marker: PhantomData<R>,
    block_size: NonZeroU64,
    block_start: u64,
}

impl<R: Read + Seek, T: TdmsReader<R>> MultiChannelContiguousReader<R, T> {
    pub fn new(reader: T, block_start: u64, block_size: NonZeroU64) -> Self {
        Self {
            reader,
            _marker: PhantomData,
            block_size,
            block_start,
        }
    }

    /// Read the data from the block for the channels specified into the output slices.
    ///
    /// Returns the number of values read from the last read channel.
    /// *ASSUMPTION*: All channels have the same number of values available. The spec
    /// doesn't enforce this but all clients have I have seen do.
    ///
    pub fn read<D: TdmsStorageType>(
        &mut self,
        channels: RecordPlan<D>,
    ) -> Result<usize, TdmsError> {
        // Since the skip is highly efficient for contiguous data, we can use a
        // single implementation.
        self.read_from(channels, 0)
    }

    /// Read the data from the block starting at a specific sample offset.
    ///
    /// For contiguous data, samples are stored sequentially per channel:
    /// [Ch1 S0][Ch1 S1]...[Ch1 SN][Ch2 S0][Ch2 S1]...
    ///
    /// To skip samples, we need to skip within each channel's contiguous data.
    pub fn read_from<D: TdmsStorageType>(
        &mut self,
        mut channels: RecordPlan<D>,
        start_sample: u64,
    ) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;

        let total_sub_blocks = self.block_size.get() / channels.block_size() as u64;

        // Calculate how many complete sub-blocks to skip and the remainder
        let sub_block_length = channels.read_instructions()[0].length as u64;
        let sub_blocks_to_skip = start_sample / sub_block_length;
        let remainder_skip = start_sample % sub_block_length;

        let mut length = 0;

        for sub_block_idx in 0..total_sub_blocks {
            if sub_block_idx < sub_blocks_to_skip {
                // Skip entire sub-block by seeking past it
                let skip_bytes = channels.block_size() as i64;
                self.reader.move_position(skip_bytes)?;
            } else if sub_block_idx == sub_blocks_to_skip {
                // First sub-block to read - apply remainder skip
                for channel in channels.read_instructions() {
                    if let RecordEntryPlan::Read {
                        block_skip: skip_first_samples,
                        ..
                    } = &mut channel.plan
                    {
                        *skip_first_samples = remainder_skip;
                    }
                }
                length += self.read_sub_block(&mut channels)?;
            } else {
                for channel in channels.read_instructions() {
                    if let RecordEntryPlan::Read {
                        block_skip: skip_first_samples,
                        ..
                    } = &mut channel.plan
                    {
                        *skip_first_samples = 0;
                    }
                }
                // Subsequent sub-blocks - no skip
                length += self.read_sub_block(&mut channels)?;
            }
        }

        Ok(length)
    }

    /// Read the data from the block with per-channel skip amounts.
    ///
    /// For contiguous data, each channel can skip independently by seeking.
    pub fn read_with_per_channel_skip<D: TdmsStorageType>(
        &mut self,
        mut channels: RecordPlan<D>,
        skip_amounts: &[u64],
    ) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;

        let total_sub_blocks = self.block_size.get() / channels.block_size() as u64;

        // Calculate per-channel sub-blocks to skip and remainders
        let sub_block_length = channels.read_instructions()[0].length as u64;
        let sub_blocks_to_skip: Vec<u64> = skip_amounts
            .iter()
            .map(|&skip| skip / sub_block_length)
            .collect();
        let remainder_skips: Vec<u64> = skip_amounts
            .iter()
            .map(|&skip| skip % sub_block_length)
            .collect();

        let mut length = 0;

        for sub_block_idx in 0..total_sub_blocks {
            // Check if any channel needs to read from this sub-block
            let any_channel_reads = sub_blocks_to_skip.iter().all(|&skip| sub_block_idx >= skip);

            if !any_channel_reads {
                // Skip entire sub-block
                let skip_bytes = channels.block_size() as i64;
                self.reader.move_position(skip_bytes)?;
            } else {
                // Build skip amounts for this sub-block
                let channel_skip_values =
                    channels
                        .read_instructions()
                        .iter_mut()
                        .filter_map(|instruction| {
                            if let RecordEntryPlan::Read {
                                block_skip: skip_first_samples,
                                ..
                            } = &mut instruction.plan
                            {
                                Some(skip_first_samples)
                            } else {
                                None
                            }
                        });
                for ((blocks_to_skip, remainder_skip), sub_block_skip) in sub_blocks_to_skip
                    .iter()
                    .zip(remainder_skips.iter())
                    .zip(channel_skip_values)
                {
                    *sub_block_skip = Self::calculate_skip_for_this_block(
                        sub_block_idx,
                        *blocks_to_skip,
                        *remainder_skip,
                    );
                }
                length += self.read_sub_block(&mut channels)?;
            }
        }

        Ok(length)
    }

    fn calculate_skip_for_this_block(
        sub_block_idx: u64,
        blocks_to_skip: u64,
        remainder_skip: u64,
    ) -> u64 {
        if sub_block_idx == blocks_to_skip {
            // First sub-block to read for this channel - use remainder
            remainder_skip
        } else if sub_block_idx > blocks_to_skip {
            // Subsequent sub-blocks - no skip
            0
        } else {
            // Should not happen if any_channel_reads is correct
            0
        }
    }

    fn read_sub_block<D: TdmsStorageType>(
        &mut self,
        channels: &mut RecordPlan<'_, D>,
    ) -> Result<usize, TdmsError> {
        let mut length = 0;

        for read_instruction in channels.read_instructions().iter_mut() {
            match &mut read_instruction.plan {
                RecordEntryPlan::Read {
                    output,
                    block_skip: skip_first_samples,
                } => {
                    let skip = (*skip_first_samples).min(read_instruction.length as u64) as usize;

                    let samples_to_read = read_instruction.length.saturating_sub(skip);

                    // Skip samples by seeking
                    if skip > 0 {
                        let skip_bytes = skip as i64 * D::SIZE_BYTES as i64;
                        self.reader.move_position(skip_bytes)?;
                    }

                    // Read the remaining samples
                    length = self.read_sequential_samples(output, samples_to_read)?;
                }
                RecordEntryPlan::Skip(bytes) => {
                    let skip_bytes = *bytes * read_instruction.length as i64;
                    self.reader.move_position(skip_bytes)?;
                }
            };
        }

        Ok(length)
    }

    /// Reads the samples until the specified value or the output ends.
    fn read_sequential_samples<'a, D: TdmsStorageType, I: Iterator<Item = &'a mut D>>(
        &mut self,
        output: &mut I,
        samples_to_read: usize,
    ) -> Result<usize, TdmsError> {
        let mut length = 0;
        for output_value in output.take(samples_to_read) {
            *output_value = self.reader.read_value()?;
            length += 1;
        }
        // Skip to end of unread samples.
        let unread_samples = samples_to_read - length;
        if unread_samples > 0 {
            self.reader
                .move_position(unread_samples as i64 * D::SIZE_BYTES as i64)?;
        }
        Ok(length)
    }
}

#[cfg(test)]
mod tests {
    use crate::{io::data_types::DataType, io::reader::BigEndianReader, meta_data::RawDataMeta};

    use super::*;
    use std::io::{Cursor, Write};

    const TEST_BUFFER_SIZE: usize = 100;

    fn create_test_buffer() -> Cursor<Vec<u8>> {
        let buffer = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(buffer);
        for index in 0..TEST_BUFFER_SIZE {
            let value = index as f64;
            cursor.write(&value.to_be_bytes()).unwrap();
        }
        cursor
    }

    fn create_test_meta_data(columns: usize) -> Vec<RawDataMeta> {
        let rows = TEST_BUFFER_SIZE / columns;
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
    fn read_data_contigious_single() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(2);

        let mut reader = MultiChannelContiguousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn read_data_contigous_multi() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContiguousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        let output_2_start = length * 2.0;
        reader.read(read_plan).unwrap();
        assert_eq!(output_1, vec![0.0, 1.0, 2.0]);
        assert_eq!(
            output_2,
            vec![output_2_start, output_2_start + 1.0, output_2_start + 2.0]
        );
    }

    #[test]
    fn read_data_contigous_multi_with_repeated_writes() {
        let mut buffer = create_test_buffer();
        let mut meta = create_test_meta_data(4);

        //drop the number of writes to less than the number of reads to test it picking up
        //repeates.
        for channel in meta.iter_mut() {
            channel.number_of_values = 2;
        }
        // This should result in:
        // ch1: 0, 1, 8, 9
        // ch2: 2, 3, 10, 11
        // ch3: 4, 5, 12, 13
        // ch4: 6, 7, 14, 15

        let mut reader = MultiChannelContiguousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        reader.read(read_plan).unwrap();
        assert_eq!(output_1, vec![0.0, 1.0, 8.0]);
        assert_eq!(output_2, vec![4.0, 5.0, 12.0]);
    }

    #[test]
    fn read_data_contigious_multi_different_lengths() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContiguousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 2];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        reader.read(read_plan).unwrap();

        let output2_start = length * 2.0;
        assert_eq!(output_1, vec![0.0, 1.0, 2.0]);
        assert_eq!(output_2, vec![output2_start, output2_start + 1.0]);
    }

    #[test]
    fn read_data_contigious_with_skip() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContiguousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        // Skip first 2 samples from each channel
        reader.read_from(read_plan, 2).unwrap();

        let output_2_start = length * 2.0;
        assert_eq!(output_1, vec![2.0, 3.0, 4.0]);
        assert_eq!(
            output_2,
            vec![
                output_2_start + 2.0,
                output_2_start + 3.0,
                output_2_start + 4.0
            ]
        );
    }

    #[test]
    fn read_data_contigious_with_skip_and_multiple_blocks() {
        let mut buffer = create_test_buffer();
        let mut meta = create_test_meta_data(2);

        // Set up for multiple sub-blocks
        for channel in meta.iter_mut() {
            channel.number_of_values = 3;
        }

        let mut reader = MultiChannelContiguousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (1usize, &mut output_2[..])];
        let read_plan = RecordPlan::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        // Skip first sample from each channel
        let values_read = reader.read_from(read_plan, 1).unwrap();

        // ch1, block 1: 0, 1, 2
        // ch2, block 1: 3, 4, 5
        // ch1, block 2: 6, 7, 8
        // ch2, block 2: 9, 10, 11
        assert_eq!(output_1, vec![1.0, 2.0, 6.0]);
        assert_eq!(output_2, vec![4.0, 5.0, 9.0]);
        assert_eq!(values_read, 3);
    }
}
