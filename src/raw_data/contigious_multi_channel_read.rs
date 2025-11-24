//! Reader of a multi-channel data block.
//!
//!

use super::records::{RecordEntryPlan, RecordStructure};
use crate::io::reader::TdmsReader;
use crate::{error::TdmsError, io::data_types::TdmsStorageType};
use std::num::NonZeroU64;
use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

/// The multichannel contigious reader will read from an contigous block.
///
/// We will assume a single datatype as it is unclear if multiple types exist in the wild.
pub struct MultiChannelContigousReader<R: Read + Seek, T: TdmsReader<R>> {
    reader: T,
    _marker: PhantomData<R>,
    block_size: NonZeroU64,
    block_start: u64,
}

impl<R: Read + Seek, T: TdmsReader<R>> MultiChannelContigousReader<R, T> {
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
        mut channels: RecordStructure<D>,
    ) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;

        let total_sub_blocks = self.block_size.get() / channels.block_size() as u64;

        let mut length = 0;

        for _ in 0..total_sub_blocks {
            length += self.read_sub_block(&mut channels)?;
        }

        Ok(length)
    }

    fn read_sub_block<D: TdmsStorageType>(
        &mut self,
        channels: &mut RecordStructure<'_, D>,
    ) -> Result<usize, TdmsError> {
        let mut length = 0;
        for read_instruction in channels.read_instructions().iter_mut() {
            match &mut read_instruction.plan {
                RecordEntryPlan::Read(output) => {
                    for _ in 0..read_instruction.length {
                        let read_value = self.reader.read_value()?;
                        if let Some(value) = output.next() {
                            *value = read_value;
                        }
                    }
                    length = read_instruction.length;
                }
                RecordEntryPlan::Skip(bytes) => {
                    let skip_bytes = *bytes * read_instruction.length as i64;
                    self.reader.move_position(skip_bytes)?;
                }
            };
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

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn read_data_contigous_multi() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

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

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        reader.read(read_plan).unwrap();
        assert_eq!(output_1, vec![0.0, 1.0, 8.0]);
        assert_eq!(output_2, vec![4.0, 5.0, 12.0]);
    }

    #[test]
    fn read_data_contigious_multi_different_lengths() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800.try_into().unwrap(),
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 2];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();

        reader.read(read_plan).unwrap();

        let output2_start = length * 2.0;
        assert_eq!(output_1, vec![0.0, 1.0, 2.0]);
        assert_eq!(output_2, vec![output2_start, output2_start + 1.0]);
    }
}
