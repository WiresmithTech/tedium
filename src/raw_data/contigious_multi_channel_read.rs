//! Reader of a multi-channel data block.
//!
//!

use crate::error::TdmsError;
use crate::io::reader::TdmsReader;
use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

use super::records::{RecordEntryPlan, RecordStructure};

/// The multichannel contigious reader will read from an contigous block.
///
/// We will assume a single datatype as it is unclear if multiple types exist in the wild.
pub struct MultiChannelContigousReader<R: Read + Seek, T: TdmsReader<R>> {
    reader: T,
    _marker: PhantomData<R>,
    block_size: u64,
    block_start: u64,
}

impl<R: Read + Seek, T: TdmsReader<R>> MultiChannelContigousReader<R, T> {
    pub fn new(reader: T, block_start: u64, block_size: u64) -> Self {
        Self {
            reader,
            _marker: PhantomData,
            block_size,
            block_start,
        }
    }

    /// Read the data from the block for the channels specified into the output slices.
    ///
    /// Returns the number of channels read in this block.
    ///
    pub fn read(&mut self, mut channels: RecordStructure<f64>) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;
        let block_end = self.block_start + self.block_size;

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
                RecordEntryPlan::SkipVariable => {
                    todo!("Variable length records not yet supported")
                }
            };
        }

        Ok(length)
    }
}

#[cfg(test)]
mod tests {
    use crate::{data_types::DataType, io::reader::BigEndianReader, meta_data::RawDataMeta};

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
    fn read_data_interleaved_single() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(2);

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800,
        );
        let mut output: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn read_data_interleaved_multi() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800,
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
    fn read_data_interleaved_multi_different_lengths() {
        let mut buffer = create_test_buffer();
        let meta = create_test_meta_data(4);
        let length = meta.first().unwrap().number_of_values as f64;

        let mut reader = MultiChannelContigousReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800,
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
