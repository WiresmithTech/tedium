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

/// The multichannel interleaved reader will read from an interleaved block.
///
/// We will assume a single datatype as it is unclear if multiple types exist in the wild.
pub struct MultiChannelInterleavedReader<R: Read + Seek, T: TdmsReader<R>> {
    reader: T,
    _marker: PhantomData<R>,
    block_size: u64,
    block_start: u64,
}

impl<R: Read + Seek, T: TdmsReader<R>> MultiChannelInterleavedReader<R, T> {
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
    /// todo: what if the lengths of entries are different? Not really possible
    /// to create but is possible in spec.
    pub fn read(&mut self, mut channels: RecordStructure<f64>) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;
        let row_count = self.block_size as usize / channels.row_size();

        for _ in 0..row_count {
            for read_instruction in channels.read_instructions().iter_mut() {
                match &mut read_instruction.plan {
                    RecordEntryPlan::Read(output) => {
                        let read_value = self.reader.read_value()?;
                        if let Some(value) = output.next() {
                            *value = read_value;
                        }
                    }
                    RecordEntryPlan::Skip(bytes) => {
                        self.reader.move_position(*bytes)?;
                    }
                    RecordEntryPlan::SkipVariable => {
                        todo!("Variable length records not yet supported")
                    }
                };
            }
        }

        Ok(row_count)
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
            800,
        );
        let mut output: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
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
            800,
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
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
            800,
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 2];
        let mut channels = vec![(0usize, &mut output_1[..]), (2usize, &mut output_2[..])];
        let read_plan =
            RecordStructure::<f64>::build_record_plan(&meta, &mut channels[..]).unwrap();
        reader.read(read_plan).unwrap();
        assert_eq!(output_1, vec![0.0, 4.0, 8.0]);
        assert_eq!(output_2, vec![2.0, 6.0]);
    }
}
