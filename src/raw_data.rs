//! Holds the capabilites for accessing the raw data blocks.

use std::{
    io::{Read, Seek, Write},
    marker::PhantomData,
};

use crate::{
    data_types::TdmsStorageType,
    error::TdmsError,
    meta_data::{RawDataMeta, SegmentMetaData, LEAD_IN_BYTES},
    reader::{BigEndianReader, LittleEndianReader, TdmsReader},
    writer::TdmsWriter,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DataLayout {
    Interleaved,
    Contigious,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Endianess {
    Big,
    Little,
}

/// Represents a block of data inside the file for fast random access.
#[derive(Clone, PartialEq, Debug)]
pub struct DataBlock {
    pub start: u64,
    ///Length allows detection where an existing segment is just extended.
    pub length: u64,
    pub layout: DataLayout,
    pub channels: Vec<RawDataMeta>,
    pub byte_order: Endianess,
}

impl DataBlock {
    /// Build a data block from the segment.
    ///
    /// The full metadata is provided seperately as this may be calculated
    /// from previous segments.
    pub fn from_segment(
        segment: &SegmentMetaData,
        segment_start: u64,
        active_channels_meta: Vec<RawDataMeta>,
    ) -> Self {
        let byte_order = if segment.toc.big_endian {
            Endianess::Big
        } else {
            Endianess::Little
        };

        let layout = if segment.toc.data_is_interleaved {
            DataLayout::Interleaved
        } else {
            DataLayout::Contigious
        };

        DataBlock {
            start: segment.raw_data_offset + LEAD_IN_BYTES + segment_start,
            length: segment.next_segment_offset - segment.raw_data_offset,
            layout,
            channels: active_channels_meta,
            byte_order,
        }
    }

    pub fn read(
        &self,
        channel_index: usize,
        reader: &mut (impl Read + Seek),
        output: &mut [f64],
    ) -> Result<usize, TdmsError> {
        //first is element size, second is total size.
        let channel_sizes: Vec<(u64, u64)> = self
            .channels
            .iter()
            .map(|channel_layout| {
                let element_size = 8; //only support doubles for testing.
                let total_size = channel_layout.number_of_values * element_size;
                (element_size, total_size)
            })
            .collect();

        let (start_offset, step) = match self.layout {
            DataLayout::Interleaved => {
                let start_offset: u64 = channel_sizes.iter().take(channel_index).map(|e| e.0).sum();
                let step =
                    channel_sizes.iter().map(|e| e.0).sum::<u64>() - channel_sizes[channel_index].0;
                (start_offset, step)
            }
            DataLayout::Contigious => {
                let start_offset = channel_sizes.iter().take(channel_index).map(|e| e.1).sum();
                (start_offset, 0)
            }
        };

        match self.byte_order {
            Endianess::Big => BlockReader::<_, _>::new(
                self.start + start_offset,
                step,
                self.channels[channel_index].number_of_values,
                BigEndianReader::from_reader(reader),
            )?
            .read(output),
            Endianess::Little => BlockReader::<_, _>::new(
                self.start + start_offset,
                step,
                self.channels[channel_index].number_of_values,
                LittleEndianReader::from_reader(reader),
            )?
            .read(output),
        }
    }
}

struct BlockReader<R: Read + Seek, T: TdmsReader<R>> {
    step_bytes: i64,
    samples: u64,
    reader: T,
    _marker: PhantomData<R>,
}

impl<R: Read + Seek, T: TdmsReader<R>> BlockReader<R, T> {
    fn new(
        start_bytes: u64,
        step_bytes: u64,
        samples: u64,
        mut reader: T,
    ) -> Result<Self, TdmsError> {
        reader.to_file_position(start_bytes)?;
        Ok(Self {
            step_bytes: step_bytes as i64,
            samples,
            reader,
            _marker: PhantomData,
        })
    }

    fn read(mut self, output: &mut [f64]) -> Result<usize, TdmsError> {
        let mut last_index = 0;
        for (index, sample) in output.iter_mut().take(self.samples as usize).enumerate() {
            if index != 0 {
                self.reader.move_position(self.step_bytes)?;
            }
            *sample = self.reader.read_value()?;
            last_index = index;
        }
        Ok(last_index + 1)
    }

    //used for testing right now.
    #[allow(dead_code)]
    fn read_vec(self) -> Result<Vec<f64>, TdmsError> {
        let mut values = vec![0.0; self.samples as usize];
        self.read(&mut values[..])?;
        Ok(values)
    }
}

/// Indicates a set of data that can be written as a binary block to a TDMS file.
trait WriteBlock {
    fn data_structure(&self) -> Vec<RawDataMeta>;
    fn write<W: Write, T: TdmsWriter<W>>(&self, writer: &mut T) -> Result<(), TdmsError>;
}

/// Implementation for a data slice of [`TDMSStorageType`] assuming it is a single channel of data.
impl<'a, D: TdmsStorageType> WriteBlock for [D] {
    fn data_structure(&self) -> Vec<RawDataMeta> {
        vec![RawDataMeta {
            data_type: D::NATURAL_TYPE,
            number_of_values: self.len() as u64,
            total_size_bytes: None,
        }]
    }

    fn write<W: Write, T: TdmsWriter<W>>(&self, writer: &mut T) -> Result<(), TdmsError> {
        for item in self {
            writer.write_value(item)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod read_tests {
    use std::io::{Cursor, Write};

    use crate::data_types::DataType;
    use crate::meta_data::{ObjectMetaData, PropertyValue, RawDataIndex, ToC};

    use super::*;

    fn dummy_segment() -> SegmentMetaData {
        SegmentMetaData {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            objects: vec![
                ObjectMetaData {
                    path: String::from("group"),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                },
                ObjectMetaData {
                    path: "group/ch1".to_string(),
                    properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataType::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
                ObjectMetaData {
                    path: "group/ch2".to_string(),
                    properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataType::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
            ],
        }
    }

    #[test]
    fn datablock_captures_sizing_from_segment() {
        let segment = dummy_segment();

        let raw_meta = segment
            .objects
            .iter()
            .filter_map(|object| {
                match &object.raw_data_index {
                    RawDataIndex::RawData(meta) => Some(meta.clone()),
                    _ => None, //not possible since we just set it above
                }
            })
            .collect::<Vec<_>>();

        let data_block = DataBlock::from_segment(&segment, 10, raw_meta);

        let expected_data_block = DataBlock {
            start: 58,
            length: 480,
            layout: DataLayout::Contigious,
            channels: vec![
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
            ],
            byte_order: Endianess::Little,
        };

        assert_eq!(data_block, expected_data_block);
    }

    #[test]
    fn data_block_gets_layout_from_segment() {
        let mut interleaved = dummy_segment();
        interleaved.toc.data_is_interleaved = true;

        let mut contiguous = dummy_segment();
        contiguous.toc.data_is_interleaved = false;

        let interleaved_block = DataBlock::from_segment(&interleaved, 0, vec![]);
        let contiguous_block = DataBlock::from_segment(&contiguous, 0, vec![]);

        assert_eq!(interleaved_block.layout, DataLayout::Interleaved);
        assert_eq!(contiguous_block.layout, DataLayout::Contigious);
    }

    #[test]
    fn data_block_gets_endianess_from_segment() {
        let mut big = dummy_segment();
        big.toc.big_endian = true;

        let mut little = dummy_segment();
        little.toc.big_endian = false;

        let big_block = DataBlock::from_segment(&big, 0, vec![]);
        let little_block = DataBlock::from_segment(&little, 0, vec![]);

        assert_eq!(big_block.byte_order, Endianess::Big);
        assert_eq!(little_block.byte_order, Endianess::Little);
    }

    fn create_test_buffer() -> Cursor<Vec<u8>> {
        let buffer = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(buffer);
        for index in 0..100 {
            let value = index as f64;
            cursor.write(&value.to_be_bytes()).unwrap();
        }
        cursor
    }

    #[test]
    fn read_data_contigous_no_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            BlockReader::<_, _>::new(0, 0, 3, BigEndianReader::from_reader(&mut buffer)).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn read_data_contigous_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            BlockReader::<_, _>::new(16, 0, 3, BigEndianReader::from_reader(&mut buffer)).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_no_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            BlockReader::<_, _>::new(0, 8, 3, BigEndianReader::from_reader(&mut buffer)).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![0.0, 2.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            BlockReader::<_, _>::new(16, 8, 3, BigEndianReader::from_reader(&mut buffer)).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![2.0, 4.0, 6.0]);
    }
}

#[cfg(test)]
mod write_tests {
    use crate::{data_types::DataType, writer::LittleEndianWriter};

    use super::*;

    #[test]
    fn single_channel_writer_generates_meta_data() {
        let data = vec![0u32; 20];
        let meta = data[..].data_structure();

        // Although total size isi calculable this is only used for strings.
        let expected_meta = RawDataMeta {
            data_type: DataType::U32,
            number_of_values: 20,
            total_size_bytes: None,
        };

        assert_eq!(meta, &[expected_meta]);
    }

    #[test]
    fn single_channel_writer_writes_with_endianess() {
        let data = vec![0u32, 1, 2, 3];

        let mut buf = vec![];
        {
            let mut writer = LittleEndianWriter::from_writer(&mut buf);
            data[..].write(&mut writer).unwrap();
        }

        assert_eq!(
            &buf[..],
            &[
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00,
                0x00, 0x00
            ]
        );
    }
}
