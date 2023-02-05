//! Holds the capabilites for accessing the raw data blocks.

use std::{
    io::{Seek, SeekFrom},
    marker::PhantomData,
};

use byteorder::{ByteOrder, ReadBytesExt};

use crate::file_types::{RawDataMeta, SegmentMetaData, LEAD_IN_BYTES};

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
}

struct BlockReader<'a, R: ReadBytesExt + Seek, O: ByteOrder> {
    step_bytes: i64,
    samples: u64,
    samples_read: u64,
    reader: &'a mut R,
    _order: PhantomData<O>,
}

impl<'a, R: ReadBytesExt + Seek, O: ByteOrder> BlockReader<'a, R, O> {
    fn new(
        start_bytes: u64,
        step_bytes: u64,
        samples: u64,
        reader: &'a mut R,
    ) -> Result<Self, std::io::Error> {
        reader.seek(SeekFrom::Start(start_bytes))?;
        Ok(Self {
            step_bytes: step_bytes as i64,
            samples,
            samples_read: 0,
            reader,
            _order: PhantomData,
        })
    }

    fn read_next(&mut self) -> Result<f64, std::io::Error> {
        if self.samples_read != 0 && self.step_bytes != 0 {
            self.reader.seek(SeekFrom::Current(self.step_bytes))?;
        }
        let sample = self.reader.read_f64::<O>()?;
        self.samples_read += 1;
        Ok(sample)
    }

    fn read(&mut self, output: &mut [f64]) -> Result<(), std::io::Error> {
        for sample in output.iter_mut() {
            *sample = self.read_next()?
        }
        Ok(())
    }

    fn read_vec(&mut self) -> Result<Vec<f64>, std::io::Error> {
        let mut values = vec![0.0; self.samples as usize];
        self.read(&mut values[..])?;
        Ok(values)
    }
}

/*
impl<'a, R: ReadBytesExt + Seek, O: ByteOrder> std::iter::Iterator for BlockReader<'a, R, O> {
    type Item = f64;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next()
    }
}
*/

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use byteorder::{BigEndian, WriteBytesExt};

    use crate::file_types::{DataTypeRaw, ObjectMetaData, PropertyValue, RawDataIndex, ToC};

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
                        data_type: DataTypeRaw::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
                ObjectMetaData {
                    path: "group/ch2".to_string(),
                    properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataTypeRaw::DoubleFloat,
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
                    data_type: DataTypeRaw::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
                RawDataMeta {
                    data_type: DataTypeRaw::DoubleFloat,
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
        let mut buffer = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(buffer);
        for index in 0..100 {
            let value = index as f64;
            cursor.write_f64::<BigEndian>(value).unwrap();
        }
        cursor
    }

    #[test]
    fn read_data_contigous_no_offset() {
        let mut buffer = create_test_buffer();

        let mut reader = BlockReader::<_, BigEndian>::new(0, 0, 3, &mut buffer).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn read_data_contigous_offset() {
        let mut buffer = create_test_buffer();

        let mut reader = BlockReader::<_, BigEndian>::new(16, 0, 3, &mut buffer).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_no_offset() {
        let mut buffer = create_test_buffer();

        let mut reader = BlockReader::<_, BigEndian>::new(0, 8, 3, &mut buffer).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![0.0, 2.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_offset() {
        let mut buffer = create_test_buffer();

        let mut reader = BlockReader::<_, BigEndian>::new(16, 8, 3, &mut buffer).unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![2.0, 4.0, 6.0]);
    }
}
