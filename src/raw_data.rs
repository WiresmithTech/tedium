//! Holds the capabilites for accessing the raw data blocks.

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

    pub fn end(&self) -> u64 {
        self.start + self.length
    }
}

#[cfg(test)]
mod test {
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
}
