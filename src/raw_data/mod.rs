//! Holds the capabilites for accessing the raw data blocks.
mod interleaved_multi_channel_read;
mod records;
mod single_channel_read;
mod write;

use records::RecordStructure;
use single_channel_read::{BlockReader, SingleChannelReader};
pub use write::{MultiChannelSlice, WriteBlock};

use std::io::{Read, Seek};

use crate::{
    error::TdmsError,
    io::reader::{BigEndianReader, LittleEndianReader, TdmsReader},
    meta_data::{RawDataMeta, Segment, LEAD_IN_BYTES},
};

use self::interleaved_multi_channel_read::MultiChannelInterleavedReader;

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
        segment: &Segment,
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

    pub fn read<'a, 'b>(
        &self,
        reader: &'a mut (impl Read + Seek),
        channels_to_read: &'b mut [(usize, &'b mut [f64])],
    ) -> Result<usize, TdmsError> {
        match self.layout {
            // No multichannel implementation for contiguous data yet.
            DataLayout::Contigious => {
                let mut length = 0;
                for channel in channels_to_read {
                    length = self.read_single(channel.0, reader, channel.1)?;
                }
                Ok(length)
            }
            DataLayout::Interleaved => {
                let record_plan =
                    RecordStructure::build_record_plan(&self.channels, channels_to_read);
                match self.byte_order {
                    Endianess::Big => MultiChannelInterleavedReader::<_, _>::new(
                        BigEndianReader::from_reader(reader),
                        self.start,
                        self.length,
                    )
                    .read(record_plan),
                    Endianess::Little => MultiChannelInterleavedReader::<_, _>::new(
                        LittleEndianReader::from_reader(reader),
                        self.start,
                        self.length,
                    )
                    .read(record_plan),
                }
            }
        }
    }

    pub fn read_single(
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
            Endianess::Big => SingleChannelReader::<_, _>::new(
                self.start + start_offset,
                step,
                self.channels[channel_index].number_of_values,
                BigEndianReader::from_reader(reader),
            )?
            .read(output),
            Endianess::Little => SingleChannelReader::<_, _>::new(
                self.start + start_offset,
                step,
                self.channels[channel_index].number_of_values,
                LittleEndianReader::from_reader(reader),
            )?
            .read(output),
        }
    }
}

#[cfg(test)]
mod read_tests {

    use crate::data_types::DataType;
    use crate::meta_data::{MetaData, ObjectMetaData, PropertyValue, RawDataIndex, ToC};

    use super::*;

    fn dummy_segment() -> Segment {
        Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
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
            }),
        }
    }

    #[test]
    fn datablock_captures_sizing_from_segment() {
        let segment = dummy_segment();

        let raw_meta = segment
            .meta_data
            .as_ref()
            .unwrap()
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
}
