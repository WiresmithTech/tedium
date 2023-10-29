//! Holds the capabilites for accessing the raw data blocks.
//!
//! Data blocks come in different formats so in here are the modules for
//! different formats as well as common elements like query planners.
mod contigious_multi_channel_read;
mod interleaved_multi_channel_read;
mod records;
mod write;

use records::RecordStructure;
pub use write::{MultiChannelSlice, WriteBlock};

use std::{
    io::{Read, Seek},
    ops::AddAssign,
};

use crate::{
    error::TdmsError,
    io::{
        data_types::TdmsStorageType,
        reader::{BigEndianReader, LittleEndianReader, TdmsReader},
    },
    meta_data::{RawDataMeta, Segment, LEAD_IN_BYTES},
};

use self::{
    contigious_multi_channel_read::MultiChannelContigousReader,
    interleaved_multi_channel_read::MultiChannelInterleavedReader,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum DataLayout {
    Interleaved,
    Contigious,
}

impl std::fmt::Display for DataLayout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataLayout::Interleaved => write!(f, "Interleaved"),
            DataLayout::Contigious => write!(f, "Contigious"),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Endianess {
    Big,
    Little,
}

/// Defines the size of the chunk and whether it is fixed or variable.
///
/// String data would make it variable for example.
#[derive(Clone, PartialEq, Debug)]
pub enum ChunkSize {
    Fixed(u64),
    Variable(u64),
}

/// Implement an addition for chunk size.
///
/// The sizes always add together, but a variable input always produces a variable output.
impl AddAssign for ChunkSize {
    fn add_assign(&mut self, rhs: Self) {
        match rhs {
            ChunkSize::Fixed(size) => match self {
                ChunkSize::Fixed(existing) => *existing += size,
                ChunkSize::Variable(existing) => *existing += size,
            },
            ChunkSize::Variable(size) => match self {
                ChunkSize::Fixed(existing) => *self = ChunkSize::Variable(*existing + size),
                ChunkSize::Variable(existing) => *existing += size,
            },
        }
    }
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

    /// Calculate the expected size of a single data chunk.
    ///
    /// A data chunk is the raw data written in a single write to the file and described in the header.
    pub fn chunk_size(&self) -> ChunkSize {
        let mut size = ChunkSize::Fixed(0);
        for channel in &self.channels {
            match channel.total_size_bytes {
                Some(total_size) => {
                    size += ChunkSize::Variable(total_size);
                }
                None => {
                    size +=
                        ChunkSize::Fixed(channel.number_of_values * channel.data_type.size() as u64)
                }
            }
        }
        size
    }

    ///Calculate the number of data chunks written to this data block.
    /// This is th number of repeated writes that have occured without new metadata.
    pub fn number_of_chunks(&self) -> usize {
        let size = self.chunk_size();

        match size {
            ChunkSize::Fixed(size) => (self.length / size) as usize,
            ChunkSize::Variable(_) => 1,
        }
    }

    /// Read the data from the block for the channels specified into the output slices.
    ///
    /// We assume all channels in the block have the same length and so return the maximum
    /// samples read in a given channel. The spec allows this assumption to be broken
    /// but no clients I have seen do.
    ///
    /// If an output slice for a channel has a length less than the number of samples it will stop
    /// reading once the end of the slice is reached.
    pub fn read<'b, D: TdmsStorageType>(
        &self,
        reader: &mut (impl Read + Seek),
        channels_to_read: &'b mut [(usize, &'b mut [D])],
    ) -> Result<usize, TdmsError> {
        let record_plan = RecordStructure::build_record_plan(&self.channels, channels_to_read)?;

        match (self.layout, self.byte_order) {
            // No multichannel implementation for contiguous data yet.
            (DataLayout::Contigious, Endianess::Big) => MultiChannelContigousReader::<_, _>::new(
                BigEndianReader::from_reader(reader),
                self.start,
                self.length,
            )
            .read(record_plan),
            (DataLayout::Contigious, Endianess::Little) => {
                MultiChannelContigousReader::<_, _>::new(
                    LittleEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read(record_plan)
            }
            (DataLayout::Interleaved, Endianess::Big) => {
                MultiChannelInterleavedReader::<_, _>::new(
                    BigEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read(record_plan)
            }
            (DataLayout::Interleaved, Endianess::Little) => {
                MultiChannelInterleavedReader::<_, _>::new(
                    LittleEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read(record_plan)
            }
        }
    }

    /// Read a single channel from the block.
    ///
    /// This is a simple wrapper around the `read` function for the common case of reading a single channel.
    pub fn read_single<D: TdmsStorageType>(
        &self,
        channel_index: usize,
        reader: &mut (impl Read + Seek),
        output: &mut [D],
    ) -> Result<usize, TdmsError> {
        //first is element size, second is total size.
        self.read(reader, &mut [(channel_index, output)])
    }
}

#[cfg(test)]
mod read_tests {

    use super::*;
    use crate::io::data_types::DataType;
    use crate::meta_data::{MetaData, ObjectMetaData, RawDataIndex, ToC};
    use crate::PropertyValue;

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

    fn raw_meta_from_segment(segment: &Segment) -> Vec<RawDataMeta> {
        segment
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
            .collect::<Vec<_>>()
    }

    #[test]
    fn datablock_captures_sizing_from_segment() {
        let segment = dummy_segment();

        let raw_meta = raw_meta_from_segment(&segment);

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

    #[test]
    fn data_block_get_chunk_size_single_type() {
        let segment = dummy_segment();
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        // 2 ch * 1000 samples * 8 bytes per sample
        assert_eq!(block.chunk_size(), ChunkSize::Fixed(16000));
    }

    #[test]
    fn data_block_get_chunk_size_multi_type() {
        let mut segment = dummy_segment();
        if let Some(metadata) = segment.meta_data.as_mut() {
            metadata.objects[1].raw_data_index = RawDataIndex::RawData(RawDataMeta {
                data_type: DataType::U32,
                number_of_values: 1000,
                total_size_bytes: None,
            });
        }
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        // (4 byte + 8 byte) * 1000 samples
        assert_eq!(block.chunk_size(), ChunkSize::Fixed(12000));
    }

    #[test]
    fn data_block_get_chunk_size_string() {
        let mut segment = dummy_segment();
        if let Some(metadata) = segment.meta_data.as_mut() {
            metadata.objects[1].raw_data_index = RawDataIndex::RawData(RawDataMeta {
                data_type: DataType::TdmsString,
                number_of_values: 1000,
                total_size_bytes: Some(12000),
            });
        }
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        // 8 byte * 1000 + the string 12000
        assert_eq!(block.chunk_size(), ChunkSize::Variable(20000));
    }

    #[test]
    fn data_block_chunk_count_single() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + 16000;
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        assert_eq!(block.number_of_chunks(), 1);
    }

    #[test]
    fn data_block_chunk_count_multi() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + (3 * 16000);
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        assert_eq!(block.number_of_chunks(), 3);
    }

    // This case should probably not occur, but lets do something sensible incase.
    #[test]
    fn data_block_chunk_count_handles_partial_with_round_down() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + (3 * 16000) + 300;
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        assert_eq!(block.number_of_chunks(), 3);
    }

    #[test]
    fn data_block_chunk_count_return_1_for_variable_type() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + 50000;
        if let Some(metadata) = segment.meta_data.as_mut() {
            metadata.objects[1].raw_data_index = RawDataIndex::RawData(RawDataMeta {
                data_type: DataType::TdmsString,
                number_of_values: 1000,
                total_size_bytes: Some(12000),
            });
        }
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels);
        assert_eq!(block.number_of_chunks(), 1);
    }
}
