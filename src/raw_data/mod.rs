//! Holds the capabilites for accessing the raw data blocks.
//!
//! Data blocks come in different formats so in here are the modules for
//! different formats as well as common elements like query planners.
mod contigious_multi_channel_read;
mod interleaved_multi_channel_read;
mod records;
mod write;

use records::RecordPlan;
pub use write::{MultiChannelSlice, WriteBlock};

use self::{
    contigious_multi_channel_read::MultiChannelContiguousReader,
    interleaved_multi_channel_read::MultiChannelInterleavedReader,
};
use crate::{
    error::TdmsError,
    io::{
        data_types::TdmsStorageType,
        reader::{BigEndianReader, LittleEndianReader, TdmsReader},
    },
    meta_data::{LEAD_IN_BYTES, RawDataMeta, Segment},
};
use std::io::{Read, Seek};
use std::num::NonZeroU64;

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
///
/// The result indicates an overflow condition.
impl ChunkSize {
    fn combine(&mut self, rhs: Self) -> Result<(), TdmsError> {
        match rhs {
            ChunkSize::Fixed(size) => match self {
                ChunkSize::Fixed(existing) => {
                    *existing = existing
                        .checked_add(size)
                        .ok_or(TdmsError::ChunkSizeOverflow)?
                }
                ChunkSize::Variable(existing) => {
                    *existing = existing
                        .checked_add(size)
                        .ok_or(TdmsError::ChunkSizeOverflow)?
                }
            },
            ChunkSize::Variable(size) => match self {
                ChunkSize::Fixed(existing) => {
                    *self = ChunkSize::Variable(
                        existing
                            .checked_add(size)
                            .ok_or(TdmsError::ChunkSizeOverflow)?,
                    )
                }
                ChunkSize::Variable(existing) => {
                    *existing = existing
                        .checked_add(size)
                        .ok_or(TdmsError::ChunkSizeOverflow)?
                }
            },
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct BlockReadChannelConfig<'a, T: TdmsStorageType> {
    pub channel_index: usize,
    pub samples_to_skip: u64,
    pub output: &'a mut [T],
}

/// Represents a block of data inside the file for fast random access.
#[derive(Clone, PartialEq, Debug)]
pub struct DataBlock {
    pub start: u64,
    ///Length allows detection where an existing segment is just extended.
    pub length: NonZeroU64,
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
    ) -> Result<Self, TdmsError> {
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
        if segment.raw_data_offset > segment.next_segment_offset {
            return Err(TdmsError::InvalidRawOffset);
        }
        let length = NonZeroU64::new(segment.next_segment_offset - segment.raw_data_offset)
            .ok_or(TdmsError::ZeroLengthDataBlock)?;
        if active_channels_meta.is_empty() {
            return Err(TdmsError::NoActiveChannelsInDataBlock);
        }

        Ok(DataBlock {
            start: segment.raw_data_offset + LEAD_IN_BYTES + segment_start,
            length,
            layout,
            channels: active_channels_meta,
            byte_order,
        })
    }

    /// Calculate the expected size of a single data chunk.
    ///
    /// A data chunk is the raw data written in a single write to the file and described in the header.
    pub fn chunk_size(&self) -> Result<ChunkSize, TdmsError> {
        let mut size = ChunkSize::Fixed(0);
        for channel in &self.channels {
            match channel.total_size_bytes {
                Some(total_size) => {
                    size.combine(ChunkSize::Variable(total_size))?;
                }
                None => {
                    let values = channel
                        .number_of_values
                        .checked_mul(channel.data_type.size() as u64)
                        .ok_or(TdmsError::ChunkSizeOverflow)?;
                    size.combine(ChunkSize::Fixed(values))?;
                }
            }
        }
        Ok(size)
    }

    ///Calculate the number of data chunks written to this data block.
    /// This is the number of repeated writes that have occurred without new metadata.
    pub fn number_of_chunks(&self) -> Result<usize, TdmsError> {
        let size = self.chunk_size()?;

        let chunk_count = match size {
            ChunkSize::Fixed(0) => 0,
            ChunkSize::Fixed(size) => (self.length.get() / size) as usize,
            ChunkSize::Variable(_) => 1,
        };
        Ok(chunk_count)
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
        let record_plan = RecordPlan::build_record_plan(&self.channels, channels_to_read)?;

        match (self.layout, self.byte_order) {
            // No multichannel implementation for contiguous data yet.
            (DataLayout::Contigious, Endianess::Big) => MultiChannelContiguousReader::<_, _>::new(
                BigEndianReader::from_reader(reader),
                self.start,
                self.length,
            )
            .read(record_plan),
            (DataLayout::Contigious, Endianess::Little) => {
                MultiChannelContiguousReader::<_, _>::new(
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

    /// Read a single channel from the block starting at a specific sample offset.
    ///
    /// This method allows skipping a specified number of samples before reading.
    /// The start_sample parameter indicates how many samples to skip in this block.
    ///
    /// Returns the number of samples actually read.
    pub fn read_single_from<D: TdmsStorageType>(
        &self,
        channel_index: usize,
        start_sample: u64,
        reader: &mut (impl Read + Seek),
        output: &mut [D],
    ) -> Result<usize, TdmsError> {
        self.read_from(reader, &mut [(channel_index, output)], start_sample)
    }

    /// Read multiple channels from the block starting at a specific sample offset.
    ///
    /// This is the core implementation that supports reading with an offset.
    /// The start_sample parameter indicates how many samples to skip in this block.
    pub fn read_from<'b, D: TdmsStorageType>(
        &self,
        reader: &mut (impl Read + Seek),
        channels_to_read: &'b mut [(usize, &'b mut [D])],
        start_sample: u64,
    ) -> Result<usize, TdmsError> {
        let record_plan = RecordPlan::build_record_plan(&self.channels, channels_to_read)?;

        match (self.layout, self.byte_order) {
            (DataLayout::Contigious, Endianess::Big) => MultiChannelContiguousReader::<_, _>::new(
                BigEndianReader::from_reader(reader),
                self.start,
                self.length,
            )
            .read_from(record_plan, start_sample),
            (DataLayout::Contigious, Endianess::Little) => {
                MultiChannelContiguousReader::<_, _>::new(
                    LittleEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read_from(record_plan, start_sample)
            }
            (DataLayout::Interleaved, Endianess::Big) => {
                MultiChannelInterleavedReader::<_, _>::new(
                    BigEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read_from(record_plan, start_sample)
            }
            (DataLayout::Interleaved, Endianess::Little) => {
                MultiChannelInterleavedReader::<_, _>::new(
                    LittleEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read_from(record_plan, start_sample)
            }
        }
    }

    /// Read multiple channels with per-channel skip amounts.
    ///
    /// Each element in channels_to_read is a tuple of (channel_index, output_buffer, skip_amount).
    /// The skip_amount specifies how many samples to skip for that channel in this block.
    ///
    /// This is used when channels have different amounts of data in a block or were
    /// written in separate blocks, requiring independent skip tracking per channel.
    pub fn read_with_per_channel_skip<'b, D: TdmsStorageType>(
        &self,
        reader: &mut (impl Read + Seek),
        channels_to_read: &'b mut [BlockReadChannelConfig<'b, D>],
    ) -> Result<usize, TdmsError> {
        // Extract skip amounts first (before mutable borrow)
        let skip_amounts: Vec<u64> = channels_to_read
            .iter()
            .map(
                |BlockReadChannelConfig {
                     samples_to_skip: skip,
                     ..
                 }| *skip,
            )
            .collect();

        // Extract the channel indices and buffers for the record plan
        let mut channel_refs: Vec<(usize, &mut [D])> = channels_to_read
            .iter_mut()
            .map(
                |BlockReadChannelConfig {
                     channel_index,
                     output,
                     ..
                 }| (*channel_index, &mut output[..]),
            )
            .collect();

        let mut record_plan = RecordPlan::build_record_plan(&self.channels, &mut channel_refs)?;

        match (self.layout, self.byte_order) {
            (DataLayout::Contigious, Endianess::Big) => MultiChannelContiguousReader::<_, _>::new(
                BigEndianReader::from_reader(reader),
                self.start,
                self.length,
            )
            .read_with_per_channel_skip(record_plan, &skip_amounts),
            (DataLayout::Contigious, Endianess::Little) => {
                MultiChannelContiguousReader::<_, _>::new(
                    LittleEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read_with_per_channel_skip(record_plan, &skip_amounts)
            }
            (DataLayout::Interleaved, Endianess::Big) => {
                for (plan_skip, skip_amount) in record_plan.block_skips_mut().zip(skip_amounts) {
                    *plan_skip = skip_amount;
                }
                MultiChannelInterleavedReader::<_, _>::new(
                    BigEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read(record_plan)
            }
            (DataLayout::Interleaved, Endianess::Little) => {
                for (plan_skip, skip_amount) in record_plan.block_skips_mut().zip(skip_amounts) {
                    *plan_skip = skip_amount;
                }
                MultiChannelInterleavedReader::<_, _>::new(
                    LittleEndianReader::from_reader(reader),
                    self.start,
                    self.length,
                )
                .read(record_plan)
            }
        }
    }
}

#[cfg(test)]
mod read_tests {

    use super::*;
    use crate::PropertyValue;
    use crate::io::data_types::DataType;
    use crate::meta_data::{MetaData, ObjectMetaData, RawDataIndex, ToC};

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

        let data_block = DataBlock::from_segment(&segment, 10, raw_meta).unwrap();

        let expected_data_block = DataBlock {
            start: 58,
            length: 480.try_into().unwrap(),
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
    fn data_block_errors_if_no_channels() {
        let segment = dummy_segment();
        let data_result = DataBlock::from_segment(&segment, 0, vec![]);
        assert!(data_result.is_err());
    }

    #[test]
    fn data_block_errors_if_length_is_zero() {
        let mut segment = dummy_segment();
        segment.raw_data_offset = segment.next_segment_offset;
        let data_result = DataBlock::from_segment(
            &segment,
            0,
            vec![RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            }],
        );
        assert!(data_result.is_err());
    }

    #[test]
    fn data_block_errors_if_raw_offset_is_greater_than_length() {
        let mut segment = dummy_segment();
        segment.raw_data_offset = segment.next_segment_offset + 1;
        let data_result = DataBlock::from_segment(
            &segment,
            0,
            vec![RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            }],
        );
        assert!(data_result.is_err());
    }

    #[test]
    fn data_block_gets_layout_from_segment() {
        let mut interleaved = dummy_segment();
        interleaved.toc.data_is_interleaved = true;

        let mut contiguous = dummy_segment();
        contiguous.toc.data_is_interleaved = false;

        let channels = vec![RawDataMeta {
            data_type: DataType::DoubleFloat,
            number_of_values: 1000,
            total_size_bytes: None,
        }];

        let interleaved_block = DataBlock::from_segment(&interleaved, 0, channels.clone()).unwrap();
        let contiguous_block = DataBlock::from_segment(&contiguous, 0, channels).unwrap();

        assert_eq!(interleaved_block.layout, DataLayout::Interleaved);
        assert_eq!(contiguous_block.layout, DataLayout::Contigious);
    }

    #[test]
    fn data_block_gets_endianess_from_segment() {
        let mut big = dummy_segment();
        big.toc.big_endian = true;

        let mut little = dummy_segment();
        little.toc.big_endian = false;

        let channels = vec![RawDataMeta {
            data_type: DataType::DoubleFloat,
            number_of_values: 1000,
            total_size_bytes: None,
        }];

        let big_block = DataBlock::from_segment(&big, 0, channels.clone()).unwrap();
        let little_block = DataBlock::from_segment(&little, 0, channels).unwrap();

        assert_eq!(big_block.byte_order, Endianess::Big);
        assert_eq!(little_block.byte_order, Endianess::Little);
    }

    #[test]
    fn data_block_get_chunk_size_single_type() {
        let segment = dummy_segment();
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        // 2 ch * 1000 samples * 8 bytes per sample
        assert_eq!(block.chunk_size().unwrap(), ChunkSize::Fixed(16000));
    }
    #[test]
    fn data_block_get_chunk_size_single_type_overflow() {
        let segment = dummy_segment();
        let mut channels = raw_meta_from_segment(&segment);
        channels[0].number_of_values = u64::MAX;
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        // 2 ch * 1000 samples * 8 bytes per sample
        assert!(block.chunk_size().is_err());
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
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        // (4 byte + 8 byte) * 1000 samples
        assert_eq!(block.chunk_size().unwrap(), ChunkSize::Fixed(12000));
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
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        // 8 byte * 1000 + the string 12000
        assert_eq!(block.chunk_size().unwrap(), ChunkSize::Variable(20000));
    }
    #[test]
    fn data_block_get_chunk_size_string_overflow() {
        let mut segment = dummy_segment();
        if let Some(metadata) = segment.meta_data.as_mut() {
            metadata.objects[1].raw_data_index = RawDataIndex::RawData(RawDataMeta {
                data_type: DataType::TdmsString,
                number_of_values: 1000,
                total_size_bytes: Some(u64::MAX),
            });
        }
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        // 8 byte * 1000 + the string 12000
        assert!(block.chunk_size().is_err());
    }

    #[test]
    fn data_block_chunk_count_single() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + 16000;
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        assert_eq!(block.number_of_chunks().unwrap(), 1);
    }

    #[test]
    fn data_block_chunk_count_empty_channels() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + 16000;
        let mut channels = raw_meta_from_segment(&segment);
        for channel in &mut channels {
            channel.number_of_values = 0;
        }
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        assert_eq!(block.number_of_chunks().unwrap(), 0);
    }

    #[test]
    fn data_block_chunk_count_multi() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + (3 * 16000);
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        assert_eq!(block.number_of_chunks().unwrap(), 3);
    }

    // This case should probably not occur, but lets do something sensible incase.
    #[test]
    fn data_block_chunk_count_handles_partial_with_round_down() {
        let mut segment = dummy_segment();
        segment.next_segment_offset = segment.raw_data_offset + (3 * 16000) + 300;
        let channels = raw_meta_from_segment(&segment);
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        assert_eq!(block.number_of_chunks().unwrap(), 3);
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
        let block = DataBlock::from_segment(&segment, 0, channels).unwrap();
        assert_eq!(block.number_of_chunks().unwrap(), 1);
    }
}
