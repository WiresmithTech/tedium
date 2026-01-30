use thiserror::Error;

use crate::io::data_types::DataType;
use crate::paths::{ChannelPath, ObjectPathOwned};

#[derive(Error, Debug)]
pub enum TdmsError {
    #[error("Matching datatype not found for code {0:X}")]
    UnknownDataType(u32),
    #[error("Index reader error")]
    IndexReaderError(#[source] Box<dyn std::error::Error + Send + Sync + 'static>),
    #[error("Group or Channel not found in index. {0}")]
    MissingObject(ObjectPathOwned),
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("String formatting error")]
    StringFormatError(#[from] std::string::FromUtf8Error),
    #[error("Unknown Property Type: {0:X}")]
    UnknownPropertyType(u32),
    #[error("Unsupported Property Type: {0:?}")]
    UnsupportedType(DataType),
    #[error("Attempted to read header where no header exists. Bytes: {0:X?}")]
    HeaderPatternNotMatched([u8; 4]),
    #[error("Tried to access a datablock that doesn't exist when reading channel: {0}")]
    DataBlockNotFound(ChannelPath, usize),
    #[error("The data block has length {0} which is not divisible by the number of channels: {1}")]
    BadDataBlockLength(usize, usize),
    #[error("Attempting to read a channel or property of type {0} as type {1}")]
    DataTypeMismatch(DataType, DataType),
    #[error("Attempted to read past the end of the file")]
    EndOfFile,
    #[error(
        "The start address for the next segment is invalid. The address overflowed. The file is likely corrupt."
    )]
    SegmentAddressOverflow,
    #[error(
        "The segment ToC expects a data block but no data channels are present. The file is likely corrupt."
    )]
    SegmentTocDataBlockWithoutDataChannels,
    #[error("Attempted to parse an invalid object path. {0}")]
    InvalidObjectPath(String),
    #[error("Attempted to parse an valid but unsuitable path to a channel. {0}")]
    InvalidChannelPath(String),
    #[error("Data blocks must have at least 1 channel")]
    NoChannels,
    #[error("Memory allocation error for vector. This is likely to be due to file corruption.")]
    VecAllocationFailed,
    #[error("Memory allocation error for string. This is likely to be due to file corruption.")]
    StringAllocationFailed,
    #[error(
        "Memory allocation error for property table. This is likely to be due to file corruption."
    )]
    PropertyTableAllocationFailed,
    #[error("A data block has a lenght of zero. The file is likely to be corrupted.")]
    ZeroLengthDataBlock,
    #[error("A data block has no active channels. The file is likely to be corrupted.")]
    NoActiveChannelsInDataBlock,
    #[error(
        "The data block wants to use the same data type as previous, but no previous type is available."
    )]
    NoPreviousType,
    #[error(
        "The raw data offset is greater than the next segment offset which is an invalid condition. The file is likely corrupt"
    )]
    InvalidRawOffset,
    #[error(
        "The calculated size for a data chunk is grater than 2^64 bytes. This isn't allowed and probably indicates a corrupt file."
    )]
    ChunkSizeOverflow,
    #[error("DAQmx Channels are not supported yet")]
    DaqmxChannelsNotSupported,
    #[cfg(feature = "chrono")]
    #[error("Failed to convert LVTime to chrono::DateTime")]
    ChronoDateTimeConversionFailed(#[source] labview_interop::types::timestamp::LVTimeError),
}
