use thiserror::Error;

use crate::io::data_types::DataType;
use crate::paths::{ChannelPath, ObjectPathOwned};

#[derive(Error, Debug)]
pub enum TdmsError {
    #[error("Matching datatype not found for code {0:X}")]
    UnknownDataType(u32),
    #[error("Index reader error")]
    IndexReaderError(#[source] Box<dyn std::error::Error>),
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
    #[error("The start address for the next segment is invalid. The address overflowed. The file is likely corrupt.")]
    SegmentAddressOverflow,
    #[error("The segment ToC expects a data block but no data channels are present. The file is likely corrupt.")]
    SegmentTocDataBlockWithoutDataChannels,
    #[error("Attempted to parse an invalid object path. {0}")]
    InvalidObjectPath(String),
    #[error("Attempted to parse an valid but unsuitable path to a channel. {0}")]
    InvalidChannelPath(String),
}
