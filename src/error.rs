use thiserror::Error;

use crate::data_types::DataType;

#[derive(Error, Debug)]
pub enum TdmsError {
    #[error("Matching datatype not found for code {0:X}")]
    UnknownDataType(u32),
    #[error("Index reader error")]
    IndexReaderError(#[source] Box<dyn std::error::Error>),
    #[error("Group or Channel not found in index. {0}")]
    MissingObject(String),
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
}
