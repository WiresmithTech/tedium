use thiserror::Error;

#[derive(Error, Debug)]
pub enum TdmsError {
    #[error("Matching datatype not found for code {0:X}")]
    UnknownDataType(u32),
    #[error("Index reader error")]
    IndexReaderError(#[from] crate::metadata_reader::TdmsReaderError),
    #[error("Group or Channel not found in index. {0}")]
    MissingObject(String),
}
