use thiserror::Error;

#[derive(Error, Debug)]
pub enum TdmsError {
    #[error("Matching datatype not found for code {0:X}")]
    UnknownDataType(u32),
}
