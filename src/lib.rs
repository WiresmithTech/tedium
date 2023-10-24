mod error;
mod file;
mod index;
mod io;
mod meta_data;
mod paths;
mod properties;
mod raw_data;

// Re-exports.
pub use file::TdmsFile;
pub use file::TdmsFileWriter;
pub use io::data_types::TdmsStorageType;
pub use paths::{ChannelPath, PropertyPath};
pub use properties::PropertyValue;
pub use raw_data::DataLayout;

// Put the types in their own namespace.
pub mod types {
    pub use crate::io::data_types::*;
}

#[cfg(test)]
mod tests {}
