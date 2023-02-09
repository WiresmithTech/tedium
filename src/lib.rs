mod data_types;
mod error;
mod index;
mod meta_data;
mod raw_data;
mod reader;
mod writer;

use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::Path,
};

use error::TdmsError;
use index::Index;
use meta_data::PropertyValue;

pub struct TdmsFile {
    index: index::Index,
    file: File,
}

impl TdmsFile {
    /// Load the file from the path. This step will load and index the metadata
    /// ready for access.
    pub fn load(path: &Path) -> Self {
        let mut file = File::open(path).unwrap();
        let file_size = file.metadata().unwrap().len();
        let mut index = Index::new();

        loop {
            let segment = meta_data::SegmentMetaData::read(&mut file).unwrap();
            let next_segment = index.add_segment(segment);
            if let Err(_) = file.seek(SeekFrom::Start(next_segment)) {
                break;
            }
            if file_size == file.stream_position().unwrap() {
                break;
            }
        }

        Self { index, file }
    }

    /// Read the property by name from the full object path.
    ///
    /// The object path is the internal representation. This function will be changed for ergonomics in the future.
    /// For now use the format `/'group'/'channel'` where you do need the single quotes.
    pub fn read_property(
        &self,
        object_path: &str,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        self.index.get_object_property(object_path, property)
    }

    /// Read all properties for the given object path.
    ///
    /// The object path is the internal representation. This function will be changed for ergonomics in the future.
    /// For now use the format `/'group'/'channel'` where you do need the single quotes.
    pub fn read_all_properties(&self, object_path: &str) -> Option<Vec<(&String, &PropertyValue)>> {
        self.index.get_object_properties(object_path)
    }

    pub fn read_channel(&mut self, object_path: &str, output: &mut [f64]) -> Result<(), TdmsError> {
        let data_positions = self.index.get_channel_data_positions(object_path).unwrap();

        let mut samples_read = 0;
        for location in data_positions {
            let block = self.index.get_data_block(location.data_block).unwrap();
            samples_read += block
                .read(
                    location.channel_index,
                    &mut self.file,
                    &mut output[samples_read..],
                )
                .unwrap();
            if samples_read >= output.len() {
                break;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {}
