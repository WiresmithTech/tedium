mod error;
mod file_types;
mod index;
mod metadata_reader;
mod raw_data;

use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::Path,
};

use error::TdmsError;
use file_types::PropertyValue;
use index::FileScanner;

pub struct TdmsReader {
    index: index::Index,
}

impl TdmsReader {
    pub fn load(path: &Path) -> Self {
        let mut file = File::open(path).unwrap();
        let file_size = file.metadata().unwrap().len();
        let mut scanner = FileScanner::new();

        loop {
            let segment = metadata_reader::read_segment(&mut file).unwrap();
            let raw_data_size = segment.next_segment_offset - segment.raw_data_offset;
            scanner.add_segment_to_index(segment);
            if let Err(_) = file.seek(SeekFrom::Current(raw_data_size as i64)) {
                break;
            }
            if file_size == file.stream_position().unwrap() {
                break;
            }
        }
        let index = scanner.into_index();

        Self { index }
    }

    pub fn read_property(
        &self,
        object_path: &str,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        self.index.get_object_property(object_path, property)
    }

    pub fn read_all_properties(&self, object_path: &str) -> Option<Vec<(&String, &PropertyValue)>> {
        self.index.get_object_properties(object_path)
    }
}

#[cfg(test)]
mod tests {}
