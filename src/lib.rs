mod data_reader;
mod error;
mod file_types;
mod index;
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
    file: File,
}

impl TdmsReader {
    pub fn load(path: &Path) -> Self {
        let mut file = File::open(path).unwrap();
        let file_size = file.metadata().unwrap().len();
        let mut scanner = FileScanner::new();

        loop {
            let segment = data_reader::read_segment(&mut file).unwrap();
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

        Self { index, file }
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
