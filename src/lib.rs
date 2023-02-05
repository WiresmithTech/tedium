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

use index::FileScanner;

pub struct TdmsReader {}

impl TdmsReader {
    pub fn load(path: &Path) {
        let mut file = File::open(path).unwrap();
        let mut scanner = FileScanner::new();

        loop {
            let segment = metadata_reader::read_segment(&mut file).unwrap();
            let raw_data_size = segment.next_segment_offset - segment.raw_data_offset;
            scanner.add_segment_to_index(segment);
            if let Err(_) = file.seek(SeekFrom::Current(raw_data_size as i64)) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {}
