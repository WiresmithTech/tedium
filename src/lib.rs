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

pub struct TdmsReader {
    index: index::Index,
}

impl TdmsReader {
    pub fn load(path: &Path) {
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
    }
}

#[cfg(test)]
mod tests {}
