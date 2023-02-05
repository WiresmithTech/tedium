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

pub struct TdmsReader {}

impl TdmsReader {
    pub fn load(path: &Path) {
        let mut file = File::open(path).unwrap();

        loop {
            let segment = metadata_reader::read_segment(&mut file).unwrap();
            println!("{segment:?}");
            let raw_data_size = segment.next_segment_offset - segment.raw_data_offset;
            println!("raw data size: {raw_data_size}");
            file.seek(SeekFrom::Current(raw_data_size as i64)).unwrap();

            index::FileScanner::new();
        }
    }
}

#[cfg(test)]
mod tests {}
