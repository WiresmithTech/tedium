#![allow(dead_code)]

use std::{fs::File, io::Cursor, path::PathBuf};
use tdms_lib::TdmsFile;

/// Open the test file assuming this is called from the root of the project.
pub fn open_test_file() -> TdmsFile<File> {
    let path = PathBuf::from("tests/tdms-test-file.tdms");
    TdmsFile::load(&path).unwrap()
}

pub fn get_empty_file() -> TdmsFile<Cursor<Vec<u8>>> {
    // use a software buffer for speed.

    let buffer: Vec<u8> = Vec::with_capacity(1024);
    let file = Cursor::new(buffer);
    println!("cursor: {file:?}");
    TdmsFile::new(file)
}
