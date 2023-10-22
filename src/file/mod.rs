//! The file module provides the public API for a TDMS file.

mod channel_reader;
mod file_writer;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::index::Index;
use crate::io::writer::{LittleEndianWriter, TdmsWriter};
use crate::meta_data::Segment;
use crate::{error::TdmsError, PropertyPath, PropertyValue};
use file_writer::TdmsFileWriter;

#[derive(Debug)]
pub struct TdmsFile<F: Write + Read + Seek + std::fmt::Debug> {
    index: Index,
    file: F,
}

impl TdmsFile<File> {
    /// Load the file from the path. This step will load and index the metadata
    /// ready for access.
    pub fn load(path: &Path) -> Result<Self, TdmsError> {
        let mut file = File::options().read(true).write(true).open(path)?;
        let file_size = file.metadata().unwrap().len();
        let mut index = Index::new();

        loop {
            let segment = Segment::read(&mut file).unwrap();
            let next_segment = index.add_segment(segment);
            if file.seek(SeekFrom::Start(next_segment)).is_err() {
                break;
            }
            if file_size == file.stream_position().unwrap() {
                break;
            }
        }

        Ok(Self { index, file })
    }

    pub fn create(path: &Path) -> Result<Self, TdmsError> {
        let file = File::options()
            .write(true)
            .create(true)
            .read(true)
            .open(path)?;
        Ok(Self::new(file))
    }
}

impl<F: Write + Read + Seek + std::fmt::Debug> TdmsFile<F> {
    pub fn new(file: F) -> Self {
        let index = Index::new();
        Self { index, file }
    }

    /// Read the property by name from the full object path.
    pub fn read_property(
        &self,
        object_path: &PropertyPath,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        self.index.get_object_property(object_path, property)
    }

    /// Read all properties for the given object path.
    pub fn read_all_properties(
        &self,
        object_path: &PropertyPath,
    ) -> Option<Vec<(&String, &PropertyValue)>> {
        self.index.get_object_properties(object_path)
    }

    pub fn writer(&mut self) -> Result<TdmsFileWriter<F, LittleEndianWriter<&mut F>>, TdmsError> {
        //make sure we are at the end.
        self.file.seek(SeekFrom::End(0))?;
        Ok(TdmsFileWriter::new(
            &mut self.index,
            LittleEndianWriter::from_writer(&mut self.file),
        ))
    }
}
