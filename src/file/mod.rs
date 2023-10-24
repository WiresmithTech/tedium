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
pub use file_writer::TdmsFileWriter;

/// A TDMS file.
///
/// This is the main entry point for reading and writing TDMS files.
///
/// To read a file use [`Self::load`]. This will load from the path and index the metadata ready for access.
///
/// To create a new file use [`Self::create`]. This will replace any existing file at the path.
///
/// To write to a file use [`Self::writer`]. This will return a writer that can be used to write data to the file.
#[derive(Debug)]
pub struct TdmsFile<F: Write + Read + Seek + std::fmt::Debug> {
    index: Index,
    file: F,
}

impl TdmsFile<File> {
    /// Load the file from the path. This step will load and index the metadata
    /// ready for access.
    pub fn load(path: &Path) -> Result<Self, TdmsError> {
        let file = File::options().read(true).write(true).open(path)?;
        Self::new(file)
    }

    /// Create a new file at the path. This will replace any existing file at the path.
    pub fn create(path: &Path) -> Result<Self, TdmsError> {
        let file = File::options()
            .write(true)
            .create(true)
            .read(true)
            .open(path)?;
        Self::new(file)
    }
}

fn build_index(file: &mut (impl Read + Seek)) -> Result<Index, TdmsError> {
    let mut index = Index::new();

    //Make sure we are at the beginning.
    file.seek(SeekFrom::Start(0))?;

    loop {
        match Segment::read(file) {
            Ok(segment) => {
                let next_segment = index.add_segment(segment);
                if file.seek(SeekFrom::Start(next_segment)).is_err() {
                    break;
                }
            }
            Err(TdmsError::EndOfFile) => break,
            Err(e) => return Err(e),
        }
    }
    Ok(index)
}

impl<F: Write + Read + Seek + std::fmt::Debug> TdmsFile<F> {
    /// Create a new file from the given stream.
    ///
    /// # Example
    /// ```rust
    /// use tedium::TdmsFile;
    /// let mut fake_file = std::io::Cursor::new(vec![]);
    /// let file = TdmsFile::new(fake_file);
    /// ```
    pub fn new(mut file: F) -> Result<Self, TdmsError> {
        let index = build_index(&mut file)?;
        Ok(Self { index, file })
    }

    /// Read the property by name from the full object path.
    /// This will return `None` if the property does not exist.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tedium::{TdmsFile, PropertyPath};
    ///
    /// let mut fake_file = std::io::Cursor::new(vec![]);
    /// let mut file = TdmsFile::new(fake_file).unwrap();
    ///
    /// let property = file.read_property(&PropertyPath::file(), "name");
    /// ```
    pub fn read_property(
        &self,
        object_path: &PropertyPath,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        self.index.get_object_property(object_path, property)
    }

    /// Read all properties for the given object path.
    ///
    /// This returns a vector of tuples of the property name and value.
    pub fn read_all_properties(
        &self,
        object_path: &PropertyPath,
    ) -> Option<Vec<(&String, &PropertyValue)>> {
        self.index.get_object_properties(object_path)
    }

    /// Get a writer for the TDMS data so that you can write data.
    ///
    /// While this is in use you will not be able to access the read API.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tedium::{TdmsFile, ChannelPath, DataLayout};
    ///
    /// let mut fake_file = std::io::Cursor::new(vec![]);
    /// let mut file = TdmsFile::new(fake_file).unwrap();
    /// let mut writer = file.writer().unwrap();
    ///
    /// writer.write_channels(
    ///    &[ChannelPath::new("group", "channel")],
    ///   &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    ///  DataLayout::Interleaved,
    /// ).unwrap();
    ///
    /// //drop the writer so we can read again.
    /// drop(writer);
    ///
    /// file.read_channel(&ChannelPath::new("group", "channel"), &mut [0.0f64; 3]).unwrap();
    pub fn writer(&mut self) -> Result<TdmsFileWriter<F, LittleEndianWriter<&mut F>>, TdmsError> {
        //make sure we are at the end.
        self.file.seek(SeekFrom::End(0))?;
        Ok(TdmsFileWriter::new(
            &mut self.index,
            LittleEndianWriter::from_writer(&mut self.file),
        ))
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_can_load_empty_buffer() {
        let buffer = Vec::new();
        let mut cursor = Cursor::new(buffer);
        let result = build_index(&mut cursor);
        assert!(result.is_ok());
    }
}