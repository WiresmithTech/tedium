//! The file module provides the public API for a TDMS file.

mod channel_reader;
mod file_writer;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use crate::meta_data::Segment;
use crate::{error::TdmsError, PropertyPath, PropertyValue};
use crate::{index::Index, ChannelPath};
use crate::{
    io::writer::{LittleEndianWriter, TdmsWriter},
    paths::path_group_name,
};
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
pub struct TdmsFile<F: Read + Seek> {
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
            .truncate(true)
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
                let next_segment = index.add_segment(segment)?;
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

impl<F: Read + Seek> TdmsFile<F> {
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

    /// Read all groups in the file.
    ///
    /// Returns an iterator to the paths for each group.
    pub fn list_groups<'a>(&'a self) -> impl Iterator<Item = &'a str> + 'a {
        // We cannot guarantee a seperate path for the group has been written
        // as they are implicitly included in the channel path as well.
        // Therefore extract all possible group names from all paths and deduplicate.
        // Use a btreeset to deduplicate the paths.
        let mut groups = std::collections::BTreeSet::new();

        let paths = self.index.all_paths();
        for path in paths {
            let group_name = path_group_name(path);
            if let Some(group_name) = group_name {
                groups.insert(group_name);
            }
        }

        groups.into_iter()
    }

    /// Read all the channels in a group.
    ///
    /// Returns an iterator to the paths for each channel.
    pub fn list_channels_in_group<'a: 'c, 'b: 'c, 'c>(
        &'a self,
        group: &'b PropertyPath,
    ) -> impl Iterator<Item = ChannelPath> + 'c {
        let paths = self.index.paths_starting_with(group.path());
        paths.filter_map(|path| ChannelPath::try_from(path).ok())
    }
}

impl<F: Write + Read + Seek> TdmsFile<F> {
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
    pub fn writer(
        &mut self,
    ) -> Result<TdmsFileWriter<'_, F, LittleEndianWriter<&mut F>>, TdmsError> {
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

    use crate::DataLayout;

    use super::*;

    fn new_empty_file() -> TdmsFile<Cursor<Vec<u8>>> {
        let buffer = Vec::new();
        let cursor = Cursor::new(buffer);
        TdmsFile::new(cursor).unwrap()
    }

    #[test]
    fn test_can_load_empty_buffer() {
        let buffer = Vec::new();
        let mut cursor = Cursor::new(buffer);
        let result = build_index(&mut cursor);
        assert!(result.is_ok());
    }

    #[test]
    fn test_list_groups_with_properties_single() {
        let mut file = new_empty_file();

        let mut writer = file.writer().unwrap();
        writer
            .write_properties(
                &PropertyPath::group("group"),
                &[("name", PropertyValue::String("my_channel".to_string()))],
            )
            .unwrap();

        drop(writer);
        let groups: Vec<_> = file.list_groups().collect();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], "group");
    }

    #[test]
    fn test_list_groups_with_properties_multiple() {
        let mut file = new_empty_file();

        let mut writer = file.writer().unwrap();
        writer
            .write_properties(
                &PropertyPath::group("group"),
                &[("name", PropertyValue::String("my_channel".to_string()))],
            )
            .unwrap();
        writer
            .write_properties(
                &PropertyPath::group("group2"),
                &[("name", PropertyValue::String("my_channel".to_string()))],
            )
            .unwrap();

        drop(writer);
        let groups: Vec<_> = file.list_groups().collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0], "group");
        assert_eq!(groups[1], "group2");
    }

    #[test]
    fn test_list_implicit_groups_from_channels() {
        let mut file = new_empty_file();

        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[ChannelPath::new("group", "channel")],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                DataLayout::Interleaved,
            )
            .unwrap();

        drop(writer);
        let groups: Vec<_> = file.list_groups().collect();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], "group");
    }

    #[test]
    fn test_list_channels_in_group_single() {
        let mut file = new_empty_file();

        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[ChannelPath::new("group", "channel")],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                DataLayout::Interleaved,
            )
            .unwrap();

        drop(writer);
        let channels: Vec<_> = file
            .list_channels_in_group(&PropertyPath::group("group"))
            .collect();
        assert_eq!(channels.len(), 1);
        assert_eq!(channels[0], ChannelPath::new("group", "channel"));
    }

    #[test]
    fn test_list_channels_in_group_multiple() {
        let mut file = new_empty_file();

        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[ChannelPath::new("group", "channel")],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                DataLayout::Interleaved,
            )
            .unwrap();
        writer
            .write_channels(
                &[ChannelPath::new("group", "channel2")],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                DataLayout::Interleaved,
            )
            .unwrap();

        drop(writer);
        let channels: Vec<_> = file
            .list_channels_in_group(&PropertyPath::group("group"))
            .collect();
        assert_eq!(channels.len(), 2);
        assert_eq!(channels[0], ChannelPath::new("group", "channel"));
        assert_eq!(channels[1], ChannelPath::new("group", "channel2"));
    }

    #[test]
    fn test_list_channels_in_group_none() {
        let mut file = new_empty_file();

        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[ChannelPath::new("group", "channel")],
                &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                DataLayout::Interleaved,
            )
            .unwrap();

        drop(writer);
        let channels: Vec<_> = file
            .list_channels_in_group(&PropertyPath::group("group2"))
            .collect();
        assert_eq!(channels.len(), 0);
    }
}
