mod channel_reader;
mod error;
mod index;
mod io;
mod meta_data;
mod paths;
mod properties;
mod raw_data;

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

use error::TdmsError;
use index::{DataFormat, Index};
use io::writer::{LittleEndianWriter, TdmsWriter};
use meta_data::{MetaData, ObjectMetaData, ToC};
use raw_data::{MultiChannelSlice, WriteBlock};

// Re-exports.
pub use io::data_types::TdmsStorageType;
pub use paths::ObjectPath;
pub use properties::PropertyValue;
pub use raw_data::DataLayout;

#[derive(Debug)]
pub struct TdmsFile<F: Write + Read + Seek + std::fmt::Debug> {
    index: index::Index,
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
            let segment = meta_data::Segment::read(&mut file).unwrap();
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
        object_path: &ObjectPath,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        self.index.get_object_property(object_path, property)
    }

    /// Read all properties for the given object path.
    pub fn read_all_properties(
        &self,
        object_path: &ObjectPath,
    ) -> Option<Vec<(&String, &PropertyValue)>> {
        self.index.get_object_properties(object_path)
    }

    pub fn writer(&mut self) -> Result<TdmsFileWriter<F, LittleEndianWriter<&mut F>>, TdmsError> {
        //make sure we are at the end.
        self.file.seek(SeekFrom::End(0))?;
        Ok(TdmsFileWriter {
            index: &mut self.index,
            writer: LittleEndianWriter::from_writer(&mut self.file),
            _file: std::marker::PhantomData,
        })
    }
}

pub struct TdmsFileWriter<'a, F: Write + 'a, W: TdmsWriter<&'a mut F>> {
    index: &'a mut Index,
    writer: W,
    _file: std::marker::PhantomData<F>,
}

impl<'a, F: Write, W: TdmsWriter<&'a mut F>> TdmsFileWriter<'a, F, W> {
    pub fn write_channels<D: TdmsStorageType>(
        &mut self,
        object_paths: &[impl AsRef<ObjectPath<'a>>],
        values: &[D],
        layout: DataLayout,
    ) -> Result<(), TdmsError> {
        let raw_data = MultiChannelSlice::from_slice(values, object_paths.len())?;
        let data_structures = raw_data
            .data_structure()
            .into_iter()
            .map(DataFormat::RawData);

        let channels = object_paths
            .iter()
            .map(|path| path.as_ref().path()) //surely a way to avoid this.
            .zip(data_structures)
            .collect();

        let (matches_live, channels) = self.index.check_write_values(channels);

        let meta = if matches_live {
            None
        } else {
            let objects: Vec<ObjectMetaData> = channels
                .into_iter()
                .map(|(path, raw_index)| ObjectMetaData {
                    path: path.to_string(),
                    properties: vec![],
                    raw_data_index: raw_index,
                })
                .collect();

            Some(MetaData { objects })
        };

        let toc = ToC {
            contains_new_object_list: !matches_live,
            data_is_interleaved: layout == DataLayout::Interleaved,
            ..Default::default()
        };
        let segment = self.writer.write_segment(toc, meta, Some(raw_data))?;
        self.index.add_segment(segment);
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), TdmsError> {
        self.writer.sync()
    }
}
#[cfg(test)]
mod tests {}
