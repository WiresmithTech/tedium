mod channel_reader;
mod data_types;
mod error;
mod index;
mod io;
mod meta_data;
mod raw_data;

use std::{
    fs::File,
    io::{Seek, SeekFrom},
    path::Path,
};

use error::TdmsError;
use index::{DataFormat, Index};
use io::writer::{LittleEndianWriter, TdmsWriter};
use meta_data::{MetaData, ObjectMetaData, PropertyValue, ToC};
use raw_data::{MultiChannelSlice, WriteBlock};

pub use raw_data::DataLayout;

pub struct TdmsFile {
    index: index::Index,
    file: File,
}

impl TdmsFile {
    /// Load the file from the path. This step will load and index the metadata
    /// ready for access.
    pub fn load(path: &Path) -> Self {
        let mut file = File::open(path).unwrap();
        let file_size = file.metadata().unwrap().len();
        let mut index = Index::new();

        loop {
            let segment = meta_data::Segment::read(&mut file).unwrap();
            let next_segment = index.add_segment(segment);
            if let Err(_) = file.seek(SeekFrom::Start(next_segment)) {
                break;
            }
            if file_size == file.stream_position().unwrap() {
                break;
            }
        }

        Self { index, file }
    }

    pub fn create(path: &Path) -> Self {
        let file = File::create(path).unwrap();
        let index = Index::new();

        Self { index, file }
    }

    /// Read the property by name from the full object path.
    ///
    /// The object path is the internal representation. This function will be changed for ergonomics in the future.
    /// For now use the format `/'group'/'channel'` where you do need the single quotes.
    pub fn read_property(
        &self,
        object_path: &str,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        self.index.get_object_property(object_path, property)
    }

    /// Read all properties for the given object path.
    ///
    /// The object path is the internal representation. This function will be changed for ergonomics in the future.
    /// For now use the format `/'group'/'channel'` where you do need the single quotes.
    pub fn read_all_properties(&self, object_path: &str) -> Option<Vec<(&String, &PropertyValue)>> {
        self.index.get_object_properties(object_path)
    }

    pub fn writer<'a>(
        &'a mut self,
    ) -> Result<TdmsFileWriter<'a, LittleEndianWriter<&'a mut File>>, TdmsError> {
        //make sure we are at the end.
        self.file.seek(SeekFrom::End(0))?;
        Ok(TdmsFileWriter {
            index: &mut self.index,
            writer: LittleEndianWriter::from_writer(&mut self.file),
        })
    }
}

pub struct TdmsFileWriter<'a, W: TdmsWriter<&'a mut File>> {
    index: &'a mut Index,
    writer: W,
}

impl<'a, W: TdmsWriter<&'a mut File>> TdmsFileWriter<'a, W> {
    pub fn write_channels(
        &mut self,
        object_paths: &[&str],
        values: &[f64],
        layout: DataLayout,
    ) -> Result<(), TdmsError> {
        let raw_data = MultiChannelSlice::from_slice(values, object_paths.len())?;
        let data_structures = raw_data
            .data_structure()
            .into_iter()
            .map(|raw_meta| DataFormat::RawData(raw_meta));

        let channels = object_paths
            .into_iter()
            .map(|name| *name) //surely a way to avoid this.
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

        let mut toc = ToC::default();
        toc.contains_new_object_list = !matches_live;
        toc.data_is_interleaved = layout == DataLayout::Interleaved;
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
