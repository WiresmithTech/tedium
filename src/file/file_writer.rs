use std::io::Write;

use crate::error::TdmsError;
use crate::index::{DataFormat, Index};
use crate::io::data_types::TdmsStorageType;
use crate::io::writer::TdmsWriter;
use crate::meta_data::{MetaData, ObjectMetaData, ToC};
use crate::paths::ChannelPath;
use crate::raw_data::{MultiChannelSlice, WriteBlock};
use crate::DataLayout;

pub struct TdmsFileWriter<'a, F: Write + 'a, W: TdmsWriter<&'a mut F>> {
    index: &'a mut Index,
    writer: W,
    _file: std::marker::PhantomData<F>,
}

impl<'a, F: Write, W: TdmsWriter<&'a mut F>> TdmsFileWriter<'a, F, W> {
    pub fn new(index: &'a mut Index, writer: W) -> Self {
        Self {
            index,
            writer,
            _file: std::marker::PhantomData,
        }
    }
    pub fn write_channels<D: TdmsStorageType>(
        &mut self,
        channels: &[impl AsRef<ChannelPath>],
        values: &[D],
        layout: DataLayout,
    ) -> Result<(), TdmsError> {
        let raw_data = MultiChannelSlice::from_slice(values, channels.len())?;
        let data_structures = raw_data
            .data_structure()
            .into_iter()
            .map(DataFormat::RawData);

        let channels = channels
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
