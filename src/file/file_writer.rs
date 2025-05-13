use std::io::Write;
use std::num::NonZeroUsize;
use crate::error::TdmsError;
use crate::index::{DataFormat, Index};
use crate::io::data_types::TdmsStorageType;
use crate::io::writer::TdmsWriter;
use crate::meta_data::{MetaData, ObjectMetaData, ToC};
use crate::paths::ChannelPath;
use crate::raw_data::{MultiChannelSlice, WriteBlock};
use crate::{DataLayout, PropertyPath, PropertyValue};

pub struct TdmsFileWriter<'a, F: Write + 'a, W: TdmsWriter<&'a mut F>> {
    index: &'a mut Index,
    writer: W,
    _file: std::marker::PhantomData<F>,
}

impl<'a, F: Write, W: TdmsWriter<&'a mut F>> TdmsFileWriter<'a, F, W> {
    /// Create a new TDMS file writer.
    ///
    /// Normally this is created by calling [`crate::TdmsFile::writer`]
    ///
    /// But you can create it directly if you want to use a custom writer.
    pub fn new(index: &'a mut Index, writer: W) -> Self {
        Self {
            index,
            writer,
            _file: std::marker::PhantomData,
        }
    }

    /// Write the data to the given channels.
    ///
    /// If you provide multiple channels then it is assumed tha the values is a 2d array layout.
    ///
    /// If layout is [`DataLayout::Interleaved`] then the data is assumed to be interleaved. i.e. ch1, ch2, ch1, ch2
    ///
    /// If layout is [`DataLayout::Contigious`] then the data is assumed to be contigious. i.e. ch1, ch1, ch1, ch2, ch2, ch2
    pub fn write_channels<D: TdmsStorageType, C: AsRef<ChannelPath>>(
        &mut self,
        channels: &[C],
        values: &[D],
        layout: DataLayout,
    ) -> Result<(), TdmsError> {
        let channel_count = NonZeroUsize::new(channels.len()).ok_or(TdmsError::NoChannels)?;
        let raw_data = MultiChannelSlice::from_slice(values, channel_count)?;
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
        self.index.add_segment(segment)?;
        Ok(())
    }

    /// Write the properties to the given path.
    /// This will overwrite any existing properties.
    ///
    /// # Example
    ///
    /// ```rust
    /// use tedium::{TdmsFile, PropertyPath, PropertyValue};
    ///
    /// let mut fake_file = std::io::Cursor::new(vec![]);
    /// let mut file = TdmsFile::new(fake_file).unwrap();
    /// let mut writer = file.writer().unwrap();
    ///
    /// writer.write_properties(
    ///    &PropertyPath::channel("group", "my_channel"),
    ///   &[("name", PropertyValue::String("my_channel".to_string()))],
    /// ).unwrap();
    /// ```
    ///
    pub fn write_properties(
        &mut self,
        path: &PropertyPath,
        properties: &[(&str, PropertyValue)],
    ) -> Result<(), TdmsError> {
        let path = path.path();
        let properties = properties
            .iter()
            .map(|(name, value)| (name.to_string(), (*value).clone()))
            .collect();

        let object = ObjectMetaData {
            path: path.to_string(),
            properties,
            raw_data_index: crate::meta_data::RawDataIndex::None,
        };

        let meta = MetaData {
            objects: vec![object],
        };

        let segment =
            self.writer
                .write_segment(ToC::default(), Some(meta), Option::<&[u8]>::None)?;
        self.index.add_segment(segment)?;
        Ok(())
    }

    /// Forces the file to sync to disk by calling the sync method on the writer.
    pub fn sync(&mut self) -> Result<(), TdmsError> {
        self.writer.sync()
    }
}
