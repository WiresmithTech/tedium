//! The index module creates the data structure which acts as
//! an in memory index of the file contents.
//!
//! This will store known objects and their properties and data locations
//! and make them easy to access.
//!
//!
mod building;
mod querying;
mod writing;

use std::collections::BTreeMap;

use crate::error::TdmsError;
use crate::meta_data::{ObjectMetaData, RawDataIndex, RawDataMeta};
use crate::paths::{ChannelPath, PropertyPath};
use crate::raw_data::DataBlock;
use crate::PropertyValue;

/// A store for a given channel point to the data block with its data and the index within that.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataLocation {
    /// The index of the data block with the data in.
    pub data_block: usize,
    /// The channel index in that block.
    pub channel_index: usize,
    /// The number of samples in this location
    pub number_of_samples: u64,
}

///Represents actual data formats that can store data.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum DataFormat {
    RawData(RawDataMeta),
}

impl DataFormat {
    ///Get the actual data format. Returns None for meta states e.g. None.
    fn from_index(index: &RawDataIndex) -> Option<Self> {
        match index {
            RawDataIndex::RawData(raw_meta) => Some(DataFormat::RawData(raw_meta.clone())),
            _ => None,
        }
    }
}

impl From<DataFormat> for RawDataIndex {
    fn from(value: DataFormat) -> Self {
        match value {
            DataFormat::RawData(raw_meta) => RawDataIndex::RawData(raw_meta),
        }
    }
}

/// Contains the data stored in the index for each object.
#[derive(Clone, PartialEq, Debug)]
struct ObjectData {
    path: String,
    properties: BTreeMap<String, PropertyValue>,
    data_locations: Vec<DataLocation>,
    latest_data_format: Option<DataFormat>,
}

impl ObjectData {
    /// Create the object data from the file metadata.
    fn from_metadata(meta: &ObjectMetaData) -> Result<Self, TdmsError> {
        let mut new = Self {
            path: meta.path.clone(),
            properties: BTreeMap::new(),
            data_locations: vec![],
            latest_data_format: None,
        };

        new.update(meta)?;

        Ok(new)
    }

    /// Update the object data from a new metadata object.
    ///
    /// For example update new properties.
    fn update(&mut self, other: &ObjectMetaData) -> Result<(), TdmsError> {
        for (name, value) in other.properties.iter() {
            self.properties.insert(name.clone(), value.clone());
        }

        // Update the format. We want to keep the latest format correct.
        // If we have a format we should save it.
        // If it matches previous, we just shouldn't update it.
        // If none, do nothing.
        match (&mut self.latest_data_format, &other.raw_data_index) {
            (None, RawDataIndex::MatchPrevious) => Err(TdmsError::NoPreviousType),
            (latest_format, raw_index) => {
                if let Some(format) = DataFormat::from_index(raw_index) {
                    *latest_format = Some(format);
                }
                Ok(())
            }
        }
    }

    /// Add a new data location.
    fn add_data_location(&mut self, location: DataLocation) {
        self.data_locations.push(location);
    }

    /// Fetch all the properties as an array.
    fn get_all_properties(&self) -> Vec<(&String, &PropertyValue)> {
        self.properties.iter().collect()
    }
}

/// The inner format for registering the objects.
type ObjectIndex = BTreeMap<String, ObjectData>;

#[derive(Default, Debug, Clone)]
pub struct Index {
    active_objects: Vec<building::ActiveObject>,
    objects: ObjectIndex,
    data_blocks: Vec<DataBlock>,
    next_segment_start: u64,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get all of the properties for the given object.
    ///
    /// Returns none if the object does not exist.
    pub fn get_object_properties(
        &self,
        path: &PropertyPath,
    ) -> Option<Vec<(&String, &PropertyValue)>> {
        self.objects
            .get(path.path())
            .map(|object| object.get_all_properties())
    }

    /// Get the property value for the given object.
    ///
    /// Errors if the object does not exist.
    /// Will contain a None if the property does not exist.
    pub fn get_object_property(
        &self,
        path: &PropertyPath,
        property: &str,
    ) -> Result<Option<&PropertyValue>, TdmsError> {
        let property = self
            .objects
            .get(path.path())
            .ok_or_else(|| TdmsError::MissingObject(path.path().to_owned()))?
            .properties
            .get(property);

        Ok(property)
    }

    pub fn get_channel_data_positions(&self, path: &ChannelPath) -> Option<&[DataLocation]> {
        self.objects
            .get(path.path())
            .map(|object| &object.data_locations[..])
    }

    /// Get the length of the channel.
    ///
    /// Returns None if the channel does not exist.
    pub fn channel_length(&self, path: &ChannelPath) -> Option<u64> {
        self.objects.get(path.path()).map(|object| {
            object
                .data_locations
                .iter()
                .map(|location| location.number_of_samples)
                .sum()
        })
    }

    pub fn get_data_block(&self, index: usize) -> Option<&DataBlock> {
        self.data_blocks.get(index)
    }
}

#[cfg(test)]
mod tests {}
