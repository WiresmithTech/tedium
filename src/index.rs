//! The index module creates the data structure which acts as
//! an in memory index of the file contents.
//!
//! This will store known objects and their properties and data locations
//! and make them easy to access.
//!
use std::collections::HashMap;

use crate::error::TdmsError;
use crate::meta_data::{ObjectMetaData, RawDataIndex, RawDataMeta, Segment};
use crate::paths::{ChannelPath, ObjectPath, PropertyPath};
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
    properties: HashMap<String, PropertyValue>,
    data_locations: Vec<DataLocation>,
    latest_data_format: Option<DataFormat>,
}

impl ObjectData {
    /// Create the object data from the file metadata.
    fn from_metadata(meta: &ObjectMetaData) -> Self {
        let mut new = Self {
            path: meta.path.clone(),
            properties: HashMap::new(),
            data_locations: vec![],
            latest_data_format: None,
        };

        new.update(meta);

        new
    }

    /// Update the object data from a new metadata object.
    ///
    /// For example update new properties.
    fn update(&mut self, other: &ObjectMetaData) {
        for (name, value) in other.properties.iter() {
            self.properties.insert(name.clone(), value.clone());
        }

        // Update the format. We want to keep the latest format correct.
        // If we have a format we should save it.
        // If it matches previous, we just shouldn't update it.
        // If none, do nothing.
        if let Some(format) = DataFormat::from_index(&other.raw_data_index) {
            self.latest_data_format = Some(format)
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

/// Data cached for the current "active" objects which are the objects
/// that we are expecting data in the next data block.
#[derive(Debug, Clone)]
struct ActiveObject {
    path: String,
    number_of_samples: u64,
}

impl ActiveObject {
    fn new(path: &str, format: &DataFormat) -> Self {
        println!("Create active object {}", path);
        let path = path.to_string();
        let number_of_samples = match format {
            DataFormat::RawData(raw) => raw.number_of_values,
        };

        Self {
            path,
            number_of_samples,
        }
    }
    fn update(&mut self, meta: &ObjectMetaData) {
        println!("Update {} with {:?}", self.path, meta.raw_data_index);
        match meta.raw_data_index {
            RawDataIndex::RawData(ref raw) => {
                self.number_of_samples = raw.number_of_values;
                println!("Set {} to {} samples", self.path, self.number_of_samples);
            }
            _ => {}
        }
    }

    /// Fetch the corresponding [`ObjectData`] for the active object.
    fn get_object_data<'c>(&self, index: &'c Objectindex) -> &'c ObjectData {
        index
            .get(&self.path)
            .expect("Should always have a registered version of active object")
    }

    /// Fetch the corresponding [`ObjectData`] for the active object in a mutable form.
    fn get_object_data_mut<'c>(&self, index: &'c mut Objectindex) -> &'c mut ObjectData {
        index
            .get_mut(&self.path)
            .expect("Should always have a registered version of active object")
    }
}

/// The inner format for registering the objects.
type Objectindex = HashMap<String, ObjectData>;

#[derive(Default, Debug, Clone)]
pub struct Index {
    active_objects: Vec<ActiveObject>,
    objects: Objectindex,
    data_blocks: Vec<DataBlock>,
    next_segment_start: u64,
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add the data for the next segment read from the file.
    ///
    /// Returns the start position of the next segment.
    pub fn add_segment(&mut self, segment: Segment) -> u64 {
        //Basic procedure.
        //1. If new object list is set, clear active objects.
        //2. Update the active object list - adding new objects or updating properties and data locations for existing objects.

        if segment.toc.contains_new_object_list {
            self.deactivate_all_objects();
        }

        if let Some(meta_data) = &segment.meta_data {
            meta_data
                .objects
                .iter()
                .for_each(|obj| match obj.raw_data_index {
                    RawDataIndex::None => self.update_meta_object(obj),
                    _ => self.update_or_activate_data_object(obj),
                });
        }

        if segment.toc.contains_raw_data {
            let data_block = DataBlock::from_segment(
                &segment,
                self.next_segment_start,
                self.get_active_raw_data_meta(),
            );

            self.insert_data_block(data_block);
        }

        self.next_segment_start += segment.total_size_bytes();
        self.next_segment_start
    }

    /// Get all of the [`RawDataMeta`] for the active channels.
    fn get_active_raw_data_meta(&self) -> Vec<RawDataMeta> {
        self.active_objects
            .iter()
            .map(|ao| {
                ao.get_object_data(&self.objects)
                    .latest_data_format
                    .clone()
                    .expect("Getting data format from object that never had one")
            })
            .map(|format| match format {
                DataFormat::RawData(raw) => raw,
            })
            .collect()
    }

    fn insert_data_block(&mut self, block: DataBlock) {
        let data_index = self.data_blocks.len();
        println!("Inserting data block at {}", data_index);
        println!("Data block {:?}", block);

        // get counts from block.
        let chunks = block.number_of_chunks();
        println!("Chunks {}", chunks);

        self.data_blocks.push(block);

        for (channel_index, active_object) in self.active_objects.iter_mut().enumerate() {
            let number_of_samples = active_object.number_of_samples * chunks as u64;
            let location = DataLocation {
                data_block: data_index,
                channel_index,
                number_of_samples,
            };
            active_object
                .get_object_data_mut(&mut self.objects)
                .add_data_location(location);
        }
    }

    /// Consumes the object and makes it inactive.
    ///
    /// Panics if the object was already listed as inactive.
    fn deactivate_all_objects(&mut self) {
        self.active_objects.clear();
    }

    /// Activate Data Object
    ///
    /// Adds the object by path to the active objects. Creates it if it doesn't exist.
    fn update_or_activate_data_object(&mut self, object: &ObjectMetaData) {
        let matching_active = self
            .active_objects
            .iter_mut()
            .find(|active_object| active_object.path == object.path);

        match matching_active {
            Some(active_object) => {
                active_object.update(object);
                active_object
                    .get_object_data_mut(&mut self.objects)
                    .update(object);
            }
            None => {
                self.update_meta_object(object);
                // Must fetch the latest format in case this is same as previous.
                let format = self
                    .channel_format(&object.path)
                    .expect("Should not reach this if there is no data with the object.");

                self.active_objects
                    .push(ActiveObject::new(&object.path, format));
            }
        }
    }

    /// Update Meta Only Object
    ///
    /// Update an object which contains no data.
    fn update_meta_object(&mut self, object: &ObjectMetaData) {
        match self.objects.get_mut(&object.path) {
            Some(found_object) => found_object.update(object),
            None => {
                let object_data = ObjectData::from_metadata(object);
                let old = self.objects.insert(object_data.path.clone(), object_data);
                assert!(
                    old.is_none(),
                    "Should not be possible to be replacing an existing object."
                );
            }
        }
    }

    /// Get the current format for the channel.
    ///
    /// Returns none if we have no channel.
    fn channel_format(&self, path: ObjectPath) -> Option<&DataFormat> {
        self.objects
            .get(path)
            .and_then(|object| object.latest_data_format.as_ref())
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

    pub fn get_data_block(&self, index: usize) -> Option<&DataBlock> {
        self.data_blocks.get(index)
    }

    /// Validates the data formats for the objects to include
    /// in the next segment.
    pub fn check_write_values<'b>(
        &self,
        objects: Vec<(&'b str, DataFormat)>,
    ) -> (bool, Vec<(&'b str, RawDataIndex)>) {
        let live_matches = if !self.active_objects.is_empty() {
            self.active_objects
                .iter()
                .zip(objects.iter())
                .fold(true, |matches, (active, new)| {
                    matches && active.path == new.0
                })
        } else {
            //empty
            false
        };

        let raw_data_formats = objects
            .into_iter()
            .map(|(path, format)| {
                let found_format = self
                    .objects
                    .get(path)
                    .and_then(|object_data| object_data.latest_data_format.as_ref());
                match found_format {
                    Some(last_format) if last_format == &format => {
                        (path, RawDataIndex::MatchPrevious)
                    }
                    _ => (path, format.into()),
                }
            })
            .collect();
        (live_matches, raw_data_formats)
    }
}

#[cfg(test)]
mod tests {
    use crate::io::data_types::DataType;
    use crate::meta_data::MetaData;
    use crate::meta_data::ObjectMetaData;
    use crate::meta_data::RawDataIndex;
    use crate::meta_data::RawDataMeta;
    use crate::meta_data::ToC;
    use crate::raw_data::{DataLayout, Endianess};
    use crate::PropertyValue;

    use super::*;

    #[test]
    fn test_single_segment() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);

        let group_properties = index
            .get_object_properties(&PropertyPath::group("group"))
            .unwrap();
        assert_eq!(
            group_properties,
            &[(&"Prop".to_string(), &PropertyValue::I32(-51))]
        );
        let ch1_properties = index
            .get_object_properties(&ChannelPath::new("group", "ch1").as_ref())
            .unwrap();
        assert_eq!(
            ch1_properties,
            &[(&String::from("Prop1"), &PropertyValue::I32(-1))]
        );
        let ch2_properties = index
            .get_object_properties(&ChannelPath::new("group", "ch2").as_ref())
            .unwrap();
        assert_eq!(
            ch2_properties,
            &[(&"Prop2".to_string(), &PropertyValue::I32(-2))]
        );

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 0,
                number_of_samples: 1000
            }]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 1,
                number_of_samples: 1000
            }]
        );
    }

    #[test]
    fn test_builds_correct_length_with_multiple_write_blocks() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 33000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);

        let group_properties = index
            .get_object_properties(&PropertyPath::group("group"))
            .unwrap();
        assert_eq!(
            group_properties,
            &[(&"Prop".to_string(), &PropertyValue::I32(-51))]
        );
        let ch1_properties = index
            .get_object_properties(&ChannelPath::new("group", "ch1").as_ref())
            .unwrap();
        assert_eq!(
            ch1_properties,
            &[(&String::from("Prop1"), &PropertyValue::I32(-1))]
        );
        let ch2_properties = index
            .get_object_properties(&ChannelPath::new("group", "ch2").as_ref())
            .unwrap();
        assert_eq!(
            ch2_properties,
            &[(&"Prop2".to_string(), &PropertyValue::I32(-2))]
        );

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 0,
                number_of_samples: 2000
            }]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 1,
                number_of_samples: 2000
            }]
        );
    }

    #[test]
    fn correctly_generates_the_data_block() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);

        let expected_data_block = DataBlock {
            start: 48,
            length: 480,
            layout: DataLayout::Contigious,
            channels: vec![
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
            ],
            byte_order: Endianess::Little,
        };

        let block = index.get_data_block(0).unwrap();
        assert_eq!(block, &expected_data_block);
    }

    #[test]
    fn correctly_generates_the_data_block_same_as_previous() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let segment2 = Segment {
            toc: ToC::from_u32(0xA),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![],
                        raw_data_index: RawDataIndex::MatchPrevious,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![],
                        raw_data_index: RawDataIndex::MatchPrevious,
                    },
                ],
            }),
        };
        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let expected_data_block = DataBlock {
            start: 576,
            length: 480,
            layout: DataLayout::Contigious,
            channels: vec![
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
            ],
            byte_order: Endianess::Little,
        };

        let block = index.get_data_block(1).unwrap();
        assert_eq!(block, &expected_data_block);
    }

    #[test]
    fn correctly_generates_the_data_block_same_as_previous_new_list() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let segment2 = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![],
                        raw_data_index: RawDataIndex::MatchPrevious,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![],
                        raw_data_index: RawDataIndex::MatchPrevious,
                    },
                ],
            }),
        };
        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let expected_data_block = DataBlock {
            start: 576,
            length: 480,
            layout: DataLayout::Contigious,
            channels: vec![
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
                RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
            ],
            byte_order: Endianess::Little,
        };

        let block = index.get_data_block(1).unwrap();
        assert_eq!(block, &expected_data_block);
    }

    #[test]
    fn does_not_generate_block_for_meta_only() {
        let segment = Segment {
            toc: ToC::from_u32(0x2),
            next_segment_offset: 20,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "group".to_string(),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                }],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);

        let block = index.get_data_block(0);
        assert_eq!(block, None);
    }

    #[test]
    fn updates_existing_properties() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            // 2 is meta data only.
            toc: ToC::from_u32(0x2),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-52))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::None,
                    },
                ],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let group_properties = index
            .get_object_properties(&PropertyPath::group("group"))
            .unwrap();
        assert_eq!(
            group_properties,
            &[(&"Prop".to_string(), &PropertyValue::I32(-52))]
        );
        let ch1_properties = index
            .get_object_properties(ChannelPath::new("group", "ch1").as_ref())
            .unwrap();
        assert_eq!(
            ch1_properties,
            &[(&"Prop1".to_string(), &PropertyValue::I32(-2))]
        );
    }

    /// This tests the second optimisation on the NI article.
    #[test]
    fn can_update_properties_with_no_changes_to_data_layout() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            toc: ToC::from_u32(0xA),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'/'ch1'".to_string(),
                    properties: vec![("Prop1".to_string(), PropertyValue::I32(-2))],
                    raw_data_index: RawDataIndex::MatchPrevious,
                }],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let group_properties = index
            .get_object_properties(&PropertyPath::group("group"))
            .unwrap();
        assert_eq!(
            group_properties,
            &[(&"Prop".to_string(), &PropertyValue::I32(-51))]
        );
        let ch1_properties = index
            .get_object_properties(ChannelPath::new("group", "ch1").as_ref())
            .unwrap();
        assert_eq!(
            ch1_properties,
            &[(&String::from("Prop1"), &PropertyValue::I32(-2))]
        );
        let ch2_properties = index
            .get_object_properties(ChannelPath::new("group", "ch2").as_ref())
            .unwrap();
        assert_eq!(
            ch2_properties,
            &[(&"Prop2".to_string(), &PropertyValue::I32(-2))]
        );

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 0,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 0,
                    number_of_samples: 1000
                }
            ]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 1,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 1,
                    number_of_samples: 1000
                }
            ]
        );
    }

    /// This tests that the previous active list is maintained with no objects updated.
    #[test]
    fn can_keep_data_with_no_objects_listed() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            toc: ToC::from_u32(0xA),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData { objects: vec![] }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 0,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 0,
                    number_of_samples: 1000
                }
            ]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 1,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 1,
                    number_of_samples: 1000
                }
            ]
        );
    }

    /// This tests that the previous active list is maintained with no metadata updated.
    #[test]
    fn can_keep_data_with_no_metadata_in_toc() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            toc: ToC::from_u32(0x8),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData { objects: vec![] }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 0,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 0,
                    number_of_samples: 1000
                }
            ]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 1,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 1,
                    number_of_samples: 1000
                }
            ]
        );
    }

    #[test]
    fn can_add_channel_to_active_list() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            toc: ToC::from_u32(0xA),
            next_segment_offset: 25000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'/'ch3'".to_string(),
                    properties: vec![("Prop3".to_string(), PropertyValue::I32(-3))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataType::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                }],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let ch3_properties = index
            .get_object_properties(ChannelPath::new("group", "ch3").as_ref())
            .unwrap();
        assert_eq!(
            ch3_properties,
            &[(&"Prop3".to_string(), &PropertyValue::I32(-3))]
        );

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 0,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 0,
                    number_of_samples: 1000
                }
            ]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 1,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 1,
                    channel_index: 1,
                    number_of_samples: 1000
                }
            ]
        );
        let ch3_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch3"))
            .unwrap();
        assert_eq!(
            ch3_data,
            &[DataLocation {
                data_block: 1,
                channel_index: 2,
                number_of_samples: 1000
            }]
        );
    }

    #[test]
    fn can_replace_the_existing_list() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 9000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'/'ch3'".to_string(),
                    properties: vec![("Prop3".to_string(), PropertyValue::I32(-3))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataType::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                }],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);

        let ch3_properties = index
            .get_object_properties(ChannelPath::new("group", "ch3").as_ref())
            .unwrap();
        assert_eq!(
            ch3_properties,
            &[(&"Prop3".to_string(), &PropertyValue::I32(-3))]
        );

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 0,
                number_of_samples: 1000
            },]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 1,
                number_of_samples: 1000
            },]
        );
        let ch3_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch3"))
            .unwrap();
        assert_eq!(
            ch3_data,
            &[DataLocation {
                data_block: 1,
                channel_index: 0,
                number_of_samples: 1000
            }]
        );
    }

    #[test]
    fn can_re_add_channel_to_active_list() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 17000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };
        let segment2 = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 9000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'/'ch3'".to_string(),
                    properties: vec![("Prop3".to_string(), PropertyValue::I32(-3))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataType::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                }],
            }),
        };
        let segment3 = Segment {
            toc: ToC::from_u32(0xA),
            next_segment_offset: 25000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'/'ch1'".to_string(),
                    properties: vec![("Prop3".to_string(), PropertyValue::I32(-3))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataType::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                }],
            }),
        };

        let mut index = Index::new();
        index.add_segment(segment);
        index.add_segment(segment2);
        index.add_segment(segment3);

        let ch1_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch1"))
            .unwrap();
        assert_eq!(
            ch1_data,
            &[
                DataLocation {
                    data_block: 0,
                    channel_index: 0,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 2,
                    channel_index: 1,
                    number_of_samples: 1000
                }
            ]
        );
        let ch2_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch2"))
            .unwrap();
        assert_eq!(
            ch2_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 1,
                number_of_samples: 1000
            },]
        );
        let ch3_data = index
            .get_channel_data_positions(&ChannelPath::new("group", "ch3"))
            .unwrap();
        assert_eq!(
            ch3_data,
            &[
                DataLocation {
                    data_block: 1,
                    channel_index: 0,
                    number_of_samples: 1000
                },
                DataLocation {
                    data_block: 2,
                    channel_index: 0,
                    number_of_samples: 1000
                }
            ]
        );
    }

    #[test]
    fn matches_live_empty_index() {
        let index = Index::default();

        let channels = vec![
            (
                "/'group'/'ch1'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "/'group'/'ch2'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];
        let (matches, data_format) = index.check_write_values(channels);
        assert_eq!(matches, false);

        let expected_format = vec![
            (
                "/'group'/'ch1'",
                RawDataIndex::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "/'group'/'ch2'",
                RawDataIndex::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];

        assert_eq!(data_format, expected_format);
    }

    #[test]
    fn matches_live_does_match() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::default();
        index.add_segment(segment);

        let channels = vec![
            (
                "/'group'/'ch1'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "/'group'/'ch2'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];
        let (matches, data_format) = index.check_write_values(channels);
        assert_eq!(matches, true);

        let expected_format = vec![
            ("/'group'/'ch1'", RawDataIndex::MatchPrevious),
            ("/'group'/'ch2'", RawDataIndex::MatchPrevious),
        ];

        assert_eq!(data_format, expected_format);
    }

    #[test]
    fn matches_live_repeated_same_format() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let segment2 = Segment {
            toc: ToC::from_u32(0xA),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![],
                        raw_data_index: RawDataIndex::MatchPrevious,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![],
                        raw_data_index: RawDataIndex::MatchPrevious,
                    },
                ],
            }),
        };

        let mut index = Index::default();
        index.add_segment(segment);
        index.add_segment(segment2);

        let channels = vec![
            (
                "/'group'/'ch1'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "/'group'/'ch2'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];
        let (matches, data_format) = index.check_write_values(channels);
        assert_eq!(matches, true);

        let expected_format = vec![
            ("/'group'/'ch1'", RawDataIndex::MatchPrevious),
            ("/'group'/'ch2'", RawDataIndex::MatchPrevious),
        ];

        assert_eq!(data_format, expected_format);
    }

    #[test]
    fn matches_live_new_format() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::default();
        index.add_segment(segment);

        let channels = vec![
            (
                "/'group'/'ch1'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "/'group'/'ch2'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 2000,
                    total_size_bytes: None,
                }),
            ),
        ];
        let (matches, data_format) = index.check_write_values(channels);
        assert_eq!(matches, true);

        let expected_format = vec![
            ("/'group'/'ch1'", RawDataIndex::MatchPrevious),
            (
                "/'group'/'ch2'",
                RawDataIndex::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 2000,
                    total_size_bytes: None,
                }),
            ),
        ];

        assert_eq!(data_format, expected_format);
    }

    #[test]
    fn matches_live_no_match_different_channels() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::default();
        index.add_segment(segment);

        let channels = vec![
            (
                "group2/ch1",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "group2/ch2",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];
        let (matches, data_format) = index.check_write_values(channels);
        assert_eq!(matches, false);

        let expected_format = vec![
            (
                "group2/ch1",
                RawDataIndex::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "group2/ch2",
                RawDataIndex::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];

        assert_eq!(data_format, expected_format);
    }

    #[test]
    fn uses_previous_data_format_even_with_no_match() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let segment2 = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "group2".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "group2/ch1".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                    ObjectMetaData {
                        path: "group2/ch2".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::RawData(RawDataMeta {
                            data_type: DataType::DoubleFloat,
                            number_of_values: 1000,
                            total_size_bytes: None,
                        }),
                    },
                ],
            }),
        };

        let mut index = Index::default();
        index.add_segment(segment);
        index.add_segment(segment2);

        let channels = vec![
            (
                "/'group'/'ch1'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
            (
                "/'group'/'ch2'",
                DataFormat::RawData(RawDataMeta {
                    data_type: DataType::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                }),
            ),
        ];
        let (matches, data_format) = index.check_write_values(channels);
        assert_eq!(matches, false);

        let expected_format = vec![
            ("/'group'/'ch1'", RawDataIndex::MatchPrevious),
            ("/'group'/'ch2'", RawDataIndex::MatchPrevious),
        ];

        assert_eq!(data_format, expected_format);
    }
}
