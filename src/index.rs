//! The registry module creates the data structure which acts as
//! an in memory index of the file contents.
//!
//! This will store known objects and their properties and data locations.

use std::collections::HashMap;

use crate::file_types::{
    ObjectMetaData, PropertyValue, RawDataIndex, RawDataMeta, SegmentMetaData,
};
use crate::raw_data::DataBlock;

/// A store for a given channel point to the data block with its data and the index within that.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DataLocation {
    /// The index of the data block with the data in.
    data_block: usize,
    /// The channel index in that block.
    channel_index: usize,
}

#[derive(Clone, PartialEq, Debug)]
struct ObjectData {
    path: String,
    properties: Vec<(String, PropertyValue)>,
    data_locations: Vec<DataLocation>,
}

impl ObjectData {
    //todo: this can be more efficient
    fn from_metadata(meta: &ObjectMetaData) -> Self {
        Self {
            path: meta.path.clone(),
            properties: meta.properties.clone(),
            data_locations: vec![],
        }
    }
    fn update(&mut self, other: &ObjectMetaData) {
        // This is not good enough. we need to replace.
        //todo
        self.properties.extend_from_slice(&other.properties);
    }

    fn add_data_location(&mut self, location: DataLocation) {
        self.data_locations.push(location);
    }
}

#[derive(Debug, Clone)]
struct ActiveObject {
    path: String,
    raw_data_meta: RawDataMeta,
}

impl ActiveObject {
    fn update(&mut self, meta: &ObjectMetaData) {
        todo!()
    }

    fn get_object_data<'b, 'c>(&'b self, registry: &'c mut ObjectRegistry) -> &'c mut ObjectData {
        registry
            .get_mut(&self.path)
            .expect("Should always have a registered version of active object")
    }
}

type ObjectRegistry = HashMap<String, ObjectData>;

#[derive(Default, Debug, Clone)]
pub struct FileScanner {
    active_objects: Vec<ActiveObject>,
    object_registry: ObjectRegistry,
    data_blocks: Vec<DataBlock>,
    next_segment_start: u64,
}

impl FileScanner {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_segment_to_registry(&mut self, segment: SegmentMetaData) {
        //Basic procedure.
        //1. If new object list is set, clear active objects.
        //2. Update the active object list - adding new objects or updating properties and data locations for existing objects.

        if segment.toc.contains_new_object_list {
            self.deactivate_all_objects();
        }

        //todo: can we re-order this to avoid building object data when not needed??

        segment
            .objects
            .iter()
            .for_each(|obj| match obj.raw_data_index {
                RawDataIndex::None => self.update_meta_object(obj),
                _ => self.update_or_activate_data_object(obj),
            });

        let data_block = DataBlock::from_segment(
            &segment,
            self.next_segment_start,
            self.get_active_raw_data_meta(),
        );

        self.next_segment_start = data_block.end();

        self.insert_data_block(data_block);
    }

    fn get_active_raw_data_meta(&self) -> Vec<RawDataMeta> {
        self.active_objects
            .iter()
            .map(|ao| ao.raw_data_meta.clone())
            .collect()
    }

    fn insert_data_block(&mut self, block: DataBlock) {
        let data_index = self.data_blocks.len();
        self.data_blocks.push(block);

        for (channel_index, active_object) in self.active_objects.iter_mut().enumerate() {
            let location = DataLocation {
                data_block: data_index,
                channel_index,
            };
            active_object
                .get_object_data(&mut self.object_registry)
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
            Some(active_object) => active_object.update(object),
            None => {
                let object_data = ObjectData::from_metadata(object);
                let raw_meta = match &object.raw_data_index {
                    RawDataIndex::RawData(meta) => meta.clone(),
                    _ => panic!("Unexepected raw type"),
                };
                self.active_objects.push(ActiveObject {
                    path: object_data.path.clone(),
                    raw_data_meta: raw_meta,
                });

                self.object_registry
                    .insert(object.path.clone(), object_data);
            }
        }
    }

    /// Update Meta Only Object
    ///
    /// Update an object which contains no data.
    fn update_meta_object(&mut self, object: &ObjectMetaData) {
        match self.object_registry.get_mut(&object.path) {
            Some(found_object) => found_object.update(object),
            None => {
                let object_data = ObjectData::from_metadata(object);
                let old = self
                    .object_registry
                    .insert(object_data.path.clone(), object_data);
                assert!(
                    matches!(old, None),
                    "Should not be possible to be replacing an existing object."
                );
            }
        }
    }

    fn into_registry(mut self) -> Index {
        self.deactivate_all_objects();

        Index {
            objects: self.object_registry,
            data_blocks: self.data_blocks,
        }
    }
}

struct Index {
    objects: HashMap<String, ObjectData>,
    data_blocks: Vec<DataBlock>,
}

impl Index {
    fn get_object_properties(&self, path: &str) -> Option<&[(String, PropertyValue)]> {
        self.objects.get(path).map(|object| &object.properties[..])
    }

    fn get_channel_data_positions(&self, path: &str) -> Option<&[DataLocation]> {
        self.objects
            .get(path)
            .map(|object| &object.data_locations[..])
    }

    fn get_data_block(&self, index: usize) -> Option<&DataBlock> {
        self.data_blocks.get(index)
    }
}

#[cfg(test)]
mod tests {
    use crate::file_types::DataTypeRaw;
    use crate::file_types::ObjectMetaData;
    use crate::file_types::PropertyValue;
    use crate::file_types::RawDataIndex;
    use crate::file_types::RawDataMeta;
    use crate::file_types::ToC;
    use crate::raw_data::{DataLayout, Endianess};

    use super::*;

    #[test]
    fn test_single_segment() {
        let segment = SegmentMetaData {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            objects: vec![
                ObjectMetaData {
                    path: "group".to_string(),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                },
                ObjectMetaData {
                    path: "group/ch1".to_string(),
                    properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataTypeRaw::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
                ObjectMetaData {
                    path: "group/ch2".to_string(),
                    properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataTypeRaw::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
            ],
        };

        let mut scanner = FileScanner::new();
        scanner.add_segment_to_registry(segment);

        let registry = scanner.into_registry();

        let group_properties = registry.get_object_properties("group").unwrap();
        assert_eq!(
            group_properties,
            &[("Prop".to_string(), PropertyValue::I32(-51))]
        );
        let ch1_properties = registry.get_object_properties("group/ch1").unwrap();
        assert_eq!(
            ch1_properties,
            &[("Prop1".to_string(), PropertyValue::I32(-1))]
        );
        let ch2_properties = registry.get_object_properties("group/ch2").unwrap();
        assert_eq!(
            ch2_properties,
            &[("Prop2".to_string(), PropertyValue::I32(-2))]
        );

        let ch1_data = registry.get_channel_data_positions("group/ch1").unwrap();
        assert_eq!(
            ch1_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 0
            }]
        );
        let ch2_data = registry.get_channel_data_positions("group/ch2").unwrap();
        assert_eq!(
            ch2_data,
            &[DataLocation {
                data_block: 0,
                channel_index: 1
            }]
        );
    }

    #[test]
    fn correctly_generates_the_data_block() {
        let segment = SegmentMetaData {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 500,
            raw_data_offset: 20,
            objects: vec![
                ObjectMetaData {
                    path: "group".to_string(),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                },
                ObjectMetaData {
                    path: "group/ch1".to_string(),
                    properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataTypeRaw::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
                ObjectMetaData {
                    path: "group/ch2".to_string(),
                    properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                    raw_data_index: RawDataIndex::RawData(RawDataMeta {
                        data_type: DataTypeRaw::DoubleFloat,
                        number_of_values: 1000,
                        total_size_bytes: None,
                    }),
                },
            ],
        };

        let mut scanner = FileScanner::new();
        scanner.add_segment_to_registry(segment);

        let registry = scanner.into_registry();

        let expected_data_block = DataBlock {
            start: 48,
            length: 480,
            layout: DataLayout::Contigious,
            channels: vec![
                RawDataMeta {
                    data_type: DataTypeRaw::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
                RawDataMeta {
                    data_type: DataTypeRaw::DoubleFloat,
                    number_of_values: 1000,
                    total_size_bytes: None,
                },
            ],
            byte_order: Endianess::Little,
        };

        let block = registry.get_data_block(0).unwrap();
        assert_eq!(block, &expected_data_block);
    }
}
