//! The registry module creates the data structure which acts as
//! an in memory index of the file contents.
//!
//! This will store known objects and their properties and data locations.

use std::collections::HashMap;

use crate::file_types::{ObjectMetaData, PropertyValue, RawDataMeta, SegmentMetaData};
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
    fn update(&mut self, other: Self) {}
}

#[derive(Default, Debug, Clone)]
struct FileScanner {
    active_objects: Vec<ObjectData>,
    inactive_objects: HashMap<String, ObjectData>,
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
            .map(ObjectData::from_metadata)
            .for_each(|obj| self.update_or_activate_object(obj));

        let data_block = DataBlock::from_segment(
            &segment,
            self.next_segment_start,
            self.get_active_raw_data_meta(),
        );

        self.next_segment_start = data_block.end();

        self.insert_data_block(data_block);
    }

    fn get_active_raw_data_meta(&self) -> Vec<RawDataMeta> {
        todo!()
    }

    fn insert_data_block(&mut self, block: DataBlock) {
        //put in datablock array and add location to all active channels.
    }

    /// Consumes the object and makes it inactive.
    ///
    /// Panics if the object was already listed as inactive.
    fn deactivate_all_objects(&mut self) {
        //drain into another vector to avoid mutability issues.
        let active_objects = self.active_objects.drain(..).collect::<Vec<ObjectData>>();
        for object in active_objects.into_iter() {
            let old_value = self.inactive_objects.insert(object.path.clone(), object);
            assert!(matches!(old_value, None));
        }
    }

    /// Activate Object
    ///
    /// Adds the object by path to the active objects. Creates it if it doesn't exist.
    fn update_or_activate_object(&mut self, object: ObjectData) {
        match self.inactive_objects.remove(&object.path) {
            None => self.active_objects.push(object),
            Some(mut old_object) => {
                old_object.update(object);
                self.active_objects.push(old_object);
            }
        }
    }

    fn into_registry(mut self) -> Registry {
        self.deactivate_all_objects();

        Registry {
            objects: self.inactive_objects,
            data_blocks: self.data_blocks,
        }
    }
}

struct Registry {
    objects: HashMap<String, ObjectData>,
    data_blocks: Vec<DataBlock>,
}

impl Registry {
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
