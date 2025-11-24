//! Index methods for adding segments to the index.
//!
//! This overlaps both reading and writing functions since both generate new indexes
//! that need to be entered into the index.
//!

use crate::{
    error::TdmsError,
    meta_data::{ObjectMetaData, RawDataIndex, RawDataMeta, Segment},
    paths::ObjectPath,
    raw_data::DataBlock,
};

use super::{DataFormat, DataLocation, ObjectData, ObjectIndex};

/// Data cached for the current "active" objects which are the objects
/// that we are expecting data in the next data block.
#[derive(Debug, Clone)]
pub struct ActiveObject {
    pub path: String,
    pub number_of_samples: u64,
}

impl ActiveObject {
    fn new(path: &str, format: &DataFormat) -> Self {
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
        if let RawDataIndex::RawData(ref raw) = meta.raw_data_index {
            self.number_of_samples = raw.number_of_values;
        }
    }

    /// Fetch the corresponding [`ObjectData`] for the active object.
    fn get_object_data<'c>(&self, index: &'c ObjectIndex) -> &'c ObjectData {
        index
            .get(&self.path)
            .expect("Should always have a registered version of active object")
    }

    /// Fetch the corresponding [`ObjectData`] for the active object in a mutable form.
    fn get_object_data_mut<'c>(&self, index: &'c mut ObjectIndex) -> &'c mut ObjectData {
        index
            .get_mut(&self.path)
            .expect("Should always have a registered version of active object")
    }
}

impl super::Index {
    /// Add the data for the next segment read from the file.
    ///
    /// Returns the start position of the next segment.
    ///
    /// Errors if:
    /// * The next segment address overflows.
    pub fn add_segment(&mut self, segment: Segment) -> Result<u64, TdmsError> {
        //Basic procedure.
        //1. If new object list is set, clear active objects.
        //2. Update the active object list - adding new objects or updating properties and data locations for existing objects.

        if segment.toc.contains_new_object_list {
            self.deactivate_all_objects();
        }

        if let Some(meta_data) = &segment.meta_data {
            for obj in meta_data.objects.iter() {
                match obj.raw_data_index {
                    RawDataIndex::None => self.update_meta_object(obj)?,
                    _ => self.update_or_activate_data_object(obj)?,
                }
            }
        }

        if segment.toc.contains_raw_data {
            let active_data_channels = self.get_active_raw_data_meta();

            if active_data_channels.is_empty() {
                return Err(TdmsError::SegmentTocDataBlockWithoutDataChannels);
            }

            let data_block =
                DataBlock::from_segment(&segment, self.next_segment_start, active_data_channels)?;

            self.insert_data_block(data_block)?;
        }

        let segment_size = segment.total_size_bytes()?;
        match self.next_segment_start.checked_add(segment_size) {
            Some(next_segment_start) => self.next_segment_start = next_segment_start,
            None => return Err(TdmsError::SegmentAddressOverflow),
        }
        Ok(self.next_segment_start)
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

    fn insert_data_block(&mut self, block: DataBlock) -> Result<(), TdmsError> {
        let data_index = self.data_blocks.len();

        // get counts from block.
        let chunks = block.number_of_chunks()?;

        self.data_blocks.push(block);

        for (channel_index, active_object) in self.active_objects.iter_mut().enumerate() {
            let number_of_samples = active_object
                .number_of_samples
                .checked_mul(chunks as u64)
                .ok_or(TdmsError::ChunkSizeOverflow)?;
            let location = DataLocation {
                data_block: data_index,
                channel_index,
                number_of_samples,
            };
            active_object
                .get_object_data_mut(&mut self.objects)
                .add_data_location(location);
        }
        Ok(())
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
    fn update_or_activate_data_object(&mut self, object: &ObjectMetaData) -> Result<(), TdmsError> {
        let matching_active = self
            .active_objects
            .iter_mut()
            .find(|active_object| active_object.path == object.path);

        match matching_active {
            Some(active_object) => {
                active_object.update(object);
                active_object
                    .get_object_data_mut(&mut self.objects)
                    .update(object)
            }
            None => {
                self.update_meta_object(object)?;
                // Must fetch the latest format in case this is same as previous.
                let format = self
                    .channel_format(&object.path)
                    .expect("Should not reach this if there is no data with the object.");

                self.active_objects
                    .push(ActiveObject::new(&object.path, format));
                Ok(())
            }
        }
    }

    /// Update Meta Only Object
    ///
    /// Update an object which contains no data.
    fn update_meta_object(&mut self, object: &ObjectMetaData) -> Result<(), TdmsError> {
        match self.objects.get_mut(&object.path) {
            Some(found_object) => found_object.update(object),
            None => {
                let object_data = ObjectData::from_metadata(object)?;
                let old = self.objects.insert(object_data.path.clone(), object_data);
                assert!(
                    old.is_none(),
                    "Should not be possible to be replacing an existing object."
                );
                Ok(())
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
}

#[cfg(test)]
mod tests {
    use crate::io::data_types::DataType;
    use crate::meta_data::MetaData;
    use crate::meta_data::ObjectMetaData;
    use crate::meta_data::RawDataIndex;
    use crate::meta_data::RawDataMeta;
    use crate::meta_data::ToC;
    use crate::paths::{ChannelPath, PropertyPath};
    use crate::raw_data::{DataLayout, Endianess};
    use crate::PropertyValue;

    use super::super::Index;
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
        index.add_segment(segment).unwrap();

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

        assert_eq!(
            index.channel_length(&ChannelPath::new("group", "ch1")),
            Some(1000)
        );
        assert_eq!(
            index.channel_length(&ChannelPath::new("group", "ch2")),
            Some(1000)
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
        index.add_segment(segment).unwrap();

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
        assert_eq!(
            index.channel_length(&ChannelPath::new("group", "ch1")),
            Some(2000)
        );
        assert_eq!(
            index.channel_length(&ChannelPath::new("group", "ch2")),
            Some(2000)
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
        index.add_segment(segment).unwrap();

        let expected_data_block = DataBlock {
            start: 48,
            length: 480.try_into().unwrap(),
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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

        let expected_data_block = DataBlock {
            start: 576,
            length: 480.try_into().unwrap(),
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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

        let expected_data_block = DataBlock {
            start: 576,
            length: 480.try_into().unwrap(),
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
    fn errors_if_no_previous_datatype_is_found() {
        let segment = Segment {
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
        let result = index.add_segment(segment);
        assert!(result.is_err());
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
        index.add_segment(segment).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();
        index.add_segment(segment3).unwrap();

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
    fn test_toc_includes_data_but_no_active_channels() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'".to_string(),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                }],
            }),
        };

        let mut index = Index::new();
        let next_segment = index.add_segment(segment);
        assert!(matches!(
            next_segment,
            Err(TdmsError::SegmentTocDataBlockWithoutDataChannels)
        ));
    }

    #[test]
    fn test_catches_next_segment_overflow() {
        let segment = Segment {
            toc: ToC::from_u32(0x2),
            next_segment_offset: 0xFFFF_FFFF_FFFF_FFE4,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'".to_string(),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                }],
            }),
        };

        let mut index = Index::new();
        let next_segment = index.add_segment(segment);
        assert!(matches!(
            next_segment,
            Err(TdmsError::SegmentAddressOverflow)
        ));
    }

    #[test]
    fn test_toc_includes_data_but_no_size() {
        let segment = Segment {
            toc: ToC::from_u32(0xE),
            next_segment_offset: 20,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![ObjectMetaData {
                    path: "/'group'".to_string(),
                    properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                    raw_data_index: RawDataIndex::None,
                }],
            }),
        };

        let mut index = Index::new();
        let next_segment = index.add_segment(segment);
        assert!(matches!(
            next_segment,
            Err(TdmsError::SegmentTocDataBlockWithoutDataChannels)
        ));
    }
}
