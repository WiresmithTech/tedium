//! Functions to support file writing capabilties.
//!
//! This overlaps significantly with [super::building] but these functions are specific only to writing.
//! They are split out to manage the module sizes.

use crate::meta_data::RawDataIndex;

use super::{DataFormat, Index};

impl Index {
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

    use crate::{
        io::data_types::DataType,
        meta_data::{MetaData, ObjectMetaData, RawDataMeta, Segment, ToC},
        PropertyValue,
    };

    use super::*;

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
        index.add_segment(segment).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
        index.add_segment(segment).unwrap();

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
        index.add_segment(segment).unwrap();

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
        index.add_segment(segment).unwrap();
        index.add_segment(segment2).unwrap();

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
