//! Module for more complex queries into the index.
//!
//! The base module will handle basic recall but this enables more complex queries of the index
//! such as getting all groups or channels.
//!

use std::ops::Bound;

use super::Index;
use crate::paths::ObjectPath;

/// Implement methods for getting the various objects from the index.
///
/// Note: This is designed only with paths and it is for the file level to convert/work with groups and channels
/// as that isn't a concept in the index.
impl Index {
    /// Get all of the objects stored in the index.
    pub fn all_paths(&self) -> impl Iterator<Item = ObjectPath<'_>> {
        self.objects.keys().map(|path| path.as_str())
    }

    /// Get all of the objects that start with the given path.
    ///
    /// This is seperated as we may be able to use techniques in the index to speed this up.
    pub fn paths_starting_with<'a>(
        &'a self,
        path: ObjectPath<'a>,
    ) -> impl Iterator<Item = ObjectPath<'a>> + 'a {
        // Since we use a BTree we can use ranges of strings to filter the interesting paths.
        // Lower range is our prefix.
        // An upper range is the prefix but with the last character incremented. This isn't trivial so we have
        // stuck with take_while as the performance benefit is likely to be low.
        self.objects
            .range::<String, _>((Bound::Included(path.to_string()), Bound::Unbounded))
            .map(|(path, _)| path.as_str())
            .take_while(move |p| p.starts_with(path))
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::meta_data::MetaData;
    use crate::meta_data::ObjectMetaData;
    use crate::meta_data::RawDataIndex;
    use crate::meta_data::Segment;
    use crate::meta_data::ToC;
    use crate::PropertyValue;

    /// Generate a test file with no data but a few objects.
    ///
    /// group
    ///   - ch1
    ///   - ch2
    /// group2
    ///   - ch1
    ///   - ch2
    /// group3
    fn generate_test_index() -> Index {
        let mut index = Index::new();
        let segment = Segment {
            toc: ToC::from_u32(0x2),
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
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::None,
                    },
                ],
            }),
        };
        index.add_segment(segment).unwrap();

        let segment = Segment {
            toc: ToC::from_u32(0x2),
            next_segment_offset: 20000,
            raw_data_offset: 20,
            meta_data: Some(MetaData {
                objects: vec![
                    ObjectMetaData {
                        path: "/'group2'".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group2'/'ch1'".to_string(),
                        properties: vec![("Prop1".to_string(), PropertyValue::I32(-1))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group2'/'ch2'".to_string(),
                        properties: vec![("Prop2".to_string(), PropertyValue::I32(-2))],
                        raw_data_index: RawDataIndex::None,
                    },
                    ObjectMetaData {
                        path: "/'group3'".to_string(),
                        properties: vec![("Prop".to_string(), PropertyValue::I32(-51))],
                        raw_data_index: RawDataIndex::None,
                    },
                ],
            }),
        };
        index.add_segment(segment).unwrap();
        index
    }

    #[test]
    fn test_all_paths() {
        let index = generate_test_index();
        let paths: Vec<_> = index.all_paths().collect();
        assert_eq!(
            paths,
            vec![
                ObjectPath::from("/'group'"),
                ObjectPath::from("/'group'/'ch1'"),
                ObjectPath::from("/'group'/'ch2'"),
                ObjectPath::from("/'group2'"),
                ObjectPath::from("/'group2'/'ch1'"),
                ObjectPath::from("/'group2'/'ch2'"),
                ObjectPath::from("/'group3'"),
            ]
        );
    }

    #[test]
    fn test_paths_starting_with() {
        let index = generate_test_index();
        let paths: Vec<_> = index
            .paths_starting_with(&ObjectPath::from("/'group2'"))
            .collect();
        assert_eq!(
            paths,
            vec![
                ObjectPath::from("/'group2'"),
                ObjectPath::from("/'group2'/'ch1'"),
                ObjectPath::from("/'group2'/'ch2'"),
            ]
        );
    }

    #[test]
    fn test_paths_starting_with_no_match() {
        let index = generate_test_index();
        let paths: Vec<_> = index
            .paths_starting_with(&ObjectPath::from("/'group4'"))
            .collect();
        assert!(paths.is_empty());
    }
}
