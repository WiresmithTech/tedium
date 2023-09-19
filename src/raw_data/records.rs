//! The record module defines functions and capabilities to plan
//! for the structure of records/entries in a data segment.
//!
//! This is used to define a read pattern for a data segment and
//! is used by the data readers to efficiently read the data.

use crate::{error::TdmsError, io::data_types::TdmsStorageType, meta_data::RawDataMeta};

// An instruction on how to move through the record based on the read instructions.
///
/// It can only read one data type at a time.
#[derive(Debug)]
pub enum RecordEntryPlan<'a, T: 'a, I: Iterator<Item = &'a mut T>> {
    // An entry that isn't to be read
    Skip(i64),
    // A entry that isn't being read but has an unknown size.
    SkipVariable,
    // Read entry to output index.
    Read(I),
}

impl<'a, T: TdmsStorageType, I: Iterator<Item = &'a mut T>> RecordEntryPlan<'a, T, I> {
    fn entry_size_bytes(&self) -> Option<usize> {
        match self {
            RecordEntryPlan::Skip(bytes) => Some(*bytes as usize),
            RecordEntryPlan::SkipVariable => None,
            RecordEntryPlan::Read(_) => Some(T::SIZE_BYTES),
        }
    }
}

/// Represents a record entry including read instructions and expected size per block.
#[derive(Debug)]
pub struct RecordEntry<'a, T: 'a> {
    // The expected number of entrys for the record.
    pub length: usize,
    // The read instructions for the record entry.
    pub plan: RecordEntryPlan<'a, T, std::slice::IterMut<'a, T>>,
}

/// The record structure encodes the structure of the block
/// ready for reading. Marking sizes and positions of readable
/// records and their outputs.
#[derive(Debug)]
pub struct RecordStructure<'a, T>(Vec<RecordEntry<'a, T>>);

impl<'o, 'b: 'o, T: TdmsStorageType> RecordStructure<'o, T> {
    /// Build a record structure for the channels specified.
    ///
    /// `channels` - This is the structure of the data segment.
    /// `outputs` - This defines the indexes of the channels to read and the output buffers to read into.
    ///
    /// todo: can we validate the record matches the data type it is trying to read?
    pub fn build_record_plan(
        channels: &[RawDataMeta],
        outputs: &'b mut [(usize, &'b mut [T])],
    ) -> Result<RecordStructure<'o, T>, TdmsError> {
        let mut plan = Self::build_base_record(channels);

        validate_types_match(outputs, channels)?;
        plan.set_readable_records(outputs);
        plan.compress_reads();
        Ok(plan)
    }

    /// Get the read instructions for the record.
    pub fn read_instructions<'a>(&'a mut self) -> &'a mut [RecordEntry<'o, T>] {
        &mut self.0[..]
    }

    /// Get the size of a single record in bytes.
    ///
    /// ## Panics
    ///
    /// This will panic if we have variable length records.
    pub fn row_size(&self) -> usize {
        self.0
            .iter()
            .map(|entry| match entry.plan.entry_size_bytes() {
                Some(bytes) => bytes,
                None => todo!("Variable length records not yet supported"),
            })
            .sum()
    }

    /// Get the size of the entire written block based on the structure.
    ///
    /// ## Panics
    ///
    /// This will panic if we have variable length records.
    pub fn block_size(&self) -> usize {
        self.0
            .iter()
            .map(|entry| {
                entry
                    .plan
                    .entry_size_bytes()
                    .expect("Variable length records not yet supported")
                    * entry.length
            })
            .sum()
    }

    /// Build a base record structure which just skips all channels.
    fn build_base_record<'a>(channels: &'a [RawDataMeta]) -> Self {
        let mut plan = Vec::with_capacity(channels.len());
        for channel in channels {
            plan.push(RecordEntry {
                length: channel.number_of_values as usize,
                plan: RecordEntryPlan::Skip(channel.data_type.size() as i64),
            })
        }
        Self(plan)
    }

    /// Set which records should be read in the plan.
    fn set_readable_records(&mut self, outputs: &'b mut [(usize, &'b mut [T])]) {
        for output in outputs {
            self.0[output.0].plan = RecordEntryPlan::Read(output.1.iter_mut());
        }
    }

    /// Combine skips to reduce the number of reads.
    fn compress_reads(&mut self) {}
}

fn validate_types_match<T: TdmsStorageType>(
    outputs: &[(usize, &mut [T])],
    channels: &[RawDataMeta],
) -> Result<(), TdmsError> {
    for (output_idx, _) in outputs.iter() {
        if !T::SUPPORTED_TYPES.contains(&channels[*output_idx].data_type) {
            return Err(TdmsError::DataTypeMismatch(
                channels[*output_idx].data_type,
                T::NATURAL_TYPE,
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{error::TdmsError, io::data_types::DataType};

    use super::*;

    #[test]
    fn test_basic_record_structure_read_all() {
        let channels = vec![
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
        ];
        let mut out1 = vec![0.0; 1000];
        let mut out2 = vec![0.0; 1000];

        let mut outputs: Vec<(usize, &mut [f64])> = vec![(0, &mut out1), (1, &mut out2)];

        let read_plan =
            RecordStructure::<f64>::build_record_plan(&channels, &mut outputs[..]).unwrap();

        assert_eq!(read_plan.0.len(), 2);
        assert_eq!(read_plan.0[0].length, 1000);
        assert_eq!(read_plan.0[1].length, 1000);
        assert!(matches!(read_plan.0[0].plan, RecordEntryPlan::Read(_)));
        assert!(matches!(read_plan.0[0].plan, RecordEntryPlan::Read(_)));
    }

    #[test]
    fn test_basic_record_structure_read_one() {
        let channels = vec![
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
        ];
        let mut out1 = vec![0.0; 1000];

        let mut outputs: Vec<(usize, &mut [f64])> = vec![(1, &mut out1)];

        let read_plan =
            RecordStructure::<f64>::build_record_plan(&channels, &mut outputs[..]).unwrap();

        assert_eq!(read_plan.0.len(), 2);
        assert_eq!(read_plan.0[1].length, 1000);
        assert!(matches!(read_plan.0[0].plan, RecordEntryPlan::Skip(8)));
        assert!(matches!(read_plan.0[1].plan, RecordEntryPlan::Read(_)));
    }

    #[test]
    fn test_error_on_type_mismatch() {
        let channels = vec![
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
        ];
        let mut out1 = vec![0; 1000];

        let mut outputs: Vec<(usize, &mut [u32])> = vec![(1, &mut out1)];

        let read_plan_result =
            RecordStructure::<u32>::build_record_plan(&channels, &mut outputs[..]);

        assert!(matches!(
            read_plan_result,
            Err(TdmsError::DataTypeMismatch(
                DataType::DoubleFloat,
                DataType::U32
            ))
        ));
    }

    #[ignore = "Not yet implemented"]
    #[test]
    fn test_compresses_similar_skips_for_performance() {
        let channels = vec![
            RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            },
            RawDataMeta {
                data_type: DataType::I32,
                number_of_values: 1000,
                total_size_bytes: None,
            },
            RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            },
        ];
        let mut out1 = vec![0.0; 1000];

        let mut outputs: Vec<(usize, &mut [f64])> = vec![(1, &mut out1)];

        let read_plan =
            RecordStructure::<f64>::build_record_plan(&channels, &mut outputs[..]).unwrap();

        assert_eq!(read_plan.0.len(), 2);
        assert_eq!(read_plan.0[1].length, 1000);
        assert!(matches!(read_plan.0[0].plan, RecordEntryPlan::Skip(12)));
        assert!(matches!(read_plan.0[1].plan, RecordEntryPlan::Read(_)));
    }

    #[test]
    fn test_returns_length_of_single_records() {
        let channels = vec![
            RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            },
            RawDataMeta {
                data_type: DataType::I32,
                number_of_values: 1000,
                total_size_bytes: None,
            },
            RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            },
        ];
        let mut out1 = vec![0i32; 1000];

        let mut outputs: Vec<(usize, &mut [i32])> = vec![(1, &mut out1)];

        let read_plan =
            RecordStructure::<i32>::build_record_plan(&channels, &mut outputs[..]).unwrap();

        assert_eq!(read_plan.row_size(), 20);
    }

    #[test]
    fn test_returns_length_of_write_block() {
        let channels = vec![
            RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            },
            RawDataMeta {
                data_type: DataType::I32,
                number_of_values: 1000,
                total_size_bytes: None,
            },
            RawDataMeta {
                data_type: DataType::DoubleFloat,
                number_of_values: 1000,
                total_size_bytes: None,
            },
        ];
        let mut out1 = vec![0i32; 1000];

        let mut outputs: Vec<(usize, &mut [i32])> = vec![(1, &mut out1)];

        let read_plan =
            RecordStructure::<i32>::build_record_plan(&channels, &mut outputs[..]).unwrap();

        assert_eq!(read_plan.block_size(), 20000);
    }
}
