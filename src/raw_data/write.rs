//! Handles writing the raw data to disk.

use crate::error::TdmsError;
use crate::io::data_types::TdmsStorageType;
use crate::meta_data::RawDataMeta;
// This is a circular reference - can we remove it?
use crate::io::writer::TdmsWriter;
use std::io::Write;
use std::num::NonZeroUsize;

/// Indicates a set of data that can be written as a binary block to a TDMS file.
pub trait WriteBlock {
    fn data_structure(&self) -> Vec<RawDataMeta>;
    fn write<W: Write, T: TdmsWriter<W>>(&self, writer: &mut T) -> Result<(), TdmsError>;
    fn size(&self) -> usize;
}

/// Implementation for a data slice of [`TDMSStorageType`] assuming it is a preformatted data block.
impl<D: TdmsStorageType> WriteBlock for &[D] {
    fn data_structure(&self) -> Vec<RawDataMeta> {
        vec![RawDataMeta {
            data_type: D::NATURAL_TYPE,
            number_of_values: self.len() as u64,
            total_size_bytes: None,
        }]
    }

    fn write<W: Write, T: TdmsWriter<W>>(&self, writer: &mut T) -> Result<(), TdmsError> {
        for item in *self {
            writer.write_value(item)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(*self)
    }
}

/// Wrap the simple single-channel slice to handle multi-channels.
pub struct MultiChannelSlice<'a, D: TdmsStorageType>(&'a [D], usize);

impl<'a, D: TdmsStorageType> MultiChannelSlice<'a, D> {
    pub fn from_slice(slice: &'a [D], channel_count: NonZeroUsize) -> Result<Self, TdmsError> {
        if (slice.len() % channel_count) == 0 {
            Ok(Self(slice, channel_count.get()))
        } else {
            Err(TdmsError::BadDataBlockLength(slice.len(), channel_count.get()))
        }
    }
}

impl<'a, D: TdmsStorageType> WriteBlock for MultiChannelSlice<'a, D> {
    fn data_structure(&self) -> Vec<RawDataMeta> {
        let basic_meta = self
            .0
            .data_structure()
            .get(0)
            .expect("Should always/only have 1 entry")
            .clone();

        let samples_per_channel: u64 = (self.0.len() / self.1) as u64;

        (0..self.1)
            .map(|_| {
                let mut meta = basic_meta.clone();
                meta.number_of_values = samples_per_channel;
                meta
            })
            .collect()
    }

    fn write<W: Write, T: TdmsWriter<W>>(&self, writer: &mut T) -> Result<(), TdmsError> {
        self.0.write(writer)
    }

    fn size(&self) -> usize {
        self.0.size()
    }
}

#[cfg(test)]
mod write_tests {
    use crate::{io::data_types::DataType, io::writer::LittleEndianWriter};

    use super::*;

    #[test]
    fn single_channel_writer_generates_meta_data() {
        let data = vec![0u32; 20];
        let meta = (&data[..]).data_structure();

        // Although total size isi calculable this is only used for strings.
        let expected_meta = RawDataMeta {
            data_type: DataType::U32,
            number_of_values: 20,
            total_size_bytes: None,
        };

        assert_eq!(meta, &[expected_meta]);
    }

    #[test]
    fn single_channel_writer_writes_with_endianess() {
        let data = vec![0u32, 1, 2, 3];

        let mut buf = vec![];
        {
            let mut writer = LittleEndianWriter::from_writer(&mut buf);
            (&data[..]).write(&mut writer).unwrap();
        }

        assert_eq!(
            &buf[..],
            &[
                0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03, 0x00,
                0x00, 0x00
            ]
        );
    }

    #[test]
    fn multi_channel_writer_generates_meta_data() {
        let data = vec![0u32; 20];
        let multi_channel = MultiChannelSlice::from_slice(&data[..], 4.try_into().unwrap()).unwrap();
        let meta = multi_channel.data_structure();

        // Although total size isi calculable this is only used for strings.
        let expected_meta = RawDataMeta {
            data_type: DataType::U32,
            number_of_values: 5,
            total_size_bytes: None,
        };

        assert_eq!(
            meta,
            &[
                expected_meta.clone(),
                expected_meta.clone(),
                expected_meta.clone(),
                expected_meta.clone()
            ]
        );
    }

    /// In this case it is bad because 20 isn't divisible by 3.
    #[test]
    fn multi_channel_writer_errors_bad_channel_length() {
        let data = vec![0u32; 20];
        let multi_channel_result = MultiChannelSlice::from_slice(&data[..], 3.try_into().unwrap());
        assert!(matches!(
            multi_channel_result,
            Err(TdmsError::BadDataBlockLength(20, 3))
        ))
    }
}
