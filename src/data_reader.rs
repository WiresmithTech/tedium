//! Contains wrappers around readers to encode TDMS specific formatting e.g. endianess.
//!
//! Also contains a TdmsDataReader trait for extending the reader with new data types.

use std::io::{BufReader, Read, Seek};
use thiserror::Error;

use crate::meta_data::{PropertyValue, ToC};
use crate::{
    data_types::{DataType, TdmsMetaData, TdmsStorageType},
    meta_data::SegmentMetaData,
};

#[derive(Error, Debug)]
pub enum TdmsReaderError {
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("String formatting error")]
    StringFormatError(#[from] std::string::FromUtf8Error),
    #[error("Unknown Property Type: {0:X}")]
    UnknownPropertyType(u32),
    #[error("Unsupported Property Type: {0:?}")]
    UnsupportedType(DataType),
    #[error("Attempted to read header where no header exists. Bytes: {0:X?}")]
    HeaderPatternNotMatched([u8; 4]),
}

pub trait TdmsReader<R: Read + Seek>: Sized {
    fn from_reader(reader: R) -> Self;
    fn read_value<T: TdmsStorageType>(&mut self) -> Result<T, TdmsReaderError>;
    fn read_meta<T: TdmsMetaData>(&mut self) -> Result<T, TdmsReaderError> {
        T::read(self)
    }
    fn read_vec<T: TdmsMetaData>(&mut self, length: usize) -> Result<Vec<T>, TdmsReaderError> {
        let mut vec = Vec::with_capacity(length);
        for _ in 0..length {
            vec.push(self.read_meta()?);
        }
        Ok(vec)
    }
    fn buffered_reader(&mut self) -> &mut BufReader<R>;

    /// Move to an absolute position in the file.
    fn to_file_position(&mut self, position: u64) -> Result<(), TdmsReaderError> {
        self.buffered_reader()
            .seek(std::io::SeekFrom::Start(position))?;
        Ok(())
    }

    /// Move relative to the current file position.
    fn move_position(&mut self, offset: i64) -> Result<(), TdmsReaderError> {
        self.buffered_reader().seek_relative(offset)?;
        Ok(())
    }

    /// Called immediately after ToC has been read so we have determined the endianess.
    fn read_segment(&mut self, toc: ToC) -> Result<SegmentMetaData, TdmsReaderError> {
        let _version: u32 = self.read_value()?;
        let next_segment_offset = self.read_value()?;
        let raw_data_offset = self.read_value()?;

        //todo handle no meta data mode.
        let object_length: u32 = self.read_value()?;
        let objects = self.read_vec(object_length as usize)?;

        Ok(SegmentMetaData {
            toc: toc,
            next_segment_offset,
            raw_data_offset,
            objects,
        })
    }
}

pub struct LittleEndianReader<R: Read>(BufReader<R>);

impl<R: Read + Seek> TdmsReader<R> for LittleEndianReader<R> {
    fn read_value<T: TdmsStorageType>(&mut self) -> Result<T, TdmsReaderError> {
        T::read_le(&mut self.0)
    }

    fn from_reader(reader: R) -> Self {
        Self(BufReader::new(reader))
    }

    fn buffered_reader(&mut self) -> &mut BufReader<R> {
        &mut self.0
    }
}

pub struct BigEndianReader<R: Read>(BufReader<R>);

impl<R: Read + Seek> TdmsReader<R> for BigEndianReader<R> {
    fn read_value<T: TdmsStorageType>(&mut self) -> Result<T, TdmsReaderError> {
        T::read_be(&mut self.0)
    }

    fn from_reader(reader: R) -> Self {
        Self(BufReader::new(reader))
    }

    fn buffered_reader(&mut self) -> &mut BufReader<R> {
        &mut self.0
    }
}

impl TdmsMetaData for PropertyValue {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<Self, TdmsReaderError> {
        let raw_type: DataType = reader.read_meta()?;

        match raw_type {
            DataType::I32 => Ok(PropertyValue::I32(reader.read_value()?)),
            DataType::U32 => Ok(PropertyValue::U32(reader.read_value()?)),
            DataType::U64 => Ok(PropertyValue::U64(reader.read_value()?)),
            DataType::DoubleFloat | DataType::DoubleFloatWithUnit => {
                Ok(PropertyValue::Double(reader.read_value()?))
            }
            DataType::TdmsString => Ok(PropertyValue::String(reader.read_value()?)),
            _ => Err(TdmsReaderError::UnsupportedType(raw_type)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Cursor;

    /// Tests the conversion against the le and be version for the value specified.
    macro_rules! test_formatting {
        ($type:ty, $test_value:literal) => {
            paste::item! {
                #[test]
                fn [< test_ $type _le >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_le_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
                    let read_value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(read_value, original_value);
                }

                #[test]
                fn [< test_ $type _be >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_be_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
                    let read_value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(read_value, original_value);
                }
            }
        };
    }

    test_formatting!(i32, -12345);
    test_formatting!(u32, 12345);
    test_formatting!(f64, 1234.1245);

    #[test]
    fn test_string() {
        //example from NI site
        let test_buffer = [
            0x23, 00, 00, 00, 0x2Fu8, 0x27, 0x4D, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x64, 0x20,
            0x54, 0x68, 0x72, 0x6F, 0x75, 0x67, 0x68, 0x70, 0x75, 0x74, 0x20, 0x44, 0x61, 0x74,
            0x61, 0x20, 0x28, 0x56, 0x6F, 0x6C, 0x74, 0x73, 0x29, 0x27,
        ];
        let mut cursor = Cursor::new(test_buffer);
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let string: String = reader.read_value().unwrap();
        assert_eq!(string, String::from("/'Measured Throughput Data (Volts)'"));
    }

    #[test]
    fn test_unknown_property_type() {
        //example from NI site
        let test_buffer = [
            0x23, 00, 00, 00, 0x2Fu8, 0x27, 0x4D, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x64, 0x20,
            0x54, 0x68, 0x72, 0x6F, 0x75, 0x67, 0x68, 0x70, 0x75, 0x74, 0x20, 0x44, 0x61, 0x74,
            0x61, 0x20, 0x28, 0x56, 0x6F, 0x6C, 0x74, 0x73, 0x29, 0x27,
        ];
        let mut cursor = Cursor::new(test_buffer);
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let result: Result<PropertyValue, TdmsReaderError> = reader.read_meta();
        println!("{result:?}");
        assert!(matches!(
            result,
            Err(TdmsReaderError::UnknownPropertyType(0x23))
        ));
    }
}
