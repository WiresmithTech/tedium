//! Contains wrappers around readers to encode TDMS specific formatting e.g. endianess.
//!
//! Also contains a TdmsDataReader trait for extending the reader with new data types.

use byteorder::{ByteOrder, ReadBytesExt};
use num_traits::FromPrimitive;
use std::{
    io::{BufReader, Read, Seek},
    marker::PhantomData,
};
use thiserror::Error;

use crate::meta_data::{DataTypeRaw, PropertyValue};

#[derive(Error, Debug)]
pub enum TdmsReaderError {
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("String formatting error")]
    StringFormatError(#[from] std::string::FromUtf8Error),
    #[error("Unknown Property Type: {0:X}")]
    UnknownPropertyType(u32),
    #[error("Unsupported Property Type: {0:?}")]
    UnsupportedType(DataTypeRaw),
    #[error("Attempted to read header where no header exists. Bytes: {0:X?}")]
    HeaderPatternNotMatched([u8; 4]),
}

type Result<T> = std::result::Result<T, TdmsReaderError>;

/// Wraps a reader with a byte order for binary reads.
pub struct TdmsReader<'r, O: ByteOrder, R: ReadBytesExt> {
    pub inner: BufReader<&'r mut R>,
    _order: PhantomData<O>,
}

impl<'r, O: ByteOrder, R: ReadBytesExt + Seek> TdmsReader<'r, O, R> {
    pub fn from_reader(reader: &'r mut R) -> Self {
        Self {
            inner: BufReader::new(reader),
            _order: PhantomData,
        }
    }

    /// Move to an absolute position in the file.
    pub fn to_file_position(&mut self, position: u64) -> Result<()> {
        self.inner.seek(std::io::SeekFrom::Start(position))?;
        Ok(())
    }

    /// Move relative to the current file position.
    pub fn move_position(&mut self, offset: i64) -> Result<()> {
        self.inner.seek_relative(offset)?;
        Ok(())
    }
}

pub trait TdmsDataReader<O, T> {
    fn read_value(&mut self) -> Result<T>;
    fn read_vec(&mut self, length: usize) -> Result<Vec<T>> {
        let mut values = Vec::with_capacity(length as usize);

        for _ in 0..length {
            values.push(self.read_value()?);
        }

        Ok(values)
    }
}

/// Macro for scripting the wrapping of the different read methods.
///
/// Should provide the type and the methods will be created with the type name.
macro_rules! read_type {
    ($type:ty) => {
        impl<'r, O: ByteOrder, R: ReadBytesExt> TdmsDataReader<O, $type> for TdmsReader<'r, O, R> {
            paste::item! {
            fn read_value (&mut self) -> Result<$type> {
                Ok(self.inner.[<read_ $type>]::<O>()?)
            }
            }
        }
    };
}

read_type!(i32);
read_type!(u32);
read_type!(u64);
read_type!(f64);

impl<'r, O: ByteOrder, R: Read> TdmsDataReader<O, String> for TdmsReader<'r, O, R> {
    fn read_value(&mut self) -> Result<String> {
        let length: u32 = self.read_value()?;
        let mut buffer = vec![0; length as usize];
        self.inner.read_exact(&mut buffer[..])?;
        let value = String::from_utf8(buffer)?;
        Ok(value)
    }
}

impl<'r, O: ByteOrder, R: ReadBytesExt> TdmsDataReader<O, DataTypeRaw> for TdmsReader<'r, O, R> {
    fn read_value(&mut self) -> Result<DataTypeRaw> {
        let prop_type: u32 = self.read_value()?;
        let prop_type = <DataTypeRaw as FromPrimitive>::from_u32(prop_type)
            .ok_or(TdmsReaderError::UnknownPropertyType(prop_type))?;
        Ok(prop_type)
    }
}

impl<'r, O: ByteOrder, R: Read> TdmsDataReader<O, PropertyValue> for TdmsReader<'r, O, R> {
    fn read_value(&mut self) -> Result<PropertyValue> {
        let raw_type: DataTypeRaw = self.read_value()?;

        match raw_type {
            DataTypeRaw::I32 => Ok(PropertyValue::I32(self.read_value()?)),
            DataTypeRaw::U32 => Ok(PropertyValue::U32(self.read_value()?)),
            DataTypeRaw::U64 => Ok(PropertyValue::U64(self.read_value()?)),
            DataTypeRaw::DoubleFloat | DataTypeRaw::DoubleFloatWithUnit => {
                Ok(PropertyValue::Double(self.read_value()?))
            }
            DataTypeRaw::TdmsString => Ok(PropertyValue::String(self.read_value()?)),
            _ => Err(TdmsReaderError::UnsupportedType(raw_type)),
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use byteorder::{BigEndian, LittleEndian};
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
                    let mut tdms_reader = TdmsReader::<LittleEndian,_>::from_reader(&mut reader);
                    let read_value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(read_value, original_value);
                }

                #[test]
                fn [< test_ $type _be >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_be_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = TdmsReader::<BigEndian,_>::from_reader(&mut reader);
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
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(&mut cursor);
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
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(&mut cursor);
        let result: Result<PropertyValue> = reader.read_value();
        println!("{result:?}");
        assert!(matches!(
            result,
            Err(TdmsReaderError::UnknownPropertyType(0x23))
        ));
    }
}
