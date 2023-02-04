//! Contains wrappers around readers to encode TDMS specific formatting e.g. endianess.
//!

use byteorder::{ByteOrder, ReadBytesExt};
use num_traits::FromPrimitive;
use std::{io::Read, marker::PhantomData};
use thiserror::Error;

use crate::file_types::{DataTypeRaw, PropertyValue};

#[derive(Error, Debug)]
enum TdmsReaderError {
    #[error("IO Error")]
    IoError(#[from] std::io::Error),
    #[error("String formatting error")]
    StringFormatError(#[from] std::string::FromUtf8Error),
    #[error("Unknown Property Type: {0:X}")]
    UnknownPropertyType(u32),
    #[error("Unsupported Property Type: {0:?}")]
    UnsupportedType(DataTypeRaw),
}

type Result<T> = std::result::Result<T, TdmsReaderError>;

/// Wraps a reader with a byte order for binary reads.
struct TdmsReader<O: ByteOrder, R: ReadBytesExt> {
    inner: R,
    _order: PhantomData<O>,
}

/// Macro for scripting the wrapping of the different read methods.
///
/// Should provide the type and the methods will be created with the type name.
macro_rules! read_type {
    ($type:ty) => {
        paste::item! {
        pub fn [<read_ $type>] (&mut self) -> Result<$type> {
            Ok(self.inner.[<read_ $type>]::<O>()?)
        }
        }
    };
}

impl<O: ByteOrder, R: ReadBytesExt> TdmsReader<O, R> {
    pub fn from_reader(reader: R) -> Self {
        Self {
            inner: reader,
            _order: PhantomData,
        }
    }
    read_type!(u32);
    read_type!(f64);

    pub fn read_string(&mut self) -> Result<String> {
        let length = self.read_u32()?;
        let mut buffer = vec![0; length as usize];
        self.inner.read_exact(&mut buffer[..])?;
        let value = String::from_utf8(buffer)?;
        Ok(value)
    }

    pub fn read_property(&mut self) -> Result<PropertyValue> {
        let prop_type = self.read_u32()?;
        let prop_type = <DataTypeRaw as FromPrimitive>::from_u32(prop_type)
            .ok_or(TdmsReaderError::UnknownPropertyType(prop_type))?;
        Err(TdmsReaderError::UnknownPropertyType(0))
    }

    fn read_property_from_type(&mut self, prop_type: DataTypeRaw) -> Result<PropertyValue> {
        match prop_type {
            _ => Err(TdmsReaderError::UnsupportedType(prop_type)),
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
                    let reader = Cursor::new(bytes);
                    let mut tdms_reader = TdmsReader::<LittleEndian,_>::from_reader(reader);
                    let read_value = tdms_reader.[< read_ $type>] ().unwrap();
                    assert_eq!(read_value, original_value);
                }

                #[test]
                fn [< test_ $type _be >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_be_bytes();
                    let reader = Cursor::new(bytes);
                    let mut tdms_reader = TdmsReader::<BigEndian,_>::from_reader(reader);
                    let read_value = tdms_reader.[< read_ $type>] ().unwrap();
                    assert_eq!(read_value, original_value);
                }
            }
        };
    }

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
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(Cursor::new(test_buffer));
        let string = reader.read_string().unwrap();
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
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(Cursor::new(test_buffer));
        let result = reader.read_property();
        println!("{result:?}");
        assert!(matches!(
            result,
            Err(TdmsReaderError::UnknownPropertyType(0x23))
        ));
    }
}
