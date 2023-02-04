//! Contains wrappers around readers to encode TDMS specific formatting e.g. endianess.
//!

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use num_traits::FromPrimitive;
use std::marker::PhantomData;
use thiserror::Error;

use crate::file_types::{
    DataTypeRaw, ObjectMetaData, PropertyValue, RawDataIndex, RawDataMeta, SegmentMetaData, ToC,
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
    UnsupportedType(DataTypeRaw),
    #[error("Attempted to read header where no header exists")]
    HeaderPatternNotMatched,
}

type Result<T> = std::result::Result<T, TdmsReaderError>;

pub fn read_segment(reader: &mut impl ReadBytesExt) -> Result<SegmentMetaData> {
    let mut tag = [0u8; 4];
    reader.read_exact(&mut tag)?;
    unsafe {
        println!("{}", std::str::from_utf8_unchecked(&tag));
    }
    if tag != [0x54, 0x44, 0x53, 0x6D] {
        return Err(TdmsReaderError::HeaderPatternNotMatched);
    }

    let toc = ToC::from_u32(reader.read_u32::<LittleEndian>()?);

    let segment = match toc.big_endian {
        true => TdmsReader::<BigEndian, _>::from_reader(reader).read_segment(toc)?,
        false => TdmsReader::<LittleEndian, _>::from_reader(reader).read_segment(toc)?,
    };
    Ok(segment)
}

/// Wraps a reader with a byte order for binary reads.
struct TdmsReader<'r, O: ByteOrder, R: ReadBytesExt> {
    inner: &'r mut R,
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

impl<'r, O: ByteOrder, R: ReadBytesExt> TdmsReader<'r, O, R> {
    pub fn from_reader(reader: &'r mut R) -> Self {
        Self {
            inner: reader,
            _order: PhantomData,
        }
    }
    read_type!(i32);
    read_type!(u32);
    read_type!(u64);
    read_type!(f64);

    pub fn read_string(&mut self) -> Result<String> {
        let length = self.read_u32()?;
        let mut buffer = vec![0; length as usize];
        self.inner.read_exact(&mut buffer[..])?;
        let value = String::from_utf8(buffer)?;
        Ok(value)
    }

    pub fn read_property(&mut self) -> Result<PropertyValue> {
        let raw_type = self.read_raw_data_type()?;

        match raw_type {
            DataTypeRaw::I32 => Ok(PropertyValue::I32(self.read_i32()?)),
            DataTypeRaw::U32 => Ok(PropertyValue::U32(self.read_u32()?)),
            DataTypeRaw::U64 => Ok(PropertyValue::U64(self.read_u64()?)),
            DataTypeRaw::DoubleFloat | DataTypeRaw::DoubleFloatWithUnit => {
                Ok(PropertyValue::Double(self.read_f64()?))
            }
            DataTypeRaw::TdmsString => Ok(PropertyValue::String(self.read_string()?)),
            _ => Err(TdmsReaderError::UnsupportedType(raw_type)),
        }
    }

    fn read_raw_data_type(&mut self) -> Result<DataTypeRaw> {
        let prop_type = self.read_u32()?;
        let prop_type = <DataTypeRaw as FromPrimitive>::from_u32(prop_type)
            .ok_or(TdmsReaderError::UnknownPropertyType(prop_type))?;
        Ok(prop_type)
    }

    fn read_property_from_type(&mut self, prop_type: DataTypeRaw) -> Result<PropertyValue> {
        match prop_type {
            _ => Err(TdmsReaderError::UnsupportedType(prop_type)),
        }
    }

    fn read_meta_data(&mut self) -> Result<Vec<ObjectMetaData>> {
        let object_count = self.read_u32()?;
        let mut objects = Vec::with_capacity(object_count as usize);

        for object in 0..object_count {
            objects.push(self.read_object_meta()?);
        }

        Ok(objects)
    }

    fn read_object_meta(&mut self) -> Result<ObjectMetaData> {
        let path = self.read_string()?;
        let raw_index = self.read_u32()?;

        let raw_data = match raw_index {
            0xFFFF_FFFF => RawDataIndex::None,
            0x69120000..=0x6912FFFF => todo!(), // daqmx 1
            0x69130000..=0x6913FFFF => todo!(), //daqmx 2
            _ => self.read_raw_data_meta(raw_index)?,
        };

        let property_count = self.read_u32()?;

        let mut properties = Vec::with_capacity(property_count as usize);

        for _prop in 0..property_count {
            let name = self.read_string()?;
            let value = self.read_property()?;
            properties.push((name, value));
        }

        Ok(ObjectMetaData {
            path,
            properties,
            raw_data_index: raw_data,
        })
    }

    fn read_raw_data_meta(&mut self, first_value: u32) -> Result<RawDataIndex> {
        let data_type = self.read_raw_data_type()?;
        let _array_dims = self.read_u32()?; //always 1.
        let number_of_values = self.read_u64()?;
        let meta = RawDataMeta {
            data_type,
            number_of_values,
            total_size_bytes: None,
        };

        Ok(RawDataIndex::RawData(meta))
    }

    /// Called immediately after ToC has been read so we have determined the endianess.
    fn read_segment(&mut self, toc: ToC) -> Result<SegmentMetaData> {
        let _version = self.read_u32()?;
        let next_segment_offset = self.read_u64()?;
        let raw_data_offset = self.read_u64()?;
        let objects = self.read_meta_data()?;

        Ok(SegmentMetaData {
            toc: toc,
            next_segment_offset,
            raw_data_offset,
            objects,
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::file_types::RawDataMeta;

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
                    let read_value = tdms_reader.[< read_ $type>] ().unwrap();
                    assert_eq!(read_value, original_value);
                }

                #[test]
                fn [< test_ $type _be >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_be_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = TdmsReader::<BigEndian,_>::from_reader(&mut reader);
                    let read_value = tdms_reader.[< read_ $type>] ().unwrap();
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
        let mut cursor = Cursor::new(test_buffer);
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(&mut cursor);
        let result = reader.read_property();
        println!("{result:?}");
        assert!(matches!(
            result,
            Err(TdmsReaderError::UnknownPropertyType(0x23))
        ));
    }

    #[test]
    fn test_properties_standard_data() {
        //example from NI "TDMS internal file format"
        let test_buffer = [
            0x02, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
            0x70, 0x72, 0x6F, 0x70, 0x20, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x76, 0x61,
            0x6C, 0x75, 0x65, 0x03, 0x00, 0x00, 0x00, 0x6E, 0x75, 0x6D, 0x03, 0x00, 0x00, 0x00,
            0x0A, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0x2F, 0x27, 0x43, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x6C, 0x31, 0x27, 0x14,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut cursor = Cursor::new(test_buffer);
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(&mut cursor);
        let objects = reader.read_meta_data().unwrap();

        let expected = vec![
            ObjectMetaData {
                path: String::from("/'Group'"),
                properties: vec![
                    (
                        String::from("prop"),
                        PropertyValue::String(String::from("value")),
                    ),
                    (String::from("num"), PropertyValue::I32(10)),
                ],
                raw_data_index: RawDataIndex::None,
            },
            ObjectMetaData {
                path: String::from("/'Group'/'Channel1'"),
                properties: vec![],
                raw_data_index: RawDataIndex::RawData(RawDataMeta {
                    data_type: DataTypeRaw::I32,
                    number_of_values: 2,
                    total_size_bytes: None,
                }),
            },
        ];

        assert_eq!(objects, expected);
    }
}
