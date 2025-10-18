//! This contains the code and structure for some of the fundamental
//! data types common to other components.

use std::io::{Read, Write};

use super::*;

/// Macro for scripting the wrapping of the different read methods.
///
/// Should provide the type which has a from_le_bytes and from_be_bytes
/// Then the natural type for the storage type.
/// and then a slice of supported [`DataType`] values.
macro_rules! numeric_type {
    ($type:ty, $natural:expr, $supported:expr) => {
        impl TdmsStorageType for $type {
            const NATURAL_TYPE: DataType = $natural;
            const SUPPORTED_TYPES: &'static [DataType] = $supported;
            fn read_le(reader: &mut impl Read) -> StorageResult<$type> {
                let mut buf = [0u8; std::mem::size_of::<$type>()];
                reader.read_exact(&mut buf)?;
                Ok(<$type>::from_le_bytes(buf))
            }
            fn read_be(reader: &mut impl Read) -> StorageResult<$type> {
                let mut buf = [0u8; std::mem::size_of::<$type>()];
                reader.read_exact(&mut buf)?;
                Ok(<$type>::from_be_bytes(buf))
            }
            fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
                writer.write_all(&self.to_le_bytes())?;
                Ok(())
            }
            fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
                writer.write_all(&self.to_be_bytes())?;
                Ok(())
            }
            fn size(&self) -> usize {
                Self::SIZE_BYTES
            }
        }
    };
}

numeric_type!(i8, DataType::I8, &[DataType::I8]);
numeric_type!(u8, DataType::U8, &[DataType::U8]);
numeric_type!(i16, DataType::I16, &[DataType::I16]);
numeric_type!(u16, DataType::U16, &[DataType::U16]);
numeric_type!(i32, DataType::I32, &[DataType::I32]);
numeric_type!(u32, DataType::U32, &[DataType::U32]);
numeric_type!(i64, DataType::I64, &[DataType::I64]);
numeric_type!(u64, DataType::U64, &[DataType::U64]);
numeric_type!(
    f64,
    DataType::DoubleFloat,
    &[DataType::DoubleFloat, DataType::DoubleFloatWithUnit]
);
numeric_type!(
    f32,
    DataType::SingleFloat,
    &[DataType::SingleFloat, DataType::SingleFloatWithUnit]
);

#[cfg(test)]
mod tests {
    use crate::io::reader::{BigEndianReader, LittleEndianReader, TdmsReader};
    use crate::io::writer::{BigEndianWriter, LittleEndianWriter, TdmsWriter};
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

                    let mut output_bytes = [0u8; std::mem::size_of::<$type>()];
                    // block to limit writer lifetime.
                    {
                        let mut writer = LittleEndianWriter::from_writer(&mut output_bytes[..]);
                        writer.write_value(&original_value).unwrap();
                    }
                    assert_eq!(bytes, output_bytes);
                }

                #[test]
                fn [< test_ $type _be >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_be_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
                    let read_value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(read_value, original_value);

                    let mut output_bytes = [0u8; std::mem::size_of::<$type>()];
                    //block to limit writer lifetime.
                    {
                    let mut writer = BigEndianWriter::from_writer(&mut output_bytes[..]);
                    writer.write_value(&original_value).unwrap();
                    }
                    assert_eq!(bytes, output_bytes);
                }
            }
        };
    }

    test_formatting!(i8, -123);
    test_formatting!(u8, 123);
    test_formatting!(i16, -1234);
    test_formatting!(u16, 1234);

    test_formatting!(i32, -12345);
    test_formatting!(u32, 12345);
    test_formatting!(i64, -21343543253);
    test_formatting!(u64, 4325465436536);
    test_formatting!(f64, 1234.1245);
    test_formatting!(f32, 1234.1245);
}
