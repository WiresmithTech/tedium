//! This contains the code and structure for some of the fundamental
//! data types common to other components.

use std::{
    fmt::Display,
    io::{Read, Write},
};

use num_derive::FromPrimitive;

use crate::error::TdmsError;

/// The data types that can be encoded into TDMS data.
///
/// The values are the codes used in the TDMS file.
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq, Eq)]
#[repr(u32)]
pub enum DataType {
    Void = 0,
    I8 = 1,
    I16 = 2,
    I32 = 3,
    I64 = 4,
    U8 = 5,
    U16 = 6,
    U32 = 7,
    U64 = 8,
    SingleFloat = 9,
    DoubleFloat = 10,
    ExtendedFloat = 11,
    SingleFloatWithUnit = 0x19,
    DoubleFloatWithUnit = 12,
    ExtendedFloatWithUnit = 13,
    TdmsString = 0x20,
    Boolean = 0x21,
    TimeStamp = 0x44,
    FixedPoint = 0x4F,
    ComplexSingleFloat = 0x0008_000c,
    ComplexDoubleFloat = 0x0010_000d,
    DAQmxRawData = 0xFFFF_FFFF,
}

//todo: validate these.
impl DataType {
    pub fn size(&self) -> u8 {
        match self {
            DataType::Void => 0,
            DataType::I8 | DataType::U8 => 1,
            DataType::I16 | DataType::U16 => 2,
            DataType::I32 | DataType::U32 => 4,
            DataType::I64 | DataType::U64 => 8,
            DataType::SingleFloat => 4,
            DataType::DoubleFloat => 8,
            DataType::ExtendedFloat => 16,
            DataType::SingleFloatWithUnit => 4,
            DataType::DoubleFloatWithUnit => 8,
            DataType::ExtendedFloatWithUnit => 16,
            DataType::TdmsString => 0,
            DataType::Boolean => 1,
            DataType::TimeStamp => 8,
            DataType::FixedPoint => 8,
            DataType::ComplexSingleFloat => 8,
            DataType::ComplexDoubleFloat => 16,
            DataType::DAQmxRawData => 4,
        }
    }
}

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::Void => write!(f, "Void"),
            DataType::I8 => write!(f, "I8"),
            DataType::I16 => write!(f, "I16"),
            DataType::I32 => write!(f, "I32"),
            DataType::I64 => write!(f, "I64"),
            DataType::U8 => write!(f, "U8"),
            DataType::U16 => write!(f, "U16"),
            DataType::U32 => write!(f, "U32"),
            DataType::U64 => write!(f, "U64"),
            DataType::SingleFloat => write!(f, "SingleFloat"),
            DataType::DoubleFloat => write!(f, "DoubleFloat"),
            DataType::ExtendedFloat => write!(f, "ExtendedFloat"),
            DataType::SingleFloatWithUnit => write!(f, "SingleFloatWithUnit"),
            DataType::DoubleFloatWithUnit => write!(f, "DoubleFloatWithUnit"),
            DataType::ExtendedFloatWithUnit => write!(f, "ExtendedFloatWithUnit"),
            DataType::TdmsString => write!(f, "TdmsString"),
            DataType::Boolean => write!(f, "Boolean"),
            DataType::TimeStamp => write!(f, "TimeStamp"),
            DataType::FixedPoint => write!(f, "FixedPoint"),
            DataType::ComplexSingleFloat => write!(f, "ComplexSingleFloat"),
            DataType::ComplexDoubleFloat => write!(f, "ComplexDoubleFloat"),
            DataType::DAQmxRawData => write!(f, "DAQmxRawData"),
        }
    }
}

type StorageResult<T> = std::result::Result<T, TdmsError>;

pub trait TdmsStorageType: Sized {
    /// The [`DataType`] that can be read as this storage type.
    const SUPPORTED_TYPES: &'static [DataType];
    /// The [`DataType`] that this storage type is naturally written as.
    const NATURAL_TYPE: DataType;
    /// Size in bytes of the type.
    const SIZE_BYTES: usize = std::mem::size_of::<Self>();
    fn read_le(reader: &mut impl Read) -> StorageResult<Self>;
    fn read_be(reader: &mut impl Read) -> StorageResult<Self>;
    /// Write the value as little endian.
    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()>;
    /// Write the value as big endian.
    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()>;
    /// Report the size of the type to allow for planning of writes.
    fn size(&self) -> usize;

    fn supports_data_type(data_type: &DataType) -> bool {
        Self::SUPPORTED_TYPES.contains(data_type)
    }
}

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

fn read_string_with_length(reader: &mut impl Read, length: u32) -> Result<String, TdmsError> {
    let mut buffer = vec![0; length as usize];
    reader.read_exact(&mut buffer[..])?;
    let value = String::from_utf8(buffer)?;
    Ok(value)
}

impl TdmsStorageType for String {
    const SUPPORTED_TYPES: &'static [DataType] = &[DataType::TdmsString];

    fn read_le(reader: &mut impl Read) -> Result<Self, TdmsError> {
        let length = u32::read_le(reader)?;
        read_string_with_length(reader, length)
    }

    fn read_be(reader: &mut impl Read) -> Result<Self, TdmsError> {
        let length = u32::read_be(reader)?;
        read_string_with_length(reader, length)
    }

    const NATURAL_TYPE: DataType = DataType::TdmsString;

    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&(self.len() as u32).to_le_bytes())?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }

    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&(self.len() as u32).to_be_bytes())?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.len() + std::mem::size_of::<u32>()
    }
}

impl TdmsStorageType for bool {
    const SUPPORTED_TYPES: &'static [DataType] = &[DataType::Boolean];

    const NATURAL_TYPE: DataType = DataType::Boolean;

    fn read_le(reader: &mut impl Read) -> StorageResult<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0] != 0)
    }

    fn read_be(reader: &mut impl Read) -> StorageResult<Self> {
        // no endianess for bool.
        Self::read_le(reader)
    }

    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&[*self as u8])?;
        Ok(())
    }

    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
        // no endianess for bool
        Self::write_le(self, writer)
    }

    fn size(&self) -> usize {
        1
    }
}

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

    #[test]
    fn test_bool_le() {
        let original_value = true;
        let bytes = [1u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        // block to limit writer lifetime.
        {
            let mut writer = LittleEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_bool_be() {
        let original_value = true;
        let bytes = [1u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        //block to limit writer lifetime.
        {
            let mut writer = BigEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_bool_le_false() {
        let original_value = false;
        let bytes = [0u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        // block to limit writer lifetime.
        {
            let mut writer = LittleEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_bool_be_false() {
        let original_value = false;
        let bytes = [0u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        //block to limit writer lifetime.
        {
            let mut writer = BigEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }
}
