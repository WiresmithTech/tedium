//! This contains the code and structure for some of the fundamental
//! data types common to other components.
//!
use std::io::Read;

use num_derive::FromPrimitive;
/// The DataTypeRaw enum's values match the binary representation of that
/// type in tdms files.
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

type IoError<T> = Result<T, std::io::Error>;

trait TdmsStorageType: Sized {
    const SUPPORTED_TYPES: &'static [DataType];
    fn read_le(reader: &mut impl Read) -> IoError<Self>;
    fn read_be(reader: &mut impl Read) -> IoError<Self>;
    fn supports_data_type(data_type: &DataType) -> bool {
        Self::SUPPORTED_TYPES.contains(&data_type)
    }
}

trait OrderedReader<'r, R: Read> {
    fn from_reader(reader: &'r mut R) -> Self;
    fn read_value<T: TdmsStorageType>(&mut self) -> IoError<T>;
}

struct LittleEndianReader<'r, R: Read>(&'r mut R);

impl<'r, R: Read> OrderedReader<'r, R> for LittleEndianReader<'r, R> {
    fn read_value<T: TdmsStorageType>(&mut self) -> IoError<T> {
        T::read_le(self.0)
    }

    fn from_reader(reader: &'r mut R) -> Self {
        Self(reader)
    }
}

struct BigEndianReader<'r, R>(&'r mut R);

impl<'r, R: Read> OrderedReader<'r, R> for BigEndianReader<'r, R> {
    fn read_value<T: TdmsStorageType>(&mut self) -> IoError<T> {
        T::read_be(self.0)
    }

    fn from_reader(reader: &'r mut R) -> Self {
        Self(reader)
    }
}

/// Macro for scripting the wrapping of the different read methods.
///
/// Should provide the type which has a from_le_bytes and from_be_bytes
/// and then a slice of supported [`DataType`] values.
macro_rules! numeric_type {
    ($type:ty, $supported:expr) => {
        impl TdmsStorageType for $type {
            const SUPPORTED_TYPES: &'static [DataType] = $supported;
            fn read_le(reader: &mut impl Read) -> IoError<$type> {
                let mut buf = [0u8; std::mem::size_of::<$type>()];
                reader.read_exact(&mut buf)?;
                Ok(<$type>::from_le_bytes(buf))
            }
            fn read_be(reader: &mut impl Read) -> IoError<$type> {
                let mut buf = [0u8; std::mem::size_of::<$type>()];
                reader.read_exact(&mut buf)?;
                Ok(<$type>::from_be_bytes(buf))
            }
        }
    };
}

numeric_type!(i32, &[DataType::I32]);
numeric_type!(u32, &[DataType::U32]);
numeric_type!(u64, &[DataType::U64]);
numeric_type!(f64, &[DataType::DoubleFloat, DataType::DoubleFloatWithUnit]);

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
}
