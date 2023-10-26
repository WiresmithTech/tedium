//! Complex numbers are stored as pairs.
//!
//! In this module we define our own complex type but could
//! use the num-complex crate in the future.

use super::*;
use std::io::{Read, Write};

/// A complex number.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Complex<T> {
    pub real: T,
    pub imaginary: T,
}

impl<T> Complex<T> {
    pub const fn new(real: T, imaginary: T) -> Self {
        Self { real, imaginary }
    }
}

impl<T> From<(T, T)> for Complex<T> {
    fn from(value: (T, T)) -> Self {
        Self::new(value.0, value.1)
    }
}

/// Macro for scripting the complex type support.
macro_rules! complex_type {
    ($type: ty, $tdms_type:expr) => {
        impl TdmsStorageType for Complex<$type> {
            const NATURAL_TYPE: DataType = $tdms_type;
            const SUPPORTED_TYPES: &'static [DataType] = &[$tdms_type];
            fn read_le(reader: &mut impl Read) -> StorageResult<Self> {
                let real = <$type>::read_le(reader)?;
                let imaginary = <$type>::read_le(reader)?;
                Ok(Self::new(real, imaginary))
            }
            fn read_be(reader: &mut impl Read) -> StorageResult<Self> {
                let real = <$type>::read_be(reader)?;
                let imaginary = <$type>::read_be(reader)?;
                Ok(Self::new(real, imaginary))
            }
            fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
                self.real.write_le(writer)?;
                self.imaginary.write_le(writer)?;
                Ok(())
            }
            fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
                self.real.write_be(writer)?;
                self.imaginary.write_be(writer)?;
                Ok(())
            }
            fn size(&self) -> usize {
                std::mem::size_of::<$type>() * 2
            }
        }
    };
}

complex_type!(f32, DataType::ComplexSingleFloat);
complex_type!(f64, DataType::ComplexDoubleFloat);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reader::{BigEndianReader, LittleEndianReader, TdmsReader};
    use crate::io::writer::{BigEndianWriter, LittleEndianWriter, TdmsWriter};
    use std::io::Cursor;

    /// Tests the conversion against the le and be version for the value specified.
    macro_rules! test_formatting {
        ($name:literal, $type:ty, $test_value:expr) => {
            paste::item! {
                #[test]
                fn [< test_ $name _le >] () {
                    let original_value: $type = $test_value;
                    let mut bytes = vec![];
                    let mut writer = LittleEndianWriter::from_writer(Cursor::new(&mut bytes));
                    writer.write_value(&original_value).unwrap();
                    drop(writer);

                    let mut reader = Cursor::new(&mut bytes);
                    let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
                    let value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(original_value, value);
                }
                #[test]
                fn [< test_ $name _be >] () {
                    let original_value: $type = $test_value;
                    let mut bytes = vec![];
                    let mut writer = BigEndianWriter::from_writer(Cursor::new(&mut bytes));
                    writer.write_value(&original_value).unwrap();
                    drop(writer);

                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
                    let value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(original_value, value);
                }
            }
        };
    }

    test_formatting!("complex_single", Complex<f32>, Complex::new(1.0, 2.0));
    test_formatting!("complex_double", Complex<f64>, Complex::new(1.0, 2.0));
}
