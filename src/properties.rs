//! Handle the properties of a channel or group
//!
//! This is based around an enum that can represent all the possible types of property values.
//!

use labview_interop::types::LVTime;

use crate::error::TdmsError;
use crate::io::data_types::{Complex, DataType, ExtendedRaw, TdmsStorageType};
use crate::io::reader::TdmsReader;
use crate::io::writer::TdmsWriter;
use crate::meta_data::TdmsMetaData;
use std::io::{Read, Seek, Write};

/// A wrapper type for data types found in tdms files
#[derive(Debug, Clone, PartialEq)]
pub enum PropertyValue {
    Void,
    Boolean(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    SingleFloat(f32),
    DoubleFloat(f64),
    Extended(ExtendedRaw),
    String(String),
    ComplexSingleFloat(Complex<f32>),
    ComplexDoubleFloat(Complex<f64>),
    Timestamp(LVTime),
}

impl PropertyValue {
    pub const fn datatype(&self) -> DataType {
        match self {
            PropertyValue::Boolean(_) => DataType::Boolean,
            PropertyValue::I32(_) => DataType::I32,
            PropertyValue::U32(_) => DataType::U32,
            PropertyValue::U64(_) => DataType::U64,
            PropertyValue::SingleFloat(_) => DataType::SingleFloat,
            PropertyValue::DoubleFloat(_) => DataType::DoubleFloat,
            PropertyValue::Extended(_) => DataType::ExtendedFloat,
            PropertyValue::String(_) => DataType::TdmsString,
            PropertyValue::I8(_) => DataType::I8,
            PropertyValue::I16(_) => DataType::I16,
            PropertyValue::I64(_) => DataType::I64,
            PropertyValue::U8(_) => DataType::U8,
            PropertyValue::U16(_) => DataType::U16,
            PropertyValue::Timestamp(_) => DataType::Timestamp,
            PropertyValue::Void => DataType::Void,
            PropertyValue::ComplexSingleFloat(_) => DataType::ComplexSingleFloat,
            PropertyValue::ComplexDoubleFloat(_) => DataType::ComplexDoubleFloat,
        }
    }
}

fn write_property_components<W: Write, T: TdmsStorageType>(
    writer: &mut impl TdmsWriter<W>,
    data_type: DataType,
    value: &T,
) -> Result<(), TdmsError> {
    writer.write_meta(&data_type)?;
    writer.write_value(value)?;
    Ok(())
}

impl TdmsMetaData for PropertyValue {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<Self, TdmsError> {
        let raw_type: DataType = reader.read_meta()?;

        match raw_type {
            DataType::Void => Ok(PropertyValue::Void),
            DataType::Boolean => Ok(PropertyValue::Boolean(reader.read_value()?)),
            DataType::I8 => Ok(PropertyValue::I8(reader.read_value()?)),
            DataType::I16 => Ok(PropertyValue::I16(reader.read_value()?)),
            DataType::I32 => Ok(PropertyValue::I32(reader.read_value()?)),
            DataType::I64 => Ok(PropertyValue::I64(reader.read_value()?)),
            DataType::U8 => Ok(PropertyValue::U8(reader.read_value()?)),
            DataType::U16 => Ok(PropertyValue::U16(reader.read_value()?)),
            DataType::U32 => Ok(PropertyValue::U32(reader.read_value()?)),
            DataType::U64 => Ok(PropertyValue::U64(reader.read_value()?)),
            DataType::SingleFloat | DataType::SingleFloatWithUnit => {
                Ok(PropertyValue::SingleFloat(reader.read_value()?))
            }
            DataType::DoubleFloat | DataType::DoubleFloatWithUnit => {
                Ok(PropertyValue::DoubleFloat(reader.read_value()?))
            }
            DataType::ExtendedFloat | DataType::ExtendedFloatWithUnit => {
                Ok(PropertyValue::Extended(reader.read_value()?))
            }
            DataType::ComplexSingleFloat => {
                Ok(PropertyValue::ComplexSingleFloat(reader.read_value()?))
            }
            DataType::ComplexDoubleFloat => {
                Ok(PropertyValue::ComplexDoubleFloat(reader.read_value()?))
            }
            DataType::TdmsString => Ok(PropertyValue::String(reader.read_value()?)),
            DataType::Timestamp => Ok(PropertyValue::Timestamp(reader.read_value()?)),
            _ => Err(TdmsError::UnsupportedType(raw_type)),
        }
    }

    fn write<W: std::io::Write>(
        &self,
        writer: &mut impl crate::io::writer::TdmsWriter<W>,
    ) -> Result<(), TdmsError> {
        match self {
            PropertyValue::Void => {
                writer.write_meta(&self.datatype())?;
                Ok(())
            }
            PropertyValue::Boolean(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::I8(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::I16(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::I32(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::I64(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::U8(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::U16(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::U32(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::U64(value) => write_property_components(writer, self.datatype(), value),
            PropertyValue::SingleFloat(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::DoubleFloat(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::Extended(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::ComplexSingleFloat(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::ComplexDoubleFloat(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::String(value) => {
                write_property_components(writer, self.datatype(), value)
            }
            PropertyValue::Timestamp(value) => {
                write_property_components(writer, self.datatype(), value)
            }
        }
    }

    fn size(&self) -> usize {
        let internal_size = match self {
            PropertyValue::Void => 0,
            PropertyValue::Boolean(value) => value.size(),
            PropertyValue::I32(value) => value.size(),
            PropertyValue::U32(value) => value.size(),
            PropertyValue::U64(value) => value.size(),
            PropertyValue::SingleFloat(value) => value.size(),
            PropertyValue::DoubleFloat(value) => value.size(),
            PropertyValue::Extended(value) => value.size(),
            PropertyValue::ComplexSingleFloat(value) => value.size(),
            PropertyValue::ComplexDoubleFloat(value) => value.size(),
            PropertyValue::String(value) => value.size(),
            PropertyValue::I8(value) => value.size(),
            PropertyValue::I16(value) => value.size(),
            PropertyValue::I64(value) => value.size(),
            PropertyValue::U8(value) => value.size(),
            PropertyValue::U16(value) => value.size(),
            PropertyValue::Timestamp(value) => value.size(),
        };
        internal_size + std::mem::size_of::<u32>()
    }
}

macro_rules! impl_conversion_for_property_value {
    ($type:ty, $variant:ident) => {
        impl From<$type> for PropertyValue {
            fn from(value: $type) -> Self {
                PropertyValue::$variant(value)
            }
        }

        impl TryFrom<PropertyValue> for $type {
            type Error = TdmsError;

            fn try_from(value: PropertyValue) -> Result<Self, Self::Error> {
                match value {
                    PropertyValue::$variant(value) => Ok(value),
                    _ => Err(TdmsError::DataTypeMismatch(
                        value.datatype(),
                        DataType::$variant,
                    )),
                }
            }
        }

        impl<'a> TryFrom<&'a PropertyValue> for $type {
            type Error = TdmsError;

            fn try_from(value: &'a PropertyValue) -> Result<Self, Self::Error> {
                match value {
                    PropertyValue::$variant(value) => Ok(*value),
                    _ => Err(TdmsError::DataTypeMismatch(
                        value.datatype(),
                        DataType::$variant,
                    )),
                }
            }
        }
    };
}

impl_conversion_for_property_value!(i8, I8);
impl_conversion_for_property_value!(i16, I16);
impl_conversion_for_property_value!(i32, I32);
impl_conversion_for_property_value!(i64, I64);
impl_conversion_for_property_value!(u8, U8);
impl_conversion_for_property_value!(u16, U16);
impl_conversion_for_property_value!(u32, U32);
impl_conversion_for_property_value!(u64, U64);
impl_conversion_for_property_value!(f32, SingleFloat);
impl_conversion_for_property_value!(f64, DoubleFloat);
impl_conversion_for_property_value!(bool, Boolean);
impl_conversion_for_property_value!(LVTime, Timestamp);
impl_conversion_for_property_value!(Complex<f32>, ComplexSingleFloat);
impl_conversion_for_property_value!(Complex<f64>, ComplexDoubleFloat);

impl From<String> for PropertyValue {
    fn from(value: String) -> Self {
        PropertyValue::String(value)
    }
}

impl TryFrom<PropertyValue> for String {
    type Error = TdmsError;

    fn try_from(value: PropertyValue) -> Result<Self, Self::Error> {
        match value {
            PropertyValue::String(value) => Ok(value),
            _ => Err(TdmsError::DataTypeMismatch(
                value.datatype(),
                DataType::TdmsString,
            )),
        }
    }
}

impl<'a> From<&'a str> for PropertyValue {
    fn from(value: &'a str) -> Self {
        PropertyValue::String(value.to_string())
    }
}

impl<'a> TryFrom<&'a PropertyValue> for &'a str {
    type Error = TdmsError;

    fn try_from(value: &'a PropertyValue) -> Result<Self, Self::Error> {
        match value {
            PropertyValue::String(value) => Ok(value.as_str()),
            _ => Err(TdmsError::DataTypeMismatch(
                value.datatype(),
                DataType::TdmsString,
            )),
        }
    }
}

#[cfg(feature = "chrono")]
mod chrono_support {
    use super::*;
    use chrono::{DateTime, Utc};

    impl TryFrom<PropertyValue> for DateTime<Utc> {
        type Error = TdmsError;

        fn try_from(value: PropertyValue) -> Result<Self, Self::Error> {
            let lvtime: LVTime = value.try_into()?;
            let chrono_time: DateTime<Utc> = lvtime
                .try_into()
                .map_err(TdmsError::ChronoDateTimeConversionFailed)?;
            Ok(chrono_time)
        }
    }

    impl<'a> TryFrom<&'a PropertyValue> for DateTime<Utc> {
        type Error = TdmsError;

        fn try_from(value: &'a PropertyValue) -> Result<Self, Self::Error> {
            let lvtime: LVTime = value.try_into()?;
            let chrono_time: DateTime<Utc> = lvtime
                .try_into()
                .map_err(TdmsError::ChronoDateTimeConversionFailed)?;
            Ok(chrono_time)
        }
    }

    impl From<DateTime<Utc>> for PropertyValue {
        fn from(value: DateTime<Utc>) -> Self {
            PropertyValue::Timestamp(value.into())
        }
    }

    #[cfg(test)]
    mod tests {
        use chrono::SubsecRound;
        use super::*;

        #[test]
        fn test_datetime_conversion() {
            let mut dt = Utc::now();
            // there is some precision loss so need to round.
            dt = dt.round_subsecs(0);
            let prop_value: PropertyValue = dt.into();
            let prop_value_ref: &PropertyValue = &prop_value;
            let dt_from_prop_value_ref: DateTime<Utc> = prop_value_ref.try_into().unwrap();
            assert_eq!(dt, dt_from_prop_value_ref);
            let dt_from_prop_value: DateTime<Utc> = prop_value.try_into().unwrap();
            assert_eq!(dt, dt_from_prop_value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reader::LittleEndianReader;
    use crate::io::writer::LittleEndianWriter;
    use std::io::Cursor;

    macro_rules! test_property_type {
        ($name:literal, $type:ty, $value:expr, $prop_value:expr) => {
            paste::item! {
                #[allow(non_snake_case)]
                #[test]
                fn [<$name _read_write>]() {
                    let mut buffer = vec![];
                    let mut writer = LittleEndianWriter::from_writer(&mut buffer);
                    $prop_value.write(&mut writer).unwrap();
                    drop(writer);
                    let mut reader = LittleEndianReader::from_reader(Cursor::new(&buffer[..]));
                    let value = PropertyValue::read(&mut reader).unwrap();
                    assert_eq!(value, $prop_value);
                }

                #[allow(non_snake_case)]
                #[test]
                fn [< $name _size >]() {
                    let mut buffer = vec![];
                    let mut writer = LittleEndianWriter::from_writer(&mut buffer);
                    $prop_value.write(&mut writer).unwrap();
                    drop(writer);
                    let size = buffer.len();
                    assert_eq!(size, $prop_value.size());
                }

                #[allow(non_snake_case)]
                #[test]
                fn [< $name _conversion >]() {
                    let value: $type = $value;
                    let prop_value: PropertyValue = value.into();
                    assert_eq!(prop_value, $prop_value);

                    let value: $type = prop_value.try_into().unwrap();
                    assert_eq!(value, $value);
                }

                #[allow(non_snake_case)]
                #[test]
                fn [< $name _conversion_reference >]() {
                    let value: $type = $value;
                    let prop_value: PropertyValue = value.into();
                    let prop_value_ref: &PropertyValue = &prop_value;
                    let value_ref: $type = prop_value_ref.try_into().unwrap();
                    assert_eq!(value_ref, $value);
                }
            }
        };
    }

    test_property_type!("u8", u8, 51, PropertyValue::U8(51));
    test_property_type!("i8", i8, -51, PropertyValue::I8(-51));
    test_property_type!("u16", u16, 51, PropertyValue::U16(51));
    test_property_type!("i16", i16, -51, PropertyValue::I16(-51));
    test_property_type!("u32", u32, 51, PropertyValue::U32(51));
    test_property_type!("i32", i32, -51, PropertyValue::I32(-51));
    test_property_type!("u64", u64, 51, PropertyValue::U64(51));
    test_property_type!("i64", i64, -51, PropertyValue::I64(-51));
    test_property_type!("f32", f32, 51.0, PropertyValue::SingleFloat(51.0));
    test_property_type!("f64", f64, 51.0, PropertyValue::DoubleFloat(51.0));
    test_property_type!("bool", bool, true, PropertyValue::Boolean(true));
    test_property_type!(
        "LVTime",
        LVTime,
        LVTime::from_unix_epoch(100.0),
        PropertyValue::Timestamp(LVTime::from_unix_epoch(100.0))
    );
    test_property_type!(
        "Complexf32",
        Complex<f32>,
        Complex::new(1.0, 2.0),
        PropertyValue::ComplexSingleFloat(Complex::new(1.0, 2.0))
    );
    test_property_type!(
        "Complexf64",
        Complex<f64>,
        Complex::new(1.0, 2.0),
        PropertyValue::ComplexDoubleFloat(Complex::new(1.0, 2.0))
    );

    /// As properties can't directly link to units, united types are loaded
    /// as plain numbers.
    #[test]
    fn test_float_with_units_treated_as_float() {
        let mut buffer = vec![];
        let mut writer = LittleEndianWriter::from_writer(&mut buffer);
        writer.write_meta(&DataType::SingleFloatWithUnit).unwrap();
        writer.write_value(&51.0f32).unwrap();
        drop(writer);

        let mut reader = LittleEndianReader::from_reader(Cursor::new(&buffer[..]));
        let value = PropertyValue::read(&mut reader).unwrap();
        assert_eq!(value, PropertyValue::SingleFloat(51.0));
    }

    /// As properties can't directly link to units, united types are loaded
    /// as plain numbers.
    #[test]
    fn test_double_float_with_units_treated_as_float() {
        let mut buffer = vec![];
        let mut writer = LittleEndianWriter::from_writer(&mut buffer);
        writer.write_meta(&DataType::DoubleFloatWithUnit).unwrap();
        writer.write_value(&51.0).unwrap();
        drop(writer);

        let mut reader = LittleEndianReader::from_reader(Cursor::new(&buffer[..]));
        let value = PropertyValue::read(&mut reader).unwrap();
        assert_eq!(value, PropertyValue::DoubleFloat(51.0));
    }

    /// As properties can't directly link to units, united types are loaded
    /// as plain numbers.
    #[test]
    fn test_extended_float_with_units_treated_as_float() {
        let mut buffer = vec![];
        let mut writer = LittleEndianWriter::from_writer(&mut buffer);

        let prop_value = ExtendedRaw::new(0x0008_000c);
        writer.write_meta(&DataType::ExtendedFloatWithUnit).unwrap();
        writer.write_value(&prop_value).unwrap();
        drop(writer);

        let mut reader = LittleEndianReader::from_reader(Cursor::new(&buffer[..]));
        let value = PropertyValue::read(&mut reader).unwrap();
        assert_eq!(value, PropertyValue::Extended(prop_value));
    }

    #[test]
    fn test_void_read() {
        let mut buffer = vec![];
        let mut writer = LittleEndianWriter::from_writer(&mut buffer);
        writer.write_meta(&DataType::Void).unwrap();
        drop(writer);

        let mut reader = LittleEndianReader::from_reader(Cursor::new(&buffer[..]));
        let value = PropertyValue::read(&mut reader).unwrap();
        assert_eq!(value, PropertyValue::Void);
    }

    #[test]
    fn test_void_roundtrip() {
        let mut buffer = vec![];
        let mut writer = LittleEndianWriter::from_writer(&mut buffer);
        PropertyValue::Void.write(&mut writer).unwrap();
        drop(writer);

        let mut reader = LittleEndianReader::from_reader(Cursor::new(&buffer[..]));
        let value = PropertyValue::read(&mut reader).unwrap();
        assert_eq!(value, PropertyValue::Void);
    }

    #[test]
    fn test_string_conversion() {
        let value = "Hello World".to_string();
        let prop_value: PropertyValue = value.clone().into();
        assert_eq!(prop_value, PropertyValue::String(value));

        let value: String = prop_value.try_into().unwrap();
        assert_eq!(value, "Hello World");
    }

    #[test]
    fn test_str_conversion() {
        let value = "Hello World";
        let prop_value: PropertyValue = value.into();
        assert_eq!(prop_value, PropertyValue::String(value.to_string()));

        let value: &str = (&prop_value).try_into().unwrap();
        assert_eq!(value, "Hello World");
    }
}
