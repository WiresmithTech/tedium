//! This module encodes the types that we will encounter in the files.
//!
//! Credit due to AJAnderson from https://github.com/AJAnderson/tdms/blob/master/tdms/src/tdms_datatypes.rs
//! for providing the basis of some of this.
//!

use num_derive::FromPrimitive;

use crate::error::TdmsError;

/// The DataTypeRaw enum's values match the binary representation of that
/// type in tdms files.
#[derive(Clone, Copy, Debug, FromPrimitive)]
#[repr(u32)]
pub enum DataTypeRaw {
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
/*/
impl DataTypeRaw {
    /// Convert a raw u32 value into a DataTypeRaw enum
    pub fn from_u32(raw_id: u32) -> Result<DataTypeRaw> {
        FromPrimitive::from_u32(raw_id).ok_or(TdmsError::UnknownDataType(raw_id))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TimeStamp {
    pub epoch: i64,
    pub radix: u64,
}

impl fmt::Display for TimeStamp {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "{}\t{}", self.epoch, self.radix)?;

        Ok(())
    }
}
*/

/// A wrapper type for data types found in tdms files
#[derive(Debug, Clone)]
pub enum PropertyValue {
    Void(()),
    Boolean(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Float(f32),
    Double(f64),
    // Extended(f128), // Can't represent this currently
    // FloatUnit(f32), // These don't exist, they're a normal f32 paired with a property
    // DoubleUnit(f64), // as above
    //ExtendedUnit(FloatWithUnit<f128>), // Can't represent this currently
    String(String),
    // DaqMx(??), // I think these don't exist, it's a normal double with properties
    // ComplexSingle(??)
    // CompledDouble(??)
    //TimeStamp(TimeStamp),
}
