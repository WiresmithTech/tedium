//! The datatypes module holds the [`DataType`] enum and the [`TdmsStorageType`] trait
//! and their implementations.
//!
//! This covers how to encode and decode individual values from the TDMS file.
//!
//! There are broadly three types that we need to support:
//!
//! * Native types - types that exist in Rust and can be read directly from the file.
//! * Raw types - types that are not supported by Rust but are supported by the TDMS file.
//!   These may not be directly useful but allow files with this data to be loaded.
//! * Custom types - Useful types that are not supported by Rust. This may be feature flagged
//!   to control additional dependencies required to support them.

mod bool;
mod complex;
mod extended;
mod native_numerics;
mod strings;
mod timestamp;

use std::fmt::Display;
use std::io::{Read, Write};

use num_derive::FromPrimitive;

use crate::error::TdmsError;

// Re-exports.
pub use bool::*;
pub use complex::*;
pub use extended::*;
pub use native_numerics::*;
pub use timestamp::*;

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
    Timestamp = 0x44,
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
            DataType::Timestamp => 8,
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
            DataType::Timestamp => write!(f, "TimeStamp"),
            DataType::FixedPoint => write!(f, "FixedPoint"),
            DataType::ComplexSingleFloat => write!(f, "ComplexSingleFloat"),
            DataType::ComplexDoubleFloat => write!(f, "ComplexDoubleFloat"),
            DataType::DAQmxRawData => write!(f, "DAQmxRawData"),
        }
    }
}

type StorageResult<T> = std::result::Result<T, TdmsError>;

pub trait TdmsStorageType: Sized + 'static {
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
