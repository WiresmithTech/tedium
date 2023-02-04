//! This module encodes the types that we will encounter in the files.
//!
//! Credit due to AJAnderson from https://github.com/AJAnderson/tdms/blob/master/tdms/src/tdms_datatypes.rs
//! for providing the basis of some of this.
//!

use num_derive::FromPrimitive;

/// The DataTypeRaw enum's values match the binary representation of that
/// type in tdms files.
#[derive(Clone, Copy, Debug, FromPrimitive, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq)]
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

/// An extracted form of a segment table of contents.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ToC {
    pub contains_meta_data: bool,
    pub contains_raw_data: bool,
    pub contains_daqmx_raw_data: bool,
    pub data_is_interleaved: bool,
    pub big_endian: bool,
    pub contains_new_object_list: bool,
}

fn mask_bit_set(value: u32, bit: u8) -> bool {
    let mask = 1u32 << bit;
    let masked = value & mask;
    masked != 0
}

impl ToC {
    pub fn from_u32(value: u32) -> Self {
        ToC {
            contains_meta_data: mask_bit_set(value, 1),
            contains_raw_data: mask_bit_set(value, 3),
            contains_daqmx_raw_data: mask_bit_set(value, 7),
            data_is_interleaved: mask_bit_set(value, 5),
            big_endian: mask_bit_set(value, 6),
            contains_new_object_list: mask_bit_set(value, 2),
        }
    }
}

/// Contains the data from the TDMS segment header.
#[derive(Debug, PartialEq, Clone)]
pub struct SegmentMetaData {
    pub toc: ToC,
    /// The total length of the segment including data but minus the lead in.
    /// Can be used to jump to the next segment in the file.
    /// Can be all 0xFF for last segment of file if it crashes during a write.
    pub next_segment_offset: u64,
    /// The full length of the meta data (exlcuding lead in?)
    pub raw_data_offset: u64,
    pub objects: Vec<ObjectMetaData>,
}

/// Contains all data from an object entry in a segment header.
#[derive(Debug, PartialEq, Clone)]
pub struct ObjectMetaData {
    pub path: String,
    pub properties: Vec<(String, PropertyValue)>,
    //now some data
    //unclear if this may be present for daqmx
    pub raw_data_index: RawDataIndex,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RawDataIndex {
    None,
    MatchPrevious,
    RawData(RawDataMeta),
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RawDataMeta {
    pub data_type: DataTypeRaw,
    pub number_of_values: u64,
    /// Only if strings
    pub total_size_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::ToC;

    #[test]
    fn test_toc_example_from_ni() {
        let toc_int = 0x0Eu32;
        let toc = ToC::from_u32(toc_int);
        println!("{toc:?}");

        assert_eq!(toc.contains_meta_data, true);
        assert_eq!(toc.contains_raw_data, true);
        assert_eq!(toc.contains_daqmx_raw_data, false);
        assert_eq!(toc.data_is_interleaved, false);
        assert_eq!(toc.big_endian, false);
        assert_eq!(toc.contains_new_object_list, true);
    }
}
