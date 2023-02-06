//! This module encodes the meta data types that we will encounter in the files.
//!
//! Credit due to AJAnderson from https://github.com/AJAnderson/tdms/blob/master/tdms/src/tdms_datatypes.rs
//! for providing the basis of some of this.
//!

use std::io::{Read, Seek};

use byteorder::{BigEndian, ByteOrder, LittleEndian, ReadBytesExt};
use num_derive::FromPrimitive;

use crate::data_reader::{TdmsDataReader, TdmsReader, TdmsReaderError};

///The fixed byte size of the lead in section.
pub const LEAD_IN_BYTES: u64 = 28;

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
#[derive(Debug, PartialEq, Eq, Clone, Copy, Default)]
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

impl<'r, O: byteorder::ByteOrder, R: ReadBytesExt> TdmsDataReader<O, ToC> for TdmsReader<'r, O, R> {
    fn read_value(&mut self) -> Result<ToC, TdmsReaderError> {
        //ToC is always
        let toc_value = self.inner.read_u32::<LittleEndian>()?;
        Ok(ToC::from_u32(toc_value))
    }
}

/// Contains the data from the TDMS segment header.
///
/// The offsets can be used to jump around the three elements that could be in the segment.
///
/// |----------------------------------------------------
/// | lead in: 28 bytes
/// |----------------------------------------------------
/// | metadata: size = raw_data_offset |
/// |--------------------------------- | next segment offset
/// | raw data                         |
/// |--------------------------------- |-----------------
#[derive(Debug, PartialEq, Clone, Default)]
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

impl SegmentMetaData {
    pub fn total_size_bytes(&self) -> u64 {
        LEAD_IN_BYTES + self.next_segment_offset
    }

    pub fn read(
        reader: &mut (impl ReadBytesExt + Seek),
    ) -> Result<SegmentMetaData, TdmsReaderError> {
        let mut tag = [0u8; 4];
        reader.read_exact(&mut tag)?;

        if tag != [0x54, 0x44, 0x53, 0x6D] {
            return Err(TdmsReaderError::HeaderPatternNotMatched(tag));
        }

        let toc = ToC::from_u32(reader.read_u32::<LittleEndian>()?);

        let segment = match toc.big_endian {
            true => TdmsReader::<BigEndian, _>::from_reader(reader).read_segment(toc)?,
            false => TdmsReader::<LittleEndian, _>::from_reader(reader).read_segment(toc)?,
        };
        Ok(segment)
    }
}

//Add to reader as custom function as prototype doesn't match generic value
impl<'r, O: ByteOrder, R: Read> TdmsReader<'r, O, R> {
    /// Called immediately after ToC has been read so we have determined the endianess.
    pub fn read_segment(&mut self, toc: ToC) -> Result<SegmentMetaData, TdmsReaderError> {
        let _version: u32 = self.read_value()?;
        let next_segment_offset = self.read_value()?;
        let raw_data_offset = self.read_value()?;

        //todo handle no meta data mode.
        let object_length: u32 = self.read_value()?;
        let objects = self.read_vec(object_length as usize)?;

        Ok(SegmentMetaData {
            toc: toc,
            next_segment_offset,
            raw_data_offset,
            objects,
        })
    }
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

impl<'r, O: ByteOrder, R: Read> TdmsDataReader<O, ObjectMetaData> for TdmsReader<'r, O, R> {
    fn read_value(&mut self) -> Result<ObjectMetaData, TdmsReaderError> {
        let path: String = self.read_value()?;

        let raw_data: RawDataIndex = self.read_value()?;

        let property_count: u32 = self.read_value()?;

        let mut properties = Vec::with_capacity(property_count as usize);

        for _prop in 0..property_count {
            let name: String = self.read_value()?;
            let value: PropertyValue = self.read_value()?;
            properties.push((name, value));
        }

        Ok(ObjectMetaData {
            path,
            properties,
            raw_data_index: raw_data,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RawDataIndex {
    None,
    MatchPrevious,
    RawData(RawDataMeta),
}

impl<'r, O: ByteOrder, R: Read> TdmsDataReader<O, RawDataIndex> for TdmsReader<'r, O, R> {
    fn read_value(&mut self) -> Result<RawDataIndex, TdmsReaderError> {
        let raw_index: u32 = self.read_value()?;

        let raw_data = match raw_index {
            0x0000_0000 => RawDataIndex::MatchPrevious,
            0xFFFF_FFFF => RawDataIndex::None,
            0x69120000..=0x6912FFFF => todo!(), // daqmx 1
            0x69130000..=0x6913FFFF => todo!(), //daqmx 2
            _ => {
                let data_type: DataTypeRaw = self.read_value()?;
                let _array_dims: u32 = self.read_value()?; //always 1.
                let number_of_values: u64 = self.read_value()?;
                let meta = RawDataMeta {
                    data_type,
                    number_of_values,
                    total_size_bytes: None,
                };
                RawDataIndex::RawData(meta)
            }
        };

        Ok(raw_data)
    }
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
    use std::io::Cursor;

    use super::*;

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

    #[test]
    fn test_segment_size_calc() {
        let segment = SegmentMetaData {
            next_segment_offset: 500,
            raw_data_offset: 20,
            ..Default::default()
        };

        assert_eq!(segment.total_size_bytes(), 528);
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
        let object_count: u32 = reader.read_value().unwrap();
        let objects: Vec<ObjectMetaData> = reader.read_vec(object_count as usize).unwrap();

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

    #[test]
    fn test_properties_raw_data_matches() {
        //example from NI "TDMS internal file format"
        let test_buffer = [
            0x02, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
            0x70, 0x72, 0x6F, 0x70, 0x20, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x76, 0x61,
            0x6C, 0x75, 0x65, 0x03, 0x00, 0x00, 0x00, 0x6E, 0x75, 0x6D, 0x03, 0x00, 0x00, 0x00,
            0x0A, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0x2F, 0x27, 0x43, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x6C, 0x31, 0x27, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut cursor = Cursor::new(test_buffer);
        let mut reader = TdmsReader::<LittleEndian, _>::from_reader(&mut cursor);
        let length: u32 = reader.read_value().unwrap();
        let objects: Vec<ObjectMetaData> = reader.read_vec(length as usize).unwrap();

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
                raw_data_index: RawDataIndex::MatchPrevious,
            },
        ];

        assert_eq!(objects, expected);
    }
}
