//! This module encodes the meta data types that we will encounter in the files.
//!
//! Credit due to AJAnderson from https://github.com/AJAnderson/tdms/blob/master/tdms/src/tdms_datatypes.rs
//! for providing the basis of some of this.
//!

use std::io::{Read, Seek, Write};

use num_traits::FromPrimitive;

use crate::error::TdmsError;
use crate::io::data_types::{DataType, TdmsStorageType};
use crate::io::reader::{BigEndianReader, LittleEndianReader, TdmsReader};
use crate::io::writer::TdmsWriter;
use crate::properties::PropertyValue;

///The fixed byte size of the lead in section.
pub const LEAD_IN_BYTES: u64 = 28;

/// Represents data that is endian agnostic.
pub trait TdmsMetaData: Sized {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<Self, TdmsError>;
    // Write the piece of meta-data, returning the total size.
    fn write<W: Write>(&self, writer: &mut impl TdmsWriter<W>) -> Result<(), TdmsError>;
    /// Report the size on disk so we can plan the write.
    fn size(&self) -> usize;
}

impl TdmsMetaData for DataType {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<Self, TdmsError> {
        let prop_type: u32 = reader.read_value()?;
        let prop_type = <DataType as FromPrimitive>::from_u32(prop_type)
            .ok_or(TdmsError::UnknownPropertyType(prop_type))?;
        Ok(prop_type)
    }

    fn write<W: Write>(&self, writer: &mut impl TdmsWriter<W>) -> Result<(), TdmsError> {
        writer.write_value(&(*self as u32))?;
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<u32>()
    }
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

fn set_bits(input: &mut u32, value: bool, bit: u8) {
    if value {
        *input |= 1u32 << bit
    }
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

    pub fn as_bytes(&self) -> [u8; 4] {
        let mut toc: u32 = 0;
        set_bits(&mut toc, self.contains_meta_data, 1);
        set_bits(&mut toc, self.contains_raw_data, 3);
        set_bits(&mut toc, self.contains_daqmx_raw_data, 7);
        set_bits(&mut toc, self.data_is_interleaved, 5);
        set_bits(&mut toc, self.big_endian, 6);
        set_bits(&mut toc, self.contains_new_object_list, 2);
        toc.to_le_bytes()
    }
}

impl TdmsMetaData for ToC {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<Self, TdmsError> {
        let toc_value = <u32 as TdmsStorageType>::read_le(reader.buffered_reader())?;
        Ok(ToC::from_u32(toc_value))
    }

    fn write<W: Write>(&self, writer: &mut impl TdmsWriter<W>) -> Result<(), TdmsError> {
        let bytes = self.as_bytes();
        for byte in &bytes {
            writer.write_value(byte)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        std::mem::size_of::<u32>()
    }
}

/// The metadata section of a segment.
#[derive(Debug, PartialEq, Clone, Default)]
pub struct MetaData {
    pub objects: Vec<ObjectMetaData>,
}

impl TdmsMetaData for MetaData {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<Self, TdmsError> {
        let object_length: u32 = reader.read_value()?;
        let objects = reader.read_vec(object_length as usize)?;
        Ok(MetaData { objects })
    }

    fn write<W: Write>(&self, writer: &mut impl TdmsWriter<W>) -> Result<(), TdmsError> {
        let objects_length: u32 = self.objects.len() as u32;
        writer.write_value(&objects_length)?;

        for object in &self.objects {
            writer.write_meta(object)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = std::mem::size_of::<u32>();
        for object in &self.objects {
            size += object.size();
        }
        size
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
pub struct Segment {
    pub toc: ToC,
    /// The total length of the segment including data but minus the lead in.
    /// Can be used to jump to the next segment in the file.
    /// Can be all 0xFF for last segment of file if it crashes during a write.
    pub next_segment_offset: u64,
    /// The full length of the meta data (exlcuding lead in?)
    pub raw_data_offset: u64,
    pub meta_data: Option<MetaData>,
}

impl Segment {
    pub fn total_size_bytes(&self) -> Result<u64, TdmsError> {
        match self.next_segment_offset.checked_add(LEAD_IN_BYTES) {
            Some(size) => Ok(size),
            None => Err(TdmsError::SegmentAddressOverflow),
        }
    }

    pub fn read(reader: &mut (impl Read + Seek)) -> Result<Segment, TdmsError> {
        let mut tag = [0u8; 4];
        match reader.read_exact(&mut tag) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(TdmsError::EndOfFile);
                } else {
                    return Err(TdmsError::IoError(e));
                }
            }
        }

        if tag != [0x54, 0x44, 0x53, 0x6D] {
            return Err(TdmsError::HeaderPatternNotMatched(tag));
        }

        //ToC is always little endian.
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        let toc = ToC::from_u32(u32::from_le_bytes(buf));

        let segment = match toc.big_endian {
            true => BigEndianReader::from_reader(reader).read_segment(toc)?,
            false => LittleEndianReader::from_reader(reader).read_segment(toc)?,
        };
        Ok(segment)
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

impl TdmsMetaData for ObjectMetaData {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<ObjectMetaData, TdmsError> {
        let path: String = reader.read_value()?;

        let raw_data: RawDataIndex = reader.read_meta()?;

        let property_count: u32 = reader.read_value()?;

        let mut properties = Vec::new();
        properties.try_reserve(property_count as usize).map_err(|_| TdmsError::PropertyTableAllocationFailed)?;

        for _prop in 0..property_count {
            let name: String = reader.read_value()?;
            let value: PropertyValue = reader.read_meta()?;
            properties.push((name, value));
        }

        Ok(ObjectMetaData {
            path,
            properties,
            raw_data_index: raw_data,
        })
    }

    fn write<W: Write>(&self, writer: &mut impl TdmsWriter<W>) -> Result<(), TdmsError> {
        writer.write_value(&self.path)?;
        writer.write_meta(&self.raw_data_index)?;
        writer.write_value(&(self.properties.len() as u32))?;

        for (prop_name, prop_value) in &self.properties {
            writer.write_value(prop_name)?;
            writer.write_meta(prop_value)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        let mut size = self.path.size();
        size += self.raw_data_index.size();
        size += std::mem::size_of::<u32>();
        for (prop_name, prop_value) in &self.properties {
            size += prop_name.size();
            size += prop_value.size();
        }
        size
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum RawDataIndex {
    None,
    MatchPrevious,
    RawData(RawDataMeta),
}

impl TdmsMetaData for RawDataIndex {
    fn read<R: Read + Seek>(reader: &mut impl TdmsReader<R>) -> Result<RawDataIndex, TdmsError> {
        let raw_index: u32 = reader.read_value()?;

        let raw_data = match raw_index {
            0x0000_0000 => RawDataIndex::MatchPrevious,
            0xFFFF_FFFF => RawDataIndex::None,
            0x69120000..=0x6912FFFF => todo!(), // daqmx 1
            0x69130000..=0x6913FFFF => todo!(), //daqmx 2
            _ => {
                let data_type: DataType = reader.read_meta()?;
                let _array_dims: u32 = reader.read_value()?; //always 1.
                let number_of_values: u64 = reader.read_value()?;
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

    fn write<W: Write>(&self, writer: &mut impl TdmsWriter<W>) -> Result<(), TdmsError> {
        match self {
            RawDataIndex::None => writer.write_value(&0xFFFF_FFFFu32)?,
            RawDataIndex::MatchPrevious => writer.write_value(&0u32)?,
            RawDataIndex::RawData(raw_meta) => {
                //size: until we add string support it is 20 bytes.
                writer.write_value(&20u32)?;
                writer.write_meta(&raw_meta.data_type)?;
                //array dim is alway 1 in TDMS v2.0.
                writer.write_value(&1u32)?;
                writer.write_value(&raw_meta.number_of_values)?
            }
        }
        Ok(())
    }

    fn size(&self) -> usize {
        match self {
            RawDataIndex::None => std::mem::size_of::<u32>(),
            RawDataIndex::MatchPrevious => std::mem::size_of::<u32>(),
            RawDataIndex::RawData(_raw_meta) => {
                3 * std::mem::size_of::<u32>() + std::mem::size_of::<u64>()
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct RawDataMeta {
    pub data_type: DataType,
    pub number_of_values: u64,
    /// Only if strings
    pub total_size_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use crate::io::writer::LittleEndianWriter;

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
    fn test_toc_example_to_bytes() {
        let toc_int = 0x0Eu32;
        let toc = ToC::from_u32(toc_int);
        println!("{toc:?}");

        //Value as little endian.
        assert_eq!(toc.as_bytes(), [0xE, 0, 0, 0]);
    }

    #[test]
    fn test_segment_size_calc() {
        let segment = Segment {
            next_segment_offset: 500,
            raw_data_offset: 20,
            ..Default::default()
        };

        assert!(matches!(segment.total_size_bytes(), Ok(528)));
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
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let result: Result<PropertyValue, TdmsError> = reader.read_meta();
        println!("{result:?}");
        assert!(matches!(result, Err(TdmsError::UnknownPropertyType(0x23))));
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
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
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
                    data_type: DataType::I32,
                    number_of_values: 2,
                    total_size_bytes: None,
                }),
            },
        ];

        assert_eq!(objects, expected);
    }
    #[test]
    fn test_properties_count_overflow_error() {
        //example from NI "TDMS internal file format"
        let test_buffer = [
            0x02, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x04, 0x00, 0x00, 0x00,
            0x70, 0x72, 0x6F, 0x70, 0x20, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x76, 0x61,
            0x6C, 0x75, 0x65, 0x03, 0x00, 0x00, 0x00, 0x6E, 0x75, 0x6D, 0x03, 0x00, 0x00, 0x00,
            0x0A, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0x2F, 0x27, 0x43, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x6C, 0x31, 0x27, 0x14,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut cursor = Cursor::new(test_buffer);
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let object_count: u32 = reader.read_value().unwrap();
        let objects: Result<Vec<ObjectMetaData>,_> = reader.read_vec(object_count as usize);
        assert!(matches!(objects, Err(TdmsError::PropertyTableAllocationFailed)));
    }

    #[test]
    fn test_metadata_errors_on_allocation_error() {
        //example from NI "TDMS internal file format" modified with wrong length.
        let test_buffer = [
            0xFF, 0xFF, 0xFF, 0xFF, 0x08, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
            0x70, 0x72, 0x6F, 0x70, 0x20, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x76, 0x61,
            0x6C, 0x75, 0x65, 0x03, 0x00, 0x00, 0x00, 0x6E, 0x75, 0x6D, 0x03, 0x00, 0x00, 0x00,
            0x0A, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0x2F, 0x27, 0x43, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x6C, 0x31, 0x27, 0x14,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let mut cursor = Cursor::new(test_buffer);
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let object_count: u32 = reader.read_value().unwrap();
        let objects_result: Result<Vec<ObjectMetaData>, _> = reader.read_vec(object_count as usize);
        assert!(matches!(objects_result, Err(TdmsError::VecAllocationFailed)));
    }
    /// Will write the value to an array and return it for comparison.
    ///
    /// The second returned value is the reported written size.
    fn write_meta_to_buffer<T: TdmsMetaData>(value: T, expected_size: usize) -> Vec<u8> {
        let mut output_buffer = vec![0u8; expected_size];
        {
            let mut cursor = Cursor::new(&mut output_buffer);
            let mut writer = LittleEndianWriter::from_writer(&mut cursor);
            writer.write_meta(&value).unwrap();
            assert_eq!(value.size(), expected_size);
        }
        output_buffer
    }

    #[test]
    fn test_properties_standard_data_write() {
        //example from NI "TDMS internal file format"
        let expected_buffer = [
            0x02, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
            0x70, 0x72, 0x6F, 0x70, 0x20, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x76, 0x61,
            0x6C, 0x75, 0x65, 0x03, 0x00, 0x00, 0x00, 0x6E, 0x75, 0x6D, 0x03, 0x00, 0x00, 0x00,
            0x0A, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0x2F, 0x27, 0x43, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x6C, 0x31, 0x27, 0x14,
            0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let meta = MetaData {
            objects: vec![
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
                        data_type: DataType::I32,
                        number_of_values: 2,
                        total_size_bytes: None,
                    }),
                },
            ],
        };

        let output = write_meta_to_buffer(meta, expected_buffer.len());
        assert_eq!(output, expected_buffer);
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
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let length: u32 = reader.read_value().unwrap();
        println!("length: {length}");
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

    #[test]
    fn test_properties_raw_data_matches_write() {
        //example from NI "TDMS internal file format"
        let expected_buffer = [
            0x02, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0xFF, 0xFF, 0xFF, 0xFF, 0x02, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
            0x70, 0x72, 0x6F, 0x70, 0x20, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x00, 0x76, 0x61,
            0x6C, 0x75, 0x65, 0x03, 0x00, 0x00, 0x00, 0x6E, 0x75, 0x6D, 0x03, 0x00, 0x00, 0x00,
            0x0A, 0x00, 0x00, 0x00, 0x13, 0x00, 0x00, 0x00, 0x2F, 0x27, 0x47, 0x72, 0x6F, 0x75,
            0x70, 0x27, 0x2F, 0x27, 0x43, 0x68, 0x61, 0x6E, 0x6E, 0x65, 0x6C, 0x31, 0x27, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];

        let meta = MetaData {
            objects: vec![
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
            ],
        };

        let output = write_meta_to_buffer(meta, expected_buffer.len());
        assert_eq!(output, expected_buffer);
    }
}
