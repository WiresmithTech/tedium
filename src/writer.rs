//! Contains wrappers for the standard writers to support
//! the TDMS use case of variable bitness.
//!

use std::io::{BufWriter, Write};

use crate::data_types::TdmsStorageType;
use crate::error::TdmsError;
use crate::meta_data::{MetaData, Segment, TdmsMetaData, ToC};
use crate::raw_data::WriteBlock;

type Result<T> = std::result::Result<T, TdmsError>;

pub trait TdmsWriter<W: Write>: Sized {
    /// Marker to place in the big_endian part of the ToC.
    const BIG_ENDIAN_FLAG: bool;
    fn from_writer(writer: W) -> Self;
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<()>;
    fn write_meta<T: TdmsMetaData>(&mut self, value: &T) -> Result<()> {
        value.write(self)
    }

    /// Writes a segment based on the data provided. Returns the read format
    /// for the segment for indexing.
    fn write_segment(
        &mut self,
        mut toc: ToC,
        meta: Option<MetaData>,
        data: Option<impl WriteBlock>,
    ) -> Result<Segment> {
        toc.big_endian = Self::BIG_ENDIAN_FLAG;

        //write the meta.
        let meta_data_bytes = if let Some(meta_data) = &meta {
            toc.contains_meta_data = true;
            meta_data.size()
        } else {
            toc.contains_meta_data = false;
            0
        };

        let data_bytes = if let Some(data) = &data {
            toc.contains_raw_data = true;
            data.size()
        } else {
            toc.contains_raw_data = false;
            0
        };
        let next_segment_offset = (meta_data_bytes + data_bytes) as u64;
        let raw_data_offset = meta_data_bytes as u64;

        for char in "TDSm".as_bytes().iter() {
            self.write_value(char)?;
        }
        self.write_meta(&toc)?;
        //Write version.
        self.write_value(&4713u32)?;
        //Write segment offset.
        self.write_value(&next_segment_offset)?;
        //Write data offset.
        self.write_value(&raw_data_offset)?;

        if let Some(meta_data) = &meta {
            self.write_meta(meta_data)?
        };

        if let Some(data_block) = data {
            data_block.write(self)?;
        }

        Ok(Segment {
            toc,
            next_segment_offset,
            raw_data_offset,
            meta_data: meta,
        })
    }

    fn sync(&mut self) -> Result<()>;
}

pub struct LittleEndianWriter<W: Write>(BufWriter<W>);

impl<W: Write> TdmsWriter<W> for LittleEndianWriter<W> {
    fn from_writer(writer: W) -> Self {
        Self(BufWriter::new(writer))
    }
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<()> {
        value.write_le(&mut self.0)
    }

    const BIG_ENDIAN_FLAG: bool = false;

    fn sync(&mut self) -> Result<()> {
        self.0.flush()?;
        Ok(())
    }
}

pub struct BigEndianWriter<W: Write>(BufWriter<W>);

impl<W: Write> TdmsWriter<W> for BigEndianWriter<W> {
    fn from_writer(writer: W) -> Self {
        Self(BufWriter::new(writer))
    }
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<()> {
        value.write_be(&mut self.0)
    }

    const BIG_ENDIAN_FLAG: bool = true;

    fn sync(&mut self) -> Result<()> {
        self.0.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::meta_data::{MetaData, ObjectMetaData, PropertyValue, RawDataIndex};

    use super::*;
    use std::{io::Cursor, mem::size_of};

    #[test]
    fn test_string() {
        //example from NI site
        let expected_buffer = [
            0x23, 00, 00, 00, 0x2Fu8, 0x27, 0x4D, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x64, 0x20,
            0x54, 0x68, 0x72, 0x6F, 0x75, 0x67, 0x68, 0x70, 0x75, 0x74, 0x20, 0x44, 0x61, 0x74,
            0x61, 0x20, 0x28, 0x56, 0x6F, 0x6C, 0x74, 0x73, 0x29, 0x27,
        ];

        let mut output_buffer = vec![0u8; 39];

        {
            let mut cursor = Cursor::new(&mut output_buffer);
            let mut writer = LittleEndianWriter::from_writer(&mut cursor);
            let value = String::from("/'Measured Throughput Data (Volts)'");
            writer.write_value(&value).unwrap();
            assert_eq!(value.size(), expected_buffer.len());
        }

        assert_eq!(output_buffer, expected_buffer);
    }

    #[test]
    fn test_write_segment() {
        //just going to use some arbitrary and tested data.
        let mut buffer = vec![0u8; 1024];

        let toc = ToC::default();

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

        let data = vec![0f64; 10];

        let meta_size = meta.size();
        let data_size = data.len() * size_of::<f64>();
        let segment;

        {
            let mut writer = LittleEndianWriter::from_writer(Cursor::new(&mut buffer));
            segment = writer
                .write_segment(toc, Some(meta), Some(&data[..]))
                .unwrap();
        }

        //check header
        assert_eq!(&buffer[0..4], "TDSm".as_bytes());

        //check toc has data and meta bits set.
        assert_eq!(segment.toc.contains_meta_data, true);
        assert_eq!(segment.toc.contains_raw_data, true);
        let mut toc_buf = [0; 4];
        toc_buf.copy_from_slice(&buffer[4..8]);
        let read_back_toc = ToC::from_u32(u32::from_le_bytes(toc_buf));
        assert_eq!(segment.toc, read_back_toc);

        //check version.
        assert_eq!(&buffer[8..12], &4713u32.to_le_bytes());

        //next segment
        let mut size_buf = [0u8; 8];
        size_buf.copy_from_slice(&buffer[12..20]);
        assert_eq!(meta_size + data_size, u64::from_le_bytes(size_buf) as usize);
        assert_eq!(meta_size + data_size, segment.next_segment_offset as usize);

        // data offset.
        let mut size_buf = [0u8; 8];
        size_buf.copy_from_slice(&buffer[20..28]);
        assert_eq!(meta_size, u64::from_le_bytes(size_buf) as usize);
        assert_eq!(meta_size, segment.raw_data_offset as usize);
    }

    #[test]
    fn test_write_segment_data_only() {
        //just going to use some arbitrary and tested data.
        let mut buffer = vec![0u8; 1024];

        let toc = ToC::default();

        let data = vec![0f64; 10];

        let meta_size = 0;
        let data_size = data.len() * size_of::<f64>();
        let segment;

        {
            let mut writer = LittleEndianWriter::from_writer(Cursor::new(&mut buffer));
            segment = writer.write_segment(toc, None, Some(&data[..])).unwrap();
        }

        //check header
        assert_eq!(&buffer[0..4], "TDSm".as_bytes());

        //check toc has data and meta bits set.
        assert_eq!(segment.toc.contains_meta_data, false);
        assert_eq!(segment.toc.contains_raw_data, true);
        let mut toc_buf = [0; 4];
        toc_buf.copy_from_slice(&buffer[4..8]);
        let read_back_toc = ToC::from_u32(u32::from_le_bytes(toc_buf));
        assert_eq!(segment.toc, read_back_toc);

        //check version.
        assert_eq!(&buffer[8..12], &4713u32.to_le_bytes());

        //next segment
        let mut size_buf = [0u8; 8];
        size_buf.copy_from_slice(&buffer[12..20]);
        assert_eq!(meta_size + data_size, u64::from_le_bytes(size_buf) as usize);
        assert_eq!(meta_size + data_size, segment.next_segment_offset as usize);

        // data offset.
        let mut size_buf = [0u8; 8];
        size_buf.copy_from_slice(&buffer[20..28]);
        assert_eq!(meta_size, u64::from_le_bytes(size_buf) as usize);
        assert_eq!(meta_size, segment.raw_data_offset as usize);
    }

    #[test]
    fn test_write_segment_meta_only() {
        //just going to use some arbitrary and tested data.
        let mut buffer = vec![0u8; 1024];

        let toc = ToC::default();

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

        let meta_size = meta.size();
        let data_size = 0;
        let segment;

        {
            let mut writer = LittleEndianWriter::from_writer(Cursor::new(&mut buffer));
            segment = writer
                .write_segment(toc, Some(meta), None::<&[f64]>)
                .unwrap();
        }

        //check header
        assert_eq!(&buffer[0..4], "TDSm".as_bytes());

        //check toc has data and meta bits set.
        assert_eq!(segment.toc.contains_meta_data, true);
        assert_eq!(segment.toc.contains_raw_data, false);
        let mut toc_buf = [0; 4];
        toc_buf.copy_from_slice(&buffer[4..8]);
        let read_back_toc = ToC::from_u32(u32::from_le_bytes(toc_buf));
        assert_eq!(segment.toc, read_back_toc);

        //check version.
        assert_eq!(&buffer[8..12], &4713u32.to_le_bytes());

        //next segment
        let mut size_buf = [0u8; 8];
        size_buf.copy_from_slice(&buffer[12..20]);
        assert_eq!(meta_size + data_size, u64::from_le_bytes(size_buf) as usize);
        assert_eq!(meta_size + data_size, segment.next_segment_offset as usize);

        // data offset.
        let mut size_buf = [0u8; 8];
        size_buf.copy_from_slice(&buffer[20..28]);
        assert_eq!(meta_size, u64::from_le_bytes(size_buf) as usize);
        assert_eq!(meta_size, segment.raw_data_offset as usize);
    }
}
