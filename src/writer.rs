//! Contains wrappers for the standard writers to support
//! the TDMS use case of variable bitness.
//!

use std::io::{BufWriter, Write};

use crate::data_types::TdmsStorageType;
use crate::error::TdmsError;
use crate::meta_data::{Segment, TdmsMetaData};
use crate::raw_data::WriteBlock;

type Result<T> = std::result::Result<T, TdmsError>;

pub trait TdmsWriter<W: Write>: Sized {
    /// Marker to place in the big_endian part of the ToC.
    const BIG_ENDIAN_FLAG: bool;
    fn from_writer(writer: W) -> Self;
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<usize>;
    fn write_meta<T: TdmsMetaData>(&mut self, value: &T) -> Result<usize> {
        value.write(self)
    }

    fn write_segment(&mut self, segment: &Segment, data: Option<impl WriteBlock>) -> Result<()> {
        let mut toc = segment.toc;
        toc.big_endian = Self::BIG_ENDIAN_FLAG;

        //write the meta.

        self.write_meta(&toc)?;
        //Write version.
        self.write_value(&4713u32)?;
        //Write segment offset. Not yet known.
        self.write_value(&0xFFFFFFFFu64)?;
        //Write data offset. Not yet known.
        self.write_value(&0xFFFFFFFFu64)?;

        let meta_data_bytes = if let Some(meta_data) = &segment.meta_data {
            self.write_meta(meta_data)?
        } else {
            0
        };

        if let Some(data_block) = data {
            data_block.write(self)?;
        }

        Ok(())
    }
}

pub struct LittleEndianWriter<W: Write>(BufWriter<W>);

impl<W: Write> TdmsWriter<W> for LittleEndianWriter<W> {
    fn from_writer(writer: W) -> Self {
        Self(BufWriter::new(writer))
    }
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<usize> {
        value.write_le(&mut self.0)
    }

    const BIG_ENDIAN_FLAG: bool = false;
}

pub struct BigEndianWriter<W: Write>(BufWriter<W>);

impl<W: Write> TdmsWriter<W> for BigEndianWriter<W> {
    fn from_writer(writer: W) -> Self {
        Self(BufWriter::new(writer))
    }
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<usize> {
        value.write_be(&mut self.0)
    }

    const BIG_ENDIAN_FLAG: bool = true;
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Cursor;

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
            let length = writer
                .write_value(&String::from("/'Measured Throughput Data (Volts)'"))
                .unwrap();
            assert_eq!(length, expected_buffer.len());
        }

        assert_eq!(output_buffer, expected_buffer);
    }
}
