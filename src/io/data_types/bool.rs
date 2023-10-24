//! This module contains the implementation of the TDMS bool data type.
//!
//! This implements the storage format for the native bool type.
//!
//! LabVIEW uses u8 to store bools so this datatype is also supported.

use super::*;

impl TdmsStorageType for bool {
    const SUPPORTED_TYPES: &'static [DataType] = &[DataType::Boolean, DataType::U8];

    const NATURAL_TYPE: DataType = DataType::Boolean;

    fn read_le(reader: &mut impl Read) -> StorageResult<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0] != 0)
    }

    fn read_be(reader: &mut impl Read) -> StorageResult<Self> {
        // no endianess for bool.
        Self::read_le(reader)
    }

    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&[*self as u8])?;
        Ok(())
    }

    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
        // no endianess for bool
        Self::write_le(self, writer)
    }

    fn size(&self) -> usize {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reader::{BigEndianReader, LittleEndianReader, TdmsReader};
    use crate::io::writer::{BigEndianWriter, LittleEndianWriter, TdmsWriter};
    use std::io::Cursor;

    #[test]
    fn test_bool_le() {
        let original_value = true;
        let bytes = [1u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        // block to limit writer lifetime.
        {
            let mut writer = LittleEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_bool_be() {
        let original_value = true;
        let bytes = [1u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        //block to limit writer lifetime.
        {
            let mut writer = BigEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_bool_le_false() {
        let original_value = false;
        let bytes = [0u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        // block to limit writer lifetime.
        {
            let mut writer = LittleEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_bool_be_false() {
        let original_value = false;
        let bytes = [0u8];
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
        let read_value: bool = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, original_value);

        let mut output_bytes = [0u8; 1];
        //block to limit writer lifetime.
        {
            let mut writer = BigEndianWriter::from_writer(&mut output_bytes[..]);
            writer.write_value(&original_value).unwrap();
        }
        assert_eq!(bytes, output_bytes);
    }
}
