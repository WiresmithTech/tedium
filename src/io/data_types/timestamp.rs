//! The timestamp type is a 128 bit value that represents the number of seconds since the 1904 epoch.
//!
//! This module wraps a raw representation of this and then we will add chrono behind a feature flag.

use super::*;
use labview_interop::types::timestamp::LVTime;

const LVTIME_SIZE: usize = 16;

impl TdmsStorageType for LVTime {
    const SUPPORTED_TYPES: &'static [DataType] = &[DataType::Timestamp];
    const NATURAL_TYPE: DataType = DataType::Timestamp;

    fn read_le(reader: &mut impl Read) -> StorageResult<Self> {
        let mut bytes = [0u8; LVTIME_SIZE];
        reader.read_exact(&mut bytes)?;
        Ok(LVTime::from_le_bytes(bytes))
    }

    fn read_be(reader: &mut impl Read) -> StorageResult<Self> {
        let mut bytes = [0u8; LVTIME_SIZE];
        reader.read_exact(&mut bytes)?;
        Ok(LVTime::from_be_bytes(bytes))
    }

    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&self.to_le_bytes())?;
        Ok(())
    }

    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&self.to_be_bytes())?;
        Ok(())
    }

    fn size(&self) -> usize {
        LVTIME_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reader::{BigEndianReader, LittleEndianReader, TdmsReader};
    use crate::io::writer::{BigEndianWriter, LittleEndianWriter, TdmsWriter};
    use std::io::Cursor;

    #[test]
    fn test_timestamp_be() {
        //Will just test using a seconds timestamp.
        let timestamp: f64 = 1234567890.123456789;
        let time = LVTime::from_unix_epoch(timestamp);

        let bytes = time.to_be_bytes();
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
        let read_value: LVTime = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, time);

        let mut output_bytes = [0u8; 16];
        let mut writer = BigEndianWriter::from_writer(&mut output_bytes[..]);
        writer.write_value(&time).unwrap();
        drop(writer);
        assert_eq!(bytes, output_bytes);
    }

    #[test]
    fn test_timestamp_le() {
        //Will just test using a seconds timestamp.
        let timestamp: f64 = 1234567890.123456789;
        let time = LVTime::from_unix_epoch(timestamp);

        let bytes = time.to_le_bytes();
        let mut reader = Cursor::new(bytes);
        let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
        let read_value: LVTime = tdms_reader.read_value().unwrap();
        assert_eq!(read_value, time);

        let mut output_bytes = [0u8; 16];
        let mut writer = LittleEndianWriter::from_writer(&mut output_bytes[..]);
        writer.write_value(&time).unwrap();
        drop(writer);
        assert_eq!(bytes, output_bytes);
    }
}
