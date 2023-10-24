//! The extended type is not well supported in Rust.
//!
//! The default here is just to keep it as a u128 value.
//!
//! We could add the extended crate to support it in the future but that
//! looks pretty limited.

use super::*;

/// A wrapper around the raw bytes that make up an extended float.
///
/// As there is no native support in Rust for extended floats, this
/// doesn't provide any additional functionality but can be used to
/// persist existing values.
///
/// In the file, they are stored as 10 bytes so we use a u128 to store.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtendedRaw(u128);

impl ExtendedRaw {
    pub fn new(value: u128) -> Self {
        Self(value)
    }
}

const EXT_SIZE: usize = 10;
const U128_SIZE: usize = std::mem::size_of::<u128>();
const SIZE_DIFF: usize = U128_SIZE - EXT_SIZE;

impl TdmsStorageType for ExtendedRaw {
    const SUPPORTED_TYPES: &'static [DataType] =
        &[DataType::ExtendedFloat, DataType::ExtendedFloatWithUnit];

    const NATURAL_TYPE: DataType = DataType::ExtendedFloat;

    fn read_le(reader: &mut impl Read) -> StorageResult<Self> {
        let mut buffer = [0u8; U128_SIZE];
        reader.read_exact(&mut buffer[0..EXT_SIZE])?;
        let value = u128::from_le_bytes(buffer);
        Ok(ExtendedRaw(value))
    }

    fn read_be(reader: &mut impl Read) -> StorageResult<Self> {
        let mut buffer = [0u8; U128_SIZE];
        reader.read_exact(&mut buffer[SIZE_DIFF..])?;
        let value = u128::from_be_bytes(buffer);
        Ok(ExtendedRaw(value))
    }

    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
        let bytes = self.0.to_le_bytes();
        writer.write_all(&bytes[0..EXT_SIZE])?;
        Ok(())
    }

    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
        let bytes = self.0.to_be_bytes();
        writer.write_all(&bytes[SIZE_DIFF..])?;
        Ok(())
    }

    fn size(&self) -> usize {
        EXT_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::reader::{BigEndianReader, LittleEndianReader, TdmsReader};
    use crate::io::writer::{BigEndianWriter, LittleEndianWriter, TdmsWriter};
    use std::io::Cursor;

    #[test]
    fn test_extended_size() {
        let value = ExtendedRaw(0);
        assert_eq!(value.size(), 10);
    }

    #[test]
    fn test_be_round_trip() {
        let mut buffer = Cursor::new(Vec::new());
        let mut writer = BigEndianWriter::from_writer(&mut buffer);
        let value = ExtendedRaw(0x0008_000c);
        writer.write_value(&value).unwrap();
        drop(writer);

        buffer.set_position(0);
        let mut reader = BigEndianReader::from_reader(buffer);
        let read_value: ExtendedRaw = reader.read_value().unwrap();
        assert_eq!(read_value.0, 0x0008_000c);
    }

    #[test]
    fn test_le_round_trip() {
        let mut buffer = Cursor::new(Vec::new());
        let mut writer = LittleEndianWriter::from_writer(&mut buffer);
        let value = ExtendedRaw(0x0008_000c);
        writer.write_value(&value).unwrap();
        drop(writer);

        buffer.set_position(0);
        let mut reader = LittleEndianReader::from_reader(buffer);
        let read_value: ExtendedRaw = reader.read_value().unwrap();
        assert_eq!(read_value.0, 0x0008_000c);
    }
}
