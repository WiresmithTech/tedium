//! Handling of string data types.

use crate::io::data_types::StorageResult;
use crate::types::DataType;
use crate::{TdmsError, TdmsStorageType};
use std::io::{Read, Write};

fn read_string_with_length(reader: &mut impl Read, length: u32) -> Result<String, TdmsError> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve(length as usize)
        .map_err(|_| TdmsError::StringAllocationFailed)?;
    // SAFETY: This is safe because:
    // 1. We just allocated capacity
    // 2. The memory will be immediately filled by read_exact
    // 3. No drop checks are needed since we're dealing with u8 (no destructors)
    //
    // If we can get a fallible allocation with fill in the future, we should use that.
    unsafe {
        buffer.set_len(length as usize);
    }
    reader.read_exact(&mut buffer[..])?;
    let value = String::from_utf8(buffer)?;
    Ok(value)
}

impl TdmsStorageType for String {
    const SUPPORTED_TYPES: &'static [DataType] = &[DataType::TdmsString];

    const NATURAL_TYPE: DataType = DataType::TdmsString;

    fn read_le(reader: &mut impl Read) -> Result<Self, TdmsError> {
        let length = u32::read_le(reader)?;
        read_string_with_length(reader, length)
    }

    fn read_be(reader: &mut impl Read) -> Result<Self, TdmsError> {
        let length = u32::read_be(reader)?;
        read_string_with_length(reader, length)
    }

    fn write_le(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&(self.len() as u32).to_le_bytes())?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }

    fn write_be(&self, writer: &mut impl Write) -> StorageResult<()> {
        writer.write_all(&(self.len() as u32).to_be_bytes())?;
        writer.write_all(self.as_bytes())?;
        Ok(())
    }

    fn size(&self) -> usize {
        self.len() + std::mem::size_of::<u32>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_string() {
        let mut buffer = Vec::new();
        let hello = "Hello, world!";
        buffer.extend((hello.len() as u32).to_le_bytes());
        buffer.extend(hello.as_bytes());
        let mut reader = std::io::Cursor::new(buffer);
        let value = String::read_le(&mut reader).unwrap();
        assert_eq!(value, hello);
    }

    #[test]
    fn test_read_string_obscene_length() {
        let mut buffer = Vec::new();
        let hello = "Hello, world!";
        buffer.extend(&[0xFF, 0xFF, 0xFF, 0xFF]);
        buffer.extend(hello.as_bytes());
        let mut reader = std::io::Cursor::new(buffer);
        let value = String::read_le(&mut reader);
        assert!(value.is_err());
    }
}
