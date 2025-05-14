//! Contains wrappers around readers to encode TDMS specific formatting e.g. endianess.

use std::io::{BufReader, Read, Seek};

use crate::error::TdmsError;
use crate::meta_data::{Segment, TdmsMetaData, ToC};

use super::data_types::TdmsStorageType;

pub trait TdmsReader<R: Read + Seek>: Sized {
    fn from_reader(reader: R) -> Self;
    fn read_value<T: TdmsStorageType>(&mut self) -> Result<T, TdmsError>;
    fn read_meta<T: TdmsMetaData>(&mut self) -> Result<T, TdmsError> {
        T::read(self)
    }
    fn read_vec<T: TdmsMetaData>(&mut self, length: usize) -> Result<Vec<T>, TdmsError> {
        // Create a new vector and pre-allocate memory for `length` elements.
        // `try_reserve` is used to handle potential allocation failures gracefully,
        // returning a `TdmsError::VecAllocationFailed` if the allocation fails.
        let mut vec = Vec::new();
        vec.try_reserve(length)
            .map_err(|_| TdmsError::VecAllocationFailed)?;
        for _ in 0..length {
            vec.push(self.read_meta()?);
        }
        Ok(vec)
    }
    fn buffered_reader(&mut self) -> &mut BufReader<R>;

    /// Move to an absolute position in the file.
    fn to_file_position(&mut self, position: u64) -> Result<(), TdmsError> {
        self.buffered_reader()
            .seek(std::io::SeekFrom::Start(position))?;
        Ok(())
    }

    /// Move relative to the current file position.
    fn move_position(&mut self, offset: i64) -> Result<(), TdmsError> {
        self.buffered_reader().seek_relative(offset)?;
        Ok(())
    }

    /// Called immediately after ToC has been read so we have determined the endianess.
    fn read_segment(&mut self, toc: ToC) -> Result<Segment, TdmsError> {
        let _version: u32 = self.read_value()?;
        let next_segment_offset = self.read_value()?;
        let raw_data_offset = self.read_value()?;

        //todo handle no meta data mode.
        let meta_data = self.read_meta()?;

        Ok(Segment {
            toc,
            next_segment_offset,
            raw_data_offset,
            meta_data: Some(meta_data),
        })
    }
}

pub struct LittleEndianReader<R: Read>(BufReader<R>);

impl<R: Read + Seek> TdmsReader<R> for LittleEndianReader<R> {
    fn read_value<T: TdmsStorageType>(&mut self) -> Result<T, TdmsError> {
        T::read_le(&mut self.0)
    }

    fn from_reader(reader: R) -> Self {
        Self(BufReader::new(reader))
    }

    fn buffered_reader(&mut self) -> &mut BufReader<R> {
        &mut self.0
    }
}

pub struct BigEndianReader<R: Read>(BufReader<R>);

impl<R: Read + Seek> TdmsReader<R> for BigEndianReader<R> {
    fn read_value<T: TdmsStorageType>(&mut self) -> Result<T, TdmsError> {
        T::read_be(&mut self.0)
    }

    fn from_reader(reader: R) -> Self {
        Self(BufReader::new(reader))
    }

    fn buffered_reader(&mut self) -> &mut BufReader<R> {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::io::Cursor;

    /// Tests the conversion against the le and be version for the value specified.
    macro_rules! test_formatting {
        ($type:ty, $test_value:literal) => {
            paste::item! {
                #[test]
                fn [< test_ $type _le >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_le_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = LittleEndianReader::from_reader(&mut reader);
                    let read_value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(read_value, original_value);
                }

                #[test]
                fn [< test_ $type _be >] () {
                    let original_value: $type = $test_value;
                    let bytes = original_value.to_be_bytes();
                    let mut reader = Cursor::new(bytes);
                    let mut tdms_reader = BigEndianReader::from_reader(&mut reader);
                    let read_value: $type = tdms_reader.read_value().unwrap();
                    assert_eq!(read_value, original_value);
                }
            }
        };
    }

    test_formatting!(i32, -12345);
    test_formatting!(u32, 12345);
    test_formatting!(f64, 1234.1245);

    #[test]
    fn test_string() {
        //example from NI site
        let test_buffer = [
            0x23, 00, 00, 00, 0x2Fu8, 0x27, 0x4D, 0x65, 0x61, 0x73, 0x75, 0x72, 0x65, 0x64, 0x20,
            0x54, 0x68, 0x72, 0x6F, 0x75, 0x67, 0x68, 0x70, 0x75, 0x74, 0x20, 0x44, 0x61, 0x74,
            0x61, 0x20, 0x28, 0x56, 0x6F, 0x6C, 0x74, 0x73, 0x29, 0x27,
        ];
        let mut cursor = Cursor::new(test_buffer);
        let mut reader = LittleEndianReader::from_reader(&mut cursor);
        let string: String = reader.read_value().unwrap();
        assert_eq!(string, String::from("/'Measured Throughput Data (Volts)'"));
    }
}
