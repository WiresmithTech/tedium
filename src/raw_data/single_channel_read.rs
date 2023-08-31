use crate::error::TdmsError;
use crate::io::reader::TdmsReader;
use std::io::{Read, Seek};
use std::marker::PhantomData;

/// Trait for the different ways of reading out a data block.
///
/// Right now this has a single channel interface but this will
/// be expanded to multichannel very soon.
pub trait BlockReader: Sized {
    /// Number of samples expected based on the block.
    /// Just a patch for the old format right now.
    /// Will dissappear with multichannel.
    fn samples(&self) -> usize;

    /// Read into the mutable slice taking the max size from
    /// the size of the slice.
    ///
    /// Returns the number of samples read.
    fn read(self, output: &mut [f64]) -> Result<usize, TdmsError>;

    //used for testing right now.
    #[allow(dead_code)]
    fn read_vec(self) -> Result<Vec<f64>, TdmsError> {
        let mut values = vec![0.0; self.samples() as usize];
        self.read(&mut values[..])?;
        Ok(values)
    }
}

pub struct SingleChannelReader<R: Read + Seek, T: TdmsReader<R>> {
    step_bytes: i64,
    samples: u64,
    reader: T,
    _marker: PhantomData<R>,
}

impl<R: Read + Seek, T: TdmsReader<R>> SingleChannelReader<R, T> {
    pub fn new(
        start_bytes: u64,
        step_bytes: u64,
        samples: u64,
        mut reader: T,
    ) -> Result<Self, TdmsError> {
        reader.to_file_position(start_bytes)?;
        Ok(Self {
            step_bytes: step_bytes as i64,
            samples,
            reader,
            _marker: PhantomData,
        })
    }
}

impl<R: Read + Seek, T: TdmsReader<R>> BlockReader for SingleChannelReader<R, T> {
    fn samples(&self) -> usize {
        self.samples as usize
    }
    fn read(mut self, output: &mut [f64]) -> Result<usize, TdmsError> {
        let mut last_index = 0;
        for (index, sample) in output.iter_mut().take(self.samples as usize).enumerate() {
            if index != 0 {
                self.reader.move_position(self.step_bytes)?;
            }
            *sample = self.reader.read_value()?;
            last_index = index;
        }
        Ok(last_index + 1)
    }
}

#[cfg(test)]
mod tests {
    use crate::io::reader::BigEndianReader;

    use super::*;
    use std::io::{Cursor, Write};

    fn create_test_buffer() -> Cursor<Vec<u8>> {
        let buffer = Vec::with_capacity(1024);
        let mut cursor = Cursor::new(buffer);
        for index in 0..100 {
            let value = index as f64;
            cursor.write(&value.to_be_bytes()).unwrap();
        }
        cursor
    }

    #[test]
    fn read_data_contigous_no_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            SingleChannelReader::<_, _>::new(0, 0, 3, BigEndianReader::from_reader(&mut buffer))
                .unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn read_data_contigous_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            SingleChannelReader::<_, _>::new(16, 0, 3, BigEndianReader::from_reader(&mut buffer))
                .unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_no_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            SingleChannelReader::<_, _>::new(0, 8, 3, BigEndianReader::from_reader(&mut buffer))
                .unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![0.0, 2.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_offset() {
        let mut buffer = create_test_buffer();

        let reader =
            SingleChannelReader::<_, _>::new(16, 8, 3, BigEndianReader::from_reader(&mut buffer))
                .unwrap();
        let output: Vec<f64> = reader.read_vec().unwrap();
        assert_eq!(output, vec![2.0, 4.0, 6.0]);
    }
}
