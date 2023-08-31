//! Reader of a multi-channel data block.
//!
//!

use crate::error::TdmsError;
use crate::io::reader::TdmsReader;
use std::{
    io::{Read, Seek},
    marker::PhantomData,
};

/// The multichannel interleaved reader will read from an interleaved block.
///
/// We will assume a single datatype as it is unclear if multiple types exist in the wild.
pub struct MultiChannelInterleavedReader<R: Read + Seek, T: TdmsReader<R>> {
    reader: T,
    _marker: PhantomData<R>,
    block_size: u64,
    block_start: u64,
}

impl<R: Read + Seek, T: TdmsReader<R>> MultiChannelInterleavedReader<R, T> {
    pub fn new(reader: T, block_start: u64, block_size: u64) -> Self {
        Self {
            reader,
            _marker: PhantomData,
            block_size,
            block_start,
        }
    }

    /// Read the data from the block for the channels specified into the output slices.
    ///
    /// Returns the number of channels read in this block.
    ///
    /// todo: what if all samples read different lengths?
    pub fn read(
        &mut self,
        row_size: usize,
        channels: &mut [(usize, &mut [f64])],
    ) -> Result<usize, TdmsError> {
        self.reader.to_file_position(self.block_start)?;
        let mut buffer = vec![0.0; row_size];
        let row_count = self.block_size as usize / (row_size * std::mem::size_of::<f64>());

        let mut output_iters: Vec<_> = channels
            .iter_mut()
            .map(|(idx, output)| (*idx, output.iter_mut()))
            .collect();

        for _ in 0..row_count {
            for item in buffer.iter_mut() {
                *item = self.reader.read_value()?;
            }
            for (channel_idx, output) in output_iters.iter_mut() {
                if let Some(output) = output.next() {
                    *output = buffer[*channel_idx];
                }
            }
        }
        Ok(row_count)
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
    fn read_data_interleaved_single() {
        let mut buffer = create_test_buffer();

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800,
        );
        let mut output: Vec<f64> = vec![0.0; 3];
        reader.read(2, &mut [(0, &mut output)]).unwrap();
        assert_eq!(output, vec![0.0, 2.0, 4.0]);
    }

    #[test]
    fn read_data_interleaved_multi() {
        let mut buffer = create_test_buffer();

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800,
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 3];
        reader
            .read(4, &mut [(0, &mut output_1), (2, &mut output_2)])
            .unwrap();
        assert_eq!(output_1, vec![0.0, 4.0, 8.0]);
        assert_eq!(output_2, vec![2.0, 6.0, 10.0]);
    }

    #[test]
    fn read_data_interleaved_multi_different_lengths() {
        let mut buffer = create_test_buffer();

        let mut reader = MultiChannelInterleavedReader::<_, _>::new(
            BigEndianReader::from_reader(&mut buffer),
            0,
            800,
        );
        let mut output_1: Vec<f64> = vec![0.0; 3];
        let mut output_2: Vec<f64> = vec![0.0; 2];
        reader
            .read(4, &mut [(0, &mut output_1), (2, &mut output_2)])
            .unwrap();
        assert_eq!(output_1, vec![0.0, 4.0, 8.0]);
        assert_eq!(output_2, vec![2.0, 6.0]);
    }
}
