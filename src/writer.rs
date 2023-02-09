//! Contains wrappers for the standard writers to support
//! the TDMS use case of variable bitness.
//!

use std::io::{BufWriter, Write};

use crate::data_types::TdmsStorageType;
use crate::error::TdmsError;

type Result<T> = std::result::Result<T, TdmsError>;

pub trait TdmsWriter<W: Write> {
    fn from_writer(writer: W) -> Self;
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<()>;
}

pub struct LittleEndianWriter<W: Write>(BufWriter<W>);

impl<W: Write> TdmsWriter<W> for LittleEndianWriter<W> {
    fn from_writer(writer: W) -> Self {
        Self(BufWriter::new(writer))
    }
    fn write_value<T: TdmsStorageType>(&mut self, value: &T) -> Result<()> {
        value.write_le(&mut self.0)
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
}
