mod contiguous_reader;
mod interleaved_reader;
mod writers;

use contiguous_reader::contiguous_reader;
use interleaved_reader::interleaved_reader;
use writers::writers;

use criterion::criterion_main;

criterion_main!(contiguous_reader, interleaved_reader, writers);
