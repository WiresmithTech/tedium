mod contiguous_reader;
mod interleaved_reader;

use contiguous_reader::contiguous_reader;
use interleaved_reader::interleaved_reader;

use criterion::criterion_main;

criterion_main!(contiguous_reader, interleaved_reader);
