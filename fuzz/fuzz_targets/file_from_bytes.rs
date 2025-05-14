#![no_main]

use std::io::Cursor;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // just generic no structural awareness.
    let _result = tedium::TdmsFile::new(Cursor::new(data));
});