use std::path::PathBuf;

use tdms_lib::TdmsReader;

fn main() {
    let mut args = std::env::args();

    let path = args.nth(1).unwrap();

    TdmsReader::load(&PathBuf::from(path));
}
