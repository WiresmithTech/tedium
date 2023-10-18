use std::path::PathBuf;

use tdms_lib::{ObjectPath, TdmsFile};

fn main() {
    let mut args = std::env::args();

    let path = args.nth(1).unwrap();
    let start = std::time::Instant::now();
    let mut reader = TdmsFile::load(&PathBuf::from(path));
    let mut data = vec![0.0f64; 200000];
    reader
        .read_channel(&ObjectPath::channel("Untitled", "Time (ms)"), &mut data[..])
        .unwrap();
    let time = start.elapsed();
    println!("{:?}", &data[0..10]);
    println!("Read in {time:?}");
}
