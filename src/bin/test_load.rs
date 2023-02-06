use std::path::PathBuf;

use tdms_lib::TdmsReader;

fn main() {
    let mut args = std::env::args();

    let path = args.nth(1).unwrap();
    let start = std::time::Instant::now();
    let mut reader = TdmsReader::load(&PathBuf::from(path));
    let mut data = vec![0.0f64; 200000];
    reader
        .read_channel("/'Untitled'/'Time (ms)'", &mut data[..])
        .unwrap();
    let time = start.elapsed();
    println!("{:?}", &data[0..10]);
    println!("Read in {time:?}");
}
