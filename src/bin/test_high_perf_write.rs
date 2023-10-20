use std::path::PathBuf;

use tdms_lib::{DataLayout, ObjectPath, TdmsFile};

fn main() {
    let mut temp_path = std::env::temp_dir();
    temp_path.push("test.tdms");

    let data = vec![0.0f64; 1024];

    let start = std::time::Instant::now();
    let mut file = TdmsFile::create(&temp_path);
    let mut writer = file.writer().unwrap();

    for _ in 0..10 {
        writer
            .write_channels(
                &[ObjectPath::channel("Untitled", "Time (ms)")],
                &data[..],
                DataLayout::Contigious,
            )
            .unwrap();
    }

    let time = start.elapsed();
    println!("Write in {time:?}");

    //std::fs::remove_file(&temp_path).unwrap();
}
