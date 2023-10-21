//! This example is designed to mimic the LabVIEW speed test examples
//! for comparison and also as a consistent example of the best speed practices.

use tdms_lib::{DataLayout, ObjectPath, TdmsFile};

const WRITE_BLOCK_SIZE: usize = 1024 * 1024; // 1MB
const WRITE_PASSES: usize = 1000;
const READ_BLOCK_SIZE: usize = 1024 * 1024; // 1MB
const READ_PASSES: usize = 1000;

fn main() {
    let mut temp_path = std::env::temp_dir();
    temp_path.push("test.tdms");

    let _ = std::fs::remove_file(&temp_path);

    let data = vec![5i16; WRITE_BLOCK_SIZE];

    let mut file = TdmsFile::create(&temp_path).unwrap();
    let mut writer = file.writer().unwrap();

    let start = std::time::Instant::now();
    for _ in 0..WRITE_PASSES {
        writer
            .write_channels(
                &[ObjectPath::channel("Benchmark", "Data")],
                &data[..],
                DataLayout::Contigious,
            )
            .unwrap();
    }

    let time = start.elapsed();
    println!("Write in {time:?}");
    let write_bytes = WRITE_BLOCK_SIZE * WRITE_PASSES * std::mem::size_of::<i16>();
    println!(
        "Write speed: {} MB/s",
        (write_bytes as f64 / time.as_secs_f64()) / 1024.0 / 1024.0
    );
    drop(writer);

    let mut read_buffer = vec![0i16; READ_BLOCK_SIZE];

    //TODO: This isn't moving through the file as we lack a random access read.
    let start = std::time::Instant::now();
    for _ in 0..READ_PASSES {
        file.read_channel(
            &ObjectPath::channel("Benchmark", "Data"),
            &mut read_buffer[..],
        )
        .unwrap();
    }
    let time = start.elapsed();
    println!("Read in {time:?}");
    let read_bytes = READ_BLOCK_SIZE * READ_PASSES * std::mem::size_of::<i16>();
    println!(
        "Read speed: {} MB/s",
        (read_bytes as f64 / time.as_secs_f64()) / 1024.0 / 1024.0
    );

    std::fs::remove_file(&temp_path).unwrap();
}
