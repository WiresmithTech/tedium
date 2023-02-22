use std::{env::args, hint::black_box, path::PathBuf, time::Instant};

fn main() {
    /// Args
    /// 1. number of groups.
    /// 2. channels per group.
    /// 3. samples per channel.
    /// 4. writes per group.
    ///
    /// Then we will iterate through the group writes for the number of writes per group.
    let args: Vec<String> = args().collect();
    let group_count: usize = args[1].parse().unwrap();
    let channels_per_group: usize = args[2].parse().unwrap();
    let samples_per_channel: usize = args[3].parse().unwrap();
    let writes_per_group: usize = args[4].parse().unwrap();

    let mut path = std::env::temp_dir();
    path.push("test_benchmark.tdms");
    //incase we left it for testing previously.
    let _ = std::fs::remove_file(path.clone());

    let channels: Vec<String> = (0..channels_per_group)
        .map(|i| format!("channel{i}"))
        .collect();
    let groups: Vec<String> = (0..group_count).map(|i| format!("group{i}")).collect();
    let samples_to_write = vec![1.0f64; samples_per_channel * channels_per_group];
    let mut read_buffer = vec![1.0f64; samples_per_channel * writes_per_group];

    let write_start = Instant::now();
    let mut file = tdms_lib::TdmsFile::create(&path);
    let mut file_write = file.writer().unwrap();
    for group in groups.iter().cycle().take(writes_per_group * group_count) {
        let paths: Vec<String> = channels.iter().map(|ch| format!("/{group}/{ch}")).collect();
        let paths_str: Vec<&str> = paths.iter().map(|path| path.as_str()).collect();
        file_write
            .write_channels(
                &paths_str[..],
                &samples_to_write,
                tdms_lib::DataLayout::Contigious,
            )
            .unwrap();
    }
    file_write.sync().unwrap();
    let write_time = write_start.elapsed();

    let read_start = Instant::now();
    let mut read_file = tdms_lib::TdmsFile::load(&path);
    let averages: Vec<f64> = groups
        .iter()
        .map(|group| channels.iter().map(move |ch| format!("/{group}/{ch}")))
        .flatten()
        .map(|ch| -> f64 {
            read_file.read_channel(&ch, &mut read_buffer[..]).unwrap();
            let sum: f64 = read_buffer.iter().sum();
            sum / read_buffer.len() as f64
        })
        .collect();
    let read_time = read_start.elapsed();

    //std::fs::remove_file(path).unwrap();

    println!("Write Time: {write_time:?}");
    println!("Read Time: {read_time:?}");
    println!("First averages: {}", averages.first().unwrap());
    println!("Last averages: {}", averages.last().unwrap())
}
