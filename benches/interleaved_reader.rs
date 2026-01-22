use criterion::{Criterion, black_box, criterion_group};
use std::io::Cursor;
use tedium::{ChannelPath, DataLayout, TdmsFile};

fn create_file(elements: usize, channels: usize) -> TdmsFile<Cursor<Vec<u8>>> {
    let buffer = (0..elements).map(|i| i as f64).collect::<Vec<_>>();

    let mut file = TdmsFile::new(Cursor::new(Vec::new())).unwrap();
    let mut writer = file.writer().unwrap();

    let channels = (0..channels)
        .map(|index| ChannelPath::new("group", &format!("channel{}", index)))
        .collect::<Vec<_>>();
    writer
        .write_channels(&channels, &buffer[..], DataLayout::Interleaved)
        .unwrap();
    drop(writer);
    file
}

fn single_channel_segment(c: &mut Criterion) {
    let elements = 1_000_000;
    let mut file = create_file(elements, 1);
    let mut output = vec![0.0f64; elements];

    let mut group = c.benchmark_group("Single Channel Segment - Interleaved");
    group.throughput(criterion::Throughput::Bytes(
        elements as u64 * std::mem::size_of::<f64>() as u64,
    ));

    group.bench_function("read single from single", |b| {
        b.iter(|| {
            file.read_channel(
                &ChannelPath::new("group", "channel0"),
                black_box(&mut output[..]),
            )
            .unwrap()
        });
    });
}

fn multi_channel_segment(c: &mut Criterion) {
    let elements = 1_000_000;
    let channels = 4;
    let elements_all = elements * channels;
    let mut file = create_file(elements_all, channels);
    let mut output = vec![0.0f64; elements];
    let mut output1 = vec![0.0f64; elements];
    let mut output2 = vec![0.0f64; elements];
    let mut output3 = vec![0.0f64; elements];

    let mut group = c.benchmark_group("Multi Channel Segment - Interleaved");
    group.throughput(criterion::Throughput::Bytes(
        elements as u64 * std::mem::size_of::<f64>() as u64,
    ));

    group.bench_function("read single from multi", |b| {
        b.iter(|| {
            file.read_channel(
                &ChannelPath::new("group", "channel0"),
                black_box(&mut output[..]),
            )
            .unwrap()
        });
    });

    group.throughput(criterion::Throughput::Bytes(
        elements_all as u64 * std::mem::size_of::<f64>() as u64,
    ));
    group.bench_function("read all from multi", |b| {
        b.iter(|| {
            file.read_channels(
                &[
                    &ChannelPath::new("group", "channel0"),
                    &ChannelPath::new("group", "channel1"),
                    &ChannelPath::new("group", "channel2"),
                    &ChannelPath::new("group", "channel3"),
                ],
                &mut [
                    black_box(&mut output[..]),
                    black_box(&mut output1[..]),
                    black_box(&mut output2[..]),
                    black_box(&mut output3[..]),
                ],
            )
            .unwrap();
        });
    });
}

criterion_group!(
    interleaved_reader,
    single_channel_segment,
    multi_channel_segment
);
