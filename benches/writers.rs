use criterion::{criterion_group, BenchmarkId, Criterion};
use std::io::Cursor;
use tedium::{ChannelPath, DataLayout, TdmsFile};

fn setup_file() -> TdmsFile<Cursor<Vec<u8>>> {
    let fake_file = Cursor::new(Vec::with_capacity(32_000_000));
    let file = TdmsFile::new(fake_file).unwrap();
    file
}

fn writer(c: &mut Criterion, layout: DataLayout) {
    let elements_per_write = 1_000_000;
    let write_data = (0..elements_per_write)
        .map(|i| i as f64)
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("Writer Single Channel Single segment");
    group.throughput(criterion::Throughput::Bytes(
        elements_per_write as u64 * std::mem::size_of::<f64>() as u64,
    ));

    group.bench_with_input(
        BenchmarkId::from_parameter(layout),
        &layout,
        |b, &layout| {
            b.iter_batched_ref(
                setup_file,
                |file| {
                    let mut writer = file.writer().unwrap();

                    let channels = vec![ChannelPath::new("group", "channel")];
                    writer
                        .write_channels(&channels, &write_data[..], layout)
                        .unwrap();
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );

    drop(group);
    let mut group = c.benchmark_group("Writer Multi Channel Single segment");
    group.throughput(criterion::Throughput::Bytes(
        elements_per_write as u64 * std::mem::size_of::<f64>() as u64,
    ));
    group.bench_with_input(
        BenchmarkId::from_parameter(layout),
        &layout,
        |b, &layout| {
            b.iter_batched_ref(
                setup_file,
                |file| {
                    let mut writer = file.writer().unwrap();

                    let channels = vec![
                        ChannelPath::new("group", "channel0"),
                        ChannelPath::new("group", "channel1"),
                        ChannelPath::new("group", "channel2"),
                        ChannelPath::new("group", "channel3"),
                    ];
                    writer
                        .write_channels(&channels, &write_data[..], layout)
                        .unwrap();
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );
    drop(group);

    let mut group = c.benchmark_group("Writer Multi Channel Multi segment");
    group.throughput(criterion::Throughput::Bytes(
        elements_per_write as u64 * std::mem::size_of::<f64>() as u64 * 4,
    ));
    group.bench_with_input(
        BenchmarkId::from_parameter(layout),
        &layout,
        |b, &layout| {
            b.iter_batched_ref(
                setup_file,
                |file| {
                    let mut writer = file.writer().unwrap();

                    let channels = vec![
                        ChannelPath::new("group", "channel0"),
                        ChannelPath::new("group", "channel1"),
                        ChannelPath::new("group", "channel2"),
                        ChannelPath::new("group", "channel3"),
                    ];
                    writer
                        .write_channels(&channels, &write_data[..], layout)
                        .unwrap();
                    writer
                        .write_channels(&channels, &write_data[..], layout)
                        .unwrap();
                    writer
                        .write_channels(&channels, &write_data[..], layout)
                        .unwrap();
                    writer
                        .write_channels(&channels, &write_data[..], layout)
                        .unwrap();
                },
                criterion::BatchSize::PerIteration,
            );
        },
    );
}

fn writer_interleaved(c: &mut Criterion) {
    writer(c, DataLayout::Interleaved);
}

fn writer_contiguous(c: &mut Criterion) {
    writer(c, DataLayout::Contigious);
}

criterion_group!(writers, writer_interleaved, writer_contiguous);
