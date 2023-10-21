//! Validate the write API and structures.
//!
mod common;

use common::get_empty_file;
use tdms_lib::{ChannelPath, DataLayout};

#[test]
fn test_multi_channel_write_interleaved() {
    let mut file = get_empty_file();
    let mut writer = file.writer().unwrap();

    let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    writer
        .write_channels(
            &[
                &ChannelPath::new("structure", "ch1"),
                &ChannelPath::new("structure", "ch2"),
            ],
            &data[..],
            DataLayout::Interleaved,
        )
        .unwrap();

    drop(writer);

    let mut buffer = vec![0.0f64; 3];
    file.read_channel(&ChannelPath::new("structure", "ch1"), &mut buffer[..])
        .unwrap();
    assert_eq!(buffer, vec![1.0, 3.0, 5.0]);
    file.read_channel(&ChannelPath::new("structure", "ch2"), &mut buffer[..])
        .unwrap();
    assert_eq!(buffer, vec![2.0, 4.0, 6.0]);
}

#[test]
fn test_multi_channel_write_contigious() {
    let mut file = get_empty_file();
    let mut writer = file.writer().unwrap();

    let data = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    writer
        .write_channels(
            &[
                &ChannelPath::new("structure", "ch1"),
                &ChannelPath::new("structure", "ch2"),
            ],
            &data[..],
            DataLayout::Contigious,
        )
        .unwrap();

    drop(writer);

    let mut buffer = vec![0.0f64; 3];
    file.read_channel(&ChannelPath::new("structure", "ch1"), &mut buffer[..])
        .unwrap();
    assert_eq!(buffer, vec![1.0, 2.0, 3.0]);
    file.read_channel(&ChannelPath::new("structure", "ch2"), &mut buffer[..])
        .unwrap();
    assert_eq!(buffer, vec![4.0, 5.0, 6.0]);
}

#[test]
fn test_fragmented_write() {
    let channel1 = ChannelPath::new("structure", "ch1");
    let channel2 = ChannelPath::new("structure", "ch2");

    let mut file = get_empty_file();
    let mut writer = file.writer().unwrap();

    let data1 = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    let data2 = vec![6.0, 7.0, 8.0, 9.0, 10.0];

    writer
        .write_channels(&[&channel1], &data1[..3], DataLayout::Contigious)
        .unwrap();
    writer
        .write_channels(&[&channel2], &data2[..], DataLayout::Contigious)
        .unwrap();
    writer
        .write_channels(&[&channel1], &data1[3..], DataLayout::Contigious)
        .unwrap();

    drop(writer);

    let mut buffer = vec![0.0; 5];
    file.read_channel(&channel1, &mut buffer[..]).unwrap();
    assert_eq!(buffer, data1);
}

#[test]
fn test_repeated_writes() {
    let channel1 = ChannelPath::new("structure", "ch1");
    let channel2 = ChannelPath::new("structure", "ch2");

    let mut file = get_empty_file();
    let mut writer = file.writer().unwrap();

    let data1 = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0];
    let data2 = vec![7.0, 8.0, 9.0, 10.0, 11.0, 12.0];

    writer
        .write_channels(&[&channel1, &channel2], &data1[..], DataLayout::Contigious)
        .unwrap();
    writer
        .write_channels(&[&channel1, &channel2], &data2[..], DataLayout::Contigious)
        .unwrap();

    drop(writer);

    let mut buffer = vec![0.0; 6];
    file.read_channel(&channel1, &mut buffer[..]).unwrap();
    assert_eq!(buffer, vec![1.0, 2.0, 3.0, 7.0, 8.0, 9.0]);

    file.read_channel(&channel2, &mut buffer[..]).unwrap();
    assert_eq!(buffer, vec![4.0, 5.0, 6.0, 10.0, 11.0, 12.0]);
}

macro_rules! read_datatype_test {
    ($file: ident, $type: ty) => {
        let channel_name = stringify!($type);
        let channel_path = ChannelPath::new("datatypes", channel_name);
        let expected = (0..100).map(|value| value as $type).collect::<Vec<$type>>();

        let mut writer = $file.writer().unwrap();
        writer
            .write_channels(&[&channel_path], &expected[..], DataLayout::Contigious)
            .unwrap();
        drop(writer);

        let mut buffer = vec![0 as $type; 100];
        $file
            .read_channel(
                &ChannelPath::new("datatypes", channel_name),
                &mut buffer[..],
            )
            .unwrap();
        assert_eq!(buffer, expected);
    };
}

#[test]
fn test_write_basic_numeric_types() {
    let mut file = common::get_empty_file();
    read_datatype_test!(file, i8);
    read_datatype_test!(file, u8);
    read_datatype_test!(file, i16);
    read_datatype_test!(file, u16);
    read_datatype_test!(file, i32);
    read_datatype_test!(file, u32);
    read_datatype_test!(file, i64);
    read_datatype_test!(file, u64);
    read_datatype_test!(file, f32);
    read_datatype_test!(file, f64);
}
