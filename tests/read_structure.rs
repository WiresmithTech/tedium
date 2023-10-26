mod common;

use labview_interop::types::LVTime;
use tedium::{types::Complex, ChannelPath};

fn test_data(channel_index: usize) -> Vec<f64> {
    let samples = match channel_index {
        0..=2 => 10000,
        _ => 5000,
    };
    let start = channel_index * 10000;
    (start..start + samples).map(|i| i as f64).collect()
}

#[test]
fn test_single_channel_read() {
    let mut file = common::open_test_file();
    let path = ChannelPath::new("structure", "ch3");
    let expected = test_data(2);

    assert_eq!(file.channel_length(&path).unwrap(), expected.len() as u64);

    let mut buffer = vec![0.0; expected.len()];
    file.read_channel(&path, &mut buffer[..]).unwrap();

    assert_eq!(buffer, expected);
}

#[test]
fn test_multi_channel_read_same_length() {
    let mut file = common::open_test_file();
    let path0 = ChannelPath::new("structure", "ch1");
    let path2 = ChannelPath::new("structure", "ch3");

    let expected0 = test_data(0);
    let expected2 = test_data(2);

    assert_eq!(file.channel_length(&path0).unwrap(), expected0.len() as u64);
    assert_eq!(file.channel_length(&path2).unwrap(), expected2.len() as u64);

    let mut buffer0 = vec![0.0; expected0.len()];
    let mut buffer2 = vec![0.0; expected2.len()];
    file.read_channels(&[&path0, &path2], &mut [&mut buffer0[..], &mut buffer2[..]])
        .unwrap();

    assert_eq!(buffer0, expected0);
    assert_eq!(buffer2, expected2);
}

#[test]
fn test_multi_channel_read_shorter() {
    let mut file = common::open_test_file();
    let read_length = 1250; //so it sits mid segment
    let expected0 = test_data(0);
    let expected4 = test_data(4);
    let mut buffer0 = vec![0.0; read_length];
    let mut buffer4 = vec![0.0; read_length];
    file.read_channels(
        &[
            &ChannelPath::new("structure", "ch1"),
            &ChannelPath::new("structure", "ch5"),
        ],
        &mut [&mut buffer0[..], &mut buffer4[..]],
    )
    .unwrap();

    assert_eq!(buffer0, expected0[0..read_length]);
    assert_eq!(buffer4, expected4[0..read_length]);
}

#[test]
fn test_read_sub_blocks() {
    let mut file = common::open_test_file();
    let read_length = 2750;
    let expected0 = test_data(0);
    let mut buffer0 = vec![0.0; read_length];
    file.read_channel(&ChannelPath::new("subblock", "ch1"), &mut buffer0[..])
        .unwrap();
    assert_eq!(buffer0, expected0[0..read_length]);
}

macro_rules! read_datatype_test {
    ($file: ident, $type: ty) => {
        let channel_name = stringify!($type);
        let expected = (0..100).map(|value| value as $type).collect::<Vec<$type>>();
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

macro_rules! read_complex_datatype_test {
    ($file: ident, $type: ty) => {
        let channel_name = "complex_".to_string() + stringify!($type);
        let expected = (1u8..4)
            .map(|value| Complex::<$type>::new(value as $type * 10.0, value.into()))
            .collect::<Vec<Complex<$type>>>();
        let mut buffer = vec![Complex::<$type>::new(0.0, 0.0); 3];
        $file
            .read_channel(
                &ChannelPath::new("datatypes", &channel_name),
                &mut buffer[..],
            )
            .unwrap();
        assert_eq!(buffer, expected);
    };
}

#[test]
fn test_read_basic_numeric_types() {
    let mut file = common::open_test_file();
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

#[test]
fn test_complex_type_read() {
    let mut file = common::open_test_file();
    read_complex_datatype_test!(file, f32);
    read_complex_datatype_test!(file, f64);
}

/// Test the boolean type - note that LabVIEW actually stores a U8.
#[test]
fn test_boolean_data_types() {
    let mut file = common::open_test_file();
    let mut buffer = vec![false; 100];
    file.read_channel(&ChannelPath::new("datatypes", "bool"), &mut buffer[..])
        .unwrap();
    assert_eq!(&buffer[..4], &[true, false, true, false]);
}

#[test]
fn test_timestamp_data_types() {
    let expected_ts_lv_epoch = [3780807865.0, 3780807866.0, 3780807867.0];
    let expected = expected_ts_lv_epoch
        .iter()
        .map(|&ts| LVTime::from_lv_epoch(ts))
        .collect::<Vec<LVTime>>();

    let mut file = common::open_test_file();
    let mut buffer = vec![LVTime::from_parts(0, 0); 100];
    file.read_channel(&ChannelPath::new("datatypes", "timestamp"), &mut buffer[..])
        .unwrap();
    assert_eq!(&buffer[..3], &expected);
}
