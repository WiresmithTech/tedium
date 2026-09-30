mod common;
use labview_interop::types::LVTime;
use std::io::Cursor;
use std::{fmt::Debug, io::Read, io::Seek, io::Write};
use tedium::types::Complex;
use tedium::{ChannelPath, DataLayout, PropertyPath, PropertyValue, TdmsError, TdmsFile};

const TEST_PROPERTIES: &[(&str, PropertyValue)] = &[
    ("i8", PropertyValue::I8(-5)),
    ("u8", PropertyValue::U8(5)),
    ("i16", PropertyValue::I16(-10)),
    ("u16", PropertyValue::U16(10)),
    ("i32", PropertyValue::I32(-20)),
    ("u32", PropertyValue::U32(20)),
    ("i64", PropertyValue::I64(-30)),
    ("u64", PropertyValue::U64(30)),
    ("f32", PropertyValue::SingleFloat(-40.0)),
    ("f64", PropertyValue::DoubleFloat(40.0)),
    ("bool_true", PropertyValue::Boolean(true)),
    ("bool_false", PropertyValue::Boolean(false)),
    (
        "complex_f32",
        PropertyValue::ComplexSingleFloat(Complex::new(60.0, 6.0)),
    ),
    (
        "complex_f64",
        PropertyValue::ComplexDoubleFloat(Complex::new(-60.0, -6.0)),
    ),
    /* (
        "timestamp",
        PropertyValue::Timestamp(LVTime::from_lv_epoch(3780807561.0)),
    ), */
];

fn test_properties<F: Write + Read + Seek + Debug>(file: TdmsFile<F>, path: PropertyPath) {
    for (name, expected) in TEST_PROPERTIES {
        let actual = file
            .read_property(&path, name)
            .expect(&format!("Failed to read property {}", name));
        assert_eq!(actual, Some(expected));
    }

    //this one wont exist as a constant.
    let actual = file
        .read_property(&path, "timestamp")
        .expect("Failed to read property timestamp");

    assert_eq!(
        actual.unwrap(),
        &PropertyValue::Timestamp(LVTime::from_lv_epoch(3780807561.0))
    );
}

#[test]
fn test_file_properties() {
    let file = common::open_test_file();
    test_properties(file, PropertyPath::file());
}

#[test]
fn test_group_properties() {
    let file = common::open_test_file();
    test_properties(file, PropertyPath::group("group"));
}

#[test]
fn test_channel_properties() {
    let file = common::open_test_file();
    test_properties(file, PropertyPath::channel("group", "channel"));
}

const NEXT_SEGMENT_POS: usize = 12;

fn mark_corrupted(bytes: &mut Vec<u8>, segment_start: usize) {
    let offset = segment_start + NEXT_SEGMENT_POS;
    bytes[offset..offset + 8].fill(0xFF);
}

#[test]
fn test_corrupted_file() {
    let mut buffer = Cursor::new(Vec::new());
    let first_segment = vec![1.0, 2.0, 3.0];
    let second_segment = vec![4.0, 5.0, 6.0];
    {
        let mut file = TdmsFile::new(&mut buffer, tedium::TdmsFileOption::default()).unwrap();
        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[&ChannelPath::new("group", "channel")],
                &first_segment[..],
                DataLayout::Interleaved,
            )
            .unwrap();
    }
    let second_segment_starts = buffer.get_ref().len();
    {
        let mut file = TdmsFile::new(&mut buffer, tedium::TdmsFileOption::default()).unwrap();
        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[&ChannelPath::new("group", "channel")],
                &second_segment[..],
                DataLayout::Interleaved,
            )
            .unwrap();
    }
    mark_corrupted(buffer.get_mut(), second_segment_starts);
    let mut output = vec![0.0; 3];
    let mut file = TdmsFile::new(&mut buffer, tedium::TdmsFileOption::default()).unwrap();
    {
        file.read_channel(&ChannelPath::new("group", "channel"), &mut output[..])
            .unwrap();
    }
    assert_eq!(output, vec![1.0, 2.0, 3.0,]);
    assert!(file.has_unfinished_segment().is_err())
}
