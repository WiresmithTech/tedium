mod common;
use labview_interop::types::LVTime;
use std::{fmt::Debug, io::Read, io::Seek, io::Write};
use tedium::{PropertyPath, PropertyValue, TdmsFile};

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
    /* (
        "timestamp",
        PropertyValue::Timestamp(LVTime::from_lv_epoch(3780807561.0)),
    ), */
];

fn write_properties<F: Write + Read + Seek + Debug>(file: &mut TdmsFile<F>, path: &PropertyPath) {
    let mut writer = file.writer().unwrap();
    writer.write_properties(path, TEST_PROPERTIES).unwrap();

    //this one wont exist as a constant.
    writer
        .write_properties(
            &path,
            &[(
                "timestamp",
                PropertyValue::Timestamp(LVTime::from_lv_epoch(3780807561.0)),
            )],
        )
        .unwrap();
}

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
    let mut file = common::get_empty_file();
    let path = PropertyPath::file();
    write_properties(&mut file, &path);

    test_properties(file, path);
}

#[test]
fn test_group_properties() {
    let mut file = common::get_empty_file();
    let path = PropertyPath::group("group");

    write_properties(&mut file, &path);
    test_properties(file, path);
}

#[test]
fn test_channel_properties() {
    let mut file = common::get_empty_file();
    let path = PropertyPath::channel("group", "channel");

    write_properties(&mut file, &path);
    test_properties(file, path);
}
