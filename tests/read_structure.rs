mod common;

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
    let expected = test_data(2);
    let mut buffer = vec![0.0; expected.len()];
    file.read_channel("/'structure'/'ch3'", &mut buffer[..])
        .unwrap();

    assert_eq!(buffer, expected);
}

#[test]
fn test_multi_channel_read_same_length() {
    let mut file = common::open_test_file();
    let expected0 = test_data(0);
    let expected2 = test_data(2);
    let mut buffer0 = vec![0.0; expected0.len()];
    let mut buffer2 = vec![0.0; expected2.len()];
    file.read_channels(
        &["/'structure'/'ch1'", "/'structure'/'ch3'"],
        &mut [&mut buffer0[..], &mut buffer2[..]],
    )
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
        &["/'structure'/'ch1'", "/'structure'/'ch5'"],
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
    file.read_channel("/'subblock'/'ch1'", &mut buffer0[..])
        .unwrap();
    assert_eq!(buffer0, expected0[0..read_length]);
}
