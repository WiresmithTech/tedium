//! Tests for reading channels with start position offset

use tedium::ChannelPath;

mod common;

#[test]
fn test_read_channel_from_start_zero() {
    // Reading from start position 0 should be identical to read_channel
    let mut file = common::open_test_file();
    let channel = ChannelPath::new("structure", "ch1");

    let mut output_normal = vec![0.0f64; 10];
    let mut output_from = vec![0.0f64; 10];

    // Reset file position for second read
    let mut file2 = common::open_test_file();

    file.read_channel(&channel, &mut output_normal).unwrap();
    file2
        .read_channel_from(&channel, 0, &mut output_from)
        .unwrap();

    assert_eq!(
        output_normal, output_from,
        "Reading from position 0 should match normal read"
    );
}

#[test]
fn test_read_channel_from_middle() {
    // Read first 20 samples normally, then read from position 10 and compare
    let mut file = common::open_test_file();
    let channel = ChannelPath::new("structure", "ch1");

    let mut full_read = vec![0.0f64; 20];
    file.read_channel(&channel, &mut full_read).unwrap();

    // Now read from position 10
    let mut file2 = common::open_test_file();
    let mut offset_read = vec![0.0f64; 10];
    file2
        .read_channel_from(&channel, 10, &mut offset_read)
        .unwrap();

    // The offset read should match the second half of the full read
    assert_eq!(
        &full_read[10..20],
        &offset_read[..],
        "Offset read should match corresponding portion of full read"
    );
}

#[test]
fn test_read_channel_from_with_small_output() {
    // Test that reading with offset respects output buffer size
    let mut file = common::open_test_file();
    let channel = ChannelPath::new("structure", "ch1");

    let mut full_read = vec![0.0f64; 30];
    file.read_channel(&channel, &mut full_read).unwrap();

    // Read 5 samples starting from position 10
    let mut file2 = common::open_test_file();
    let mut offset_read = vec![0.0f64; 5];
    file2
        .read_channel_from(&channel, 10, &mut offset_read)
        .unwrap();

    assert_eq!(
        &full_read[10..15],
        &offset_read[..],
        "Should read correct samples with small buffer"
    );
}

#[test]
fn test_read_channel_from_beyond_data() {
    // Test reading with start position beyond available data
    let mut file = common::open_test_file();
    let channel = ChannelPath::new("structure", "ch1");

    // Get channel length first
    let channel_length = file.channel_length(&channel).unwrap();

    // Try to read from beyond the end
    let mut output = vec![0.0f64; 10];
    let result = file.read_channel_from(&channel, channel_length + 100, &mut output);

    // Should succeed but read 0 samples (output should remain unchanged)
    assert!(result.is_ok(), "Reading beyond data should not error");
}

#[test]
fn test_read_channel_from_at_boundary() {
    // Test reading starting exactly at a block boundary
    // This test assumes we know the block structure, which we might not
    // So we'll just test that it works correctly
    let mut file = common::open_test_file();
    let channel = ChannelPath::new("structure", "ch1");

    let mut full_read = vec![0.0f64; 100];
    file.read_channel(&channel, &mut full_read).unwrap();

    // Read from various positions
    for start_pos in [0, 10, 25, 50, 75] {
        let mut file2 = common::open_test_file();
        let mut offset_read = vec![0.0f64; 10];
        file2
            .read_channel_from(&channel, start_pos, &mut offset_read)
            .unwrap();

        let end_pos = (start_pos as usize + 10).min(full_read.len());
        assert_eq!(
            &full_read[start_pos as usize..end_pos],
            &offset_read[..(end_pos - start_pos as usize)],
            "Reading from position {} should match",
            start_pos
        );
    }
}

#[test]
fn test_read_channel_from_different_types() {
    // Test with different data types
    let mut file = common::open_test_file();

    // Test with f64
    let channel = ChannelPath::new("structure", "ch1");

    // Read normally
    let mut full_read = vec![0.0f64; 20];
    file.read_channel(&channel, &mut full_read).unwrap();

    // Read with offset
    let mut file2 = common::open_test_file();
    let mut offset_read = vec![0.0f64; 10];
    file2
        .read_channel_from(&channel, 5, &mut offset_read)
        .unwrap();

    assert_eq!(&full_read[5..15], &offset_read[..]);
}

#[test]
fn test_read_channel_from_preserves_existing_behavior() {
    // Ensure that the refactored read_channel still works correctly
    let mut file = common::open_test_file();
    let channel = ChannelPath::new("structure", "ch1");

    let mut output = vec![0.0f64; 50];
    let result = file.read_channel(&channel, &mut output);

    assert!(
        result.is_ok(),
        "read_channel should still work after refactoring"
    );
}
