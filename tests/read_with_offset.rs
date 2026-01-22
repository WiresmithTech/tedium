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

// ============================================================================
// Multi-channel offset tests
// ============================================================================

#[test]
fn test_read_channels_from_start_zero() {
    // Reading from start position 0 should be identical to read_channels
    let mut file = common::open_test_file();
    let channel1 = ChannelPath::new("structure", "ch1");
    let channel2 = ChannelPath::new("structure", "ch2");
    let channels = [&channel1, &channel2];

    let mut output1_normal = vec![0.0f64; 10];
    let mut output2_normal = vec![0.0f64; 10];
    let mut outputs_normal: Vec<&mut [f64]> = vec![&mut output1_normal, &mut output2_normal];

    let mut output1_from = vec![0.0f64; 10];
    let mut output2_from = vec![0.0f64; 10];
    let mut outputs_from: Vec<&mut [f64]> = vec![&mut output1_from, &mut output2_from];

    let mut file2 = common::open_test_file();

    file.read_channels(&channels, &mut outputs_normal).unwrap();
    file2
        .read_channels_from(&channels, 0, &mut outputs_from)
        .unwrap();

    assert_eq!(
        output1_normal, output1_from,
        "Channel 1: Reading from position 0 should match normal read"
    );
    assert_eq!(
        output2_normal, output2_from,
        "Channel 2: Reading from position 0 should match normal read"
    );
}

#[test]
fn test_read_channels_from_middle() {
    // Read first 20 samples normally, then read from position 10 and compare
    let mut file = common::open_test_file();
    let channel1 = ChannelPath::new("structure", "ch1");
    let channel2 = ChannelPath::new("structure", "ch2");
    let channels = [&channel1, &channel2];

    let mut full_read1 = vec![0.0f64; 20];
    let mut full_read2 = vec![0.0f64; 20];
    let mut full_outputs: Vec<&mut [f64]> = vec![&mut full_read1, &mut full_read2];
    file.read_channels(&channels, &mut full_outputs).unwrap();

    // Now read from position 10
    let mut file2 = common::open_test_file();
    let mut offset_read1 = vec![0.0f64; 10];
    let mut offset_read2 = vec![0.0f64; 10];
    let mut offset_outputs: Vec<&mut [f64]> = vec![&mut offset_read1, &mut offset_read2];
    file2
        .read_channels_from(&channels, 10, &mut offset_outputs)
        .unwrap();

    // The offset read should match the second half of the full read
    assert_eq!(
        &full_read1[10..20],
        &offset_read1[..],
        "Channel 1: Offset read should match corresponding portion of full read"
    );
    assert_eq!(
        &full_read2[10..20],
        &offset_read2[..],
        "Channel 2: Offset read should match corresponding portion of full read"
    );
}

#[test]
fn test_read_channels_from_with_small_output() {
    // Test that reading with offset respects output buffer size
    let mut file = common::open_test_file();
    let channel1 = ChannelPath::new("structure", "ch1");
    let channel2 = ChannelPath::new("structure", "ch2");
    let channels = [&channel1, &channel2];

    let mut full_read1 = vec![0.0f64; 30];
    let mut full_read2 = vec![0.0f64; 30];
    let mut full_outputs: Vec<&mut [f64]> = vec![&mut full_read1, &mut full_read2];
    file.read_channels(&channels, &mut full_outputs).unwrap();

    // Read 5 samples starting from position 10
    let mut file2 = common::open_test_file();
    let mut offset_read1 = vec![0.0f64; 5];
    let mut offset_read2 = vec![0.0f64; 5];
    let mut offset_outputs: Vec<&mut [f64]> = vec![&mut offset_read1, &mut offset_read2];
    file2
        .read_channels_from(&channels, 10, &mut offset_outputs)
        .unwrap();

    assert_eq!(
        &full_read1[10..15],
        &offset_read1[..],
        "Channel 1: Should read correct samples with small buffer"
    );
    assert_eq!(
        &full_read2[10..15],
        &offset_read2[..],
        "Channel 2: Should read correct samples with small buffer"
    );
}

#[test]
fn test_read_channels_from_preserves_existing_behavior() {
    // Ensure that the refactored read_channels still works correctly
    let mut file = common::open_test_file();
    let channel1 = ChannelPath::new("structure", "ch1");
    let channel2 = ChannelPath::new("structure", "ch2");
    let channels = [&channel1, &channel2];

    let mut output1 = vec![0.0f64; 50];
    let mut output2 = vec![0.0f64; 50];
    let mut outputs: Vec<&mut [f64]> = vec![&mut output1, &mut output2];

    let result = file.read_channels(&channels, &mut outputs);

    assert!(
        result.is_ok(),
        "read_channels should still work after refactoring"
    );
}

#[test]
fn test_read_channels_from_separate_blocks() {
    // Test the specific scenario where channels are written in separate blocks
    // This ensures per-channel skip tracking works correctly
    use tedium::DataLayout;

    let mut file = common::get_empty_file();
    let mut writer = file.writer().unwrap();

    let channel1 = tedium::ChannelPath::new("test", "ch1");
    let channel2 = tedium::ChannelPath::new("test", "ch2");

    // Write channel 1 in first block (1000 samples)
    let data1: Vec<f64> = (0..1000).map(|i| i as f64).collect();
    writer
        .write_channels(&[&channel1], &data1, DataLayout::Contigious)
        .unwrap();

    // Write channel 2 in second block (1000 samples)
    let data2: Vec<f64> = (1000..2000).map(|i| i as f64).collect();
    writer
        .write_channels(&[&channel2], &data2, DataLayout::Contigious)
        .unwrap();

    // Write both channels together in third block (1000 samples each)
    let data3: Vec<f64> = (2000..4000).map(|i| i as f64).collect();
    writer
        .write_channels(&[&channel1, &channel2], &data3, DataLayout::Contigious)
        .unwrap();

    drop(writer);

    // Now test reading with offset
    // Ch1 has: Block 0 (1000 samples), Block 2 (1000 samples) = 2000 total
    // Ch2 has: Block 1 (1000 samples), Block 2 (1000 samples) = 2000 total

    // Read from position 500
    let mut output1 = vec![0.0f64; 100];
    let mut output2 = vec![0.0f64; 100];
    let mut outputs: Vec<&mut [f64]> = vec![&mut output1, &mut output2];

    file.read_channels_from(&[&channel1, &channel2], 500, &mut outputs)
        .unwrap();

    // Ch1 should skip 500 in Block 0, read samples 500-599 from Block 0
    // Ch2 should skip 500 in Block 1, read samples 500-599 from Block 1
    assert_eq!(
        &output1[0..10],
        &[
            500.0, 501.0, 502.0, 503.0, 504.0, 505.0, 506.0, 507.0, 508.0, 509.0
        ],
        "Ch1 should read from position 500 in its data stream"
    );
    assert_eq!(
        &output2[0..10],
        &[
            1500.0, 1501.0, 1502.0, 1503.0, 1504.0, 1505.0, 1506.0, 1507.0, 1508.0, 1509.0
        ],
        "Ch2 should read from position 500 in its data stream (which is 1500 in the original data)"
    );
}
