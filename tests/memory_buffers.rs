use std::io::Cursor;
use tedium::{ChannelPath, DataLayout, TdmsFile};

#[test]
fn test_can_write_and_read_from_buffer() {
    let mut buffer = Cursor::new(Vec::new());
    let mut file = TdmsFile::new(&mut buffer).unwrap();

    let data_to_write = vec![1.0, 2.0, 3.0];

    {
        let mut writer = file.writer().unwrap();
        writer
            .write_channels(
                &[&ChannelPath::new("group", "channel")],
                &data_to_write[..],
                DataLayout::Interleaved,
            )
            .unwrap();
    }

    // Verify reading from the open buffer file that we have just written to.
    let mut output_buffer = vec![0.0; 3];
    file.read_channel(
        &ChannelPath::new("group", "channel"),
        &mut output_buffer[..],
    )
    .unwrap();
    assert_eq!(output_buffer, data_to_write);

    // Reload the buffer and verify that we can read the data again.
    let mut file2 = TdmsFile::new(&mut buffer).unwrap();
    let mut output_buffer = vec![0.0; 3];
    file2
        .read_channel(
            &ChannelPath::new("group", "channel"),
            &mut output_buffer[..],
        )
        .unwrap();
    assert_eq!(output_buffer, data_to_write);
}
