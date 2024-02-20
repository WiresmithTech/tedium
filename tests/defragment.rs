mod common;
use tedium::{ChannelPath, PropertyPath, PropertyValue, TdmsFile};


#[test]
fn test_defragment() {
    // get the test file
    let mut input_file = common::open_test_file();

    // create a new tdms file for our defragmented output
    let path = std::path::Path::new("tests/test_defragment_output.tdms");
    let mut output_file = TdmsFile::create(path).unwrap();

    // get the properties of the input file and write them to the properties of the output file
    let mut all_properties = input_file.read_all_properties(&PropertyPath::file()).unwrap();

    let mut writer = output_file.writer().unwrap();

    // convert all_properties to a `&[(&str, PropertyValue)]` we can pass to the writer.write_properties method
    let mut properties: Vec<(&str, PropertyValue)> = Vec::new();
    for (key, value) in all_properties.iter() {
        let property = value.clone();
        properties.push((key.as_str(), property.clone()));
    }

    writer.write_properties(&PropertyPath::file(), &properties).unwrap();

    // First, collect all group names
    let groups: Vec<PropertyPath> = input_file.list_groups().into_iter().collect();

    // print the number of groups
    println!("num groups: {:?}", groups.len());

    // Then iterate over the collected group names
    for group_path in groups {

        // print the group path
        println!("group_path: {:?}", group_path);

        // Collect channel names for each group before mutating `input_file`
        let channels: Vec<ChannelPath> = input_file.list_channels_in_group(&group_path).into_iter().collect();

        // print the number of channels
        println!("num channels: {:?}", channels.len());

        for channel_path in channels {

            // print the channel path
            println!("channel_path: {:?}", channel_path);

            let channel_length = input_file.channel_length(&channel_path).unwrap();

            // print the channel length
            println!("channel_length: {:?}", channel_length);

            // create a buffer to hold the channel data, and read the channel data into it
            let mut data = vec![0.0; channel_length as usize];

            // Since we're no longer borrowing `input_file` to list groups/channels, this mutable borrow is okay
            input_file.read_channel(&channel_path, &mut data).unwrap();

            // print the data
            println!("data: {:?}", data);

            // Write the channel data to the output file
            writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
        }
    }

}
