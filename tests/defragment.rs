mod common;
use tedium::{ChannelPath, PropertyPath, PropertyValue, TdmsFile, DataType, TdmsFileWriter};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};


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

        println!("==== GROUP BEGIN =====");

        // print the group path
        println!("group_path: {:?}", group_path);

        // Collect channel names for each group before mutating `input_file`
        let channels: Vec<ChannelPath> = input_file.list_channels_in_group(&group_path).into_iter().collect();

        // print the number of channels
        println!("num channels: {:?}", channels.len());

        for channel_path in channels {

            println!("---- CHANNEL BEGIN ----");

            // print the channel path
            println!("channel_path: {:?}", channel_path);

            // #todo: refactor this into a `copy_channel()` function

            let channel_length = input_file.channel_length(&channel_path).unwrap();

            // print the channel length
            println!("channel_length: {:?}", channel_length);


            // print the channel type
            let channel_type = input_file.get_channel_type(&channel_path);
            match channel_type {
                Some(t) => println!("channel_type: {:?}", t),
                None => println!("channel_type: None"),
            }

            let channel_type = input_file.get_channel_type(&channel_path);

            match channel_type {

                Some(DataType::SingleFloat) => {
                    let mut data: Vec<f32> = vec![0.0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::DoubleFloat) => {
                    let mut data: Vec<f64> = vec![0.0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::I8) => {
                    let mut data: Vec<i8> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::I16) => {
                    let mut data: Vec<i16> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::I32) => {
                    let mut data: Vec<i32> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::I64) => {
                    let mut data: Vec<i64> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::U8) => {
                    let mut data: Vec<u8> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::U16) => {
                    let mut data: Vec<u16> = vec![0; channel_length as usize];
                },
                Some(DataType::U32) => {
                    let mut data: Vec<u32> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(DataType::U64) => {
                    let mut data: Vec<u64> = vec![0; channel_length as usize];
                    input_file.read_channel(&channel_path, &mut data).unwrap();
                    writer.write_channels(&[channel_path], &data, tedium::DataLayout::Interleaved).unwrap();
                },
                Some(data_type) => println!("Unsupported data type: {}", data_type),
                None => println!("None"),
            }

            println!("---- CHANNEL END ----");

        };
        println!("==== GROUP END =====");
    }


}
