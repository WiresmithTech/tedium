use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use tedium::{Segment, TdmsError};
fn main() {
    let path = std::env::args().nth(1).expect("Usage: tdmsdump <file>");
    let path = PathBuf::from(path);
    let mut file = std::fs::File::open(&path).expect("Failed to open file");


    let file_size = file.metadata().expect("Failed to get file metadata").len();
    let mut next_segment = 0;


    loop {
        println!("Reading segment at {}", next_segment);
        if next_segment >= file_size {
            break;
        }
        file.seek(SeekFrom::Start(next_segment)).expect("Failed to move to next segment");
        match Segment::read(&mut file) {
            Ok(segment) => {
                print_segment(&segment, next_segment as usize);
                next_segment = next_segment.checked_add(segment.total_size_bytes().unwrap()).expect("File Overflow");
            }
            Err(TdmsError::EndOfFile) => {
                panic!("Hit end of file unexpectedly");
                break;
            },
            Err(e) => {
                panic!("Error reading segment: {:?}", e);
            }
        }
    }


}

fn print_segment(segment: &Segment, start: usize) {
    println!("New Segment at {}", start);
    println!("TOC: {:?}", segment.toc);
    match segment.meta_data {
        None => println!("No meta data"),
        Some(ref meta) => {
            for object in &meta.objects {
                println!("Object: {}", object.path);
                println!("Properties:");
                for (key, value) in &object.properties {
                    println!("  {}: {:?}", key, value);
                }
                println!("Data Details: {:?}", object.raw_data_index);
            }
        }
    }
}