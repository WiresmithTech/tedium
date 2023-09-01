use std::path::PathBuf;
use tdms_lib::TdmsFile;

/// Open the test file assuming this is called from the root of the project.
pub fn open_test_file() -> TdmsFile {
    let path = PathBuf::from("tests/tdms-test-file.tdms");
    TdmsFile::load(&path)
}
