# Integration Tests

This folder contains a series of integration tests which can be run as usual with cargo.

These are based around the tdms-test-file.tdms which contains the following structure to exercise the library.

## Group "structure"

The structure group is designed to allow us to validate the different data read methods.

On the surface it has 6 channels (ch1 to ch6). ch1 to ch3 have 10000 elements so ch1 is 0..9999, ch2 is 10000..19999 etc.
ch4 to ch6 have 5000 elements following the same pattern.

Internally these are written in 10 segments each, 5 segments of each are decimated and 5 are contigious data.