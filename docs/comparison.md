# Comparison to Other Libraries

This isn't really for marketing - I'm just trying to get my head around what is a useful feature set to aim for.

| Feature          | Tedium | LabVIEW | C    | Matlab   | Python (npTDMS)  |
|------------------|--------|---------|------|----------|------------------|
| Read Channels    |   ✅   |    ✅  |  ✅  |   ✅    |   ✅            |
| Read Group Data  |   ✅3  |    ✅  |  ✅  |   ✅    |                  |
| Read Random Access|   4    |    ✅  |  ✅6 |    1.    |   ✅5           |
| Read Raw Segment |         |   ✅2  |      |          |   ✅5           |
| Read String Chans.|   4    |   ✅   |  ✅  |    ?    |                  |
| Read DAQmx Data   |   4    |    ✅  |  ✅  |    ?    |    ✅           |
| Read Waveforms    |        |    ✅  |  ✅  |         |    ✅           |
| Write Any Channels|  ✅   |    ✅   | ✅  |          |   ✅            |
| Write Entire Group|  ✅3  |    ✅   |      |    ✅   |   ✅            |
| Stream into segment | 4    |    ✅2 |      |          |   ✅            |



1. Matlab does allow you to limit the amount of data read each read and starts from the end of the previous read when used as datastore. It also as a time-based method
2. Advanced API
3. No Seperate API for this
4. Planned
5. It can create an iterator for each segment data for a channel. You can call into subsets but loads entire segments to do this.
6. As a single channel