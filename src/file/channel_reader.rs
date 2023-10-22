use crate::paths::ChannelPath;
use crate::{error::TdmsError, index::DataLocation, io::data_types::TdmsStorageType, TdmsFile};

#[derive(Eq, PartialEq, Clone, Debug)]
struct MultiChannelLocation {
    ///The data block index/number.
    data_block: usize,
    ///The channel locations in this block.
    /// `None` means the channel has no data in this block.
    ///
    /// todo: can we avoid a vec here? It should be small
    /// so smallvec or array may work.
    channel_indexes: Vec<Option<usize>>,
}

#[derive(Eq, PartialEq, Clone, Debug)]
struct ChannelProgress {
    samples_read: usize,
    samples_target: usize,
}

impl ChannelProgress {
    fn new(samples_target: usize) -> Self {
        Self {
            samples_read: 0,
            samples_target,
        }
    }

    fn is_complete(&self) -> bool {
        self.samples_read >= self.samples_target
    }

    fn add_samples(&mut self, samples: usize) {
        self.samples_read += samples;
    }
}

impl<F: std::io::Read + std::io::Seek + std::io::Write + std::fmt::Debug> TdmsFile<F> {
    /// Read a single channel from the tdms file.
    ///
    /// channel should provide a path to the channel and output is a mutable slice for the data to be written into.
    ///
    /// If there is more data in the file than the size of the slice, we will stop reading at the end of the slice.
    pub fn read_channel<D: TdmsStorageType>(
        &mut self,
        channel: &ChannelPath,
        output: &mut [D],
    ) -> Result<(), TdmsError> {
        let data_positions = self
            .index
            .get_channel_data_positions(channel)
            .ok_or_else(|| TdmsError::MissingObject(channel.path().to_owned()))?;

        let mut samples_read = 0;

        for location in data_positions {
            let block = self
                .index
                .get_data_block(location.data_block)
                .ok_or_else(|| {
                    TdmsError::DataBlockNotFound(channel.clone(), location.data_block)
                })?;

            samples_read += block.read_single(
                location.channel_index,
                &mut self.file,
                &mut output[samples_read..],
            )?;

            if samples_read >= output.len() {
                break;
            }
        }

        Ok(())
    }

    /// Read multiple channels from the tdms file.
    ///
    /// channels should provide a slice of paths to the channels and output is a set of  mutable slice for the data to be written into.
    /// Each channel will be read for the length of its corresponding slice.
    pub fn read_channels<'a, D: TdmsStorageType>(
        &mut self,
        channels: &[impl AsRef<ChannelPath>],
        output: &mut [&mut [D]],
    ) -> Result<(), TdmsError> {
        let channel_positions = channels
            .iter()
            .map(|channel| {
                self.index
                    .get_channel_data_positions(channel.as_ref())
                    .ok_or_else(|| TdmsError::MissingObject(channel.as_ref().path().to_owned()))
            })
            .collect::<Result<Vec<&[DataLocation]>, TdmsError>>()?;

        let read_plan = read_plan(&channel_positions[..]);

        let mut channel_progress: Vec<ChannelProgress> = output
            .iter()
            .map(|out_slice| ChannelProgress::new(out_slice.len()))
            .collect();

        for location in read_plan {
            let block = self
                .index
                .get_data_block(location.data_block)
                .ok_or_else(|| {
                    TdmsError::DataBlockNotFound(
                        ChannelPath::new("MIXED", "MIXED"),
                        location.data_block,
                    )
                })?;

            let mut channels_to_read = get_block_read_data(&location, output, &channel_progress);

            let location_samples_read = block.read(&mut self.file, &mut channels_to_read)?;

            let read_complete =
                update_progress(location, &mut channel_progress, location_samples_read);

            if read_complete {
                break;
            }
        }

        Ok(())
    }
}

/// Get the read parameters and output for this particular block.
fn get_block_read_data<'a, 'b: 'o, 'c: 'o, 'o, D: TdmsStorageType>(
    location: &'a MultiChannelLocation,
    output: &'b mut [&'c mut [D]],
    channel_progress: &[ChannelProgress],
) -> Vec<(usize, &'o mut [D])> {
    location
        .channel_indexes
        .iter()
        .zip(output.iter_mut())
        .zip(channel_progress.iter())
        .filter_map(|((channel_id, output), progress)| {
            match (channel_id, progress) {
                // If we have it our target, ignore this channel.
                (Some(_), progress) if progress.is_complete() => None,
                // More to read - include this channel.
                (Some(idx), progress) => Some((*idx, &mut output[progress.samples_read..])),
                _ => None,
            }
        })
        .collect::<Vec<_>>()
}

/// Update the progress of the channels we have read.
///
/// Returns true if all are complete.
fn update_progress(
    location: MultiChannelLocation,
    channel_progress: &mut [ChannelProgress],
    iteration_samples: usize,
) -> bool {
    assert!(channel_progress.len() == location.channel_indexes.len());

    for (ch_idx, block_idx) in location.channel_indexes.iter().enumerate() {
        if block_idx.is_some() {
            channel_progress[ch_idx].add_samples(iteration_samples);
        }
    }
    all_channels_complete(channel_progress)
}

fn all_channels_complete(channel_progress: &[ChannelProgress]) -> bool {
    channel_progress
        .iter()
        .all(|progress| progress.is_complete())
}

/// Plan the locations that we need to visit for each channel.
///
/// todo:: Can we make this an iterator to avoid the vec allocation.
/// todo: pretty sure we can use iterators more effectively here.
fn read_plan(channel_positions: &[&[DataLocation]]) -> Vec<MultiChannelLocation> {
    let channels = channel_positions.len();
    let mut next_location = vec![0usize; channels];
    let mut blocks: Vec<MultiChannelLocation> = Vec::new();

    loop {
        let next_block = channel_positions
            .iter()
            .zip(next_location.iter())
            .map(|(locations, &index)| {
                if let Some(location) = locations.get(index) {
                    location.data_block
                } else {
                    usize::MAX
                }
            })
            .min();

        // Empty iterator check.
        let Some(next_block) = next_block else {
            return blocks;
        };

        //All out of range check.
        if next_block == usize::MAX {
            return blocks;
        };

        let channel_indexes: Vec<Option<usize>> = channel_positions
            .iter()
            .zip(next_location.iter_mut())
            .map(|(locations, index)| {
                let next_location = locations.get(*index);
                let Some(next_location) = next_location else {
                    return None;
                };

                if next_location.data_block == next_block {
                    *index += 1;
                    Some(next_location.channel_index)
                } else {
                    None
                }
            })
            .collect();

        blocks.push(MultiChannelLocation {
            data_block: next_block,
            channel_indexes,
        })
    }
}

#[cfg(test)]
mod tests {

    use crate::index::DataLocation;

    use super::*;

    #[test]
    fn test_read_plan_single_channel() {
        let channel_locations = vec![
            DataLocation {
                data_block: 20,
                channel_index: 1,
            },
            DataLocation {
                data_block: 21,
                channel_index: 1,
            },
        ];

        let plan = read_plan(&[&channel_locations[..]]);

        let expected_plan = vec![
            MultiChannelLocation {
                data_block: 20,
                channel_indexes: vec![Some(1)],
            },
            MultiChannelLocation {
                data_block: 21,
                channel_indexes: vec![Some(1)],
            },
        ];

        assert_eq!(plan, expected_plan);
    }

    #[test]
    fn test_read_plan_multi_channel_simple() {
        let channel_location_1 = vec![
            DataLocation {
                data_block: 20,
                channel_index: 1,
            },
            DataLocation {
                data_block: 21,
                channel_index: 1,
            },
        ];

        let channel_location_2 = vec![
            DataLocation {
                data_block: 20,
                channel_index: 2,
            },
            DataLocation {
                data_block: 21,
                channel_index: 0,
            },
        ];

        let plan = read_plan(&[&channel_location_1[..], &channel_location_2[..]]);

        let expected_plan = vec![
            MultiChannelLocation {
                data_block: 20,
                channel_indexes: vec![Some(1), Some(2)],
            },
            MultiChannelLocation {
                data_block: 21,
                channel_indexes: vec![Some(1), Some(0)],
            },
        ];

        assert_eq!(plan, expected_plan);
    }

    #[test]
    fn test_read_plan_multi_channel_complex() {
        let channel_location_1 = vec![
            DataLocation {
                data_block: 20,
                channel_index: 1,
            },
            DataLocation {
                data_block: 21,
                channel_index: 1,
            },
            DataLocation {
                data_block: 25,
                channel_index: 0,
            },
        ];

        let channel_location_2 = vec![
            DataLocation {
                data_block: 20,
                channel_index: 2,
            },
            DataLocation {
                data_block: 21,
                channel_index: 0,
            },
            DataLocation {
                data_block: 22,
                channel_index: 1,
            },
        ];

        let plan = read_plan(&[&channel_location_1[..], &channel_location_2[..]]);

        let expected_plan = vec![
            MultiChannelLocation {
                data_block: 20,
                channel_indexes: vec![Some(1), Some(2)],
            },
            MultiChannelLocation {
                data_block: 21,
                channel_indexes: vec![Some(1), Some(0)],
            },
            MultiChannelLocation {
                data_block: 22,
                channel_indexes: vec![None, Some(1)],
            },
            MultiChannelLocation {
                data_block: 25,
                channel_indexes: vec![Some(0), None],
            },
        ];

        assert_eq!(plan, expected_plan);
    }

    #[test]
    fn test_progress_complete() {
        let mut progress = ChannelProgress::new(10);
        progress.add_samples(5);
        progress.add_samples(5);

        assert!(progress.is_complete());
    }

    #[test]
    fn test_progress_complete_over() {
        let mut progress = ChannelProgress::new(10);
        progress.add_samples(5);
        progress.add_samples(6);

        assert!(progress.is_complete());
    }
}
