use crate::paths::ChannelPath;
use crate::{TdmsFile, error::TdmsError, index::DataLocation, io::data_types::TdmsStorageType};

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
    /// Remaining samples this channel needs to skip
    samples_to_skip: u64,
}

impl ChannelProgress {
    fn new(samples_target: usize) -> Self {
        Self {
            samples_read: 0,
            samples_target,
            samples_to_skip: 0,
        }
    }

    fn new_with_offset(samples_target: usize, start_offset: u64) -> Self {
        Self {
            samples_read: 0,
            samples_target,
            samples_to_skip: start_offset,
        }
    }

    fn is_complete(&self) -> bool {
        self.samples_read >= self.samples_target
    }

    fn add_samples(&mut self, samples: usize) {
        self.samples_read += samples;
    }
}

impl<F: std::io::Read + std::io::Seek> TdmsFile<F> {
    /// Get the length of the channel.
    pub fn channel_length(&self, channel: &ChannelPath) -> Option<u64> {
        self.index.channel_length(channel)
    }

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
        self.read_channel_from(channel, 0, output)
    }

    /// Read a single channel from the tdms file starting at a specific sample position.
    ///
    /// channel should provide a path to the channel.
    /// start is the number of samples to skip before reading.
    /// output is a mutable slice for the data to be written into.
    ///
    /// If there is more data in the file than the size of the slice, we will stop reading at the end of the slice.
    ///
    /// # Performance
    ///
    /// This method optimizes reading by skipping entire data blocks when possible.
    /// For example, if you want to start reading at sample 1500 and the first block contains
    /// 1000 samples, it will skip the entire first block and start reading from sample 500
    /// of the second block.
    pub fn read_channel_from<D: TdmsStorageType>(
        &mut self,
        channel: &ChannelPath,
        start: u64,
        output: &mut [D],
    ) -> Result<(), TdmsError> {
        let data_positions = self
            .index
            .get_channel_data_positions(channel)
            .ok_or_else(|| TdmsError::MissingObject(channel.path().to_owned()))?;

        let mut samples_to_skip = start;
        let mut samples_read = 0;

        for location in data_positions {
            // Skip entire blocks if possible
            if samples_to_skip >= location.number_of_samples {
                samples_to_skip -= location.number_of_samples;
                continue;
            }

            let block = self
                .index
                .get_data_block(location.data_block)
                .ok_or_else(|| {
                    TdmsError::DataBlockNotFound(channel.clone(), location.data_block)
                })?;

            // Read from this block with offset
            samples_read += block.read_single_from(
                location.channel_index,
                samples_to_skip,
                &mut self.file,
                &mut output[samples_read..],
            )?;

            // After the first partial read, no more samples to skip
            samples_to_skip = 0;

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
    pub fn read_channels<D: TdmsStorageType>(
        &mut self,
        channels: &[impl AsRef<ChannelPath>],
        output: &mut [&mut [D]],
    ) -> Result<(), TdmsError> {
        self.read_channels_from(channels, 0, output)
    }

    /// Read multiple channels from the tdms file starting at a specific sample position.
    ///
    /// All channels will start reading from the same sample offset.
    /// This is efficient for time-aligned data where all channels share the same time base.
    ///
    /// channels should provide a slice of paths to the channels.
    /// start is the number of samples to skip before reading (same for all channels).
    /// output is a set of mutable slices for the data to be written into.
    /// Each channel will be read for the length of its corresponding slice.
    ///
    /// # Performance
    ///
    /// This method optimizes reading by skipping entire data blocks when possible.
    /// A block is only skipped if all channels have their start position beyond that block.
    pub fn read_channels_from<D: TdmsStorageType>(
        &mut self,
        channels: &[impl AsRef<ChannelPath>],
        start: u64,
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
            .map(|out_slice| ChannelProgress::new_with_offset(out_slice.len(), start))
            .collect();

        for location in read_plan {
            // Calculate per-channel skip amounts for this block
            // We need two lists: one for all channels (for progress tracking)
            // and one for only channels being read (for the read method)
            let mut all_channel_skips = Vec::with_capacity(location.channel_indexes.len());
            let mut read_channel_skips = Vec::new();
            let mut any_skip_needed = false;
            let mut any_channel_needs_read = false;

            for (ch_idx_in_list, (ch_idx_in_block, progress)) in location
                .channel_indexes
                .iter()
                .zip(channel_progress.iter())
                .enumerate()
            {
                if ch_idx_in_block.is_some() && !progress.is_complete() {
                    // Get the number of samples this channel has in this block
                    let block_samples =
                        get_channel_samples_in_block(&location, &channel_positions, ch_idx_in_list);
                    let skip = progress.samples_to_skip.min(block_samples);
                    all_channel_skips.push(skip);
                    read_channel_skips.push(skip);

                    if skip > 0 {
                        any_skip_needed = true;
                    }
                    if skip < block_samples {
                        any_channel_needs_read = true;
                    }
                } else {
                    all_channel_skips.push(0);
                }
            }

            // If no channel needs to read, skip this block entirely
            if !any_channel_needs_read {
                // Update skip progress for all channels
                for (progress, &skip) in channel_progress.iter_mut().zip(all_channel_skips.iter()) {
                    progress.samples_to_skip = progress.samples_to_skip.saturating_sub(skip);
                }
                continue;
            }

            let block = self
                .index
                .get_data_block(location.data_block)
                .ok_or_else(|| {
                    TdmsError::DataBlockNotFound(
                        ChannelPath::new("MIXED", "MIXED"),
                        location.data_block,
                    )
                })?;

            // Use fast path if no skip needed, slow path otherwise
            let location_samples_read = if any_skip_needed {
                let mut channels_with_skip =
                    get_block_read_data_with_skip(&location, output, &channel_progress, &read_channel_skips);
                block.read_with_per_channel_skip(&mut self.file, &mut channels_with_skip)?
            } else {
                let mut channels_to_read =
                    get_block_read_data(&location, output, &channel_progress);
                block.read(&mut self.file, &mut channels_to_read)?
            };

            // Update progress: record skipped samples and read samples
            for (ch_idx, (progress, &skip)) in location
                .channel_indexes
                .iter()
                .zip(channel_progress.iter_mut().zip(all_channel_skips.iter()))
            {
                if ch_idx.is_some() {
                    progress.samples_to_skip = progress.samples_to_skip.saturating_sub(skip);
                    progress.add_samples(location_samples_read);
                }
            }

            if all_channels_complete(&channel_progress) {
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

/// Get the read parameters, output, and skip amounts for this particular block.
///
/// skip_amounts should only contain skips for channels that are present in this block.
fn get_block_read_data_with_skip<'a, 'b: 'o, 'c: 'o, 'o, D: TdmsStorageType>(
    location: &'a MultiChannelLocation,
    output: &'b mut [&'c mut [D]],
    channel_progress: &[ChannelProgress],
    skip_amounts: &[u64],
) -> Vec<(usize, &'o mut [D], u64)> {
    let mut skip_idx = 0;
    location
        .channel_indexes
        .iter()
        .zip(output.iter_mut())
        .zip(channel_progress.iter())
        .filter_map(|((channel_id, output), progress)| {
            match (channel_id, progress) {
                // If we have it our target, ignore this channel.
                (Some(_), progress) if progress.is_complete() => None,
                // More to read - include this channel with its skip amount.
                (Some(idx), progress) => {
                    let skip = skip_amounts[skip_idx];
                    skip_idx += 1;
                    Some((*idx, &mut output[progress.samples_read..], skip))
                }
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
    assert_eq!(channel_progress.len(), location.channel_indexes.len());

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

/// Get the number of samples for a specific channel in a block.
///
/// Returns 0 if the channel is not present in this block.
fn get_channel_samples_in_block(
    location: &MultiChannelLocation,
    channel_positions: &[&[DataLocation]],
    channel_idx: usize,
) -> u64 {
    // Find the data location for this channel in this block
    for data_loc in channel_positions[channel_idx] {
        if data_loc.data_block == location.data_block {
            return data_loc.number_of_samples;
        }
    }
    0
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
                let next_location = locations.get(*index)?;

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
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 21,
                channel_index: 1,
                number_of_samples: 1000,
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
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 21,
                channel_index: 1,
                number_of_samples: 1000,
            },
        ];

        let channel_location_2 = vec![
            DataLocation {
                data_block: 20,
                channel_index: 2,
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 21,
                channel_index: 0,
                number_of_samples: 1000,
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
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 21,
                channel_index: 1,
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 25,
                channel_index: 0,
                number_of_samples: 1000,
            },
        ];

        let channel_location_2 = vec![
            DataLocation {
                data_block: 20,
                channel_index: 2,
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 21,
                channel_index: 0,
                number_of_samples: 1000,
            },
            DataLocation {
                data_block: 22,
                channel_index: 1,
                number_of_samples: 1000,
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
