use crate::paths::ChannelPath;
use crate::raw_data::BlockReadChannelConfig;
use crate::{TdmsFile, error::TdmsError, index::DataLocation, io::data_types::TdmsStorageType};

#[derive(Eq, PartialEq, Clone, Debug)]
struct ChannelReadPlan {
    index: usize,
    samples_to_skip: u64,
}

#[derive(Eq, PartialEq, Clone, Debug)]
struct BlockRead {
    ///The data block index/number.
    data_block: usize,
    ///The channel locations in this block.
    /// `None` means the channel has no data in this block.
    ///
    /// todo: can we avoid a vec here? It should be small
    /// so smallvec or array may work.
    channel_indexes: Vec<Option<ChannelReadPlan>>,
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

        let plan = read_plan(&[data_positions], &[start]);
        self.execute_read_plan(plan, &mut [output])
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

        let start_skips: Vec<u64> = vec![start; channels.len()];
        let plan = read_plan(&channel_positions[..], &start_skips);

        self.execute_read_plan(plan, output)
    }

    /// Execute a read plan, reading data from blocks into the output slices.
    ///
    /// This is the core read execution logic used by all read methods.
    /// The plan specifies which blocks to read and any per-channel skip amounts.
    fn execute_read_plan<D: TdmsStorageType>(
        &mut self,
        plan: Vec<BlockRead>,
        output: &mut [&mut [D]],
    ) -> Result<(), TdmsError> {
        let mut channel_progress: Vec<ChannelProgress> = output
            .iter()
            .map(|out_slice| ChannelProgress::new(out_slice.len()))
            .collect();

        for location in plan {
            // Check if any channel needs to skip at the start of this block
            let any_skip_needed = location
                .channel_indexes
                .iter()
                .any(|plan| plan.is_some() && plan.as_ref().unwrap().samples_to_skip > 0);

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
                    get_block_read_data_with_skip(&location, output, &channel_progress);
                block.read_with_per_channel_skip(&mut self.file, &mut channels_with_skip)?
            } else {
                let mut channels_to_read =
                    get_block_read_data(&location, output, &channel_progress);
                block.read(&mut self.file, &mut channels_to_read)?
            };

            // Update progress
            for (plan, progress) in location
                .channel_indexes
                .iter()
                .zip(channel_progress.iter_mut())
            {
                if plan.is_some() {
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
    location: &'a BlockRead,
    output: &'b mut [&'c mut [D]],
    channel_progress: &[ChannelProgress],
) -> Vec<(usize, &'o mut [D])> {
    location
        .channel_indexes
        .iter()
        .zip(output.iter_mut())
        .zip(channel_progress.iter())
        .filter_map(|((plan, output), progress)| {
            match (plan, progress) {
                // If we have hit our target, ignore this channel.
                (Some(_), progress) if progress.is_complete() => None,
                // More to read - include this channel.
                (Some(plan), progress) => Some((plan.index, &mut output[progress.samples_read..])),
                _ => None,
            }
        })
        .collect::<Vec<_>>()
}

/// Get the read parameters, output, and skip amounts for this particular block.
fn get_block_read_data_with_skip<'a, 'b: 'o, 'c: 'o, 'o, D: TdmsStorageType>(
    location: &'a BlockRead,
    output: &'b mut [&'c mut [D]],
    channel_progress: &[ChannelProgress],
) -> Vec<BlockReadChannelConfig<'b, D>> {
    location
        .channel_indexes
        .iter()
        .zip(output.iter_mut())
        .zip(channel_progress.iter())
        .filter_map(|((plan, output), progress)| {
            match (plan, progress) {
                // If we have hit our target, ignore this channel.
                (Some(_), progress) if progress.is_complete() => None,
                // More to read - include this channel with its skip amount.
                (Some(plan), progress) => Some(BlockReadChannelConfig {
                    channel_index: plan.index,
                    output: &mut output[progress.samples_read..],
                    samples_to_skip: plan.samples_to_skip,
                }),
                _ => None,
            }
        })
        .collect::<Vec<_>>()
}

fn all_channels_complete(channel_progress: &[ChannelProgress]) -> bool {
    channel_progress
        .iter()
        .all(|progress| progress.is_complete())
}

/// Plan the locations that we need to visit for each channel.
///
/// Blocks are skipped entirely when all channels can skip them.
/// The first block that needs reading for each channel includes the partial skip amount.
///
/// todo:: Can we make this an iterator to avoid the vec allocation.
/// todo: pretty sure we can use iterators more effectively here.
fn read_plan(channel_positions: &[&[DataLocation]], start_skips: &[u64]) -> Vec<BlockRead> {
    let channels = channel_positions.len();
    let mut next_location = vec![0usize; channels];
    let mut remaining_skips: Vec<u64> = start_skips.to_vec();
    let mut blocks: Vec<BlockRead> = Vec::new();

    loop {
        // Find the minimum data block among all channels' next locations
        let next_block = channel_positions
            .iter()
            .zip(next_location.iter())
            .filter_map(|(locations, &index)| locations.get(index).map(|loc| loc.data_block))
            .min();

        let Some(next_block) = next_block else {
            return blocks;
        };

        // Build channel read plans for this block
        let mut channel_read_plans: Vec<Option<ChannelReadPlan>> = Vec::with_capacity(channels);
        let mut any_needs_read = false;

        for ch_idx in 0..channels {
            let locations = &channel_positions[ch_idx];
            let loc_idx = next_location[ch_idx];

            match locations.get(loc_idx) {
                Some(loc) if loc.data_block == next_block => {
                    let block_samples = loc.number_of_samples;
                    let skip = remaining_skips[ch_idx];

                    if skip >= block_samples {
                        // Can skip entire block for this channel - don't include in read
                        channel_read_plans.push(None);
                    } else {
                        // Need to read from this block (possibly after partial skip)
                        any_needs_read = true;
                        channel_read_plans.push(Some(ChannelReadPlan {
                            index: loc.channel_index,
                            samples_to_skip: skip,
                        }));
                    }

                    // Advance to next location and update remaining skip
                    next_location[ch_idx] += 1;
                    remaining_skips[ch_idx] = remaining_skips[ch_idx].saturating_sub(block_samples);
                }
                _ => {
                    // Channel not in this block
                    channel_read_plans.push(None);
                }
            }
        }

        // Only add the block if at least one channel needs to read
        if any_needs_read {
            blocks.push(BlockRead {
                data_block: next_block,
                channel_indexes: channel_read_plans,
            });
        }
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

        let plan = read_plan(&[&channel_locations[..]], &[0]);

        let expected_plan = vec![
            BlockRead {
                data_block: 20,
                channel_indexes: vec![Some(ChannelReadPlan {
                    index: 1,
                    samples_to_skip: 0,
                })],
            },
            BlockRead {
                data_block: 21,
                channel_indexes: vec![Some(ChannelReadPlan {
                    index: 1,
                    samples_to_skip: 0,
                })],
            },
        ];

        assert_eq!(plan, expected_plan);
    }

    #[test]
    fn test_read_plan_single_channel_with_skip() {
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
            DataLocation {
                data_block: 22,
                channel_index: 1,
                number_of_samples: 1000,
            },
        ];

        // Skip 1500 samples: skip entire first block (1000), partial skip on second (500)
        let plan = read_plan(&[&channel_locations[..]], &[1500]);

        let expected_plan = vec![
            BlockRead {
                data_block: 21,
                channel_indexes: vec![Some(ChannelReadPlan {
                    index: 1,
                    samples_to_skip: 500,
                })],
            },
            BlockRead {
                data_block: 22,
                channel_indexes: vec![Some(ChannelReadPlan {
                    index: 1,
                    samples_to_skip: 0,
                })],
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

        let plan = read_plan(&[&channel_location_1[..], &channel_location_2[..]], &[0, 0]);

        let expected_plan = vec![
            BlockRead {
                data_block: 20,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 0,
                    }),
                    Some(ChannelReadPlan {
                        index: 2,
                        samples_to_skip: 0,
                    }),
                ],
            },
            BlockRead {
                data_block: 21,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 0,
                    }),
                    Some(ChannelReadPlan {
                        index: 0,
                        samples_to_skip: 0,
                    }),
                ],
            },
        ];

        assert_eq!(plan, expected_plan);
    }

    #[test]
    fn test_read_plan_multi_channel_with_skip() {
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

        // Skip 500 for both channels - partial skip on first block
        let plan = read_plan(
            &[&channel_location_1[..], &channel_location_2[..]],
            &[500, 500],
        );

        let expected_plan = vec![
            BlockRead {
                data_block: 20,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 500,
                    }),
                    Some(ChannelReadPlan {
                        index: 2,
                        samples_to_skip: 500,
                    }),
                ],
            },
            BlockRead {
                data_block: 21,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 0,
                    }),
                    Some(ChannelReadPlan {
                        index: 0,
                        samples_to_skip: 0,
                    }),
                ],
            },
        ];

        assert_eq!(plan, expected_plan);
    }

    #[test]
    fn test_read_plan_multi_channel_skip_entire_block() {
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

        // Skip 1000 for both channels - skip entire first block
        let plan = read_plan(
            &[&channel_location_1[..], &channel_location_2[..]],
            &[1000, 1000],
        );

        let expected_plan = vec![BlockRead {
            data_block: 21,
            channel_indexes: vec![
                Some(ChannelReadPlan {
                    index: 1,
                    samples_to_skip: 0,
                }),
                Some(ChannelReadPlan {
                    index: 0,
                    samples_to_skip: 0,
                }),
            ],
        }];

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

        let plan = read_plan(&[&channel_location_1[..], &channel_location_2[..]], &[0, 0]);

        let expected_plan = vec![
            BlockRead {
                data_block: 20,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 0,
                    }),
                    Some(ChannelReadPlan {
                        index: 2,
                        samples_to_skip: 0,
                    }),
                ],
            },
            BlockRead {
                data_block: 21,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 0,
                    }),
                    Some(ChannelReadPlan {
                        index: 0,
                        samples_to_skip: 0,
                    }),
                ],
            },
            BlockRead {
                data_block: 22,
                channel_indexes: vec![
                    None,
                    Some(ChannelReadPlan {
                        index: 1,
                        samples_to_skip: 0,
                    }),
                ],
            },
            BlockRead {
                data_block: 25,
                channel_indexes: vec![
                    Some(ChannelReadPlan {
                        index: 0,
                        samples_to_skip: 0,
                    }),
                    None,
                ],
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
