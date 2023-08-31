use crate::{error::TdmsError, index::DataLocation, TdmsFile};

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

impl TdmsFile {
    pub fn read_channel(&mut self, object_path: &str, output: &mut [f64]) -> Result<(), TdmsError> {
        let data_positions = self
            .index
            .get_channel_data_positions(object_path)
            .ok_or_else(|| TdmsError::MissingObject(object_path.to_string()))?;

        let mut samples_read = 0;

        for location in data_positions {
            let block = self
                .index
                .get_data_block(location.data_block)
                .ok_or_else(|| {
                    TdmsError::DataBlockNotFound(object_path.to_string(), location.data_block)
                })?;

            samples_read += block.read(
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

    pub fn read_channels(
        &mut self,
        paths: &[impl AsRef<str>],
        output: &mut [Vec<f64>],
    ) -> Result<(), TdmsError> {
        let data_positions = paths
            .iter()
            .map(|object_path| {
                self.index
                    .get_channel_data_positions(object_path.as_ref())
                    .ok_or_else(|| TdmsError::MissingObject(object_path.as_ref().to_string()))
            })
            .collect::<Result<Vec<&[DataLocation]>, TdmsError>>()?;

        let read_plan = read_plan(&data_positions[..]);

        let mut samples_read = vec![0; paths.len()];
        let sample_target: Vec<usize> = output.iter().map(|out_slice| out_slice.len()).collect();
        for location in read_plan {
            let block = self
                .index
                .get_data_block(location.data_block)
                .ok_or_else(|| {
                    TdmsError::DataBlockNotFound(
                        String::from("Multichannel read"),
                        location.data_block,
                    )
                })?;

            // Try reading each channel from each location.
            // Unfortunately this didn't help performance as I think each block read
            // still performs a seek which kills the buffering. Will need a multi-channel
            // read option to really boost performance.
            for ((channel, samples_read), output) in location
                .channel_indexes
                .iter()
                .zip(samples_read.iter_mut())
                .zip(output.iter_mut())
            {
                if let Some(index) = channel {
                    *samples_read +=
                        block.read(*index, &mut self.file, &mut output[*samples_read..])?;
                    if *samples_read >= output.len() {
                        //need to end future channel reads??
                    }
                }
            }

            if samples_read
                .iter()
                .zip(sample_target.iter())
                .all(|(read, target)| read == target)
            {
                break;
            }
        }

        Ok(())
    }
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
                    return location.data_block;
                } else {
                    return usize::MAX;
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
                    *index = *index + 1;
                    return Some(next_location.channel_index);
                } else {
                    return None;
                }
            })
            .collect();

        blocks.push(MultiChannelLocation {
            data_block: next_block,
            channel_indexes: channel_indexes,
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
}
