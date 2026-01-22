# Multi-Channel Start Position API - Revised Design

## Problem with Initial Implementation

The initial implementation assumed a uniform start position would work for all channels. However, this breaks when channels are written in separate blocks:

```
Block 0: [Ch1: 1000 samples]
Block 1: [Ch2: 1000 samples]
Block 2: [Ch1: 1000 samples][Ch2: 1000 samples]

read_channels_from([Ch1, Ch2], start=500, ...)

Expected behavior:
- Ch1: Skip 500 in Block 0, read 500 from Block 0, read all of Block 2
- Ch2: Skip 500 in Block 1, read 500 from Block 1, read all of Block 2

Current (broken) behavior:
- Ch1: Skip 500 in Block 0 ✓
- Ch2: Skip 500 in Block 2 ✗ (should skip in Block 1)
```

The issue is that each channel needs to track its own skip progress independently.

## Revised Design

### Key Insight

While the API accepts a uniform start position, internally we need to track skip progress per channel because:
1. Channels may appear in different blocks
2. Channels may have different amounts of data in each block
3. Each channel needs to skip `start` samples from its own data stream

### Approach

1. **API remains simple**: `read_channels_from(channels, start, output)` - uniform start for all
2. **Internal tracking is per-channel**: Each channel tracks how many samples it has skipped
3. **Optimization**: Use fast path when no channels need skipping in a block

### Implementation Strategy

#### 1. Per-Channel Skip Tracking

```rust
struct ChannelProgress {
    samples_read: usize,
    samples_target: usize,
    samples_to_skip: u64,  // Remaining samples this channel needs to skip
}
```

Each channel independently tracks:
- How many samples it still needs to skip
- How many samples it has read
- When it's complete

#### 2. Block-Level Skip Calculation

For each block, calculate per-channel skip amounts:

```rust
// For each channel in the block
for (channel_idx, progress) in channels.iter().zip(progress.iter()) {
    let skip_in_this_block = min(progress.samples_to_skip, block_samples_for_channel);
    channel_skips.push(skip_in_this_block);
}
```

#### 3. Fast Path Optimization

```rust
// Check if any channel needs to skip in this block
let any_skip_needed = channel_skips.iter().any(|&skip| skip > 0);

if !any_skip_needed {
    // Use existing fast read method
    block.read(&mut file, &mut channels_to_read)?;
} else {
    // Use new per-channel skip method
    block.read_with_per_channel_skip(&mut file, &mut channels_to_read, &channel_skips)?;
}
```

This ensures:
- **90% case (no skip)**: Uses existing fast code path
- **10% case (with skip)**: Uses new slower but correct code path

### DataBlock API Changes

Add a new method that accepts per-channel skip amounts:

```rust
impl DataBlock {
    /// Read multiple channels with per-channel skip amounts.
    ///
    /// The skip_amounts slice must have the same length as channels_to_read.
    /// Each element specifies how many samples to skip for that channel in this block.
    pub fn read_with_per_channel_skip<'b, D: TdmsStorageType>(
        &self,
        reader: &mut (impl Read + Seek),
        channels_to_read: &'b mut [(usize, &'b mut [D])],
        skip_amounts: &[u64],
    ) -> Result<usize, TdmsError>
```

### Contiguous Layout Implementation

For contiguous data `[Ch1 S0][Ch1 S1]...[Ch1 SN][Ch2 S0][Ch2 S1]...[Ch2 SN]`:

```rust
fn read_with_per_channel_skip<D: TdmsStorageType>(
    &mut self,
    channels: &mut RecordStructure<'_, D>,
    skip_amounts: &[u64],
) -> Result<usize, TdmsError> {
    let mut length = 0;
    let mut skip_idx = 0;
    
    for read_instruction in channels.read_instructions().iter_mut() {
        match &mut read_instruction.plan {
            RecordEntryPlan::Read(output) => {
                let skip = skip_amounts[skip_idx] as usize;
                skip_idx += 1;
                
                // Skip samples by seeking
                if skip > 0 {
                    let skip_bytes = skip as i64 * D::SIZE_BYTES as i64;
                    self.reader.move_position(skip_bytes)?;
                }
                
                // Read remaining samples
                let samples_to_read = read_instruction.length.saturating_sub(skip);
                for _ in 0..samples_to_read {
                    let read_value = self.reader.read_value()?;
                    if let Some(value) = output.next() {
                        *value = read_value;
                    }
                }
                length = samples_to_read;
            }
            RecordEntryPlan::Skip(bytes) => {
                let skip_bytes = *bytes * read_instruction.length as i64;
                self.reader.move_position(skip_bytes)?;
            }
        };
    }
    
    Ok(length)
}
```

### Interleaved Layout Implementation

For interleaved data `[Ch1 S0][Ch2 S0][Ch1 S1][Ch2 S1]...`:

**Challenge**: Cannot skip different amounts per channel efficiently because data is interleaved row-by-row.

**Solution**: Skip the minimum across all channels, then read and discard for channels that need more skipping:

```rust
fn read_with_per_channel_skip<D: TdmsStorageType>(
    &mut self,
    mut channels: RecordStructure<D>,
    skip_amounts: &[u64],
) -> Result<usize, TdmsError> {
    // Find minimum skip (we can skip entire rows up to this point)
    let min_skip = skip_amounts.iter().copied().min().unwrap_or(0);
    
    // Skip entire rows
    if min_skip > 0 {
        let skip_bytes = min_skip as i64 * channels.row_size() as i64;
        self.reader.move_position(skip_bytes)?;
    }
    
    // Calculate remaining skip per channel
    let remaining_skips: Vec<u64> = skip_amounts.iter()
        .map(|&skip| skip.saturating_sub(min_skip))
        .collect();
    
    // Read rows, discarding samples for channels that still need to skip
    let total_rows = self.block_size.get() as usize / channels.row_size();
    let rows_to_process = total_rows.saturating_sub(min_skip as usize);
    
    let mut samples_read = 0;
    for row in 0..rows_to_process {
        let mut channel_idx = 0;
        for read_instruction in channels.read_instructions().iter_mut() {
            match &mut read_instruction.plan {
                RecordEntryPlan::Read(output) => {
                    let read_value = self.reader.read_value()?;
                    
                    // Only write if we've skipped enough for this channel
                    if row as u64 >= remaining_skips[channel_idx] {
                        if let Some(value) = output.next() {
                            *value = read_value;
                        }
                    }
                    channel_idx += 1;
                }
                RecordEntryPlan::Skip(bytes) => {
                    self.reader.move_position(*bytes)?;
                }
            };
        }
        samples_read += 1;
    }
    
    Ok(samples_read)
}
```

### Updated read_channels_from Implementation

```rust
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

    // Initialize progress with uniform start offset for all channels
    let mut channel_progress: Vec<ChannelProgress> = output
        .iter()
        .map(|out_slice| ChannelProgress::new_with_offset(out_slice.len(), start))
        .collect();

    for location in read_plan {
        // Calculate per-channel skip amounts for this block
        let mut channel_skips = Vec::new();
        let mut any_skip_needed = false;
        
        for (ch_idx, progress) in location.channel_indexes.iter().zip(channel_progress.iter()) {
            if ch_idx.is_some() && !progress.is_complete() {
                // Get the number of samples this channel has in this block
                let block_samples = get_channel_samples_in_block(&location, &channel_positions, ch_idx);
                let skip = progress.samples_to_skip.min(block_samples);
                channel_skips.push(skip);
                if skip > 0 {
                    any_skip_needed = true;
                }
            } else {
                channel_skips.push(0);
            }
        }

        // Skip entire block if no channels need to read
        if location.channel_indexes.iter().all(|idx| idx.is_none()) {
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

        let mut channels_to_read = get_block_read_data(&location, output, &channel_progress);

        // Use fast path if no skip needed, slow path otherwise
        let location_samples_read = if any_skip_needed {
            block.read_with_per_channel_skip(&mut self.file, &mut channels_to_read, &channel_skips)?
        } else {
            block.read(&mut self.file, &mut channels_to_read)?
        };

        // Update progress: record skipped samples and read samples
        for (ch_idx, (progress, &skip)) in location
            .channel_indexes
            .iter()
            .zip(channel_progress.iter_mut().zip(channel_skips.iter()))
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
```

## Benefits of This Approach

1. **Correctness**: Each channel independently tracks its skip progress
2. **Performance**: Fast path for the common case (no skip needed)
3. **Simplicity**: API remains simple with uniform start position
4. **Flexibility**: Can be extended to per-channel start positions if needed

## Testing Strategy

Add tests for:
1. Channels written in separate blocks with offset
2. Channels with different amounts of data per block
3. Mixed scenarios (some channels in block, some not)
4. Performance comparison: with skip vs without skip

## Migration Path

The existing `read_channels()` continues to work unchanged. The new `read_channels_from()` is additive.
