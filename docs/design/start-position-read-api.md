# Start Position Read API Design

## Overview

This document describes the design and implementation of the start position read API for TDMS files. This API allows reading channel data starting from a specific sample position, with support for both single-channel and multi-channel reads.

## Background

TDMS files store data in blocks, where each block can contain data for one or more channels. The data can be laid out in two ways:

1. **Contiguous**: `[Ch1 S0][Ch1 S1]...[Ch1 SN][Ch2 S0][Ch2 S1]...[Ch2 SN]`
2. **Interleaved**: `[Ch1 S0][Ch2 S0][Ch1 S1][Ch2 S1]...[Ch1 SN][Ch2 SN]`

Additionally, channels may be written in separate blocks:
```
Block 0: [Ch1: 1000 samples]
Block 1: [Ch2: 1000 samples]
Block 2: [Ch1: 1000 samples][Ch2: 1000 samples]
```

## Single-Channel API

### API Signature

```rust
pub fn read_channel_from<D: TdmsStorageType>(
    &mut self,
    channel: &ChannelPath,
    start: u64,
    output: &mut [D],
) -> Result<(), TdmsError>
```

### Implementation Strategy

1. **Block-Level Skipping**: Skip entire data blocks when `start >= block.number_of_samples`
2. **Within-Block Offset**: Use [`DataBlock::read_single_from()`](../../src/raw_data/mod.rs:251) for partial block reads
3. **Progress Tracking**: Track `samples_to_skip` and decrement as blocks are processed

### Example

```rust
// Channel has data in blocks: [Block 0: 1000][Block 1: 1000][Block 2: 500]
// Read from position 1500

// Block 0: Skip entirely (1500 >= 1000), samples_to_skip = 500
// Block 1: Skip entirely (500 >= 1000), samples_to_skip = 0  // Wait, this is wrong!
// Actually: Skip entirely (500 < 1000), so we DON'T skip, we read with offset 500
```

Correct flow:
```
samples_to_skip = 1500
Block 0 (1000 samples): samples_to_skip >= 1000, skip block, samples_to_skip = 500
Block 1 (1000 samples): samples_to_skip < 1000, read with offset 500, samples_to_skip = 0
Block 2 (500 samples): samples_to_skip = 0, read normally
```

## Multi-Channel API

### API Signature

```rust
pub fn read_channels_from<D: TdmsStorageType>(
    &mut self,
    channels: &[impl AsRef<ChannelPath>],
    start: u64,
    output: &mut [&mut [D]],
) -> Result<(), TdmsError>
```

### Design Challenge

While the API accepts a uniform `start` position for all channels, internally we need per-channel skip tracking because:

1. **Channels may appear in different blocks**
   ```
   Block 0: [Ch1: 1000 samples]
   Block 1: [Ch2: 1000 samples]
   ```
   With `start=500`:
   - Ch1 should skip 500 in Block 0
   - Ch2 should skip 500 in Block 1 (not Block 0!)

2. **Channels may have different amounts of data per block**
   ```
   Block 0: [Ch1: 1000 samples]
   Block 1: [Ch1: 500 samples][Ch2: 1500 samples]
   ```

### Implementation Strategy

#### 1. Per-Channel Skip Tracking

```rust
struct ChannelProgress {
    samples_read: usize,
    samples_target: usize,
    samples_to_skip: u64,  // Remaining samples this channel needs to skip
}
```

Each channel independently tracks how many samples it still needs to skip.

#### 2. Block Processing Algorithm

For each block in the read plan:

```rust
for location in read_plan {
    // Calculate per-channel skip amounts for this block
    for each channel in location {
        let block_samples = get_channel_samples_in_block(channel);
        let skip = min(progress.samples_to_skip, block_samples);
        channel_skips.push(skip);
        
        if skip > 0 {
            any_skip_needed = true;
        }
        if skip < block_samples {
            any_channel_needs_read = true;
        }
    }
    
    // Skip entire block if no channel needs to read
    if !any_channel_needs_read {
        update_skip_progress();
        continue;
    }
    
    // Read block using appropriate method
    if any_skip_needed {
        block.read_with_per_channel_skip(file, channels_with_skip)?;
    } else {
        block.read(file, channels_to_read)?;  // Fast path
    }
    
    // Update progress
    for each channel {
        progress.samples_to_skip -= skip;
        progress.samples_read += samples_read;
    }
}
```

#### 3. Fast Path Optimization

**90% case (no skip needed)**: Use existing [`DataBlock::read()`](../../src/raw_data/mod.rs:190)
- No skip calculation overhead
- Existing optimized code path

**10% case (skip needed)**: Use new [`DataBlock::read_with_per_channel_skip()`](../../src/raw_data/mod.rs:314)
- Per-channel skip handling
- Slightly slower but correct

### Data Structure Design

The review suggested including skip amounts in the `channels_to_read` structure:

```rust
// Instead of:
channels_to_read: &[(usize, &mut [D])]
skip_amounts: &[u64]

// Use:
channels_to_read: &[(usize, &mut [D], u64)]
```

This bundles the skip amount with each channel, making the API cleaner and reducing parameter count.

## Layout-Specific Implementations

### Contiguous Layout

For contiguous data, each channel's data is stored sequentially, so we can seek independently:

```rust
fn read_with_per_channel_skip<D: TdmsStorageType>(
    &mut self,
    channels: &mut RecordStructure<'_, D>,
    skip_amounts: &[u64],
) -> Result<usize, TdmsError> {
    let mut skip_idx = 0;
    
    for read_instruction in channels.read_instructions() {
        match read_instruction.plan {
            RecordEntryPlan::Read(output) => {
                let skip = skip_amounts[skip_idx];
                skip_idx += 1;
                
                // Skip samples by seeking
                if skip > 0 {
                    let skip_bytes = skip * D::SIZE_BYTES;
                    self.reader.move_position(skip_bytes)?;
                }
                
                // Read remaining samples
                let samples_to_read = read_instruction.length - skip;
                for _ in 0..samples_to_read {
                    let value = self.reader.read_value()?;
                    if let Some(out) = output.next() {
                        *out = value;
                    }
                }
            }
            RecordEntryPlan::Skip(bytes) => {
                self.reader.move_position(bytes * read_instruction.length)?;
            }
        }
    }
}
```

### Interleaved Layout

For interleaved data, we cannot skip different amounts per channel efficiently because data is stored row-by-row. Solution:

1. Skip the minimum across all channels (entire rows)
2. Read remaining rows, discarding samples for channels that need more skipping

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
        let skip_bytes = min_skip * channels.row_size();
        self.reader.move_position(skip_bytes)?;
    }
    
    // Calculate remaining skip per channel
    let remaining_skips: Vec<usize> = skip_amounts
        .iter()
        .map(|&skip| skip.saturating_sub(min_skip))
        .collect();
    
    // Read rows, discarding samples for channels that still need to skip
    let total_rows = self.block_size / channels.row_size();
    let rows_to_process = total_rows.saturating_sub(min_skip);
    
    for row in 0..rows_to_process {
        let mut channel_idx = 0;
        for read_instruction in channels.read_instructions() {
            match read_instruction.plan {
                RecordEntryPlan::Read(output) => {
                    let value = self.reader.read_value()?;
                    
                    // Only write if we've skipped enough for this channel
                    if row >= remaining_skips[channel_idx] {
                        if let Some(out) = output.next() {
                            *out = value;
                        }
                    }
                    channel_idx += 1;
                }
                RecordEntryPlan::Skip(bytes) => {
                    self.reader.move_position(bytes)?;
                }
            }
        }
    }
}
```

## Performance Characteristics

### Single-Channel Read

- **Block skipping**: O(blocks_to_skip) - just iteration, no I/O
- **Within-block seek**: O(1) - single seek operation
- **Read**: O(samples_to_read) - linear in output size

### Multi-Channel Read (No Skip)

- **Fast path**: Same as existing `read_channels()` implementation
- **No overhead**: Skip calculation is skipped when `start = 0`

### Multi-Channel Read (With Skip)

- **Contiguous layout**: O(channels) seeks + O(samples_to_read)
- **Interleaved layout**: O(rows_to_discard) + O(samples_to_read)
  - Slightly slower due to read-and-discard for differential skips
  - Still efficient because we skip entire rows when possible

## Testing Strategy

### Single-Channel Tests

1. ✅ Read from position 0 (should match normal read)
2. ✅ Read from middle position
3. ✅ Read with small output buffer
4. ✅ Read beyond available data
5. ✅ Read from various positions
6. ✅ Different data types
7. ✅ Backward compatibility

### Multi-Channel Tests

1. ✅ Read from position 0 (should match normal read)
2. ✅ Read from middle position
3. ✅ Read with small output buffers
4. ✅ Backward compatibility
5. ✅ Channels written in separate blocks (critical test case)

## Future Enhancements

### Per-Channel Start Positions

If needed, the API could be extended to support different start positions per channel:

```rust
pub fn read_channels_from_individual<D: TdmsStorageType>(
    &mut self,
    channels: &[(impl AsRef<ChannelPath>, u64)],  // (channel, start)
    output: &mut [&mut [D]],
) -> Result<(), TdmsError>
```

This would require minimal changes since the internal implementation already tracks skip per channel.

### Streaming API

For very large files, a streaming iterator API could be added:

```rust
pub fn channel_stream_from<D: TdmsStorageType>(
    &mut self,
    channel: &ChannelPath,
    start: u64,
) -> impl Iterator<Item = Result<D, TdmsError>>
```

## Summary

The start position read API provides efficient random access to TDMS file data:

- **Simple API**: Uniform start position for all channels
- **Efficient**: Block-level skipping and fast path optimization
- **Correct**: Per-channel skip tracking handles all edge cases
- **Flexible**: Can be extended to per-channel start positions if needed

The implementation balances simplicity, performance, and correctness by using per-channel tracking internally while presenting a simple uniform API externally.
