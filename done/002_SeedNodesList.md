# Support Multiple Seed Nodes for Join/Rejoin Operations

## Status: COMPLETED

## Problem

Currently, `RapidClusterOptions.SeedAddress` only supports a single seed node endpoint. This creates several issues:

1. **Single point of failure during join**: If the configured seed node is down when a new node tries to join, the join fails even if other cluster members are healthy.

2. **Limited resilience during rejoin**: When a node is kicked and needs to rejoin, it currently only uses members from its last known `MembershipView`. If those members have also failed, it cannot rejoin even if the original seed nodes are still alive.

3. **Operational complexity**: Operators must ensure the single seed node is always available, or reconfigure joining nodes if the seed changes.

## Proposed Solution

### 1. Extend `RapidClusterOptions` with Multiple Seed Nodes

Replace the single `SeedAddress` with a list `SeedAddresses`:

```csharp
public sealed class RapidClusterOptions
{
    // ... existing properties ...

    /// <summary>
    /// The seed node endpoints to join. If null/empty or contains only ListenAddress, starts a new cluster.
    /// Nodes are tried in round-robin order until join succeeds.
    /// </summary>
    public List<Endpoint>? SeedAddresses { get; set; }
}
```

### 2. Modify Join Logic for Round-Robin Seed Selection

Update `MembershipService.JoinClusterAsync()` to cycle through seed nodes:

- Maintain a seed index that advances on each retry attempt
- Use modulo arithmetic to cycle: `seedIndex % seedAddresses.Count`
- `MaxJoinRetries` applies to total attempts across all seeds (not per-seed)
- Log which seed is being tried on each attempt

**Pseudocode:**
```
seedIndex = 0
for attempt = 0 to MaxJoinRetries:
    currentSeed = seedAddresses[seedIndex % seedAddresses.Count]
    seedIndex++
    
    result = TryJoinClusterAsync(currentSeed)
    if result.Success:
        return success
    // Transient or retryable failure - continue with next seed
```

### 3. Modify Rejoin Logic to Supplement with Configured Seeds

Update `MembershipService.RejoinClusterAsync()` to combine seed sources:

1. Start with configured `SeedAddresses` (try these first)
2. Append members from last known `MembershipView` (excluding self)
3. Deduplicate the combined list (using `EndpointAddressComparer`)
4. Iterate through combined list trying each as a potential seed

**Rationale for order**: Configured seeds are tried first because:
- They are explicitly designated as stable/well-known entry points
- Last known view members may have changed since we were kicked
- Operators expect configured seeds to be the primary contact points

## Implementation Details

### Files to Modify

1. **`RapidClusterOptions.cs`**
   - Replace `SeedAddress` with `SeedAddresses` (list)

2. **`MembershipService.cs`**
   - Store `_seedAddresses` (list) instead of `_seedAddress` (single)
   - Modify `InitializeAsync()` to check if any seed matches self (start new cluster)
   - Modify `JoinClusterAsync()` for round-robin iteration
   - Modify `RejoinClusterAsync()` to combine seeds + last known members

3. **Logging (`MembershipServiceLogger.cs`)**
   - Add log messages for seed selection during join attempts

### Edge Cases

1. **Empty seed list**: Treated same as starting a new cluster
2. **All seeds are self**: Start a new cluster
3. **Duplicate seeds in config**: Deduplicate before use
4. **Seed becomes available mid-retry**: Round-robin naturally retries it
5. **All seeds permanently fail**: After `MaxJoinRetries` total attempts, throw `JoinException`

## Testing

### Unit Tests
- `RapidClusterOptions` property behavior

### Simulation Tests
- Join with multiple seeds where first N seeds are down
- Rejoin combining configured seeds + last known view
- Round-robin behavior across retry attempts

### Integration Tests
- Join cluster with multiple seed endpoints configured
- Seed failover during join process

## Implementation Notes

### Changes Made

1. **`src/RapidCluster.Core/RapidClusterOptions.cs`**
   - Replaced `SeedAddress` (single `Endpoint?`) with `SeedAddresses` (`IList<Endpoint>?`)
   - Added pragma to suppress CA2227 (collection properties should be read-only) since Options classes need settable collections

2. **`src/RapidCluster.Core/MembershipService.cs`**
   - Changed `_seedAddress` field to `_seedAddresses` (list)
   - Constructor processes seed addresses: filters out self, removes duplicates while preserving order (important for deterministic behavior)
   - `InitializeAsync()` checks if seed list is empty (start new cluster) vs non-empty (join)
   - `JoinClusterAsync()` cycles through seeds in round-robin fashion - `MaxJoinRetries` is total attempts across all seeds
   - `TryJoinClusterAsync()` accepts seed address as parameter
   - `RejoinClusterAsync()` combines configured seeds first, then members from last known `MembershipView`, with deduplication

3. **`src/RapidCluster.Core/Logging/MembershipServiceLogger.cs`**
   - Added new logging methods: `JoiningClusterWithSeeds`, `JoinAttemptWithSeed`, `RejoinWithCandidateSeeds`

4. **Updated all test files** that referenced `SeedAddress` to use `SeedAddresses = [address]`:
   - `tests/RapidCluster.Tests/Simulation/Infrastructure/RapidSimulationNode.cs`
   - `tests/RapidCluster.Tests/Unit/RapidClusterOptionsTests.cs`
   - `tests/RapidCluster.Tests/Integration/TestCluster.cs`

5. **Updated sample:**
   - `samples/RapidCluster.Examples/Program.cs`

6. **Updated README.md:**
   - Changed example code and configuration table to reference `SeedAddresses`

### Key Design Decisions

- **Order preservation**: Seed nodes are tried in the order provided in config. Deduplication preserves first-occurrence order for deterministic behavior.
- **Round-robin**: On each retry attempt, advance to the next seed (`seedIndex % seedAddresses.Count`)
- **Rejoin order**: Configured seeds first, then last known view members (excluding self), deduplicated
- **MaxJoinRetries**: Total attempts across all seeds, not per-seed
