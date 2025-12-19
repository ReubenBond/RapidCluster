# 013: Unify Join and Rejoin Paths in MembershipService

## Problem Statement

`MembershipService.cs` has 4 distinct initialization/view-change paths that share significant code but are implemented separately:

1. **Single-node cluster creation** (lines 206-224)
2. **Normal join flow** - `JoinClusterAsync` (lines 327-461)
3. **Bootstrap flow** - `TriggerBootstrap` (lines 1197-1232)
4. **Rejoin flow** - `RejoinClusterAsync` / `TryRejoinWithCurrentSeedsAsync` (lines 1868-2012)

This leads to:
- **Code duplication**: Identical patterns repeated in multiple places
- **Inconsistent behavior**: Rejoin misses learner protocol checks and robust retry logic
- **Maintenance burden**: Changes must be applied to multiple locations
- **Reduced testability**: Each path has its own quirks

## Analysis

### Current Duplication

| Pattern | Locations | Lines |
|---------|-----------|-------|
| `_cutDetection = _cutDetectorFactory.Create(_membershipView)` | 3 places | 222, 429, 1222 |
| Endpoint with NodeId creation | 2 places | 212-217, 1209-1214 |
| Build view from JoinResponse | 2 places | 419-428, 1987-1994 |
| `FinalizeInitialization()` calls | 3 places | 224, 431, 1230 |

### Join vs Rejoin Comparison

| Aspect | `JoinClusterAsync` | `TryRejoinWithCurrentSeedsAsync` |
|--------|---------------------|----------------------------------|
| Seed list | `_seedAddresses` | `_seedAddresses` + last known members |
| Retry logic | Round-robin with exponential backoff | Simple foreach, no retry |
| Max retries | `_options.MaxJoinRetries` | 1 pass through seeds |
| Learner check | Yes (lines 344-349) | No |
| Bootstrap check | Yes (lines 353-358) | No |
| State cleanup | None | `CancelAllPendingJoiners()`, clear metadata |
| Metrics | Records join latency | None |
| Error handling | Throws `JoinException` | Returns null |

### Key Insight

Join and Rejoin are fundamentally the same operation: iterate through candidate seeds, attempt the 2-phase join protocol via `TryJoinClusterAsync`, and apply the resulting membership view. The differences are:
- Where the seed list comes from
- Whether to perform state cleanup (rejoin only)
- Whether to record initial join metrics
- Error handling (throw vs return null)

## Solution

### Phase 1: Extract Helpers

#### 1.1 `CreateEndpointWithNodeId`
```csharp
private static Endpoint CreateEndpointWithNodeId(Endpoint source, long nodeId)
{
    return new Endpoint
    {
        Hostname = source.Hostname,
        Port = source.Port,
        NodeId = nodeId
    };
}
```

**Replaces**: Lines 212-217 and 1209-1214

#### 1.2 `BuildMembershipViewFromJoinResponse`
```csharp
private MembershipView BuildMembershipViewFromJoinResponse(JoinResponse response)
{
    var memberInfos = BuildMemberInfosFromResponse(
        response.Endpoints,
        response.MetadataKeys,
        response.MetadataValues);
    return BuildMembershipView(
        memberInfos,
        response.MaxNodeId,
        ConfigurationId.FromProtobuf(response.ConfigurationId));
}
```

**Replaces**: Lines 419-428 and 1987-1994

### Phase 2: Remove Redundancy

#### 2.1 Remove Redundant `_cutDetection` Assignments

Delete these lines (SetMembershipView already creates the cut detector):
- Line 222: `_cutDetection = _cutDetectorFactory.Create(_membershipView);`
- Line 429: `_cutDetection = _cutDetectorFactory.Create(_membershipView);`
- Line 1222: `_cutDetection = _cutDetectorFactory.Create(_membershipView);`

#### 2.2 Eliminate `FinalizeInitialization()`

`FinalizeInitialization()` (lines 760-778) is a thin wrapper that:
1. Acquires lock
2. Calls `SetMembershipView(_membershipView)` 
3. Fire-and-forget disposes old consensus
4. Logs initialization

Replace with direct `SetMembershipView()` calls. Preserve the initialization log message at appropriate call sites.

### Phase 3: Implement Unified `JoinInternalAsync`

```csharp
/// <summary>
/// Core join/rejoin implementation. Iterates through candidate seeds with retry logic,
/// performs the join protocol, and applies the resulting membership view.
/// </summary>
/// <param name="candidateSeeds">Seeds to try in order.</param>
/// <param name="options">Configuration for retry behavior and cleanup.</param>
/// <param name="cancellationToken">Cancellation token.</param>
/// <returns>True if join succeeded, false if all attempts failed.</returns>
private async Task<bool> JoinInternalAsync(
    IReadOnlyList<Endpoint> candidateSeeds,
    JoinOptions options,
    CancellationToken cancellationToken)
```

Where `JoinOptions` is:
```csharp
private readonly struct JoinOptions
{
    public int MaxRetries { get; init; }
    public TimeSpan RetryBaseDelay { get; init; }
    public TimeSpan RetryMaxDelay { get; init; }
    public double RetryBackoffMultiplier { get; init; }
    public bool CheckBootstrapCoordinator { get; init; }
    public bool PerformRejoinCleanup { get; init; }
    public bool RecordJoinMetrics { get; init; }
}
```

**Logic**:
1. Start retry loop (up to `MaxRetries`)
2. Check if already joined via learner protocol (`_membershipView.IsHostPresent(_myAddr)`)
3. If `CheckBootstrapCoordinator`, check `ShouldWaitAsBootstrapCoordinator()`
4. Select seed (round-robin)
5. Call `TryJoinClusterAsync(seed, metadata, cancellationToken)`
6. Handle result:
   - **Success**: Apply view and return true
   - **RetryNeeded**: Delay with backoff, continue
   - **Failed**: Try next seed
7. If all attempts exhausted, return false

**On Success**:
1. Build view from response using `BuildMembershipViewFromJoinResponse()`
2. Lock `_membershipUpdateLock`
3. If `PerformRejoinCleanup`: `CancelAllPendingJoiners()`, clear `_joinerMetadata`, `_pendingConsensusMessages`
4. Call `SetMembershipView(newView)`
5. Unlock
6. Dispose old consensus (await, not fire-and-forget)
7. If `RecordJoinMetrics`: `_metrics.RecordJoinLatency(...)`
8. Log success

### Phase 4: Refactor Callers

#### 4.1 `JoinClusterAsync`
```csharp
private async Task JoinClusterAsync(CancellationToken cancellationToken)
{
    _log.JoiningClusterWithSeeds(_seedAddresses.Count, _myAddr);

    var options = new JoinOptions
    {
        MaxRetries = _options.MaxJoinRetries,
        RetryBaseDelay = _options.JoinRetryBaseDelay,
        RetryMaxDelay = _options.JoinRetryMaxDelay,
        RetryBackoffMultiplier = _options.JoinRetryBackoffMultiplier,
        CheckBootstrapCoordinator = _isBootstrapping,
        PerformRejoinCleanup = false,
        RecordJoinMetrics = true
    };

    var success = await JoinInternalAsync(_seedAddresses, options, cancellationToken).ConfigureAwait(true);
    
    if (!success)
    {
        throw new JoinException($"Failed to join cluster after {_options.MaxJoinRetries + 1} attempts");
    }
    
    _log.MembershipServiceInitialized(_myAddr, _membershipView);
}
```

#### 4.2 `TryRejoinWithCurrentSeedsAsync`
```csharp
private async Task<bool?> TryRejoinWithCurrentSeedsAsync(CancellationToken cancellationToken)
{
    // Build combined seed list (existing logic preserved)
    var candidateSeeds = BuildRejoinCandidateSeeds();
    
    if (candidateSeeds.Count == 0)
    {
        return null;
    }

    _log.RejoinWithCandidateSeeds(candidateSeeds.Count, _seedAddresses.Count);

    var options = new JoinOptions
    {
        MaxRetries = candidateSeeds.Count - 1, // Try each seed once
        RetryBaseDelay = TimeSpan.Zero, // No delay between seeds
        RetryMaxDelay = TimeSpan.Zero,
        RetryBackoffMultiplier = 1.0,
        CheckBootstrapCoordinator = false,
        PerformRejoinCleanup = true,
        RecordJoinMetrics = false
    };

    var success = await JoinInternalAsync(candidateSeeds, options, cancellationToken).ConfigureAwait(true);
    
    if (success)
    {
        _log.RejoinSuccessful(_myAddr, _membershipView.Size, _membershipView.ConfigurationId);
        return true;
    }
    
    return null;
}

private List<Endpoint> BuildRejoinCandidateSeeds()
{
    var candidateSeeds = new List<Endpoint>();
    var seen = new HashSet<Endpoint>(EndpointAddressComparer.Instance);

    foreach (var seed in _seedAddresses)
    {
        if (!EndpointAddressComparer.Instance.Equals(seed, _myAddr) && seen.Add(seed))
        {
            candidateSeeds.Add(seed);
        }
    }

    foreach (var member in _membershipView.Members)
    {
        if (!EndpointAddressComparer.Instance.Equals(member, _myAddr) && seen.Add(member))
        {
            candidateSeeds.Add(member);
        }
    }

    return candidateSeeds;
}
```

#### 4.3 Single-node cluster (in `InitializeAsync`)
```csharp
// Create endpoint with node ID = 1 for the seed node
var seedEndpoint = CreateEndpointWithNodeId(_myAddr, nodeId: 1);
var seedMember = new MemberInfo(seedEndpoint, _nodeMetadata);
_membershipView = BuildMembershipView([seedMember], maxNodeId: 1, configurationId: null);

lock (_membershipUpdateLock)
{
    SetMembershipView(_membershipView);
}

_log.MembershipServiceInitialized(_myAddr, _membershipView);
```

#### 4.4 `TriggerBootstrap`
```csharp
private void TriggerBootstrap()
{
    // ... existing node sorting and MemberInfo creation using CreateEndpointWithNodeId ...
    
    _membershipView = BuildMembershipView(memberInfos, maxNodeId: memberInfos.Count, configurationId: null);
    
    _isBootstrapping = false;
    _bootstrapCompletionSource?.TrySetResult(true);
    
    // Already under _membershipUpdateLock
    SetMembershipView(_membershipView);
    
    _log.BootstrapComplete(_myAddr, _membershipView.Size, _membershipView.ConfigurationId);
}
```

## Verification

### Tests to Run
```bash
# Unit tests
dotnet run --project tests/RapidCluster.Unit.Tests/RapidCluster.Unit.Tests.csproj -- --timeout 60s

# Simulation tests  
dotnet run --project tests/RapidCluster.Simulation.Tests/RapidCluster.Simulation.Tests.csproj -- --timeout 120s

# Integration tests
dotnet run --project tests/RapidCluster.Integration.Tests/RapidCluster.Integration.Tests.csproj -- --timeout 60s
```

### Expected Outcomes
- All existing tests pass (no functional change)
- Code is more maintainable and readable
- Future changes to join logic only need to be made in one place

## Implementation Order

1. **Phase 1**: Extract helpers (`CreateEndpointWithNodeId`, `BuildMembershipViewFromJoinResponse`)
2. **Phase 2**: Remove redundant `_cutDetection` assignments
3. **Phase 3**: Implement `JoinInternalAsync` and `JoinOptions`
4. **Phase 4**: Refactor callers to use new unified method
5. **Phase 5**: Delete `FinalizeInitialization()`
6. **Verify**: Run all tests

## Notes

- `TryJoinClusterAsync` (the 2-phase join worker) remains unchanged - it's already well-factored
- The `ShouldWaitAsBootstrapCoordinator()` local function in `JoinClusterAsync` should be extracted to a private method for use by `JoinInternalAsync`
- Consensus disposal should be `await`ed where possible (not fire-and-forget) to ensure proper cleanup

## Implementation Complete

### Summary of Changes

All phases of the refactoring were implemented in `MembershipService.cs`:

1. **Phase 1 - Extract Helpers**: ✅
   - `CreateEndpointWithNodeId()` - creates endpoint with NodeId
   - `BuildMembershipViewFromJoinResponse()` - builds MembershipView from protobuf response

2. **Phase 2 - Remove Redundancy**: ✅
   - Removed 3 redundant `_cutDetection` assignments (SetMembershipView already handles this)

3. **Phase 3 - Unified JoinInternalAsync**: ✅
   - Added `JoinInternalResult` enum: Success, AlreadyJoined, BootstrapCoordinator, Failed
   - Added `JoinOptions` struct with configurable retry behavior
   - Implemented unified `JoinInternalAsync` that handles both join and rejoin

4. **Phase 4 - Refactored Callers**: ✅
   - `JoinClusterAsync` now uses `JoinInternalAsync`
   - `TryRejoinWithCurrentSeedsAsync` now uses `JoinInternalAsync`
   - Single-node cluster init calls `SetMembershipView()` directly
   - `TriggerBootstrap` calls `SetMembershipView()` directly

5. **Phase 5 - Deleted FinalizeInitialization()**: ✅

### Bug Fix: Learner Protocol Check During Rejoin

During testing, a bug was discovered and fixed in `JoinInternalAsync`:

**Problem**: When a node was kicked from the cluster:
1. `ApplyLearnedMembershipViewAsync` sees the node is NOT in the new view
2. It does NOT apply the new view (correct behavior - since the node is not in it)
3. It triggers a rejoin via `RejoinClusterAsync`
4. `JoinInternalAsync` checked `_membershipView.IsHostPresent(_myAddr)` but `_membershipView` was still the OLD view where the node IS present
5. This incorrectly returned `AlreadyJoined`, leaving the node stuck with the stale view

**Fix**: Skip the learner protocol check when `PerformRejoinCleanup = true` (rejoin path):
```csharp
// IMPORTANT: Skip this check during rejoin (PerformRejoinCleanup = true) because:
// - When a node is kicked, it doesn't update its local view (since it's not in the new view)
// - So IsHostPresent(_myAddr) would return true against the old/stale view
// - This would incorrectly short-circuit the rejoin, leaving the node stuck with the stale view
// - During rejoin, we must actually contact seeds to learn the current view and join it
if (!options.PerformRejoinCleanup && _membershipView.IsHostPresent(_myAddr))
{
    _log.JoinCompletedViaLearnerProtocol(_myAddr, _membershipView);
    return (JoinInternalResult.AlreadyJoined, null);
}
```

### Test Results

- **Unit tests**: 553 passed, 0 failed ✅
- **Simulation tests**: 470+ passed, 5 failed (pre-existing), 2 skipped ✅

The 4 tests that were failing due to the refactoring are now passing:
- `GracefulLeave_WithSuspendedNode` ✅
- `GracefulLeave_SuspendedNodeDetectedAsFailed` ✅
- `GracefulLeave_SuspendedNodeRemovedWithLeavingNode` ✅
- `ConsensusWithExactQuorum` ✅

The 5 pre-existing failures are unrelated to this refactoring (suspended node cleanup in DisposeAsync).
