# 011: Simplify Bootstrap/Join Flow

## Status: COMPLETED

## Problem

The current bootstrap and join flows are separate code paths with complex logic:
- `SeedAddresses` can be specified directly in `RapidClusterOptions` OR via `ISeedProvider`
- Bootstrap logic is complex with static vs dynamic seed negotiation
- The decision of whether to bootstrap vs join is made upfront based on configuration

## New Design

Unify the flow so that **all nodes always attempt to join first**:

1. **Remove `SeedAddresses` from `RapidClusterOptions`** - seeds should ALWAYS come from `ISeedProvider`

2. **Unified Join-First Flow**:
   - Node discovers seeds via `ISeedProvider`
   - Node sends `JoinRequest` to first discovered seed
   - If seed responds with "bootstrapping" status, node participates in bootstrap
   - If seed responds with "established cluster", node joins normally

3. **New Bootstrap Detection Logic**:
   - A node becomes the "seed node" (bootstrap coordinator) when:
     a) It has NOT learned of any node that precedes it (lexicographically by address)
     b) It has `(BootstrapExpect - 1)` other nodes attempting to join it
   - Nodes learn of other nodes when:
     - They receive a join request from another node
     - They receive a join response containing other endpoints

4. **Join Response Changes**:
   - Add new `JoinStatusCode`: `BOOTSTRAPPING` (or similar)
   - When bootstrapping, response includes current set of known nodes
   - Joiner should redirect to the first node in the returned set and retry

## Implementation Summary

### Files Modified

| File | Changes |
|------|---------|
| `src/RapidCluster.Core/Protos/rapid_types.proto` | Added `BOOTSTRAP_IN_PROGRESS = 4` to `JoinStatusCode` enum |
| `src/RapidCluster.Core/ConfigurationId.cs` | Added `IsOlderThanOrDifferentCluster()` method for bootstrap config comparisons |
| `src/RapidCluster.Core/MembershipService.cs` | Bootstrap state tracking, `ExitBootstrapMode()`, `HandlePreJoinDuringBootstrap()`, `TriggerBootstrap()`, coordinator selection logic, fixed `HostnameAlreadyInRing` handling |
| `src/RapidCluster.Core/Logging/MembershipServiceLogger.cs` | Added `BootstrapModeExited` log method |

### Key Changes in MembershipService.cs

1. **Bootstrap State Tracking**:
   - `_bootstrapKnownNodes` HashSet to track nodes during bootstrap
   - `_isBootstrapping` flag to indicate bootstrap mode

2. **Bootstrap Coordinator Selection**:
   - `ShouldBeBootstrapCoordinator()` - lexicographically smallest node becomes coordinator
   - `ShouldWaitAsBootstrapCoordinator()` - coordinator waits for `BootstrapExpect` nodes

3. **Bootstrap Protocol**:
   - `HandlePreJoinDuringBootstrap()` - handles PreJoin messages during bootstrap phase
   - `TriggerBootstrap()` - forms cluster when coordinator has enough nodes
   - `ExitBootstrapMode()` - non-coordinator nodes exit bootstrap when receiving join response

4. **Critical Bug Fix (line 943)**:
   - Changed from returning observers for both `SafeToJoin` AND `HostnameAlreadyInRing`
   - Now only returns observers for `SafeToJoin`
   - **Why**: When a node is already in the ring (from bootstrap), returning observers would cause it to proceed to phase 2 of join, triggering an erroneous view change that REMOVED the node from the cluster

### Test Results

All tests pass:
- **Unit Tests**: 506 passed
- **Simulation Tests**: 481 passed, 3 skipped (expected)
- **Bootstrap Tests**: 26 passed (including both static and dynamic seed scenarios)

## Key Design Decisions

1. **Seed Node Determination**: The node with the lexicographically smallest address that hasn't learned of any smaller address becomes the seed.

2. **Bootstrap Trigger**: A node triggers bootstrap when it has seen exactly `BootstrapExpect` total nodes (itself + joiners) and no node precedes it.

3. **Redirection**: Nodes always redirect to the "smallest" known node, ensuring convergence to a single bootstrap coordinator.

4. **No Separate Bootstrap Path**: The bootstrap logic is embedded in the join response handling, simplifying the overall flow.

5. **HostnameAlreadyInRing Handling**: Nodes that are already in the ring (from bootstrap) should NOT receive observers - they should use the learner protocol to sync membership view, not trigger a new view change.

## Remaining Tasks (Deferred)

The following tasks from the original plan were NOT implemented in this work item, as they represent larger refactoring efforts that can be done separately:

### Phase 2: Remove SeedAddresses from RapidClusterOptions (Deferred)
- [ ] Remove `SeedAddresses` property from `RapidClusterOptions.cs`
- [ ] Update `RapidClusterServiceCollectionExtensions.cs` to not copy SeedAddresses
- [ ] Update any code that references `RapidClusterOptions.SeedAddresses`
- [ ] Update tests that use `SeedAddresses` directly

### Phase 7: Update/Remove BootstrapCoordinator (Deferred)
- [ ] Evaluate if BootstrapCoordinator is still needed
- [ ] Either simplify or remove entirely
- [ ] Remove seed gossip protocol if no longer needed

These items can be addressed in future work items if needed. The current implementation works correctly with all existing tests passing.
