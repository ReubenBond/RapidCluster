# 010: Bootstrap Seed Gossip Protocol

## Problem Description

When cluster seed nodes are discovered dynamically (via DNS, service discovery, Kubernetes, etc.), different nodes may see different seed lists at startup due to:
- DNS propagation delays
- Service discovery timing differences
- Load balancer randomization
- Cloud provider API eventual consistency

This can lead to **split-brain scenarios** where different groups of nodes form separate clusters, each computing a different ClusterId from their respective seed sets.

## Current State

The foundation for the seed gossip protocol has been implemented:

1. **`ISeedProvider` interface** returns `SeedDiscoveryResult` with:
   - `Seeds` - the discovered endpoints
   - `IsStatic` - boolean indicating if seeds are guaranteed identical across all nodes

2. **`RapidClusterSeedOptions`** has `IsStatic` flag (defaults to `false` for safety)

3. **`MembershipService`** stores `_seedsAreStatic` from the seed provider result

4. **Providers implemented**:
   - `ConfigurationSeedProvider` - reads from `RapidClusterSeedOptions`, honors `IsStatic` flag
   - `ServiceDiscoverySeedProvider` (Aspire) - returns `IsStatic = false`
   - `FileBasedBootstrapProvider` (E2E tests) - returns `IsStatic = true`

**What's NOT implemented:**
- The actual branching logic in `MembershipService.BootstrapClusterAsync()` based on `_seedsAreStatic`
- The seed gossip protocol messages and coordinator
- Proto messages for bootstrap negotiation

## Proposed Solution

### Algorithm Overview

When `IsStatic = false`, use a seed gossip protocol to ensure all bootstrap nodes agree on the same seed set before forming the cluster:

```
1. Start with ClusterId = 0 (special "forming" state)
2. Each node gossips its known seeds to other known seeds
3. Nodes merge received seeds (union), sort deterministically, take first BootstrapExpect
4. Once a node has BootstrapExpect seeds and all seeds agree on the same set, propose the hash
5. All nodes must agree on the same hash before forming
6. Only then compute final ClusterId and form cluster
```

### Key Design Decisions

1. **Separate `BootstrapCoordinator` class** - cleanly separated from `MembershipService`
2. **New proto messages** for seed gossip (not reusing existing consensus messages)
3. **Timeout handling** - nodes that don't respond within timeout are excluded
4. **Already-formed detection** - if a node discovers an already-formed cluster, abort bootstrap and join normally

### Implementation Plan

#### Phase 1: Proto Messages

Add to `src/RapidCluster.Core/Protos/rapid_types.proto`:

```protobuf
// Bootstrap negotiation status
enum BootstrapStatus {
  BOOTSTRAP_STATUS_UNSPECIFIED = 0;
  BOOTSTRAP_STATUS_NEGOTIATING = 1;   // Still collecting seeds
  BOOTSTRAP_STATUS_AGREED = 2;        // Ready to form with this seed set
  BOOTSTRAP_STATUS_ALREADY_FORMED = 3; // Cluster already exists, join instead
}

// Sent between seeds during bootstrap negotiation
message BootstrapSeedGossip {
  Endpoint sender = 1;
  repeated Endpoint known_seeds = 2;  // All seeds this node knows about
  bytes seed_set_hash = 3;            // SHA256 of sorted seed set (for agreement check)
  int32 bootstrap_expect = 4;         // Expected cluster size
}

// Response to seed gossip
message BootstrapSeedGossipResponse {
  Endpoint sender = 1;
  BootstrapStatus status = 2;
  repeated Endpoint known_seeds = 3;  // Seeds known by responder
  bytes seed_set_hash = 4;            // Responder's current seed set hash
  ConfigurationId configuration_id = 5; // Set if ALREADY_FORMED
}
```

#### Phase 2: BootstrapCoordinator Class

Create `src/RapidCluster.Core/Bootstrap/BootstrapCoordinator.cs`:

```csharp
internal sealed class BootstrapCoordinator
{
    // Manages the seed gossip protocol
    // Returns the agreed-upon seed set once all nodes agree
    // Throws BootstrapAlreadyCompleteException if cluster already exists
    
    public async Task<IReadOnlyList<Endpoint>> NegotiateSeedsAsync(
        IReadOnlyList<Endpoint> initialSeeds,
        int bootstrapExpect,
        CancellationToken cancellationToken);
}
```

#### Phase 3: MembershipService Integration

Update `MembershipService.BootstrapClusterAsync()`:

```csharp
private async Task BootstrapClusterAsync(CancellationToken cancellationToken)
{
    if (_seedsAreStatic)
    {
        // Current fast path - deterministic bootstrap
        await BootstrapWithStaticSeedsAsync(cancellationToken);
    }
    else
    {
        // New path - seed gossip protocol
        await BootstrapWithDynamicSeedsAsync(cancellationToken);
    }
}

private async Task BootstrapWithDynamicSeedsAsync(CancellationToken cancellationToken)
{
    var coordinator = new BootstrapCoordinator(...);
    try
    {
        var agreedSeeds = await coordinator.NegotiateSeedsAsync(
            _seedAddresses, _bootstrapExpect, cancellationToken);
        
        // All nodes agreed - proceed with deterministic bootstrap using agreed seeds
        _seedAddresses = agreedSeeds.ToList();
        await BootstrapWithStaticSeedsAsync(cancellationToken);
    }
    catch (BootstrapAlreadyCompleteException)
    {
        // Cluster already formed - fall back to normal join
        await JoinClusterAsync(cancellationToken);
    }
}
```

#### Phase 4: Exception and Logging

- Create `BootstrapAlreadyCompleteException` in `src/RapidCluster.Core/Exceptions/`
- Add `BootstrapCoordinatorLogger` for structured logging

#### Phase 5: Message Handling

Add handlers in `MembershipService.HandleMessageAsync()`:
- `BootstrapSeedGossip` → `HandleBootstrapSeedGossip()`
- Return current seeds and status

#### Phase 6: Testing

Add simulation tests in `tests/RapidCluster.Simulation.Tests/`:
- `DynamicSeedBootstrapTests.cs`
  - Nodes with different initial seeds converge
  - Split seed sets eventually merge
  - Already-formed cluster detected correctly
  - Timeout handling when seeds unreachable

## File Structure

```
src/RapidCluster.Core/
  Bootstrap/
    BootstrapCoordinator.cs        # NEW
    BootstrapCoordinatorLogger.cs  # NEW
  Exceptions/
    BootstrapAlreadyCompleteException.cs  # NEW
  Logging/
    BootstrapCoordinatorLogger.cs  # NEW (source-generated)
  Protos/
    rapid_types.proto              # MODIFY - add bootstrap messages
  MembershipService.cs             # MODIFY - add branching logic
```

## Implementation Checklist

- [x] **Phase 1: Proto Messages**
  - [x] Add `BootstrapStatus` enum to `rapid_types.proto`
  - [x] Add `BootstrapSeedGossip` message
  - [x] Add `BootstrapSeedGossipResponse` message
  - [x] Add message cases to `RapidClusterRequest` and `RapidClusterResponse`
  - [x] Regenerate protobuf code

- [x] **Phase 2: BootstrapCoordinator**
  - [x] Create `Bootstrap/` directory
  - [x] Implement `BootstrapCoordinator` class
  - [x] Implement seed set hashing (SHA256 of sorted addresses)
  - [x] Implement gossip round logic
  - [x] Implement agreement detection
  - [x] Add timeout handling

- [x] **Phase 3: Exception and Logging**
  - [x] Create `BootstrapAlreadyCompleteException`
  - [x] Create `BootstrapCoordinatorLogger` (source-generated)

- [x] **Phase 4: MembershipService Integration**
  - [x] Add `BootstrapWithDynamicSeedsAsync()` method
  - [x] Update `BootstrapClusterAsync()` to branch on `_seedsAreStatic`
  - [x] Add `HandleBootstrapSeedGossip()` handler
  - [x] Wire up message routing in `HandleMessageAsync()`
  - [x] Fix race condition: check if gossip sender is already in membership before responding ALREADY_FORMED

- [x] **Phase 5: Simulation Tests**
  - [x] Create `DynamicSeedBootstrapTests.cs`
  - [x] Test: nodes with different seeds converge
  - [x] Test: already-formed cluster triggers join (via AGREED status for members)
  - [x] Test: deterministic seed set hash
  - [x] Test: all nodes have same membership view after bootstrap
  - [x] Test: all nodes have same configuration ID
  - [x] Test: can join additional nodes after bootstrap
  - [x] Test: can remove nodes after bootstrap
  - [x] Test: handles node crash after formation

- [ ] **Phase 6: Documentation**
  - [ ] Update `007_EnhancedClusterBootstrapping.md` with implementation notes
  - [ ] Add XML docs to all public APIs

## Implementation Notes

### Race Condition Fix (2024-12-18)

A race condition was discovered during testing where:
1. Node A completes seed gossip first and forms the cluster
2. While nodes B and C are still gossiping, node A responds with `ALREADY_FORMED`
3. Nodes B and C fall back to join protocol instead of completing bootstrap
4. This causes consensus to incorrectly remove nodes that are already members

**Solution**: When responding to gossip from a node that's already in our membership view,
respond with `AGREED` status (instead of `ALREADY_FORMED`) and include the full membership.
This tells the sender they're part of the cluster and should complete bootstrap normally.

Key changes:
- `MembershipService.HandleBootstrapSeedGossip()`: Check if sender is in membership before responding
- `BootstrapCoordinator.ProcessGossipResults()`: Handle `AGREED` response with ConfigurationId > 0 as signal to complete bootstrap

## Testing Strategy

### Unit Tests
- `BootstrapCoordinator` seed set merging logic
- Seed set hash computation is deterministic
- Agreement detection works correctly

### Simulation Tests
- Multiple nodes with overlapping but different seed sets converge
- Node discovering already-formed cluster falls back to join
- Timeout when bootstrap_expect seeds not reachable
- Gossip completes within reasonable number of rounds

### Integration Tests (if needed)
- Real network gossip with service discovery

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Gossip takes too long | Configurable timeout, exponential backoff |
| Network partition during bootstrap | Timeout and retry with fresh seeds |
| Hash collision (extremely unlikely) | Use SHA256, include all seed details |
| Race between bootstrap and join | Check cluster status before each gossip round |

## Success Criteria

1. Nodes with different initial seed lists form a single cluster
2. No split-brain when using dynamic seed discovery
3. Performance: bootstrap completes within 2x the static bootstrap time
4. Existing static seed behavior unchanged (no regression)
