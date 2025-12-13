# Implement Monotonic Node IDs for Paxos Correctness

## Status: COMPLETED

## Problem

When a node restarts with the same hostname and port, it's a new incarnation but currently gets the same Paxos identifier (computed as XxHash32 of hostname + port). This violates Paxos's assumption that each participant has a unique identity across time.

### Why This Matters

Paxos correctness relies on the property that if a node makes a promise or casts a vote, that commitment is durable. When a node restarts:

1. It loses all in-memory Paxos state (promises, votes, accepted values)
2. But it gets the same NodeIndex (hash of hostname:port)
3. Other nodes may still hold Phase1b/Phase2b messages from the old incarnation
4. The new incarnation could make conflicting promises/votes with the same identity

This could theoretically lead to safety violations, though it's rare in practice because:
- Restarts typically take longer than consensus rounds
- The cluster usually reconfigures to remove the failed node before it returns

### Current Stopgap

We use `XxHash32.HashToUInt32()` for deterministic NodeIndex computation. This is better than `GetHashCode()` (which is randomized per-process in .NET), but doesn't solve the incarnation problem.

## Implemented Solution

Assigned each node a unique, monotonically increasing `MonotonicNodeId` when it joins the cluster. This ID is stored in the `Endpoint` protobuf message and is used for Paxos rank computation.

### Key Design Decisions

1. **Storage**: Added `monotonic_node_id` field to `Endpoint` in the protobuf definition
2. **Assignment**: Assigned during join protocol as `max_node_id + 1`
3. **Tracking**: `max_node_id` tracked in `MembershipView` and propagated via consensus
4. **Comparison**: Used `EndpointAddressComparer` throughout to compare endpoints by hostname:port only, ignoring `MonotonicNodeId`

## Implementation Details

### Files Modified

#### Proto Changes (`rapid.proto`)
```protobuf
message Endpoint {
  string hostname = 1;
  int32 port = 2;
  int64 monotonic_node_id = 3;  // Assigned during join
}

message MemberInfo {
  // existing fields...
  int64 monotonic_node_id = 4;  // Included in membership proposals
}
```

#### Core Classes Modified

1. **`MembershipView.cs`**
   - Added `MaxMonotonicId` property to track highest assigned ID
   - Added `GetMonotonicNodeId(Endpoint)` method
   - Added `ToBuilder(int maxRingCount)` overload
   - Fixed `GetRingNumbers()` to use `EndpointAddressComparer`
   - Fixed `FindIndex()` to use `EndpointAddressComparer`

2. **`MembershipViewBuilder.cs`**
   - Added constructor overload taking `maxRingCount` parameter
   - Updated `Build()` to propagate `MaxMonotonicId` from MemberInfo

3. **`MembershipService.cs`**
   - Modified `CreateMembershipProposal()` to assign `MonotonicNodeId` to new joiners
   - Fixed `CreateMembershipProposal()` to use `EndpointAddressComparer` when removing leaving nodes
   - Fixed `DecideViewChange()` to use `ToBuilder(_options.ObserversPerSubject)`
   - Updated `GetMetadata()` to return dictionary with `EndpointAddressComparer`

4. **`Paxos.cs`**
   - Modified `GetMyRank()` to use `MonotonicNodeId` for rank computation
   - Added `_membershipViewAccessor` dependency
   - Changed rank computation from XxHash32 to use monotonic ID

5. **`ConsensusCoordinator.cs`**
   - Added `IMembershipViewAccessor` dependency
   - Passed accessor to `Paxos` constructor

6. **`SimpleCutDetector.cs`** and **`MultiNodeCutDetector.cs`**
   - Updated dictionaries to use `EndpointAddressComparer.Instance`

7. **`ClusterIntegrationTests.cs`**
   - Fixed test to use `ContainsKey()` instead of `Contains()` on dictionary keys

### Key Implementation Pattern

Throughout the codebase, when comparing `Endpoint` instances for membership purposes (hostname:port identity), we use `EndpointAddressComparer.Instance` to ignore `MonotonicNodeId`. The `MonotonicNodeId` is only used for Paxos rank computation, not for node identity.

```csharp
// Correct - uses EndpointAddressComparer
var index = newMembers.FindIndex(e => EndpointAddressComparer.Instance.Equals(e, endpoint));

// Incorrect - uses protobuf's default equality which compares ALL fields
newMembers.Remove(endpoint);  // Would fail if MonotonicNodeId differs
```

## Follow-up Fix: MaxMonotonicId Propagation (2024-12-14)

A follow-up issue was discovered where `MaxMonotonicId` was not being transmitted in `MembershipViewResponse` and `JoinResponse` protobuf messages. This caused nodes that learned views via the stale view detection protocol (Paxos learner) or via `JoinResponse` to incorrectly compute `MaxMonotonicId` from the current endpoints' `MonotonicNodeId` values - which fails when a node with the highest ID has left the cluster.

### Additional Proto Changes
```protobuf
message JoinResponse {
  // existing fields...
  int64 max_monotonic_id = 8;  // Highest monotonic_node_id ever assigned
}

message MembershipViewResponse {
  // existing fields...
  int64 max_monotonic_id = 7;  // Highest monotonic_node_id ever assigned
}
```

### Additional MembershipService.cs Changes
- Updated `HandleMembershipViewRequest()` to include `MaxMonotonicId` in response
- Updated `ApplyLearnedMembershipView()` to pass `viewResponse.MaxMonotonicId` to `MembershipViewBuilder`
- Updated `NotifyWaitingJoiners()` to include `MaxMonotonicId` in `JoinResponse`
- Updated `HandleJoinMessageAsync()` to include `MaxMonotonicId` in `JoinResponse` for already-in-ring case
- Updated `JoinClusterAsync()` to pass `successfulResponse.MaxMonotonicId` to `MembershipViewBuilder`
- Updated `ResetStateAfterRejoin()` to pass `response.MaxMonotonicId` to `MembershipViewBuilder`

### Test Fix
- Fixed `MultipleNodesRestart_AllGetNewMonotonicNodeIds` to use 5 nodes instead of 4, so that crashing 2 nodes still leaves a quorum (3 out of 5) that can reach consensus.

## Testing

All 906 tests pass:
- Unit tests verify rank computation and endpoint comparison
- Simulation tests verify cluster behavior with joins, leaves, crashes, and restarts
- Integration tests verify real networking scenarios

## Open Questions (Resolved)

1. **NodeId overflow**: Using int64 gives us 9 quintillion IDs - sufficient for any practical use.

2. **NodeId gaps**: Gaps are fine - we don't need contiguous IDs.

3. **Seed node bootstrap**: Seed node gets `MonotonicNodeId = 1` during `CreateSeedView()`.

## References

- [Fast Paxos paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf) - Section on coordinator identification
- [Paxos Made Simple](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) - Proposer uniqueness requirements
