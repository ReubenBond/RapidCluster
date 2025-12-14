# Rename monotonic_node_id to node_id in rapid.proto

## Status: COMPLETED

## Completion Notes

All renames completed successfully on 2025-12-13:

### Proto changes (`src/RapidCluster.Core/Protos/rapid.proto`)
- `Endpoint.monotonic_node_id` → `node_id`
- `JoinResponse.max_monotonic_id` → `max_node_id`
- `MembershipProposal.max_monotonic_id` → `max_node_id`
- `MembershipViewResponse.max_monotonic_id` → `max_node_id`

### C# changes
- `MonotonicNodeId` → `NodeId` (property on Endpoint, auto-generated from proto)
- `MaxMonotonicId` → `MaxNodeId` (on JoinResponse, MembershipProposal, MembershipViewResponse)
- `GetMonotonicNodeId()` → `GetNodeId()` (in MembershipView)
- `GetNextMonotonicId()` → `GetNextNodeId()` (in MembershipViewBuilder)
- `SetMaxMonotonicId()` → `SetMaxNodeId()` (in MembershipViewBuilder)
- `_maxMonotonicId` → `_maxNodeId` (private field in MembershipViewBuilder)
- Local variables renamed for consistency (e.g., `originalMonotonicId` → `originalNodeId`)

### Files modified
- `src/RapidCluster.Core/Protos/rapid.proto`
- `src/RapidCluster.Core/MembershipView.cs`
- `src/RapidCluster.Core/MembershipViewBuilder.cs`
- `src/RapidCluster.Core/MembershipService.cs`
- `src/RapidCluster.Core/Paxos.cs`
- `src/RapidCluster.Core/Endpoint.IComparable.cs`
- `src/RapidCluster.Core/ListEndpointComparer.cs`
- `src/RapidCluster.Core/MetadataManager.cs`
- `tests/RapidCluster.Tests/Simulation/NodeRestartTests.cs`
- `tests/RapidCluster.Tests/Simulation/Infrastructure/InvariantChecker.cs`

### Verification
- Build: ✅ Succeeded with 0 warnings, 0 errors
- Tests: ✅ All 703+ tests passed (3 skipped as expected)

## Problem

The field `monotonic_node_id` in `Endpoint` message should be renamed to `node_id` for simplicity and clarity. Similarly, `max_monotonic_id` fields should be renamed to `max_node_id`.

## Changes Required

### 1. In `rapid.proto`

#### Endpoint message
```protobuf
// Before
message Endpoint {
  bytes hostname = 1;
  int32 port = 2;
  int64 monotonic_node_id = 3;
}

// After
message Endpoint {
  bytes hostname = 1;
  int32 port = 2;
  int64 node_id = 3;
}
```

#### JoinResponse message
```protobuf
// Rename max_monotonic_id to max_node_id
```

#### MembershipProposal message
```protobuf
// Rename max_monotonic_id to max_node_id
```

### 2. Update C# code

After regenerating the protobuf files, update all references:
- `MonotonicNodeId` → `NodeId` (on Endpoint)
- `MaxMonotonicId` → `MaxNodeId` (on JoinResponse and MembershipProposal)

## Files to Modify

- `src/RapidCluster.Core/Protos/rapid.proto`
- All C# files referencing `MonotonicNodeId` or `MaxMonotonicId`

## Testing

Run full test suite after changes:
```bash
cd tests/RapidCluster.Tests
dotnet run -- --timeout 60s
```
