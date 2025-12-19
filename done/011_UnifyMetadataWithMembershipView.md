# 011: Unify Metadata with MembershipView (remove MetadataManager)

> Depends on: `tasks/done/010_IncludeClusterIdInConfigurationId.md` (completed).

## Status: COMPLETED

## Goal

Unify endpoint metadata with membership state by embedding per-node metadata directly into the membership view.

- Introduce a new `MemberInfo` type that represents one node's:
  - `Endpoint`
  - `Metadata` (protobuf `RapidCluster.Pb.Metadata`)
- Change `MembershipView` so it contains collections of `MemberInfo` rather than "endpoints + separate metadata map".
- Remove `MetadataManager` entirely.

This should simplify metadata tracking/propagation and eliminate the risk of membership/metadata divergence.

## Implementation Summary

### Key Design Decisions

1. **`MemberInfo` equality is based on endpoint address only** (hostname + port), ignoring metadata and NodeId. This ensures that the same node can be identified even if metadata differs.

2. **Metadata is immutable once a node joins** - established at join time and propagated via view sync.

3. **Ring ordering ignores metadata** - hashing is based on endpoint address only to ensure deterministic ring placement.

4. **Protobuf wire format unchanged** - kept existing parallel arrays for metadata in messages (`MetadataKeys`/`MetadataValues`), materializing them into `MemberInfo` inside `MembershipService`.

### Files Created

- `src/RapidCluster.Core/MemberInfo.cs` - New class combining `Endpoint` and `Metadata` with address-based equality.

### Files Modified

- `src/RapidCluster.Core/MembershipView.cs` - Internal storage changed to `ImmutableArray<ImmutableArray<MemberInfo>>`. Added `MemberInfos`, `GetMemberInfo()`, `GetMetadata()`, `GetAllMetadata()` methods.

- `src/RapidCluster.Core/MembershipViewBuilder.cs` - Internal storage changed to `SortedSet<MemberInfo>`. Added `RingAdd(MemberInfo)` and `RingAdd(Endpoint, Metadata)` overloads.

- `src/RapidCluster.Core/MembershipService.cs` - Removed `_metadataManager` field. All metadata operations now go through `MembershipView`. `SetMembershipView()` reduced from 4 to 3 parameters.

- `src/RapidCluster.Core/RapidClusterImpl.cs` - Removed `_metadataManager` dependency. Now uses `view.ToClusterMembershipView()` without metadata parameter.

- `src/RapidCluster.Core/EndPointConversions.cs` - `ToClusterMembershipView()` now reads metadata from `MemberInfo` in the view instead of external dictionary.

- `src/RapidCluster.Core/RapidClusterServiceCollectionExtensions.cs` - Removed `MetadataManager` DI registration.

- `tests/RapidCluster.Simulation.Tests/Infrastructure/RapidSimulationNode.cs` - Removed `MetadataManager` instantiation.

### Files Deleted

- `src/RapidCluster.Core/MetadataManager.cs` - Entire class removed.
- `tests/RapidCluster.Unit.Tests/MetadataManagerTests.cs` - Tests for removed class.

## Test Results

- **Unit tests**: 500 passed
- **Simulation tests**: 465+ passed (2 skipped as expected)
- **Integration tests**: 28 passed

All existing metadata-related integration tests (e.g., `ComplexMetadataPropagatedCorrectly`, `SubscriptionWithMetadata`) continue to pass, validating that metadata propagation still works correctly through the new unified model.

## Open Questions (Resolved)

- **Proto approach**: Kept existing parallel arrays for wire format (Option 2) - simpler, no breaking changes.
- **Can metadata change after join?**: No, metadata is immutable after join.
- **Should ring ordering ignore metadata?**: Yes, ring ordering is based on endpoint address hash only.
