# 011: Unify Metadata with MembershipView (remove MetadataManager)

> Depends on: `tasks/todo/010_IncludeClusterIdInConfigurationId.md` (must complete after 010).

## Goal

Unify endpoint metadata with membership state by embedding per-node metadata directly into the membership view.

- Introduce a new `MemberInfo` type that represents one node’s:
  - `Endpoint`
  - `Metadata` (protobuf `RapidCluster.Pb.Metadata`)
- Change `MembershipView` so it contains collections of `MemberInfo` rather than “endpoints + separate metadata map”.
- Remove `MetadataManager` entirely.

This should simplify metadata tracking/propagation and eliminate the risk of membership/metadata divergence.

## Current State

- `MetadataManager` is a singleton holding `Endpoint -> Metadata` in a locked `SortedDictionary`.
- `MembershipService` updates metadata via `_metadataManager.Add(...)`.
- Join and view-sync messages send metadata using parallel arrays:
  - `JoinResponse.MetadataKeys` / `JoinResponse.MetadataValues`
  - `MembershipViewResponse.MetadataKeys` / `MembershipViewResponse.MetadataValues`
- `RapidClusterImpl` builds the public view by reading `_metadataManager.GetAllMetadata()` alongside `_viewAccessor` updates.

Known code touchpoints:

- `src/RapidCluster.Core/MetadataManager.cs`
- `src/RapidCluster.Core/MembershipService.cs` (populates/reads metadata for JoinResponse and view sync)
- `src/RapidCluster.Core/RapidClusterImpl.cs` (combines internal view + metadata map)
- `src/RapidCluster.Core/RapidClusterServiceCollectionExtensions.cs` (registers MetadataManager singleton)
- Tests:
  - `tests/RapidCluster.Unit.Tests/MetadataManagerTests.cs`
  - Simulation harness uses `MetadataManager` in `tests/RapidCluster.Simulation.Tests/Infrastructure/RapidSimulationNode.cs`

## Proposed Design

### 1) New `MemberInfo` type

Create an internal model type (location TBD, likely `src/RapidCluster.Core/MemberInfo.cs`):

- `public sealed record MemberInfo(Endpoint Endpoint, Metadata Metadata)` (or immutable class/struct).
- Ensure deterministic equality semantics are well-defined (likely compare endpoints by address, and metadata by bytes/map equality).

### 2) MembershipView stores members as MemberInfo

Update `MembershipView` so that:

- `Members` becomes `ImmutableArray<MemberInfo>`.
- Ring storage becomes `ImmutableArray<ImmutableArray<MemberInfo>>`.
- Any structures used for fast membership checks (e.g., `_allNodes`, `_nodeIdByEndpoint`) still key off endpoint (address comparer ignoring node id).

Public surface implications:

- `MembershipView.Configuration` should likely become `MembershipViewConfiguration` containing the information needed to rebuild a builder:
  - if `MembershipViewConfiguration` currently holds `ImmutableArray<Endpoint>` it will need a `MemberInfo` equivalent.

### 3) MembershipViewBuilder builds rings of MemberInfo

Update `MembershipViewBuilder`:

- Accept initial members as `IEnumerable<MemberInfo>`.
- Hash/ordering logic:
  - continues to sort by endpoint address hashes.
  - ensure hashing ignores metadata and node_id for ring order (metadata should not change ring ordering).
- Join/leave operations should update the member list as `MemberInfo` objects.

### 4) Protocol representation

We already have parallel arrays keys/values, but with `MemberInfo` we want a single representation.

Preferred evolution:

- Update protobuf messages that return membership lists to include metadata “inline” with the member.
- Options:
  1) Introduce protobuf `message MemberInfoPb { Endpoint endpoint = 1; Metadata metadata = 2; }` and use `repeated MemberInfoPb members = ...`
  2) Keep existing parallel arrays for wire format, but immediately materialize them into `MemberInfo` inside `MembershipService`.

Option (1) is cleaner but is wire-changing; decide based on whether we are okay with proto evolution at this stage.

### 5) Remove MetadataManager and simplify data flow

- Delete `MetadataManager`.
- `MembershipService` becomes the sole source of truth for metadata, by producing new `MembershipView` instances that already contain metadata.
- `RapidClusterImpl` no longer needs separate metadata lookup; it converts the internal view directly into `ClusterMembershipView`.
- DI registration of `MetadataManager` is removed.

### 6) Metadata update semantics

Define how metadata changes are handled:

- If metadata is immutable per node after join, we only set it at join time.
- If metadata can change dynamically later, then changing metadata must become a membership “change event” (new view) OR a separate metadata update mechanism. Decide explicitly.

For this task, assume:

- Metadata is established at join time and on view sync.

## Testing Plan

### Unit tests

- Replace `MetadataManagerTests` with tests against:
  - `MembershipView` / `MembershipViewBuilder` member metadata retention
  - Correct join-response/view-response materialization into `MemberInfo`
  - Deterministic ordering and equality/invariants when metadata differs

### Simulation tests

- Ensure metadata propagates correctly through join and view refresh.
- Validate no divergence between membership and metadata (single source of truth).

### Integration tests

- Form a cluster with metadata set on each node; verify:
  - public `ClusterMembershipView` exposes correct metadata per member.
  - metadata remains intact across joins/leaves and view refresh.

## Implementation Steps (when we start coding)

1. Introduce `MemberInfo` model type.
2. Refactor `MembershipView` + `MembershipViewBuilder` to operate on `MemberInfo`.
3. Update join/view-sync message handling to populate metadata inline.
4. Remove `MetadataManager` and update DI + consumers.
5. Update public view conversion (`RapidClusterImpl` / `ToClusterMembershipView`).
6. Replace/update tests (unit + simulation + integration).

## Open Questions

- Proto approach: do we introduce a new `MemberInfo` message type or keep parallel arrays?
- Can metadata change after join, or is it immutable?
- Should ring ordering explicitly ignore metadata?
