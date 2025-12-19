# 010: Include ClusterId in ConfigurationId

## Goal

Introduce a stable, immutable **ClusterId** that uniquely identifies a Rapid cluster instance, and include it in all protocol messages via a new `ConfigurationId` message type.

- `ClusterId` is computed **exactly once** during cluster bootstrapping.
- `ClusterId` is **deterministically derived** from the initial bootstrap membership view seed set (e.g., seed addresses).
- `ClusterId` MUST be propagated end-to-end and validated on receipt.
- Nodes must **ignore or reject** messages with a mismatched `ClusterId`.
- `ClusterId` never changes once a cluster is established.

This change hardens safety by preventing cross-talk between clusters that happen to share addresses/ports or stale nodes that rejoin the wrong cluster.

## Non-Goals

- No attempt to support cluster merges.
- No attempt to rotate/renegotiate ClusterId.
- ClusterId is not persisted to disk (out of scope).

## Current State (as of today)

- Protobuf messages carry an `int64 configurationId` field.
- `RapidCluster.ConfigurationId` is a struct wrapping a monotonic `Version` (`long`).
- Many message paths rely on either:
  - direct `long` comparisons, or
  - passing `ConfigurationId` around with implicit `long` conversions.
- The `PingPongFailureDetectorFactory` currently takes a `Func<long>? GetLocalConfigurationId` callback and uses `OnStaleViewDetected` with `long` IDs. Both should be updated to use `ConfigurationId` and, for local state, read the current `MembershipView` via `IMembershipViewAccessor` so it can always include `ClusterId + Version`.

## Proposed Design

### 1) Model Types

Add a new type:

- `public readonly struct ClusterId` (or similar) in `src/RapidCluster.Core/`.
  - Recommended representation: `ulong` (64-bit) or `byte[16]` (128-bit).
  - Prefer `ulong` for simplicity and proto efficiency.

Update `RapidCluster.ConfigurationId`:

- Extend to include `ClusterId` + `Version`.
- Responsibilities:
  - Comparisons/order should remain primarily on `Version` (ClusterId must match for meaningful comparisons).
  - Provide explicit conversion helpers between:
    - protobuf `Pb.ConfigurationId` message
    - `RapidCluster.ConfigurationId`

Important invariants:

- `ClusterId` must be non-default (`!= 0`) for any message once initialized.
- `MembershipView.Empty` may use `ClusterId.Empty` (0), but this must never be transmitted.

### 2) Protobuf

Replace/introduce a new message type in `src/RapidCluster.Core/Protos/rapid_types.proto`:

- `message ConfigurationId { uint64 clusterId = 1; int64 version = 2; }`

Update all messages that currently contain `int64 configurationId` / `currentConfigurationId` to use the new message.

Messages to update (non-exhaustive; confirm by search):

- `PreJoinMessage`
- `JoinMessage`
- `JoinResponse`
- `AlertMessage`
- `MembershipProposal`
- `FastRoundPhase2bMessage`
- `Phase1aMessage`
- `Phase1bMessage`
- `Phase2aMessage`
- `Phase2bMessage`
- `ProbeMessage`
- `ProbeResponse`
- `MembershipViewRequest` (field currently `currentConfigurationId`)
- `MembershipViewResponse`

### 3) Deterministic ClusterId generation (bootstrap only)

Compute ClusterId at bootstrap using a deterministic hash of the **first membership view input**.

Input recommendation:

- The (ordered) unique seed endpoints used to form the initial cluster identity.
  - Must be deterministic across nodes.
  - Should be derived from:
    - `RapidClusterOptions.SeedAddresses` plus self, OR
    - the output of `ISeedProvider` (plus self) before filtering to join.

Canonicalization rules (proposed):

- Use the *address* identity (hostname bytes + port); ignore `node_id`.
- Sort endpoints using existing `EndpointAddressComparer.Instance`.
- Serialize each endpoint as: `hostname (raw bytes) + 0x00 + port (big endian)`.
- Hash with `XxHash64` (already used in `MembershipViewBuilder`) with a fixed seed.
- Result is `ulong clusterId`.

Notes:

- If seed list changes across processes, they will compute different ClusterIds and refuse to talk. That is intended.
- `StartNewCluster()` should be the only path that fabricates a new ClusterId.

### 4) Propagation strategy

#### MembershipView

- `MembershipView` should carry `ConfigurationId` whose `ClusterId` is stable.
- `MembershipViewBuilder.Build(previous)` increments `Version` only, preserving `ClusterId` from `previous`.

#### MembershipService

- Set `_membershipView.ConfigurationId.ClusterId` when:
  - starting a new cluster (seed/bootstrap), and
  - joining/learning the view from remote (taken from response).

- When receiving a request/response:
  - Extract sender config (ClusterId + Version).
  - Validate:
    - if local is initialized (`ClusterId != 0`) and sender `ClusterId` mismatches => **hard failure** (reject/throw) + error log.
    - if local is not initialized (`ClusterId == 0`) then only accept a non-zero ClusterId via the join/learner paths.

Join handshake rule:

- Joiners do **not** need to know ClusterId a priori.
- Initial join request(s) must carry `clusterId = 0`.
- Seed/join response returns the established ClusterId.
- After learning ClusterId, the joiner must include it on subsequent messages.

### 5) PingPongFailureDetector

User requirement:

- PingPongFailureDetector should use `IMembershipViewAccessor` to obtain the current view, instead of a `Func<long>`, and `OnStaleViewDetected` should be updated to pass a full `ConfigurationId` (not `long`).

Proposed changes:

- Remove `GetLocalConfigurationId` and have the detector/factory depend on `IMembershipViewAccessor`.
- Change `OnStaleViewDetected` signature to accept `ConfigurationId` (the remote responder’s config).
- On probe send:
  - read `viewAccessor.Current` and set `ProbeMessage.ConfigurationId` (clusterId + version).
- On probe response:
  - validate remote clusterId matches current view.
  - stale view detection compares versions only if clusterId matches.
  - if stale view is detected, invoke `OnStaleViewDetected(remoteConfigurationId)`.

### 6) “Never omit and never fabricate”

Implementation rules to follow:

- Only `StartNewCluster()`/bootstrap path creates a brand-new ClusterId.
- Any node that is joining uses ClusterId learned from seed/join response.
- Any membership view refresh (learner path) must preserve ClusterId from the learned view.
- No other layers should create ClusterId defaults.

## Testing Plan

We need coverage across multiple test styles, focusing on:

### Unit tests

- `ConfigurationIdTests`
  - serialization/conversion to/from protobuf message
  - equality semantics: mismatch ClusterId != equal even if Version same
  - ordering/compare guards: version compare only valid if ClusterId matches (decide behavior: throw vs compare anyway)

- `RapidClusterUtilsTests` / message wrapper tests
  - all modified protobuf messages contain and roundtrip `ConfigurationId` (clusterId + version)

- `PingPongFailureDetectorTests`
  - uses `IMembershipViewAccessor` to populate probe config
  - stale view detection triggers only on same clusterId
  - mismatch clusterId response is ignored / does not trigger refresh

### Simulation tests (Clockwork)

- Cluster formation test asserts:
  - all nodes converge on same ClusterId
  - ClusterId remains constant across joins/leaves

- Negative test:
  - create two clusters in same simulation network, attempt cross-talk; ensure messages are rejected and invariants hold.

### Integration tests

- Two independent TestClusters (different seed sets) running concurrently:
  - ensure join attempts with wrong clusterId fail or are ignored.
  - ensure normal 3-node cluster still forms.

## Rollout / Migration Considerations

- This is a wire-breaking change (proto field type changes). If backward compatibility is required, we can:
  - keep `int64 configurationId` and add `uint64 clusterId` alongside, then migrate.
  - otherwise do a clean proto version bump.

Decision needed: whether we require mixed-version compatibility.

## Implementation Steps (when we start coding)

1. Add `tasks/todo` and this work-item.
2. Define `ClusterId` type and extend `ConfigurationId`.
3. Update `rapid_types.proto` and regenerate protobuf C#.
4. Update all message constructors/usages to set/read new config.
5. Implement bootstrap ClusterId hash.
6. Propagate validation logic (MembershipService pipeline + failure detector + consensus paths).
7. Update and add unit tests.
8. Add simulation + integration tests.
9. Run `dotnet build RapidCluster.slnx` and relevant test projects.

## Open Questions

- Wire-compatibility: do we require mixed-version compatibility?
- What is the canonical "seed set" used to hash ClusterId when `ISeedProvider` output is dynamic?

(Decisions captured: joiners learn ClusterId during join; mismatches are hard failures; ClusterId is not persisted.)

## Implementation Notes (completed)

### Phase 1: ConfigurationId struct with ClusterId + Version (COMPLETED)

1. **Core Infrastructure** (`src/RapidCluster.Core/ConfigurationId.cs`):
   - Created `ClusterId` struct (wraps `ulong`)
   - Created `ConfigurationId` struct with `ClusterId` and `Version` properties
   - Implements `IEquatable`, `IComparable`, comparison operators (`<`, `>`, `==`, etc.)
   - Added `ToProtobuf()` method and `FromProtobuf()` static method
   - Added extension method `ToConfigurationId()` for `Pb.ConfigurationId?`

2. **Protobuf Updates** (`src/RapidCluster.Core/Protos/rapid_types.proto`):
   - Changed all `int64 configurationId` fields to `message ConfigurationId { uint64 clusterId = 1; int64 version = 2; }`
   - Updated all message types: `PreJoinMessage`, `JoinMessage`, `JoinResponse`, `AlertMessage`, `MembershipProposal`, `FastRoundPhase2bMessage`, `Phase1aMessage`, `Phase1bMessage`, `Phase2aMessage`, `Phase2bMessage`, `ProbeMessage`, `ProbeResponse`, `MembershipViewRequest`, `MembershipViewResponse`

3. **Core Library Migration**:
   - `MembershipService.cs` - Fully migrated
   - `MembershipView.cs` - Uses `ConfigurationId` struct
   - `Paxos.cs` - Uses `ConfigurationId` struct
   - `FastPaxos.cs` - Uses `ConfigurationId` struct
   - `ConsensusCoordinator.cs` - Uses `ConfigurationId` struct
   - `PingPongFailureDetector.cs` - Uses `ConfigurationId` struct with `IMembershipViewAccessor`
   - Logging infrastructure updated with `LoggableConfigurationId` wrapper

4. **Test Project Updates**:
   - Unit tests: Updated `PaxosTests.cs`, `MembershipViewTests.cs`, `ListEndpointComparerTests.cs`, `PingPongFailureDetectorTests.cs`
   - Simulation tests: Updated `RapidSimulationNode.cs`, `ClusterBasicTests.cs`, `ConsensusProtocolTests.cs`, `LargeScaleClusterTests.cs`
   - Added `Utils.CreateMembershipView(numNodes, configVersion)` overload for tests that need specific config versions

5. **Samples Updated**:
   - `samples/RapidCluster.Examples/Program.cs`
   - `samples/RapidCluster.Aspire/RapidCluster.Aspire.Node/Program.cs`
   - `tests/RapidCluster.EndToEnd/RapidCluster.EndToEnd.Node/Program.cs`

### Test Results

All tests pass:
- Unit tests: 522 passed
- Simulation tests: 444 passed (2 skipped as expected)

### Remaining Work (Phase 2)

The following items are still TODO:
- ~~Implement deterministic `ClusterId` generation during bootstrap (hash of seed endpoints)~~ DONE
- ~~Add validation logic to reject messages with mismatched `ClusterId`~~ DONE
- ~~Add tests for cross-cluster rejection (simulation/integration tests)~~ DONE
- ~~Update `MembershipViewBuilder.Build()` to preserve `ClusterId` from previous view~~ DONE

**All Phase 2 work is complete.**

### Phase 2: ClusterId Validation (COMPLETED)

1. **Strict ClusterId Validation** - ClusterId mismatch now always results in rejection:
   - `HandleConsensusMessages` (`MembershipService.cs:1075-1080`): Rejects messages when `messageConfigId.ClusterId != currentConfigId.ClusterId`
   - `FilterAlertMessages` (`MembershipService.cs:1391-1410`): Instance method that filters out alert messages with mismatched ClusterIds, logging via `AlertFilteredByClusterId`
   - **No exceptions for Empty ClusterId** - all mismatches are rejected

2. **Logging Infrastructure**:
   - `LoggingHelpers.cs`: Added `LoggableClusterId` struct for high-performance logging
   - `MembershipServiceLogger.cs`: Added two log methods:
     - `ClusterIdMismatch` (Error level) - consensus message rejected due to ClusterId mismatch
     - `AlertFilteredByClusterId` (Debug level) - alert message filtered due to ClusterId mismatch

3. **ConfigurationId Comparison Behavior**:
   - `ConfigurationId.CompareTo()` throws `InvalidOperationException` when ClusterIds don't match (no exceptions for Empty)
   - Updated doc comment to reflect strict behavior
   - Unit tests updated: `CompareTo_With_Empty_ClusterId_On_Left_Throws` and `CompareTo_With_Empty_ClusterId_On_Right_Throws`

