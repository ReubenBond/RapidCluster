# Work Item 014: Deep Refactor of MembershipService

**Goal:** Simplify `MembershipService.cs` by extracting distinct responsibilities into helper classes, unifying logic, and reducing cyclomatic complexity. The target is to reduce the file size and make the core protocol logic more readable.

**Status:** In Progress
**Created:** 2025-12-18

## Problem Description
`MembershipService.cs` has grown to ~2350 lines. It currently handles:
1.  **Core Rapid protocol:** Phase 1/2, view changes, consensus.
2.  **Bootstrap coordination:** A distinct state machine for cluster formation (`_isBootstrapping`).
3.  **Protobuf Mapping:** Verbose conversion between domain objects and GRPC messages.
4.  **View Management:** Logic for calculating diffs and applying proposals is mixed with side effects.

This mixing of concerns leads to:
*   Duplicated logic (e.g., between `DecideViewChange` and `CreateMembershipProposal`).
*   Fragile concurrency (broad locks covering mixed concerns).
*   Hard-to-read methods (e.g., `HandleMessageAsync` is dominated by boilerplate).

## Proposed Solution

We will refactor `MembershipService` in 3 phases.

### Phase 1: Extract Bootstrap Controller (Complete)

Move all bootstrap-specific logic to a new `BootstrapController` class.

**Tasks:**
1.  Create `src/RapidCluster.Core/Discovery/BootstrapController.cs`.
2.  Move state fields from `MembershipService` to `BootstrapController`:
    *   `_isBootstrapping`
    *   `_bootstrapKnownNodes`
    *   `_bootstrapCompletionSource`
    *   `_seedAddresses` (maybe? check dependencies)
3.  Move methods to `BootstrapController`:
    *   `HandlePreJoinDuringBootstrap`
    *   `TriggerBootstrap`
    *   `ExitBootstrapMode`
    *   `ShouldWaitAsBootstrapCoordinator`
    *   `HandleBootstrapInProgressResponse`
    *   `ShouldBeBootstrapCoordinator`
4.  Update `MembershipService` to delegate to `BootstrapController`.
    *   `InitializeAsync` will call `_bootstrapController.StartAsync()`.
    *   `HandlePreJoinMessage` will check `_bootstrapController.IsBootstrapping`.

### Phase 2: Centralize Protocol Mapping (Pending)

Extract Protobuf conversion logic to `MembershipProtocolMapper`.

**Tasks:**
1.  Create `src/RapidCluster.Core/Messaging/MembershipProtocolMapper.cs` (or `RapidCluster.Core/Protos/ProtoExtensions.cs` if preferred).
2.  Move helper methods:
    *   `BuildMembershipViewFromJoinResponse`
    *   `BuildMemberInfosFromResponse`
    *   `CreateEndpointWithNodeId` (maybe?)
3.  Refactor `HandleMessageAsync` handlers to use the mapper for creating responses.
    *   Example: `return _mapper.CreateJoinResponse(...)` instead of manual object initialization.

### Phase 3: Encapsulate View Logic (Pending)

Move pure logic for view transitions into `MembershipView` or `MembershipState`.

**Tasks:**
1.  Enhance `MembershipView.cs`:
    *   Add `MembershipView ApplyProposal(MembershipProposal proposal, Dictionary<Endpoint, Metadata> joinerMetadata)`
        *   *Note:* This replaces the logic in `DecideViewChange`.
    *   Add `MembershipDiff Diff(MembershipView other)`
        *   Returns struct with `AddedNodes`, `RemovedNodes`.
2.  Refactor `MembershipService.DecideViewChange`:
    *   Use `_membershipView.ApplyProposal(...)` to get the new view.
    *   Pass the new view to `SetMembershipView`.
3.  Refactor `MembershipService.SetMembershipView`:
    *   Use `_membershipView.Diff(newView)` to get changes.
    *   Focus solely on side effects (metrics, logging, failure detectors).

## Definition of Done
*   `MembershipService.cs` line count significantly reduced (target < 1500 lines).
*   `BootstrapController` handles all bootstrap state.
*   Protobuf mapping is centralized.
*   View transition logic is unit-testable within `MembershipView`.
*   All existing tests (Unit, Integration, Simulation) pass.
