# 011: Refactor Paxos roles (Proposer/Acceptor/Learner)

## Goal

Refactor `Paxos` / `FastPaxos` / `ConsensusCoordinator` to more clearly separate responsibilities along standard Paxos roles:

- **Proposer**: initiates rounds/proposals and drives the protocol forward.
- **Acceptor**: keeps durable (in-memory) promise/accepted state and responds to protocol messages.
- **Learner**: determines when a value is chosen and publishes the outcome.

Key constraints:

- Create a **single Acceptor implementation** that is shared by both Classic Paxos and Fast Paxos.
- Today `Paxos` is both **Proposer + Acceptor**; split it cleanly.
- Today `FastPaxos` is only a **Proposer**; keep that, but make its naming/shape consistent.
- Rename classes/methods/message-types to be more intuitive and easier to map to the specification, while keeping enough traceability (via small comments) to ensure correctness.
- Ensure sufficient **test coverage** to validate conformance to Paxos/Fast Paxos behaviors.

## Non-Goals

- No persistence/durability beyond current in-memory behavior.
- No feature changes to membership semantics (only refactor + naming + testability).
- No API churn outside the consensus/messaging surface unless required.

## Current State

- `src/RapidCluster.Core/Paxos.cs` mixes proposer state machine and acceptor state.
- `src/RapidCluster.Core/FastPaxos.cs` acts as a proposer only.
- `src/RapidCluster.Core/ConsensusCoordinator.cs` orchestrates membership consensus and integrates both Paxos and Fast Paxos.
- Message types and method names reflect protocol phases (e.g., Phase1a/1b/2a/2b) but are not always “friendly” to reason about.

## Proposed Design

### 1) Split roles into explicit types

Introduce a small set of focused types in `src/RapidCluster.Core/` (names to finalize during implementation):

- `PaxosProposer`
  - Classic Paxos proposer logic (prepare/accept, timeout/retry handling as applicable).
- `FastPaxosProposer`
  - Fast Paxos proposer logic (fast-round proposal, fallback to classic as applicable).
- `PaxosAcceptor` (shared)
  - Holds promise/accepted state.
  - Handles both classic and fast-paxos acceptor-side message handling.
- `PaxosLearner` (optional but preferred)
  - Responsible for tracking votes/acks and determining “chosen” values for each relevant round.

Notes:

- If a standalone `Learner` ends up adding too much complexity, learner responsibilities may remain in `ConsensusCoordinator`, but the goal is to make “chosen-ness” logic as isolated and spec-mappable as possible.
- The shared `PaxosAcceptor` should be the only place that mutates acceptor state, so it is easy to unit test acceptor invariants.

### 2) ConsensusCoordinator becomes an orchestrator

Refactor `ConsensusCoordinator` to primarily:

- Create and wire `PaxosProposer` / `FastPaxosProposer` / `PaxosAcceptor` (and `PaxosLearner` if present).
- Translate between “cluster membership consensus intent” (e.g., propose membership view) and the underlying proposer APIs.
- Be the integration point for broadcasting and for surfacing the final `ConsensusResult`.

### 3) Naming strategy for messages and methods

Prefer names that are both:

- Easy to map to the spec, and
- Intuitive without needing to remember “Phase1a”.

Suggested direction:

- Classic Paxos:
  - `PrepareRequest` (Phase1a)
  - `PromiseResponse` (Phase1b)
  - `AcceptRequest` (Phase2a)
  - `AcceptedResponse` (Phase2b)
- Fast Paxos:
  - `FastAcceptRequest` / `FastAcceptedResponse` (if these concepts exist in current message set)
  - Preserve “fast-round” terminology in names where it’s important.

Implementation detail:

- Keep small comments on handlers indicating the corresponding phase (e.g., `// Phase1a (Prepare)`), but avoid making the *public* method names phase-number-centric.

### 4) Compatibility / migration approach

- First, mechanically split `Paxos` into new types without altering behavior.
- Then, rename/massage APIs and message types in a second pass.
- Ensure at each step:
  - existing tests remain green, and
  - new tests cover the refactor-specific invariants.

## Testing Plan

Add/expand coverage in both unit and simulation tests to make conformance easy to validate:

### Unit tests (preferred for role invariants)

Add tests in `tests/RapidCluster.Unit.Tests/` that validate:

- **Acceptor invariants**
  - Promise monotonicity (never promises a lower ballot/round than previously promised).
  - Accepted value/ballot rules (can’t accept below promise; accepted ballot monotonicity).
  - Correct behavior when receiving duplicate/re-ordered messages.
- **Classic proposer behavior**
  - Produces correct outbound messages given prior `PromiseResponse` sets.
  - Correctly adopts a previously accepted value when present in promises.
- **Fast proposer behavior**
  - Sends fast-round requests and transitions/fallback behavior is correct.

Where possible, structure tests in a spec-like style (“Given/When/Then”) and keep them stable against minor internal refactor.

### Simulation tests (distributed behavior)

Add at least one deterministic scenario in `tests/RapidCluster.Simulation.Tests/` that exercises:

- Multi-node consensus under message drops/reordering.
- A fast-path case and a fallback-to-classic case.

## Acceptance Criteria

- `Paxos` no longer acts as both proposer and acceptor.
- A single acceptor implementation is used by both classic and fast paths.
- All existing tests pass.
- New tests exist that explicitly validate acceptor invariants and proposer/learner correctness.

## Completed Work

### Role Separation

Implemented explicit Paxos roles as separate types:

- `PaxosAcceptor`: owns promise/accepted state and handles Phase1a/Phase2a.
- `PaxosProposer`: drives Phase1a/Phase1b and broadcasts Phase2a.
- `PaxosLearner`: collects Phase2b votes and publishes `Decided`.
- `FastPaxosProposer`: extracted from `FastPaxos` to make proposer responsibility explicit.

`Paxos` and `FastPaxos` remain as thin facades to minimize call-site churn.

### Tests Added

Unit tests were added to validate role invariants and behaviors:

- `tests/RapidCluster.Unit.Tests/PaxosAcceptorTests.cs`: promise monotonicity + accept rules.
- `tests/RapidCluster.Unit.Tests/PaxosProposerTests.cs`: ignores config/round mismatch; broadcasts Phase2a once when majority reached.
- `tests/RapidCluster.Unit.Tests/PaxosLearnerTests.cs`: quorum decision, duplicate handling, cancel behavior.

### Validation

- `dotnet build RapidCluster.slnx`
- Unit tests: `tests/RapidCluster.Unit.Tests` passed.
- Simulation tests: `tests/RapidCluster.Simulation.Tests` passed (existing skips unchanged).

## Deferred / Not Done

- No message/type aliasing was introduced (e.g., `PrepareRequest` vs `Phase1aMessage`) per instruction; phase-based message names remain.

## Implementation Notes / Pointers

Files likely involved:

- `src/RapidCluster.Core/Paxos.cs`
- `src/RapidCluster.Core/FastPaxos.cs`
- `src/RapidCluster.Core/ConsensusCoordinator.cs`
- `src/RapidCluster.Core/Protos/rapid_types.proto` (if message types are renamed)
- `tests/RapidCluster.Unit.Tests/PaxosTests.cs` / `tests/RapidCluster.Unit.Tests/FastPaxosTests.cs` (or new focused test files)
- `tests/RapidCluster.Simulation.Tests/ConsensusProtocolTests.cs` (or similar)
