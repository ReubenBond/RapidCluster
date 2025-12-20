# 012 - Event-Driven ConsensusCoordinator (remove mandatory delays)

## Problem
`ConsensusCoordinator` currently performs classic Paxos rounds using a time-driven loop that includes an unconditional `Task.Delay(...)` per round. This imposes a *mandatory wait* even when the system already has enough information to:
- decide immediately (learner reached majority),
- cancel immediately,
- or retry immediately (e.g., in response to `PaxosNackMessage` indicating our rank is too low).

This can unnecessarily increase convergence time in healthy clusters and can cause latency spikes under fast message delivery.

## Goal
Make classic Paxos progression **event-driven**:
- Inbound messages should be able to advance state immediately.
- Timeouts should exist but should be **cancellable** and should not block progress.
- Retrying a higher round due to NACK should occur immediately (no waiting for a scheduled delay).

## Non-goals
- Change Paxos safety rules.
- Change the wire protocol.
- Optimize backoff formulas (keep existing delay computation semantics as the timeout duration).

## Proposed Design
### 1) Coordinator becomes a single-threaded event loop
Replace `RunClassicRoundsAsync` delay-driven loop with an internal event-processing loop (state machine) that serializes:
- inbound Paxos messages
- round timeouts
- cancellation

Implementation sketch:
- Introduce a private `Channel<ConsensusEvent>` in `ConsensusCoordinator`.
- `HandleMessages(...)` enqueues a `ConsensusEvent` instead of directly mutating roles.
- `RunConsensusLoopAsync(...)` continuously reads events until decided/cancelled.

This removes concurrency between "message arrival" and "round progression" by ensuring the coordinator processes both on one logical thread.

### 2) Explicit, cancellable round timeout scheduling
When starting a classic round `r`:
- Coordinator calls `_paxosProposer.StartPhase1a(r, ...)`.
- Coordinator schedules a `RoundTimeout(r)` event via `Task.Delay(delay)` and enqueues when it fires.
- If the coordinator advances to a different round (e.g., NACK jump, new attempt), it cancels the old timer.

### 3) Make NACK handling a signal, not an action
Currently `PaxosProposer.HandlePaxosNackMessage` may directly call `StartPhase1a(...)`, which means "a message handler" can mutate proposer state + broadcast outside of coordinator control.

Replace this with:
- `PaxosProposer.HandlePaxosNackMessage(...)` returning an optional requested round jump (e.g., `int? requestedRound`).
- The coordinator receives `PaxosNackMessage`, asks proposer what it implies, then decides whether to start a new round immediately.

This ensures the coordinator owns the round timeline.

### 4) Decide/cancel completion remains centralized
Existing centralized completion helpers (`CompleteDecided`, `CompleteCancelled`) remain, but are called from within the single-threaded coordinator loop.

## API Changes
- `PaxosProposer.HandlePaxosNackMessage(PaxosNackMessage nack, CancellationToken cancellationToken = default)`
  - change to `int? HandlePaxosNackMessage(PaxosNackMessage nack)` (or similar), returning the next round to attempt.
  - coordinator performs the `StartPhase1a(round, cancellationToken)` call.

## Testing Plan
Add/adjust unit tests to verify:
- **No mandatory delay:** when the learner decides immediately after starting a classic round, coordinator completes without waiting for the configured delay.
- **Immediate retry on NACK:** injecting a NACK that indicates a higher promised rank triggers an immediate round jump (and restarts timer) without waiting.

Implementation approach for tests:
- Use direct `ConsensusCoordinator.HandleMessages(...)` calls to feed Phase1b/Phase2b/NACK messages.
- Use very large `ConsensusFallbackTimeoutBaseDelay` to make delay effects observable.
- Validate completion time bounds using `Stopwatch` and `Task.WhenAny`/timeouts.

## Implementation Notes
- Introduce internal event types:
  - `StartFastRound`, `StartClassicRound`, `ClassicTimeout(round)`, `MessageReceived(request)`, `DisposeRequested`.
- Ensure event enqueueing is safe under disposal.
- Preserve existing metrics recording, but ensure round-start/round-complete events correspond to actual started rounds.

## Rollout
- Implement refactor + tests.
- Run unit tests.
- Consider running simulation tests for extra confidence after unit pass.
