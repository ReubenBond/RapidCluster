# Aspire Integration Tests: Node Failure and Rejoin Scenarios

## Problem

The Aspire integration tests (`samples/RapidCluster.Aspire/RapidCluster.Aspire.Tests/`) currently cover cluster formation and health check scenarios, but do not test node failure detection or node rejoin behavior. These are critical scenarios for a distributed cluster membership system.

## Current Limitation

The `Aspire.Hosting.Testing` NuGet package (v13.0.2) does not expose APIs to control individual resource lifecycles:

- `DistributedApplication.StopResourceAsync()` - **Does not exist**
- `DistributedApplication.StartResourceAsync()` - **Does not exist**

The testing framework is designed for starting the entire AppHost and running integration tests against running resources, not for controlling individual resource lifecycles dynamically.

## Desired Tests

1. **Node Failure Detection** - Stop a node and verify the remaining cluster members detect the failure and update their membership views
2. **Node Rejoin After Restart** - Stop a node, restart it, and verify it successfully rejoins the cluster
3. **Multiple Node Failures** - Stop multiple nodes and verify the cluster continues operating with reduced membership
4. **Health Status Degradation** - Verify health checks properly report degraded status when cluster membership is affected

## Proposed Solutions

### Option 1: Wait for Aspire API Updates
Monitor future releases of `Aspire.Hosting.Testing` for resource lifecycle control APIs. Microsoft may add these capabilities in future versions.

### Option 2: Use Process-Level Control
Instead of using Aspire's resource management, use .NET's `Process` class to find and terminate the node processes directly:
```csharp
// Find the process by port or name
// Kill the process
// Verify cluster detects failure
// Restart the process
// Verify rejoin
```

**Challenges:**
- Need to identify correct process (multiple `dotnet.exe` instances)
- Need to handle process cleanup properly
- Less reliable than Aspire-native control

### Option 3: Custom Resource Command Extension
Implement a custom Aspire resource command that exposes stop/start functionality:
```csharp
// In AppHost
nodes[i] = builder.AddProject<Projects.RapidCluster_Aspire_Node>($"cluster-{i}")
    .WithCommand("stop", "Stop", async context => { ... })
    .WithCommand("start", "Start", async context => { ... });
```

**Challenges:**
- Requires modifying the AppHost
- May not integrate cleanly with health checks

### Option 4: Rely on Simulation Tests (Current State)
The simulation tests already cover node failure/rejoin scenarios comprehensively:
- `tests/RapidCluster.Tests/Simulation/NodeFailureTests.cs`
- `tests/RapidCluster.Tests/Simulation/NodeRejoinTests.cs`
- `tests/RapidCluster.Tests/Simulation/GracefulLeaveTests.cs`

These tests are deterministic, fast, and reproducible. The Aspire tests can focus on real-world deployment concerns (HTTPS, health checks, service discovery) while simulation tests cover distributed protocol behavior.

## Recommendation

**Short-term:** Continue relying on simulation tests for node failure/rejoin scenarios. The current Aspire tests verify that the cluster forms correctly in a real Aspire environment with HTTPS and health checks.

**Long-term:** Revisit when Aspire.Hosting.Testing adds resource lifecycle control APIs, or implement Option 2 (process-level control) if these tests become critical for CI/CD validation.

## Related Files

- `samples/RapidCluster.Aspire/RapidCluster.Aspire.Tests/ClusterIntegrationTests.cs` - Current Aspire tests
- `tests/RapidCluster.Tests/Simulation/NodeFailureTests.cs` - Simulation failure tests
- `tests/RapidCluster.Tests/Simulation/NodeRejoinTests.cs` - Simulation rejoin tests
- `tests/RapidCluster.Tests/Integration/ClusterIntegrationTests.cs` - Non-Aspire integration tests

## References

- [Aspire Testing Documentation](https://learn.microsoft.com/en-us/dotnet/aspire/fundamentals/testing)
- [Aspire.Hosting.Testing NuGet Package](https://www.nuget.org/packages/Aspire.Hosting.Testing)

---

## Resolution (December 2025)

**Decision**: Option 4 - Rely on Simulation Tests

After thorough investigation, the simulation tests provide comprehensive coverage of node failure and rejoin scenarios:

### Simulation Test Coverage

**NodeFailureTests.cs** (14 tests):
- `NodeCrashRemovesFromCluster` - Verifies crashed nodes are removed
- `CrashedNodeCannotReceiveMessages` - Verifies crashed nodes are excluded
- `ClusterContinuesAfterSingleNodeCrash` - Cluster resilience
- `CrashDuringIdleStateHandledGracefully` - Edge case handling
- `TwoNodeFailuresInFiveNodeCluster` - Multiple failure handling
- `SequentialFailuresHandled` - Sequential crash handling
- `SimultaneousFailuresHandled` - Concurrent crash handling
- `SeedNodeCrashDoesNotAffectExistingCluster` - Seed node failure
- `JoinThroughCrashedSeedFails` - Join failure handling
- `AlternativeSeedAllowsJoin` - Alternative seed usage
- `NodeCrashDuringJoinProtocol` - (Skipped - complex race condition)
- `NodeCrashDuringLeave` - Crash during leave handling

**NodeRejoinTests.cs** (13 tests):
- `NodeCanRejoinAfterGracefulLeave` - Basic rejoin
- `NodeCanRejoinAfterCrashAndFailureDetection` - Crash recovery rejoin
- `NodeCanRejoinMultipleTimes` - Multiple rejoin cycles
- `SeedNodeCanLeaveAndNewNodeCanJoin` - Seed node replacement
- `MultipleNodesCanRejoinAfterLeaving` - Multiple node rejoin
- `NewNodeCanJoinDuringFailureConvergence` - Concurrent operations
- `RapidLeaveRejoinCyclesSucceed` - Stress testing
- `AlternatingJoinLeaveStabilizes` - Cluster stability
- `IsolatedNodeCanRejoinAfterPartitionHeals` - Partition recovery
- `RejoinThroughDifferentSeedAfterOriginalSeedCrashes` - Seed failover
- `ClusterRecoversFromHalfNodeLoss` - Major failure recovery
- `ClusterMaintainsConsistencyThroughFailureRecoveryCycles` - Consistency verification

### Rationale

1. **Simulation tests are deterministic** - Same seed produces identical results, making debugging reproducible
2. **Fast execution** - No real I/O or network delays
3. **Comprehensive coverage** - Can test edge cases like partitions, simultaneous failures, rapid cycles
4. **Aspire tests serve different purpose** - Verify real-world deployment (HTTPS, health checks, service discovery)

### Future Consideration

Revisit this decision when:
- `Aspire.Hosting.Testing` adds `StopResourceAsync`/`StartResourceAsync` APIs
- Node failure/rejoin in Aspire environment becomes a critical CI/CD requirement

**Status**: Closed - No action required
