using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for cluster invariants using the simulation harness.
/// Verifies safety and liveness properties hold during various scenarios.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class InvariantVerificationTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private InvariantChecker _checker = null!;
    private const int TestSeed = 56789;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        _checker = new InvariantChecker(_harness);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public void MembershipViewNeverEmptyForInitializedNode()
    {
        var seedNode = _harness.CreateSeedNode();

        Assert.NotNull(seedNode.CurrentView);
        Assert.True(seedNode.CurrentView.Size > 0);
    }

    [Fact]
    public void SelfAlwaysInMembershipView()
    {
        var seedNode = _harness.CreateSeedNode();

        var view = seedNode.CurrentView;
        var selfAddress = string.Create(CultureInfo.InvariantCulture, $"{seedNode.Address.Hostname.ToStringUtf8()}:{seedNode.Address.Port}");
        var viewAddresses = view.Members.Select(m => string.Create(CultureInfo.InvariantCulture, $"{m.Hostname.ToStringUtf8()}:{m.Port}")).ToHashSet(StringComparer.Ordinal);

        Assert.Contains(selfAddress, viewAddresses);
    }

    [Fact]
    public void AllNodesInViewAreKnownNodes()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.RunUntilConverged(expectedSize: 2);

        var knownAddresses = _harness.Nodes
            .Select(n => string.Create(CultureInfo.InvariantCulture, $"{n.Address.Hostname.ToStringUtf8()}:{n.Address.Port}"))
            .ToHashSet(StringComparer.Ordinal);

        var viewAddresses = seedNode.CurrentView.Members
            .Select(m => string.Create(CultureInfo.InvariantCulture, $"{m.Hostname.ToStringUtf8()}:{m.Port}"))
            .ToHashSet(StringComparer.Ordinal);

        // All addresses in view should be known nodes
        Assert.Subset(viewAddresses, knownAddresses);
    }

    [Fact]
    public void NoSplitBrainWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckNoSplitBrain();

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void NoSplitBrainWithTwoNodes()
    {
        var seedNode = _harness.CreateSeedNode();
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.RunUntilConverged(expectedSize: 2);

        var result = _checker.CheckNoSplitBrain();

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void ConfigurationIdMonotonicityWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckConfigurationIdMonotonicity();

        Assert.True(result);
    }

    [Fact]
    public void ConfigurationIdMonotonicityAfterJoin()
    {
        var seedNode = _harness.CreateSeedNode();
        var initialConfigId = seedNode.CurrentView.ConfigurationId;
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.RunUntilConverged(expectedSize: 2);

        // Config ID should have changed after membership change (it's a hash, not monotonically increasing)
        Assert.NotEqual(initialConfigId, seedNode.CurrentView.ConfigurationId);

        var result = _checker.CheckConfigurationIdMonotonicity();
        Assert.True(result);
    }

    [Fact]
    public void MembershipConsistencyWithNoNodes()
    {
        var result = _checker.CheckMembershipConsistency();
        Assert.True(result);
    }

    [Fact]
    public void MembershipConsistencyWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckMembershipConsistency();
        Assert.True(result);
    }

    [Fact]
    public void MembershipConsistencyWithConvergedCluster()
    {
        var seedNode = _harness.CreateSeedNode();
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.RunUntilConverged(expectedSize: 2);

        var result = _checker.CheckMembershipConsistency();
        Assert.True(result);
    }

    [Fact]
    public void CheckAllWithEmptyCluster()
    {
        var result = _checker.CheckAll();
        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void CheckAllWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckAll();
        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void CheckAllWithTwoNodes()
    {
        var seedNode = _harness.CreateSeedNode();
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.RunUntilConverged(expectedSize: 2);

        var result = _checker.CheckAll();
        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    /// <summary>
    /// Verifies that the liveness checker can detect when the system has pending tasks
    /// and is capable of making progress. This tests the eventual progress guarantee
    /// that the cluster should maintain under normal operation.
    /// </summary>
    [Fact]
    public void EventualProgressGuaranteeWithPendingTasks()
    {
        // Create a task that will allow progress
        var task = new Task(() => { });
        task.Start(_harness.TaskScheduler);

        // CheckLiveness should succeed when there are tasks to execute
        var result = _checker.CheckLiveness(maxSteps: 100);

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    /// <summary>
    /// Verifies that the liveness checker correctly identifies deadlock scenarios
    /// where no progress can be made. Tests the negative case where the system
    /// has no pending tasks and no scheduled timers, indicating a potential deadlock.
    /// </summary>
    [Fact]
    public void EventualProgressDetectsDeadlock()
    {
        // With no pending tasks and no time-based triggers, liveness check should fail
        var result = _checker.CheckLiveness(maxSteps: 100);

        // Should fail since no progress can be made
        Assert.False(result);
        Assert.True(_checker.HasViolations);
    }

    /// <summary>
    /// Tests the liveness property that join operations eventually complete.
    /// Verifies that a node attempting to join the cluster will finish the join
    /// protocol within a reasonable timeout, ensuring the system doesn't hang.
    /// </summary>
    [Fact]
    public void JoinEventuallyCompletes()
    {
        var seedNode = _harness.CreateSeedNode();

        // Start a join operation and drive to completion
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized);
    }

    /// <summary>
    /// Tests the liveness property that node failures are eventually detected by the cluster.
    /// When a node crashes, the failure detection mechanism should identify it within the
    /// configured timeout period and trigger membership updates.
    /// Uses a 3-node cluster so the remaining 2 nodes can reach consensus to remove the failed node.
    /// </summary>
    [Fact]
    public void FailureDetectionEventuallyOccurs()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.RunUntilConverged(expectedSize: 3);

        // Crash one joiner
        _harness.CrashNode(joiner2);

        // Failure should eventually be detected - run until remaining nodes see size of 2
        var detected = _harness.RunUntil(() => seedNode.MembershipSize == 2 && joiner1.MembershipSize == 2, maxIterations: 100000);

        Assert.True(detected, "Remaining nodes did not detect joiner failure");
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner1.MembershipSize);
    }

    /// <summary>
    /// Verifies that after a network partition is healed, the cluster eventually reconverges
    /// to a consistent state. This tests the self-healing property where temporary network
    /// issues don't permanently damage the cluster's ability to reach consensus.
    /// </summary>
    [Fact]
    public void PartitionHealEventuallyConverges()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.RunUntilConverged(expectedSize: 2);

        // Create partition
        _harness.PartitionNodes(seedNode, joiner);

        // Heal partition
        _harness.HealPartition(seedNode, joiner);

        // Nodes should eventually re-converge
        var converged = _harness.RunUntilConverged(expectedSize: 2, maxIterations: 50000);

        Assert.True(converged, "Nodes did not reconverge after partition heal");
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void ViolationsClearWorks()
    {
        // Initially no violations
        Assert.False(_checker.HasViolations);
        Assert.Empty(_checker.Violations);

        // Clear should not throw even when empty
        _checker.Clear();

        Assert.False(_checker.HasViolations);
        Assert.Empty(_checker.Violations);
    }

    [Fact]
    public void ViolationsListIsImmutableCopy()
    {
        var violations1 = _checker.Violations;
        var violations2 = _checker.Violations;

        // Each call should return a new copy
        Assert.NotSame(violations1, violations2);
    }
}
