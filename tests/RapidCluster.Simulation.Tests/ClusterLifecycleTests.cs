using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for complete cluster lifecycle scenarios using the simulation harness.
/// These tests verify end-to-end behavior including startup, scale-up/down, and recovery scenarios.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class ClusterLifecycleTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 90123;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public void FullClusterLifecycle()
    {
        // Create seed node
        var seedNode = _harness.CreateSeedNode();
        Assert.True(seedNode.IsInitialized);
        Assert.Equal(1, seedNode.MembershipSize);

        // Join additional nodes
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // All nodes operational
        Assert.All(_harness.Nodes, n => Assert.True(n.IsInitialized));
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));

        // Shutdown all nodes
        foreach (var node in _harness.Nodes.ToList())
        {
            _harness.CrashNode(node);
        }

        Assert.Empty(_harness.Nodes);
    }

    [Fact]
    public void ClusterScaleUpAndDown()
    {
        // Start with seed
        var seedNode = _harness.CreateSeedNode();

        // Scale up to 4 nodes
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence();
        Assert.Equal(4, _harness.Nodes.Count);

        // Scale down by removing nodes
        _harness.CrashNode(joiner3);
        _harness.CrashNode(joiner2);

        Assert.Equal(2, _harness.Nodes.Count);
    }

    [Fact]
    public void EmergencyShutdownAllNodes()
    {
        // Create several seed nodes (each is independent)
        for (var i = 0; i < 5; i++)
        {
            _harness.CreateSeedNode(i);
        }

        Assert.Equal(5, _harness.Nodes.Count);

        // Emergency shutdown all nodes at once
        var nodesToCrash = _harness.Nodes.ToList();
        foreach (var node in nodesToCrash)
        {
            _harness.CrashNode(node);
        }

        Assert.Empty(_harness.Nodes);
    }

    [Fact]
    public void JoinThroughDifferentSeeds()
    {
        // Create initial two-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence();

        // Join through joiner1 instead of original seed
        var joiner2 = _harness.CreateJoinerNode(joiner1, nodeId: 2);

        _harness.WaitForConvergence();

        Assert.True(joiner2.IsInitialized);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void RecoveryFromPartitionedState()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence();

        // Create partition
        _harness.PartitionNodes(seedNode, joiner);

        // Heal partition (before failure detection)
        _harness.HealPartition(seedNode, joiner);

        // Both nodes should still be operational
        Assert.True(seedNode.IsInitialized);
        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void ClusterContinuesAfterSeedNodeRemoval()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Remove the original seed
        _harness.CrashNode(seedNode);

        // Remaining nodes should continue operating
        Assert.True(joiner1.IsInitialized);
        Assert.True(joiner2.IsInitialized);
        Assert.Equal(2, _harness.Nodes.Count);
    }

    [Fact]
    public void NewJoinsWorkAfterMembershipChange()
    {
        // Use 3-node cluster so remaining 2 nodes can reach quorum after crash
        var seedNode = _harness.CreateSeedNode();
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Remove joiner2
        _harness.CrashNode(joiner2);

        // Wait for the remaining nodes to detect the failure and remove joiner2 from membership
        // With 3 nodes, the remaining 2 can reach quorum for consensus
        _harness.WaitForConvergence();

        // Add a new joiner through seed
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        Assert.True(joiner3.IsInitialized);
        _harness.WaitForConvergence();
    }

    [Fact]
    public void ViewAccessorInitiallyHasView()
    {
        var seedNode = _harness.CreateSeedNode();

        Assert.NotNull(seedNode.ViewAccessor);
        Assert.NotNull(seedNode.ViewAccessor.CurrentView);
        Assert.Equal(1, seedNode.ViewAccessor.CurrentView.Size);
    }

    [Fact]
    public void ViewAccessorUpdatesOnJoin()
    {
        var seedNode = _harness.CreateSeedNode();
        var initialConfigId = seedNode.ViewAccessor.CurrentView.ConfigurationId;
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence();

        // View should have been updated
        Assert.True(seedNode.ViewAccessor.CurrentView.ConfigurationId > initialConfigId);
        Assert.Equal(2, seedNode.ViewAccessor.CurrentView.Size);
    }

    [Fact]
    public void JoinerViewAccessorHasCorrectView()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Joiner's view accessor should have correct view
        Assert.NotNull(joiner.ViewAccessor);
        Assert.Equal(2, joiner.ViewAccessor.CurrentView.Size);
    }
}
