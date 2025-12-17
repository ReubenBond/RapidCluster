using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for network partition scenarios using the simulation harness.
/// Covers simple partitions, isolation scenarios, split-brain prevention, and partition/heal sequences.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class NetworkPartitionTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 34567;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _harness.DisposeAsync();
    }

    [Fact]
    public void BidirectionalPartitionBlocksMessages()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Create bidirectional partition
        _harness.PartitionNodes(seedNode, joiner);

        // Verify partition is in place
        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);
        var joinerAddr = RapidClusterUtils.Loggable(joiner.Address);

        Assert.False(_harness.Network.CanDeliver(seedAddr, joinerAddr));
        Assert.False(_harness.Network.CanDeliver(joinerAddr, seedAddr));
    }

    [Fact]
    public void UnidirectionalPartitionAllowsOneWay()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);
        var joinerAddr = RapidClusterUtils.Loggable(joiner.Address);

        // Create unidirectional partition (seed -> joiner blocked, joiner -> seed allowed)
        _harness.Network.CreatePartition(seedAddr, joinerAddr);

        Assert.False(_harness.Network.CanDeliver(seedAddr, joinerAddr));
        Assert.True(_harness.Network.CanDeliver(joinerAddr, seedAddr));
    }

    [Fact]
    public void PartitionedNodeEventuallyDetected()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Isolate joiner2
        _harness.IsolateNode(joiner2);

        // Wait for failure detection and removal
        // With 3 nodes, the remaining 2 can reach consensus to remove the isolated node
        // Only check the non-isolated nodes - the isolated node won't see the updated view
        _harness.WaitForConvergence([seedNode, joiner1], expectedSize: 2);

        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner1.MembershipSize);
    }

    [Fact]
    public void PartitionHealRestoresConnectivity()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Create partition
        _harness.PartitionNodes(seedNode, joiner);

        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);
        var joinerAddr = RapidClusterUtils.Loggable(joiner.Address);

        Assert.False(_harness.Network.CanDeliver(seedAddr, joinerAddr));

        // Heal partition
        _harness.HealPartition(seedNode, joiner);

        Assert.True(_harness.Network.CanDeliver(seedAddr, joinerAddr));
        Assert.True(_harness.Network.CanDeliver(joinerAddr, seedAddr));
    }

    [Fact]
    public void IsolatedNodeCannotCommunicate()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Isolate joiner2
        _harness.IsolateNode(joiner2);

        var joiner2Addr = RapidClusterUtils.Loggable(joiner2.Address);
        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);
        var joiner1Addr = RapidClusterUtils.Loggable(joiner1.Address);

        // Joiner2 should not be able to communicate with anyone
        Assert.False(_harness.Network.CanDeliver(joiner2Addr, seedAddr));
        Assert.False(_harness.Network.CanDeliver(joiner2Addr, joiner1Addr));
        Assert.False(_harness.Network.CanDeliver(seedAddr, joiner2Addr));
        Assert.False(_harness.Network.CanDeliver(joiner1Addr, joiner2Addr));
    }

    [Fact]
    public void ReconnectedNodeRestoresConnectivity()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Isolate
        _harness.IsolateNode(joiner);

        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);
        var joinerAddr = RapidClusterUtils.Loggable(joiner.Address);

        Assert.False(_harness.Network.CanDeliver(seedAddr, joinerAddr));

        // Reconnect
        _harness.ReconnectNode(joiner);

        Assert.True(_harness.Network.CanDeliver(seedAddr, joinerAddr));
    }

    [Fact]
    public void MultipleNodesIsolatedSimultaneously()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Isolate multiple nodes
        _harness.IsolateNode(joiner2);
        _harness.IsolateNode(joiner3);

        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);
        var joiner1Addr = RapidClusterUtils.Loggable(joiner1.Address);
        var joiner2Addr = RapidClusterUtils.Loggable(joiner2.Address);
        var joiner3Addr = RapidClusterUtils.Loggable(joiner3.Address);

        // Seed and joiner1 can still communicate
        Assert.True(_harness.Network.CanDeliver(seedAddr, joiner1Addr));
        Assert.True(_harness.Network.CanDeliver(joiner1Addr, seedAddr));

        // Isolated nodes cannot communicate with anyone
        Assert.False(_harness.Network.CanDeliver(joiner2Addr, seedAddr));
        Assert.False(_harness.Network.CanDeliver(joiner3Addr, seedAddr));
    }

    [Fact]
    public async Task InvariantCheckerDetectsSplitBrainAttempt()
    {
        await using var simulationHarness = new RapidSimulationCluster(seed: TestSeed);
        var checker = new InvariantChecker(simulationHarness);

        var seedNode = simulationHarness.CreateSeedNode();

        // Check that single node doesn't trigger split-brain detection
        var result = checker.CheckNoSplitBrain();
        Assert.True(result);
        Assert.False(checker.HasViolations);
    }

    [Fact]
    public void PartitionThenHealBeforeDetection()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Create partition
        _harness.PartitionNodes(seedNode, joiner);

        // Heal immediately (before failure detection)
        _harness.HealPartition(seedNode, joiner);

        // Both nodes should still see each other
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void RepeatedPartitionHealCycles()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        for (var i = 0; i < 5; i++)
        {
            // Create and heal partition rapidly
            _harness.PartitionNodes(seedNode, joiner);
            _harness.HealPartition(seedNode, joiner);
        }

        // Cluster should still be intact
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void HealAllPartitionsWorks()
    {
        var seedNode = _harness.CreateSeedNode();

        var seedAddr = RapidClusterUtils.Loggable(seedNode.Address);

        // Create multiple partitions
        _harness.Network.CreatePartition(seedAddr, "fake:1");
        _harness.Network.CreatePartition(seedAddr, "fake:2");
        _harness.Network.CreatePartition(seedAddr, "fake:3");

        Assert.False(_harness.Network.CanDeliver(seedAddr, "fake:1"));

        // Heal all partitions at once
        _harness.Network.HealAllPartitions();

        Assert.True(_harness.Network.CanDeliver(seedAddr, "fake:1"));
        Assert.True(_harness.Network.CanDeliver(seedAddr, "fake:2"));
        Assert.True(_harness.Network.CanDeliver(seedAddr, "fake:3"));
    }

}
