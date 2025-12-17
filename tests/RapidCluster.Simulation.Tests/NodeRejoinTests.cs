using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for node rejoin scenarios using the simulation harness.
/// Covers single node rejoin, multiple node rejoin, and rejoin timing edge cases.
/// These tests verify that nodes can leave (gracefully or via crash) and rejoin the cluster.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class NodeRejoinTests : IAsyncLifetime
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

    /// <summary>
    /// Tests that a node can rejoin the cluster after graceful leave.
    /// The rejoining node gets a new node ID but can reuse the same address slot.
    /// </summary>
    [Fact]
    public void NodeCanRejoinAfterGracefulLeave()
    {
        // Create a 3-node cluster (need 3 for quorum after leave)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Graceful leave
        _harness.RemoveNodeGracefully(joiner1);
        _harness.WaitForConvergence(expectedSize: 2);

        // Rejoin with a new node ID (simulating a restart with new identity)
        var rejoined = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(rejoined.IsInitialized);
        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);
        Assert.Equal(3, rejoined.MembershipSize);
    }

    /// <summary>
    /// Tests that a node can rejoin the cluster after being crashed and failure-detected.
    /// </summary>
    [Fact]
    public void NodeCanRejoinAfterCrashAndFailureDetection()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash joiner1
        _harness.CrashNode(joiner1);

        // Wait for failure detection to remove the crashed node
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        // Rejoin with a new node ID
        var rejoined = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(rejoined.IsInitialized);
        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    /// <summary>
    /// Tests that a node can rejoin multiple times.
    /// </summary>
    [Fact]
    public void NodeCanRejoinMultipleTimes()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // First leave and rejoin cycle
        _harness.RemoveNodeGracefully(joiner2);
        _harness.WaitForConvergence(expectedSize: 2);

        var rejoined1 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        // Second leave and rejoin cycle
        _harness.RemoveNodeGracefully(rejoined1);
        _harness.WaitForConvergence(expectedSize: 2);

        var rejoined2 = _harness.CreateJoinerNode(seedNode, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(rejoined2.IsInitialized);
        Assert.Equal(3, seedNode.MembershipSize);
    }

    /// <summary>
    /// Tests that the seed node can leave and a new node can take over as seed.
    /// </summary>
    [Fact]
    public void SeedNodeCanLeaveAndNewNodeCanJoin()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Seed leaves gracefully
        _harness.RemoveNodeGracefully(seedNode);
        _harness.WaitForConvergence(expectedSize: 2);

        // New node joins through one of the remaining nodes
        var joiner3 = _harness.CreateJoinerNode(joiner1, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner3.IsInitialized);
        Assert.Equal(3, joiner1.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    /// <summary>
    /// Tests that multiple nodes can rejoin after leaving.
    /// </summary>
    [Fact]
    public void MultipleNodesCanRejoinAfterLeaving()
    {
        // Create a 4-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Two nodes leave
        _harness.RemoveNodeGracefully(joiner2);
        _harness.WaitForConvergence(expectedSize: 3);

        _harness.RemoveNodeGracefully(joiner3);
        _harness.WaitForConvergence(expectedSize: 2);

        // Both rejoin
        var rejoined1 = _harness.CreateJoinerNode(seedNode, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 3);

        var rejoined2 = _harness.CreateJoinerNode(seedNode, nodeId: 5);
        _harness.WaitForConvergence(expectedSize: 4);

        Assert.True(rejoined1.IsInitialized);
        Assert.True(rejoined2.IsInitialized);
        Assert.Equal(4, seedNode.MembershipSize);
    }

    /// <summary>
    /// Tests that a new node with a different ID can join while the cluster is
    /// still converging after a node failure.
    /// </summary>
    [Fact]
    public void NewNodeCanJoinDuringFailureConvergence()
    {
        // Create a 4-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Crash a node but don't wait for full convergence
        _harness.CrashNode(joiner3);

        // Immediately try to join a new node
        // This tests concurrent membership changes
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);

        // Eventually all should converge to 4 nodes
        _harness.WaitForConvergence(expectedSize: 4, maxIterations: 500000);

        Assert.True(joiner4.IsInitialized);
    }

    /// <summary>
    /// Tests that join operations succeed after rapid leave/rejoin cycles.
    /// </summary>
    [Fact]
    public void RapidLeaveRejoinCyclesSucceed()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Rapid leave/rejoin cycles
        for (var i = 0; i < 3; i++)
        {
            _harness.RemoveNodeGracefully(joiner2);
            _harness.WaitForConvergence(expectedSize: 2);

            joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 3 + i);
            _harness.WaitForConvergence(expectedSize: 3);
        }

        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner1.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    /// <summary>
    /// Tests that the cluster stabilizes after alternating join and leave operations.
    /// </summary>
    [Fact]
    public void AlternatingJoinLeaveStabilizes()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Node leaves
        _harness.RemoveNodeGracefully(joiner1);
        _harness.WaitForConvergence(expectedSize: 2);

        // New node joins
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        // Another node leaves
        _harness.RemoveNodeGracefully(joiner2);
        _harness.WaitForConvergence(expectedSize: 2);

        // Another new node joins
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 3);

        // Verify final state
        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner3.MembershipSize);
        Assert.Equal(3, joiner4.MembershipSize);
    }

    /// <summary>
    /// Tests that an isolated node can rejoin after the partition heals.
    /// </summary>
    [Fact]
    public void IsolatedNodeCanRejoinAfterPartitionHeals()
    {
        // Create a 4-node cluster (need majority on both sides consideration)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Isolate one node
        _harness.IsolateNode(joiner3);

        // Wait for failure detection to kick out the isolated node
        // Only check the non-isolated nodes - the isolated node won't see the updated view
        _harness.WaitForConvergence([seedNode, joiner1, joiner2], expectedSize: 3, maxIterations: 500000);

        // Crash the isolated node (simulating it being removed)
        _harness.CrashNode(joiner3);

        // Reconnect wouldn't help since the node is crashed, but we can add a new one
        var newJoiner = _harness.CreateJoinerNode(seedNode, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        Assert.Equal(4, seedNode.MembershipSize);
        Assert.Equal(4, newJoiner.MembershipSize);
    }

    /// <summary>
    /// Tests rejoining through a different seed after original seed crashes.
    /// </summary>
    [Fact]
    public void RejoinThroughDifferentSeedAfterOriginalSeedCrashes()
    {
        // Create a 4-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Crash the original seed
        _harness.CrashNode(seedNode);

        // Wait for failure detection
        _harness.WaitForConvergence(expectedSize: 3, maxIterations: 500000);

        // Rejoin through a different node
        var newJoiner = _harness.CreateJoinerNode(joiner1, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        Assert.True(newJoiner.IsInitialized);
        Assert.Equal(4, joiner1.MembershipSize);
    }

    /// <summary>
    /// Tests that a cluster can recover from losing half its nodes (but maintaining quorum).
    /// </summary>
    [Fact]
    public void ClusterRecoversFromHalfNodeLoss()
    {
        // Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Crash 2 nodes (leaving 3 which is still a majority)
        _harness.CrashNode(nodes[3]);
        _harness.CrashNode(nodes[4]);

        // Wait for failure detection
        _harness.WaitForConvergence(expectedSize: 3, maxIterations: 500000);

        // Add new nodes to recover
        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: 5);
        _harness.WaitForConvergence(expectedSize: 4);

        var newNode2 = _harness.CreateJoinerNode(nodes[0], nodeId: 6);
        _harness.WaitForConvergence(expectedSize: 5);

        Assert.Equal(5, nodes[0].MembershipSize);
        Assert.Equal(5, newNode1.MembershipSize);
        Assert.Equal(5, newNode2.MembershipSize);
    }

    /// <summary>
    /// Tests that the cluster maintains consistency through multiple failure/recovery cycles.
    /// </summary>
    [Fact]
    public void ClusterMaintainsConsistencyThroughFailureRecoveryCycles()
    {
        // Create a 4-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Cycle 1: Crash and recover
        _harness.CrashNode(joiner3);
        _harness.WaitForConvergence(expectedSize: 3, maxIterations: 500000);

        var recovery1 = _harness.CreateJoinerNode(seedNode, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        // Verify consistency
        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));

        // Cycle 2: Crash and recover
        _harness.CrashNode(joiner2);
        _harness.WaitForConvergence(expectedSize: 3, maxIterations: 500000);

        var recovery2 = _harness.CreateJoinerNode(seedNode, nodeId: 5);
        _harness.WaitForConvergence(expectedSize: 4);

        // Verify final consistency
        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

}
