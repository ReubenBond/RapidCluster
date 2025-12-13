using System.Diagnostics.CodeAnalysis;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for node restart scenarios using the simulation harness.
/// These tests verify that nodes can be restarted (crashed/left and recreated with the same address)
/// and that the cluster correctly assigns new MonotonicNodeIds to restarted nodes.
/// 
/// This is distinct from NodeRejoinTests which tests nodes joining with different addresses.
/// Restart tests specifically verify the MonotonicNodeId assignment logic for Paxos correctness.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class NodeRestartTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 98765;

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
    /// Tests that a node restarted at the same address gets a new MonotonicNodeId.
    /// This is critical for Paxos correctness - the restarted node must not reuse the old identity.
    /// </summary>
    [Fact]
    public void RestartedNode_GetsNewMonotonicNodeId()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Record the original MonotonicNodeId for joiner1
        var originalMonotonicId = joiner1.CurrentView.GetMonotonicNodeId(joiner1.Address);
        Assert.True(originalMonotonicId > 0, "Original node should have a MonotonicNodeId > 0");

        // Crash joiner1 (simulates process death)
        _harness.CrashNode(joiner1);

        // Wait for failure detection to remove the crashed node
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        // Restart with the SAME nodeId (same address)
        var restartedNode = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 3);

        // Verify the restarted node got a NEW (higher) MonotonicNodeId
        var newMonotonicId = restartedNode.CurrentView.GetMonotonicNodeId(restartedNode.Address);
        Assert.True(newMonotonicId > originalMonotonicId,
            $"Restarted node should have a higher MonotonicNodeId. Original: {originalMonotonicId}, New: {newMonotonicId}");

        // Verify all nodes agree on the new MonotonicNodeId
        Assert.Equal(newMonotonicId, seedNode.CurrentView.GetMonotonicNodeId(restartedNode.Address));
        Assert.Equal(newMonotonicId, joiner2.CurrentView.GetMonotonicNodeId(restartedNode.Address));
    }

    /// <summary>
    /// Tests that a node gracefully leaving and restarting at the same address gets a new MonotonicNodeId.
    /// </summary>
    [Fact]
    public void GracefulLeaveAndRestart_GetsNewMonotonicNodeId()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Record the original MonotonicNodeId
        var originalMonotonicId = joiner1.CurrentView.GetMonotonicNodeId(joiner1.Address);

        // Graceful leave
        _harness.RemoveNodeGracefully(joiner1);
        _harness.WaitForConvergence(expectedSize: 2);

        // Restart with the SAME nodeId (same address)
        var restartedNode = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 3);

        // Verify the restarted node got a NEW (higher) MonotonicNodeId
        var newMonotonicId = restartedNode.CurrentView.GetMonotonicNodeId(restartedNode.Address);
        Assert.True(newMonotonicId > originalMonotonicId,
            $"Restarted node should have a higher MonotonicNodeId. Original: {originalMonotonicId}, New: {newMonotonicId}");
    }

    /// <summary>
    /// Tests that MonotonicNodeIds are monotonically increasing across multiple restarts.
    /// </summary>
    [Fact]
    public void MultipleRestarts_MonotonicNodeIdsAlwaysIncrease()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        var previousMonotonicId = joiner1.CurrentView.GetMonotonicNodeId(joiner1.Address);
        var currentNode = joiner1;

        // Restart the same node 3 times
        for (var i = 0; i < 3; i++)
        {
            // Crash the node
            _harness.CrashNode(currentNode);
            _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

            // Restart with same address
            currentNode = _harness.CreateJoinerNode(seedNode, nodeId: 1);
            _harness.WaitForConvergence(expectedSize: 3);

            // Verify MonotonicNodeId increased
            var newMonotonicId = currentNode.CurrentView.GetMonotonicNodeId(currentNode.Address);
            Assert.True(newMonotonicId > previousMonotonicId,
                $"Restart {i + 1}: MonotonicNodeId should increase. Previous: {previousMonotonicId}, New: {newMonotonicId}");
            previousMonotonicId = newMonotonicId;
        }
    }

    /// <summary>
    /// Tests that the seed node can be restarted and gets a new MonotonicNodeId.
    /// </summary>
    [Fact]
    public void SeedNodeRestart_GetsNewMonotonicNodeId()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Record the original MonotonicNodeId for seed
        var originalMonotonicId = seedNode.CurrentView.GetMonotonicNodeId(seedNode.Address);

        // Crash the seed node
        _harness.CrashNode(seedNode);
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        // Restart seed with same address (nodeId: 0)
        // Note: Must join through an existing member since it's no longer the seed
        var restartedSeed = _harness.CreateJoinerNode(joiner1, nodeId: 0);
        _harness.WaitForConvergence(expectedSize: 3);

        // Verify the restarted seed got a NEW (higher) MonotonicNodeId
        var newMonotonicId = restartedSeed.CurrentView.GetMonotonicNodeId(restartedSeed.Address);
        Assert.True(newMonotonicId > originalMonotonicId,
            $"Restarted seed should have a higher MonotonicNodeId. Original: {originalMonotonicId}, New: {newMonotonicId}");
    }

    /// <summary>
    /// Tests that multiple nodes can be restarted at their original addresses.
    /// </summary>
    [Fact]
    public void MultipleNodesRestart_AllGetNewMonotonicNodeIds()
    {
        // Create a 5-node cluster (need 5 so crashing 2 leaves a majority of 3)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);

        _harness.WaitForConvergence(expectedSize: 5);

        // Record original MonotonicNodeIds
        var originalId1 = joiner1.CurrentView.GetMonotonicNodeId(joiner1.Address);
        var originalId2 = joiner2.CurrentView.GetMonotonicNodeId(joiner2.Address);

        // Crash both nodes (3 remaining is still a majority)
        _harness.CrashNode(joiner1);
        _harness.CrashNode(joiner2);
        _harness.WaitForConvergence(expectedSize: 3, maxIterations: 500000);

        // Restart both with same addresses
        var restarted1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 4);

        var restarted2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence(expectedSize: 5);

        // Verify both got new MonotonicNodeIds
        var newId1 = restarted1.CurrentView.GetMonotonicNodeId(restarted1.Address);
        var newId2 = restarted2.CurrentView.GetMonotonicNodeId(restarted2.Address);

        Assert.True(newId1 > originalId1,
            $"Restarted node 1 should have higher MonotonicNodeId. Original: {originalId1}, New: {newId1}");
        Assert.True(newId2 > originalId2,
            $"Restarted node 2 should have higher MonotonicNodeId. Original: {originalId2}, New: {newId2}");

        // Verify they got different IDs from each other
        Assert.NotEqual(newId1, newId2);
    }

    /// <summary>
    /// Tests that MaxMonotonicId in the view is tracked correctly across restarts.
    /// </summary>
    [Fact]
    public void MaxMonotonicId_TrackedCorrectlyAcrossRestarts()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Initial MaxMonotonicId should be 3 (seed=1, joiner1=2, joiner2=3)
        var initialMaxId = seedNode.CurrentView.MaxMonotonicId;
        Assert.Equal(3, initialMaxId);

        // Crash and restart joiner1
        _harness.CrashNode(joiner1);
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        var restarted = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 3);

        // MaxMonotonicId should have increased
        var newMaxId = seedNode.CurrentView.MaxMonotonicId;
        Assert.True(newMaxId > initialMaxId,
            $"MaxMonotonicId should increase after restart. Initial: {initialMaxId}, New: {newMaxId}");

        // All nodes should agree on MaxMonotonicId
        Assert.Equal(newMaxId, joiner2.CurrentView.MaxMonotonicId);
        Assert.Equal(newMaxId, restarted.CurrentView.MaxMonotonicId);
    }

    /// <summary>
    /// Tests that a node restart during ongoing consensus doesn't cause issues.
    /// </summary>
    [Fact]
    public void RestartDuringConsensus_ClusterConverges()
    {
        // Create a 4-node cluster for better fault tolerance
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Crash joiner1 while another node is joining
        _harness.CrashNode(joiner1);

        // Immediately try to add a new node (creates concurrent membership change)
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);

        // Wait for cluster to stabilize
        _harness.WaitForConvergence(expectedSize: 4, maxIterations: 500000);

        // Now restart joiner1 at its original address
        var restarted1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 5);

        // Verify all nodes converged
        Assert.All(_harness.Nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    /// <summary>
    /// Tests rapid restart cycles (crash and restart in quick succession).
    /// </summary>
    [Fact]
    public void RapidRestartCycles_ClusterRemainsConsistent()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        var currentNode = joiner1;
        var previousMaxId = seedNode.CurrentView.MaxMonotonicId;

        // Rapid crash/restart cycles
        for (var i = 0; i < 3; i++)
        {
            _harness.CrashNode(currentNode);
            _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

            currentNode = _harness.CreateJoinerNode(seedNode, nodeId: 1);
            _harness.WaitForConvergence(expectedSize: 3);

            // Verify MaxMonotonicId increased
            var currentMaxId = seedNode.CurrentView.MaxMonotonicId;
            Assert.True(currentMaxId > previousMaxId,
                $"Cycle {i + 1}: MaxMonotonicId should increase. Previous: {previousMaxId}, Current: {currentMaxId}");
            previousMaxId = currentMaxId;
        }

        // Verify final consistency
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
        Assert.All(_harness.Nodes, n => Assert.Equal(previousMaxId, n.CurrentView.MaxMonotonicId));
    }

    /// <summary>
    /// Tests that restarted nodes can participate in consensus with their new identity.
    /// </summary>
    [Fact]
    public void RestartedNode_CanParticipateInConsensus()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash and restart joiner1
        _harness.CrashNode(joiner1);
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        var restarted1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 3);

        // Now add a new node - the restarted node must participate in consensus
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 4);

        // Verify all nodes see the new member
        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));

        // Verify the restarted node has the correct view (use IsHostPresent which uses EndpointAddressComparer)
        Assert.True(restarted1.CurrentView.IsHostPresent(joiner3.Address),
            "Restarted node should see the new member joiner3 in its view");
    }

    /// <summary>
    /// Tests that nodes with the same address but different MonotonicNodeIds are handled correctly.
    /// This verifies the EndpointAddressComparer is used correctly throughout the system.
    /// </summary>
    [Fact]
    public void SameAddressDifferentMonotonicId_HandledCorrectly()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Get the original MonotonicNodeId from the membership view
        var originalMonotonicId = joiner1.CurrentView.GetMonotonicNodeId(joiner1.Address);
        var originalAddress = joiner1.Address;

        // Crash and restart
        _harness.CrashNode(joiner1);
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        var restarted = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 3);

        // The restarted node should have the same hostname:port
        var newAddress = restarted.Address;
        Assert.Equal(originalAddress.Hostname, newAddress.Hostname);
        Assert.Equal(originalAddress.Port, newAddress.Port);

        // But a different (higher) MonotonicNodeId from the membership view
        var newMonotonicId = restarted.CurrentView.GetMonotonicNodeId(restarted.Address);
        Assert.True(newMonotonicId > originalMonotonicId,
            $"Restarted node should have higher MonotonicNodeId. Original: {originalMonotonicId}, New: {newMonotonicId}");

        // Verify membership operations work correctly
        Assert.True(seedNode.CurrentView.IsHostPresent(newAddress));

        // The membership view should have exactly one member with this hostname:port
        var viewMembers = seedNode.CurrentView.Members;
        Assert.Single(viewMembers, m => m.Hostname == newAddress.Hostname && m.Port == newAddress.Port);
    }
}
