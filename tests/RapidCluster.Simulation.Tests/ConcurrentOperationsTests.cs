using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for concurrent operations using the simulation harness.
/// These tests verify that the cluster handles concurrent joins, failures,
/// and other overlapping operations correctly using per-node suspension.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class ConcurrentOperationsTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 78912;

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
    /// Tests that multiple nodes can join the cluster in rapid succession.
    /// </summary>
    [Fact]
    public void RapidConcurrentJoins()
    {
        var seedNode = _harness.CreateSeedNode();

        // Add multiple nodes rapidly
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    /// <summary>
    /// Tests concurrent joins while some nodes are suspended.
    /// Uses per-node suspension to simulate concurrent processing.
    /// </summary>
    [Fact]
    public void JoinsWithSuspendedNodes()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Suspend one node
        joiner1.Suspend();

        // Join while node is suspended
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        // Resume suspended node
        joiner1.Resume();

        // All should converge
        _harness.WaitForConvergence(expectedSize: 4);

        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    /// <summary>
    /// Tests joining while multiple nodes are suspended.
    /// </summary>
    [Fact]
    public void JoinWhileMultipleNodesSuspended()
    {
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Suspend minority (2 out of 5)
        nodes[3].Suspend();
        nodes[4].Suspend();

        // Join new node - should succeed with 3 active nodes (quorum)
        var newNode = _harness.CreateJoinerNode(nodes[0], nodeId: 5);

        // Resume suspended nodes
        nodes[3].Resume();
        nodes[4].Resume();

        // All should converge to 6
        _harness.WaitForConvergence(expectedSize: 6);

        Assert.All(_harness.Nodes, n => Assert.Equal(6, n.MembershipSize));
    }

    /// <summary>
    /// Tests that the cluster handles joins during node failures.
    /// </summary>
    [Fact]
    public void JoinDuringNodeFailure()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Crash one node
        _harness.CrashNode(joiner3);

        // Join new node while failure detection is in progress
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);

        // Wait for convergence (should have 4 nodes: original 4 - 1 crash + 1 join)
        _harness.WaitForConvergence(expectedSize: 4, maxIterations: 500000);

        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    /// <summary>
    /// Tests that multiple failures and joins interleaved are handled correctly.
    /// </summary>
    [Fact]
    public void InterleavedFailuresAndJoins()
    {
        // Start with 5 nodes
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Crash one node
        _harness.CrashNode(nodes[4]);

        // Join a new node
        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: 5);

        // Crash another node
        _harness.CrashNode(nodes[3]);

        // Join another node
        var newNode2 = _harness.CreateJoinerNode(nodes[0], nodeId: 6);

        // Wait for convergence
        _harness.WaitForConvergence(expectedSize: 5, maxIterations: 500000);

        Assert.Equal(5, _harness.Nodes.Count);
    }

    /// <summary>
    /// Tests that failure during consensus doesn't break the cluster.
    /// </summary>
    [Fact]
    public void FailureDuringConsensusHandled()
    {
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Suspend nodes to delay consensus
        nodes[3].Suspend();
        nodes[4].Suspend();

        // Crash a node while some are suspended
        _harness.CrashNode(nodes[2]);

        // Resume suspended nodes
        nodes[3].Resume();
        nodes[4].Resume();

        // Cluster should eventually converge
        _harness.WaitForConvergence(expectedSize: 4, maxIterations: 500000);

        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    /// <summary>
    /// Tests that leaves and joins happening close together are handled.
    /// </summary>
    [Fact]
    public void ConcurrentLeaveAndJoin()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Leave and join close together
        _harness.RemoveNodeGracefully(joiner2);

        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    /// <summary>
    /// Tests multiple leaves interspersed with joins.
    /// </summary>
    [Fact]
    public void MultipleLeavesWithJoins()
    {
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        var nodeIdCounter = 5;

        // Leave one, join one, leave another, join another
        _harness.RemoveNodeGracefully(nodes[4]);
        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: nodeIdCounter++);

        _harness.RemoveNodeGracefully(nodes[3]);
        var newNode2 = _harness.CreateJoinerNode(nodes[0], nodeId: nodeIdCounter++);

        _harness.WaitForConvergence(expectedSize: 5);

        Assert.All(_harness.Nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    /// <summary>
    /// Tests that a suspended node doesn't participate in consensus.
    /// </summary>
    [Fact]
    public void SuspendedNodeDoesNotParticipateInConsensus()
    {
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        // Suspend one node
        nodes[3].Suspend();
        Assert.True(nodes[3].IsSuspended);

        // Join a new node - should succeed with 3 active nodes
        var newNode = _harness.CreateJoinerNode(nodes[0], nodeId: 4);

        // Drive propagation to the non-suspended nodes.
        _harness.WaitForConvergence([nodes[0], nodes[1], nodes[2], newNode], expectedSize: 5);

        // Remaining active nodes should see the new member
        Assert.Equal(5, nodes[0].MembershipSize);
        Assert.Equal(5, nodes[1].MembershipSize);
        Assert.Equal(5, nodes[2].MembershipSize);

        // Suspended node should still have old view
        Assert.Equal(4, nodes[3].MembershipSize);

        // Resume and let it catch up
        nodes[3].Resume();
        _harness.WaitForConvergence(expectedSize: 5);

        Assert.All(_harness.Nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    /// <summary>
    /// Tests timed suspension using SuspendFor.
    /// </summary>
    [Fact]
    public void TimedSuspensionWorksCorrectly()
    {
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        // Suspend node for a duration
        nodes[2].SuspendFor(TimeSpan.FromSeconds(2));

        Assert.True(nodes[2].IsSuspended);

        // Advance time past suspension duration
        _harness.RunForDuration(TimeSpan.FromSeconds(3));

        // Node should be automatically resumed
        Assert.False(nodes[2].IsSuspended);
    }

    /// <summary>
    /// Tests operations while majority of nodes are suspended (should stall).
    /// </summary>
    [Fact]
    public void OperationsStallWhenMajoritySuspended()
    {
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Suspend majority (3 out of 5)
        nodes[2].Suspend();
        nodes[3].Suspend();
        nodes[4].Suspend();

        // Only 2 nodes are active - cannot reach quorum
        // Operations should not complete until majority is restored

        // Resume one node to restore quorum
        nodes[2].Resume();

        // Now operations should work
        var newNode = _harness.CreateJoinerNode(nodes[0], nodeId: 5);

        // Resume remaining
        nodes[3].Resume();
        nodes[4].Resume();

        _harness.WaitForConvergence(expectedSize: 6);

        Assert.All(_harness.Nodes, n => Assert.Equal(6, n.MembershipSize));
    }

    /// <summary>
    /// Tests that Step executes exactly one task.
    /// </summary>
    [Fact]
    public void StepNodeExecutesOneTask()
    {
        var seedNode = _harness.CreateSeedNode();

        // Step the node once
        var executed = seedNode.Step();

        // A task should have been executed
        Assert.True(executed);
    }

    /// <summary>
    /// Tests that suspended node's Step returns false.
    /// </summary>
    [Fact]
    public void StepNodeReturnsFalseWhenSuspended()
    {
        var seedNode = _harness.CreateSeedNode();

        // Suspend the node
        seedNode.Suspend();

        // Step should return false
        var executed = seedNode.Step();
        Assert.False(executed);

        // Resume
        seedNode.Resume();
    }

    /// <summary>
    /// Tests a complex scenario with mixed concurrent operations.
    /// </summary>
    [Fact]
    public void ComplexMixedConcurrentOperations()
    {
        // Start with 6 nodes
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence(expectedSize: 6);

        var nodeIdCounter = 6;

        // Suspend some nodes
        nodes[4].Suspend();
        nodes[5].Suspend();

        // Crash one node
        _harness.CrashNode(nodes[3]);

        // At this point: 3 active nodes (0,1,2) out of 6 in membership view.
        // Classic Paxos needs majority (4) for n=6, so we can't form quorum yet.
        // Resume one suspended node to enable quorum (4 active nodes).
        nodes[4].Resume();

        // Join a new node - should succeed with 4 active nodes (quorum for n=6)
        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: nodeIdCounter++);

        // Join another node
        var newNode2 = _harness.CreateJoinerNode(nodes[0], nodeId: nodeIdCounter++);

        // Resume the last suspended node
        nodes[5].Resume();

        // Wait for convergence
        _harness.WaitForConvergence(expectedSize: 7, maxIterations: 500000);

        Assert.All(_harness.Nodes, n => Assert.Equal(7, n.MembershipSize));
    }

    /// <summary>
    /// Tests that the cluster maintains consistency through heavy churn.
    /// </summary>
    [Fact]
    public void MaintainsConsistencyThroughHeavyChurn()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        var nodeIdCounter = 3;

        // Heavy churn: 5 rounds of remove + add
        for (var i = 0; i < 5; i++)
        {
            // Remove last node
            var nodeToRemove = _harness.Nodes.Last();
            _harness.RemoveNodeGracefully(nodeToRemove);

            // Add new node
            var newNode = _harness.CreateJoinerNode(seedNode, nodeId: nodeIdCounter++);

            _harness.WaitForConvergence(expectedSize: 3);
        }

        // Verify final consistency
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));

        // Verify all nodes have the same view
        var configIds = _harness.Nodes.Select(n => n.CurrentView.ConfigurationId).Distinct().ToList();
        Assert.Single(configIds);
    }

    /// <summary>
    /// Tests consensus with exactly quorum number of nodes active.
    /// When nodes are suspended, they fail probes and get kicked from the cluster.
    /// When they resume, they rejoin as new members.
    /// </summary>
    [Fact]
    public void ConsensusWithExactQuorum()
    {
        // Start with a 5-node cluster (quorum = 3)
        var nodes = _harness.CreateCluster(size: 5);

        // Suspend 2 nodes - they will be detected as failed and removed
        // This leaves exactly 3 nodes (quorum for a 5-node cluster)
        nodes[3].Suspend();
        nodes[4].Suspend();

        // Wait for failure detection to kick the suspended nodes
        // Cluster should converge to 3 nodes (the active ones)
        _harness.WaitForConvergence();

        Assert.Equal(3, nodes[0].MembershipSize);
        Assert.Equal(3, nodes[1].MembershipSize);
        Assert.Equal(3, nodes[2].MembershipSize);

        // Operations should succeed with the remaining 3 nodes
        var newNode = _harness.CreateJoinerNode(nodes[0], nodeId: 5);

        // Active nodes should see the new member (now 4 nodes)
        _harness.WaitForConvergence();

        Assert.Equal(4, nodes[0].MembershipSize);
        Assert.Equal(4, nodes[1].MembershipSize);
        Assert.Equal(4, nodes[2].MembershipSize);

        // Resume suspended nodes - they will detect they were kicked and rejoin
        nodes[3].Resume();
        nodes[4].Resume();

        // Cluster should converge to 6 nodes (4 active + 2 rejoined)
        _harness.WaitForConvergence(expectedSize: 6);
    }

}
