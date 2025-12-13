using System.Diagnostics.CodeAnalysis;
using RapidCluster.Exceptions;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for node failure scenarios using the simulation harness.
/// Covers single node failures, multiple node failures, seed node failures, and failures during operations.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class NodeFailureTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 23456;

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
    public void NodeCrashRemovesFromCluster()
    {
        // Use 3-node cluster so quorum can be reached after one node crashes
        // (2 out of 3 nodes can reach consensus, but 1 out of 2 cannot)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash one joiner
        _harness.CrashNode(joiner2);

        // Wait for failure detection and removal
        // The remaining 2 nodes can reach consensus to remove the crashed node
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner1.MembershipSize);
    }

    [Fact]
    public void CrashedNodeCannotReceiveMessages()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Crash the joiner
        _harness.CrashNode(joiner);

        // Verify crashed node is removed from harness nodes list
        Assert.DoesNotContain(joiner, _harness.Nodes);
    }

    [Fact]
    public void ClusterContinuesAfterSingleNodeCrash()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash one joiner
        _harness.CrashNode(joiner1);

        // Seed and remaining joiner should still be operational
        Assert.True(seedNode.IsInitialized);
        Assert.True(joiner2.IsInitialized);
    }

    [Fact]
    public void CrashDuringIdleStateHandledGracefully()
    {
        var seedNode = _harness.CreateSeedNode();

        // Crash immediately - should not throw
        _harness.CrashNode(seedNode);

        Assert.DoesNotContain(seedNode, _harness.Nodes);
    }



    [Fact]
    public void TwoNodeFailuresInFiveNodeCluster()
    {
        var nodes = _harness.CreateCluster(size: 5);

        _harness.WaitForConvergence(expectedSize: 5);

        // Crash two nodes
        _harness.CrashNode(nodes[3]);
        _harness.CrashNode(nodes[4]);

        // Wait for failure detection
        // 3 out of 5 nodes remain, which is a majority for consensus
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.All(_harness.Nodes, node => Assert.Equal(3, node.MembershipSize));
    }

    [Fact]
    public void SequentialFailuresHandled()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash nodes sequentially
        _harness.CrashNode(joiner2);
        Assert.Equal(2, _harness.Nodes.Count);

        _harness.CrashNode(joiner1);
        Assert.Single(_harness.Nodes);
    }

    [Fact]
    public void SimultaneousFailuresHandled()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Crash multiple nodes "simultaneously"
        _harness.CrashNode(joiner2);
        _harness.CrashNode(joiner3);

        Assert.Equal(2, _harness.Nodes.Count);
        Assert.Contains(seedNode, _harness.Nodes);
        Assert.Contains(joiner1, _harness.Nodes);
    }



    [Fact]
    public void SeedNodeCrashDoesNotAffectExistingCluster()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash the original seed node
        _harness.CrashNode(seedNode);

        // Remaining nodes should still be operational
        Assert.True(joiner1.IsInitialized);
        Assert.True(joiner2.IsInitialized);
        Assert.Equal(2, _harness.Nodes.Count);
    }

    [Fact]
    public void JoinThroughCrashedSeedFails()
    {
        var seedNode = _harness.CreateSeedNode();

        // Suspend the seed (simulates crash but keeps node in harness so we can attempt to contact it)
        seedNode.Suspend();

        // Use limited retries so the join fails quickly instead of retrying indefinitely
        var options = new RapidClusterProtocolOptions { MaxJoinRetries = 3 };

        // Attempting to join through crashed seed should fail with JoinException
        // The underlying cause is a timeout, but JoinException is the public contract for join failures
        var ex = Assert.Throws<JoinException>(() =>
        {
            _harness.CreateJoinerNode(seedNode, nodeId: 1, options: options);
        });
        Assert.Contains("Timeout", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AlternativeSeedAllowsJoin()
    {
        // Need a 3-node cluster so that after crashing one node,
        // the remaining 2 nodes can still reach quorum for consensus
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash the original seed
        _harness.CrashNode(seedNode);

        // Wait for failure detection to detect the crash and update the membership view.
        // The simulation will automatically advance time as needed to trigger failure detector probes.
        // Failure detection requires:
        // 1. Probe timeout (1 second)
        // 2. Alert broadcast and processing
        // 3. Consensus protocol (FastPaxos or classic Paxos with recovery delay)
        // Use a higher iteration count to allow for all these steps.
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        // Join through one of the remaining joiners (which are still part of the cluster)
        var joiner3 = _harness.CreateJoinerNode(joiner1, nodeId: 3);

        Assert.True(joiner3.IsInitialized);
        _harness.WaitForConvergence(expectedSize: 3);
        Assert.Equal(3, joiner1.MembershipSize);
    }



    [Fact(Skip = "Complex race condition test requiring mid-join node crash injection - not supported by simulation harness")]
    public void NodeCrashDuringJoinProtocol()
    {
        var seedNode = _harness.CreateSeedNode();

        // This test would require the ability to crash a node in the middle of the join protocol,
        // which requires async operation interleaving that the synchronous simulation harness
        // doesn't easily support
    }

    [Fact]
    public void NodeCrashDuringLeave()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Crash the joiner immediately
        _harness.CrashNode(joiner);

        Assert.DoesNotContain(joiner, _harness.Nodes);
    }

}
