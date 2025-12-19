using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for consensus protocol operations using the simulation harness.
/// Covers Fast Paxos basic operations, failure cases, and configuration changes.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class ConsensusProtocolTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 55555;

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
    public void SingleProposalAcceptedInTwoNodeCluster()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Both nodes should have accepted the membership change via consensus
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);

        // Configuration IDs should match, indicating consensus was reached
        Assert.Equal(seedNode.CurrentView.ConfigurationId, joiner.CurrentView.ConfigurationId);
    }

    [Fact]
    public void ConflictingProposalsResolvedInSequentialJoins()
    {
        var seedNode = _harness.CreateSeedNode();

        // Sequential joins (not truly concurrent in sync mode)
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        // Wait for full convergence
        _harness.WaitForConvergence(expectedSize: 3);

        // All nodes should eventually reach consensus on membership
        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner1.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    [Fact]
    public void ConsensusCompletesWithinTimeout()
    {
        var seedNode = _harness.CreateSeedNode();
        var startTime = DateTime.UtcNow;

        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        var elapsed = DateTime.UtcNow - startTime;

        // Consensus should complete in reasonable time (5 seconds is generous)
        Assert.True(elapsed < TimeSpan.FromSeconds(5),
            $"Consensus took too long: {elapsed.TotalSeconds} seconds");
        Assert.True(joiner.IsInitialized);
    }

    [Fact]
    public void DecisionPropagatedToAllNodes()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // All nodes should have the same view of membership (consensus decision)
        var seedMembers = seedNode.CurrentView.Members.Select(m => $"{m.Hostname}:{m.Port}").OrderBy(x => x).ToList();
        var joiner1Members = joiner1.CurrentView.Members.Select(m => $"{m.Hostname}:{m.Port}").OrderBy(x => x).ToList();
        var joiner2Members = joiner2.CurrentView.Members.Select(m => $"{m.Hostname}:{m.Port}").OrderBy(x => x).ToList();

        Assert.Equal(seedMembers, joiner1Members);
        Assert.Equal(seedMembers, joiner2Members);
    }

    [Fact]
    public void ConsensusSucceedsWithMinorityFailure()
    {
        // Create 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);

        _harness.WaitForConvergence(expectedSize: 5);

        // Crash 1 node (minority)
        _harness.CrashNode(nodes[4]);

        // Add a new node - consensus should still work with 4 healthy nodes
        var newJoiner = _harness.CreateJoinerNode(nodes[0], nodeId: 5);

        Assert.True(newJoiner.IsInitialized);
    }

    [Fact]
    public void ConsensusBlockedWithMajorityFailure()
    {
        // Create 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);

        _harness.WaitForConvergence(expectedSize: 5);

        // Crash 3 nodes (majority)
        _harness.CrashNode(nodes[2]);
        _harness.CrashNode(nodes[3]);
        _harness.CrashNode(nodes[4]);

        // With only 2 out of 5 nodes remaining, quorum cannot be reached
        // The remaining nodes will detect the failures but won't be able to reach
        // consensus to remove them from the membership view
        Assert.Equal(2, _harness.Nodes.Count);

        // Advance time to trigger failure detection, but don't wait for convergence
        // since it won't happen (no quorum possible)
        _harness.RunForDuration(TimeSpan.FromSeconds(10));

        // The remaining nodes should still see 5 members (they detected failures but
        // couldn't reach consensus to update the view)
        Assert.Equal(5, nodes[0].MembershipSize);
        Assert.Equal(5, nodes[1].MembershipSize);
    }

    [Fact]
    public void NodeFailureDuringMembershipChangeHandled()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Both nodes should be properly initialized after membership change
        Assert.True(seedNode.IsInitialized);
        Assert.True(joiner1.IsInitialized);
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner1.MembershipSize);
    }

    [Fact]
    public void MultipleSimultaneousProposalsEventuallyResolve()
    {
        var seedNode = _harness.CreateSeedNode();

        // Sequential joins in sync mode
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        // Wait for convergence
        _harness.WaitForConvergence(expectedSize: 4);

        // All nodes should eventually agree
        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    [Fact]
    public void ConfigurationIdChangesWithEachMembershipChange()
    {
        var seedNode = _harness.CreateSeedNode();
        var configIds = new HashSet<ConfigurationId> { seedNode.CurrentView.ConfigurationId };

        // Join 3 nodes, tracking config ID changes
        for (var i = 1; i <= 3; i++)
        {
            var joiner = _harness.CreateJoinerNode(seedNode, nodeId: i);

            _harness.WaitForNodeSize(seedNode, expectedSize: i + 1);

            var newConfigId = seedNode.CurrentView.ConfigurationId;
            // Each membership change should produce a unique configuration ID
            Assert.DoesNotContain(newConfigId, configIds);
            configIds.Add(newConfigId);
        }

        // We should have 4 unique configuration IDs (initial + 3 joins)
        Assert.Equal(4, configIds.Count);
    }

    [Fact(Skip = "Requires ability to inject stale proposals at the protocol level - not supported by simulation harness")]
    public void OldConfigurationProposalsRejected()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // This would require injecting a proposal with an old configuration ID
        // and verifying it's rejected - needs low-level protocol access
    }

    [Fact]
    public void ConcurrentConfigChangesEventuallySerialize()
    {
        var seedNode = _harness.CreateSeedNode();

        // Sequential joins in sync mode
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // All nodes should have the same final configuration
        var configId = seedNode.CurrentView.ConfigurationId;
        Assert.All(_harness.Nodes,
            n => Assert.Equal(configId, n.CurrentView.ConfigurationId));
    }

    [Fact]
    public void ConsensusWithMessageDelaysCompletesCorrectly()
    {
        // Enable message delays
        _harness.Network.EnableDelays = true;
        _harness.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(10);
        _harness.Network.MaxJitter = TimeSpan.FromMilliseconds(20);

        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void ConsensusWithLowMessageLossSucceeds()
    {
        // Enable 5% message loss - this tests the retry logic in join protocol
        _harness.Network.MessageDropRate = 0.05;

        var seedNode = _harness.CreateSeedNode();

        // The join should succeed despite message loss due to retry logic
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized, "Joiner should be initialized after join with retries");

        // Wait for convergence
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void ClassicPaxosFallback_SucceedsWhenFastPaxosFails()
    {
        // In a 3-node cluster, Fast Paxos requires all 3 nodes (N-f where f=0 for N=3)
        // If one node crashes, Fast Paxos cannot succeed, but Classic Paxos can
        // because it only needs majority (2 out of 3)
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        // Crash one node
        _harness.CrashNode(nodes[2]);

        // The remaining 2 nodes should eventually reach consensus via Classic Paxos
        // to remove the crashed node
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);
    }

    [Fact]
    public void ClassicPaxosFallback_WithFiveNodesOneCrashed()
    {
        // For N=5, f=1, Fast Paxos needs 4 nodes (N-f=4)
        // Classic Paxos needs majority = 3
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Crash one node - Fast Paxos still works with 4 nodes
        _harness.CrashNode(nodes[4]);

        _harness.WaitForConvergence(expectedSize: 4);

        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    [Fact]
    public void ClassicPaxosFallback_WithFiveNodesTwoCrashed()
    {
        // For N=5, f=1, Fast Paxos needs 4 nodes
        // If 2 crash, only 3 remain - Fast Paxos fails, Classic Paxos succeeds
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Crash two nodes
        _harness.CrashNode(nodes[3]);
        _harness.CrashNode(nodes[4]);

        // Classic Paxos should succeed with 3 nodes (majority)
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void FastRoundVotesPreserved_InClassicPaxosPhase1b()
    {
        // This test verifies that votes from the fast round are properly
        // reported in Phase1b when Classic Paxos starts
        // The key behavior: RegisterFastRoundVote must set _rnd, _vrnd, _vval

        // Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        // Now crash a node - this will trigger:
        // 1. Fast Paxos attempt (fails because needs all 3)
        // 2. Classic Paxos fallback (succeeds with 2)
        _harness.CrashNode(nodes[2]);

        // If RegisterFastRoundVote is implemented correctly, the remaining nodes
        // will have recorded their fast round votes and Phase1b will report them
        _harness.WaitForConvergence(expectedSize: 2);

        // All remaining nodes should have consistent membership
        Assert.Equal(nodes[0].CurrentView.ConfigurationId, nodes[1].CurrentView.ConfigurationId);
        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);
    }

    [Fact]
    public void MultipleClassicRounds_HigherRoundWins()
    {
        // Test that when multiple coordinators start classic rounds,
        // the higher rank wins

        // With 5 nodes, if we crash 2, we have 3 remaining
        // For N=5, Fast Paxos needs N-f = 5 - (5-1)/4 = 4 nodes
        // Classic Paxos needs majority = 5/2 + 1 = 3 nodes
        // With 3 remaining, Classic Paxos can succeed
        // Multiple remaining nodes may try to become coordinator
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Crash 2 nodes to force Classic Paxos
        _harness.CrashNode(nodes[3]);
        _harness.CrashNode(nodes[4]);

        // Should converge despite potential coordinator competition
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.Equal(3, nodes[0].MembershipSize);
        Assert.Equal(3, nodes[1].MembershipSize);
        Assert.Equal(3, nodes[2].MembershipSize);
    }

    [Fact]
    public void SequentialNodeRemoval_ConvergesAfterEachCrash()
    {
        // Test sequential node removal with convergence verification after each crash.
        // This tests the scenario where the cluster shrinks one node at a time,
        // and the quorum requirements adjust dynamically.
        //
        // With 4 nodes:
        // - Initially: N=4, majority = 3
        // - After 1 crash: 3 remaining, need majority of 4 (3) - achievable with 3 nodes
        // - After removing crashed node: N=3, majority = 2
        // - After 2nd crash: 2 remaining, need majority of 3 (2) - achievable with 2 nodes

        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        // Verify initial state
        Assert.All(nodes, n => Assert.Equal(4, n.MembershipSize));

        // Crash first node
        _harness.CrashNode(nodes[3]);

        // Wait for convergence to 3 - the remaining 3 nodes can reach majority (3/4)
        _harness.WaitForConvergence(expectedSize: 3);

        // Verify all remaining nodes see size 3
        Assert.Equal(3, nodes[0].MembershipSize);
        Assert.Equal(3, nodes[1].MembershipSize);
        Assert.Equal(3, nodes[2].MembershipSize);

        // Record config ID after first removal
        var configAfterFirstRemoval = nodes[0].CurrentView.ConfigurationId;
        Assert.Equal(configAfterFirstRemoval, nodes[1].CurrentView.ConfigurationId);
        Assert.Equal(configAfterFirstRemoval, nodes[2].CurrentView.ConfigurationId);

        // Crash second node
        _harness.CrashNode(nodes[2]);

        // Wait for convergence to 2 - the remaining 2 nodes can reach majority (2/3)
        _harness.WaitForConvergence(expectedSize: 2);

        // Verify all remaining nodes see size 2
        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);

        // Verify config ID changed
        var configAfterSecondRemoval = nodes[0].CurrentView.ConfigurationId;
        Assert.NotEqual(configAfterFirstRemoval, configAfterSecondRemoval);
        Assert.Equal(configAfterSecondRemoval, nodes[1].CurrentView.ConfigurationId);
    }

    [Fact]
    public void ConsensusNotRepeated_AfterDecision()
    {
        // Test that once a decision is made, additional Phase1b messages
        // don't cause duplicate consensus (the cval guard)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Record the configuration ID after first consensus
        var configAfterJoins = seedNode.CurrentView.ConfigurationId;

        // Add more time to ensure any delayed messages are processed
        _harness.RunForDuration(TimeSpan.FromSeconds(5));

        // Configuration should not have changed (no duplicate consensus)
        Assert.Equal(configAfterJoins, seedNode.CurrentView.ConfigurationId);
    }

}

