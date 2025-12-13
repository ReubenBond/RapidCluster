using System.Diagnostics.CodeAnalysis;
using RapidCluster.Exceptions;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for join protocol edge cases and phase 2 failures using the simulation harness.
/// These tests verify that the join protocol handles message drops, configuration changes,
/// and other edge cases during the join process.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class JoinProtocolTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 67891;

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
    /// Tests that a basic join succeeds in normal conditions.
    /// </summary>
    [Fact]
    public void BasicJoinSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Tests that join returns correct membership after successful join.
    /// </summary>
    [Fact]
    public void JoinReturnCorrectMembership()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Both nodes should see each other
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);

        // Verify membership contains both endpoints
        var joinerView = joiner.CurrentView;
        var addresses = joinerView.Members.Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}").ToHashSet();

        Assert.Contains($"{seedNode.Address.Hostname.ToStringUtf8()}:{seedNode.Address.Port}", addresses);
        Assert.Contains($"{joiner.Address.Hostname.ToStringUtf8()}:{joiner.Address.Port}", addresses);
    }

    /// <summary>
    /// Tests that configuration ID is set after successful join.
    /// </summary>
    [Fact]
    public void JoinSetsConfigurationId()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Both should have non-zero configuration IDs (version > 0 after join)
        Assert.True(seedNode.CurrentView.ConfigurationId.Version > 0);
        Assert.True(joiner.CurrentView.ConfigurationId.Version > 0);

        // Both should have the same configuration ID
        Assert.Equal(seedNode.CurrentView.ConfigurationId, joiner.CurrentView.ConfigurationId);
    }



    /// <summary>
    /// Tests that join succeeds despite random message drops.
    /// The join protocol includes retry logic that should handle transient failures.
    /// </summary>
    [Fact]
    public void JoinSucceedsWithRandomMessageDrops()
    {
        // Enable random message drops (10% drop rate)
        _harness.Network.MessageDropRate = 0.1;

        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized);
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Tests that join succeeds with higher message drop rates due to retry logic.
    /// </summary>
    [Fact]
    public void JoinSucceedsWithHigherMessageDropRate()
    {
        // Enable random message drops (20% drop rate)
        _harness.Network.MessageDropRate = 0.2;

        var seedNode = _harness.CreateSeedNode();

        // Join should still succeed due to retry logic
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized);
        _harness.WaitForConvergence(expectedSize: 2);
    }

    /// <summary>
    /// Tests that multiple joins succeed with message drops enabled.
    /// </summary>
    [Fact]
    public void MultipleJoinsSucceedWithMessageDrops()
    {
        // Enable random message drops (10% drop rate)
        _harness.Network.MessageDropRate = 0.1;

        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);
    }



    /// <summary>
    /// Tests that a new joiner can still join while another join is being processed.
    /// This tests the configuration change handling during join.
    /// </summary>
    [Fact]
    public void JoinDuringAnotherJoinSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();

        // Start first join
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Immediately start second join (config may have changed)
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner1.IsInitialized);
        Assert.True(joiner2.IsInitialized);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    /// <summary>
    /// Tests that join handles configuration changes caused by leaves.
    /// </summary>
    [Fact]
    public void JoinAfterLeaveSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Remove a node (changes configuration)
        _harness.RemoveNodeGracefully(joiner2);
        _harness.WaitForConvergence(expectedSize: 2);

        // New join after config change should succeed
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner3.IsInitialized);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    /// <summary>
    /// Tests that join handles configuration changes caused by failures.
    /// </summary>
    [Fact]
    public void JoinAfterNodeFailureSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Crash a node (changes configuration after failure detection)
        _harness.CrashNode(joiner2);
        _harness.WaitForConvergence(expectedSize: 2, maxIterations: 500000);

        // New join after failure should succeed
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner3.IsInitialized);
    }



    /// <summary>
    /// Tests joining through a non-seed node.
    /// </summary>
    [Fact]
    public void JoinThroughNonSeedNodeSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Join through joiner1 instead of seed
        var joiner2 = _harness.CreateJoinerNode(joiner1, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner2.IsInitialized);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    /// <summary>
    /// Tests joining through different cluster members in sequence.
    /// </summary>
    [Fact]
    public void JoinThroughDifferentMembersSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Join through different members
        var joiner3 = _harness.CreateJoinerNode(joiner1, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 4);

        var joiner4 = _harness.CreateJoinerNode(joiner2, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 5);

        var joiner5 = _harness.CreateJoinerNode(joiner3, nodeId: 5);
        _harness.WaitForConvergence(expectedSize: 6);

        Assert.All(_harness.Nodes, n => Assert.Equal(6, n.MembershipSize));
    }



    /// <summary>
    /// Tests that join fails when consensus is impossible due to node isolation.
    /// In a 2-node cluster where one node is isolated, the remaining node cannot reach
    /// the consensus threshold alone, so any new join attempt will fail with JoinException
    /// after exhausting retries (observers don't respond with successful join confirmation).
    /// </summary>
    [Fact]
    public void JoinFailsWhenConsensusImpossibleDueToIsolation()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Isolate joiner1 - now seedNode is alone and cannot reach consensus
        // (needs 2 nodes to agree in a 2-node cluster)
        _harness.IsolateNode(joiner1);

        // Attempting to join should fail because consensus cannot be reached.
        // The join protocol contacts observers (assigned by the seed) and waits for
        // them to confirm the join via consensus. With joiner1 isolated, the cluster
        // cannot reach consensus, so the joining node never gets a successful response
        // from observers, resulting in JoinException after exhausting retries.
        //
        // We configure limited retries so the join fails deterministically within
        // the simulation iteration limit. Without this, the default infinite retries
        // would cause the simulation to hit its iteration limit and throw TimeoutException.
        var limitedRetryOptions = new RapidClusterProtocolOptions
        {
            MaxJoinRetries = 3
        };
        Assert.Throws<JoinException>(() =>
        {
            _harness.CreateJoinerNode(seedNode, nodeId: 2, limitedRetryOptions);
        });
    }



    /// <summary>
    /// Tests rapid consecutive joins.
    /// </summary>
    [Fact]
    public void RapidConsecutiveJoinsSucceed()
    {
        var seedNode = _harness.CreateSeedNode();

        // Rapid consecutive joins
        for (var i = 1; i <= 5; i++)
        {
            var joiner = _harness.CreateJoinerNode(seedNode, nodeId: i);
            Assert.True(joiner.IsInitialized);
        }

        _harness.WaitForConvergence(expectedSize: 6);

        Assert.All(_harness.Nodes, n => Assert.Equal(6, n.MembershipSize));
    }

    /// <summary>
    /// Tests join with network delays enabled.
    /// </summary>
    [Fact]
    public void JoinSucceedsWithNetworkDelays()
    {
        // Enable network delays
        _harness.Network.EnableDelays = true;
        _harness.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(10);
        _harness.Network.MaxJitter = TimeSpan.FromMilliseconds(20);

        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Tests that join protocol completes within reasonable time even with delays.
    /// </summary>
    [Fact]
    public void JoinCompletesWithDelaysAndDrops()
    {
        // Enable both delays and drops
        _harness.Network.EnableDelays = true;
        _harness.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(5);
        _harness.Network.MaxJitter = TimeSpan.FromMilliseconds(10);
        _harness.Network.MessageDropRate = 0.05; // 5% drop rate

        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        Assert.True(joiner.IsInitialized);
    }



    /// <summary>
    /// Tests that join fails gracefully when the seed is completely partitioned.
    /// </summary>
    [Fact]
    public void JoinAfterPartitionHealSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Create and heal a partition
        _harness.PartitionNodes(seedNode, joiner1);
        _harness.HealPartition(seedNode, joiner1);

        // Join after partition heal should succeed
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner2.IsInitialized);
    }

    /// <summary>
    /// Tests that after a network partition heals and the cluster converges,
    /// new nodes can join through any surviving member.
    /// </summary>
    /// <remarks>
    /// Uses a 6-node cluster to avoid the pathological kick-rejoin cycle that occurs
    /// in small (3-node) clusters. With K=5 effective observers, partitions are handled
    /// more gracefully and don't cause constant membership churn.
    /// </remarks>
    [Fact]
    public void JoinThroughUnpartitionedMemberSucceeds()
    {
        // Use a 6-node cluster for stability. In a 3-node cluster, a single partition
        // causes a pathological kick-rejoin cycle because K=2 effective observers means
        // one node reporting failure is enough to trigger removal with majority consensus.
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence(expectedSize: 6);

        var seedNode = nodes[0];
        var partitionedNode = nodes[1];

        // Partition node1 from node0 (but not from other nodes)
        // This creates a partial partition where node1 can still communicate with nodes 2-5
        _harness.PartitionNodes(seedNode, partitionedNode);

        // Give failure detection time to detect the partition.
        // With a larger cluster, the partition may or may not cause node removal
        // depending on how many observers report the failure.
        _harness.RunForDuration(TimeSpan.FromSeconds(5));

        // Heal the partition to restore connectivity
        _harness.HealPartition(seedNode, partitionedNode);

        // Wait briefly for the cluster to stabilize after healing
        _harness.RunForDuration(TimeSpan.FromSeconds(2));

        // Get remaining nodes to find one we can join through
        var aliveNodes = _harness.Nodes.Where(n => n.IsInitialized && n.MembershipSize > 0).ToList();
        Assert.NotEmpty(aliveNodes);

        var joinPoint = aliveNodes[0];

        // Now join through a surviving node - the cluster should be stable
        var joiner = _harness.CreateJoinerNode(joinPoint, nodeId: 10);

        // The join should succeed - joiner should be initialized
        Assert.True(joiner.IsInitialized);

        // Verify that joiner is part of the cluster
        Assert.True(joiner.MembershipSize >= 2);
    }



    /// <summary>
    /// Tests join immediately after cluster initialization.
    /// </summary>
    [Fact]
    public void JoinImmediatelyAfterClusterInit()
    {
        var seedNode = _harness.CreateSeedNode();

        // Join immediately - no waiting
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Tests that multiple sequential configuration changes don't break join.
    /// </summary>
    [Fact]
    public void JoinAfterMultipleConfigurationChanges()
    {
        var seedNode = _harness.CreateSeedNode();

        // Create multiple configuration changes
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence(expectedSize: 2);

        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence(expectedSize: 3);

        _harness.RemoveNodeGracefully(joiner2);
        _harness.WaitForConvergence(expectedSize: 2);

        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        _harness.RemoveNodeGracefully(joiner1);
        _harness.WaitForConvergence(expectedSize: 2);

        // Join after multiple configuration changes
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner4.IsInitialized);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

}
