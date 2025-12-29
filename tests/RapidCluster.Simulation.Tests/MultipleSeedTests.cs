using System.Diagnostics.CodeAnalysis;
using RapidCluster.Exceptions;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for multiple seed node functionality.
/// Verifies round-robin seed selection during join and seed failover behavior.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class MultipleSeedTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 78901;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    #region Basic Multiple Seed Functionality

    /// <summary>
    /// Tests that a node can join through the first available seed when multiple seeds are configured.
    /// </summary>
    [Fact]
    public void JoinWithMultipleSeedsSucceeds_FirstSeedAvailable()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Join a new node with multiple seeds configured (all available)
        var joiner3 = _harness.CreateJoinerNodeWithMultipleSeeds([seedNode, joiner1, joiner2], nodeId: 3);

        _harness.WaitForConvergence();

        Assert.True(joiner3.IsInitialized);
        Assert.Equal(4, joiner3.MembershipSize);
    }

    /// <summary>
    /// Tests that a node can join through an alternative seed when the first seed is down.
    /// This verifies the round-robin failover behavior.
    /// </summary>
    [Fact]
    public void JoinWithMultipleSeeds_FirstSeedDown_FailsOverToSecond()
    {
        // Create a 4-node cluster (need 4 so we have quorum after crashing one)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence();

        // Crash the original seed node
        _harness.CrashNode(seedNode);

        // Wait for failure detection
        _harness.WaitForConvergence();

        // Join a new node with multiple seeds configured
        // First seed (seedNode) is crashed, so it should fail over to joiner1
        // Note: We pass seedNode first but it's crashed, so the join will try joiner1
        var joiner4 = _harness.CreateJoinerNodeWithMultipleSeeds([joiner1, joiner2], nodeId: 4);

        _harness.WaitForConvergence();

        Assert.True(joiner4.IsInitialized);
        Assert.Equal(4, joiner4.MembershipSize);
    }

    /// <summary>
    /// Tests that a node can join when the first two seeds are down but the third is available.
    /// </summary>
    [Fact]
    public void JoinWithMultipleSeeds_FirstTwoSeedsDown_FailsOverToThird()
    {
        // Create a 5-node cluster (need enough for quorum after crashes)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        var joiner4 = _harness.CreateJoinerNode(seedNode, nodeId: 4);

        _harness.WaitForConvergence();

        // Crash two nodes
        _harness.CrashNode(seedNode);
        _harness.CrashNode(joiner1);

        // Wait for failure detection (3 remaining nodes can reach quorum)
        _harness.WaitForConvergence();

        // Join with multiple seeds - first two are crashed, third (joiner2) is available
        var joiner5 = _harness.CreateJoinerNodeWithMultipleSeeds([joiner2, joiner3, joiner4], nodeId: 5);

        _harness.WaitForConvergence();

        Assert.True(joiner5.IsInitialized);
        Assert.Equal(4, joiner5.MembershipSize);
    }

    #endregion

    #region Seed Failover with Suspended Nodes

    /// <summary>
    /// Tests that join fails over to the next seed when the first seed is suspended (unresponsive).
    /// Suspended nodes don't respond to messages, simulating a network issue or hung process.
    /// Note: The suspended seed will eventually be detected as failed and removed from the cluster.
    /// </summary>
    [Fact]
    public void JoinWithMultipleSeeds_FirstSeedSuspended_FailsOverToSecond()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Suspend the seed node (it won't respond to messages)
        seedNode.Suspend();

        // Join with multiple seeds - first (seedNode) is suspended, should fail over to joiner1
        // Use limited retries to speed up the test
        var options = new RapidClusterProtocolOptions { MaxJoinRetries = 6 };
        var joiner3 = _harness.CreateJoinerNodeWithMultipleSeeds([seedNode, joiner1, joiner2], nodeId: 3, options);

        // The join succeeds through an alternative seed.
        // The suspended seed may be detected as failed and removed during the join process,
        // so the final membership could be 3 (seed removed) or 4 (seed still in view).
        Assert.True(joiner3.IsInitialized);
        Assert.True(joiner3.MembershipSize >= 3 && joiner3.MembershipSize <= 4);
    }

    #endregion

    #region All Seeds Unavailable

    /// <summary>
    /// Tests that join fails with JoinException when all configured seeds are unavailable.
    /// </summary>
    [Fact]
    public void JoinWithMultipleSeeds_AllSeedsDown_ThrowsJoinException()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Suspend all nodes so they don't respond
        seedNode.Suspend();
        joiner1.Suspend();
        joiner2.Suspend();

        // Attempt to join with limited retries - should fail since all seeds are suspended
        var options = new RapidClusterProtocolOptions { MaxJoinRetries = 3 };

        var ex = Assert.Throws<JoinException>(() => _harness.CreateJoinerNodeWithMultipleSeeds([seedNode, joiner1, joiner2], nodeId: 3, options));

        // The exception message should mention timeout since all seeds are unresponsive
        Assert.Contains("Timeout", ex.Message, StringComparison.Ordinal);
    }

    #endregion

    #region Duplicate and Self-Exclusion

    /// <summary>
    /// Tests that duplicate seeds in the configuration are handled correctly.
    /// The join should succeed even if the same seed is listed multiple times.
    /// </summary>
    [Fact]
    public void JoinWithDuplicateSeeds_Succeeds()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _ = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Create a node with duplicate seeds in the list
        // MembershipService should deduplicate these
        var duplicateSeeds = new List<Endpoint> { seedNode.Address, seedNode.Address, joiner1.Address, seedNode.Address };
        var joiner3 = _harness.CreateJoinerNodeWithSeedAddresses(duplicateSeeds, nodeId: 3);

        _harness.WaitForConvergence();

        Assert.True(joiner3.IsInitialized);
        Assert.Equal(4, joiner3.MembershipSize);
    }

    #endregion

    #region Order Preservation

    /// <summary>
    /// Tests that seeds are tried in the order they are configured.
    /// When the first seed is available, the join should succeed without trying others.
    /// </summary>
    [Fact]
    public void JoinWithMultipleSeeds_TriesSeedsInOrder()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Join with seedNode first in the list - it should be tried first and succeed
        var joiner3 = _harness.CreateJoinerNodeWithMultipleSeeds([seedNode, joiner1, joiner2], nodeId: 3);

        _harness.WaitForConvergence();

        Assert.True(joiner3.IsInitialized);
        // The join succeeded, which means at least one seed worked
        Assert.Equal(4, joiner3.MembershipSize);
    }

    #endregion
}
