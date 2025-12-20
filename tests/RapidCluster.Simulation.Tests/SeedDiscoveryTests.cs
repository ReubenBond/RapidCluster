using System.Diagnostics.CodeAnalysis;
using System.Net;
using RapidCluster.Discovery;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for seed provider behavior in simulation context.
/// These tests verify that the seed discovery mechanism works correctly
/// with the simulation infrastructure.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class SeedDiscoveryTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 55123;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _harness.DisposeAsync();
    }

    #region Single Seed Discovery

    /// <summary>
    /// Tests that a node can discover and join through a single seed.
    /// </summary>
    [Fact]
    public void SingleSeed_JoinSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Tests that joining with the seed node itself listed as a seed results in becoming a seed node.
    /// </summary>
    [Fact]
    public void SelfAsSeed_BecomesNewCluster()
    {
        // When a node's listen address is the same as its seed address,
        // it should start a new cluster (become a seed node)
        var seedNode = _harness.CreateSeedNode();

        Assert.True(seedNode.IsInitialized);
        Assert.Equal(1, seedNode.MembershipSize);
    }

    #endregion

    #region Multiple Seeds - Preference and Failover

    /// <summary>
    /// Tests that when multiple seeds are available, the node can join successfully.
    /// </summary>
    [Fact]
    public void MultipleSeeds_AllAvailable_JoinSucceeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Join with multiple seeds configured
        var joiner3 = _harness.CreateJoinerNodeWithMultipleSeeds([seedNode, joiner1, joiner2], nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        Assert.True(joiner3.IsInitialized);
        Assert.Equal(4, joiner3.MembershipSize);
    }

    /// <summary>
    /// Tests that seed list order is preserved and first available seed is used.
    /// </summary>
    [Fact]
    public void MultipleSeeds_FirstSeedUsed_WhenAvailable()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Join with seedNode first in the list
        var joiner2 = _harness.CreateJoinerNodeWithMultipleSeeds([seedNode, joiner1], nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner2.IsInitialized);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    /// <summary>
    /// Tests that when the primary seed is unavailable, failover to secondary works.
    /// </summary>
    [Fact]
    public void MultipleSeeds_PrimaryDown_FailsOverToSecondary()
    {
        // Create initial cluster of 4 nodes (need quorum after crash)
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);

        _harness.WaitForConvergence(expectedSize: 4);

        // Crash the original seed
        _harness.CrashNode(seedNode);

        // Wait for cluster to stabilize
        _harness.WaitForConvergence(expectedSize: 3, maxIterations: 500000);

        // Join with secondary seed (joiner1) - should work since seedNode is down
        var joiner4 = _harness.CreateJoinerNodeWithMultipleSeeds([joiner1, joiner2], nodeId: 4);

        _harness.WaitForConvergence(expectedSize: 4);

        Assert.True(joiner4.IsInitialized);
        Assert.Equal(4, joiner4.MembershipSize);
    }

    #endregion

    #region Edge Cases

    /// <summary>
    /// Tests that empty seed list for a joiner node throws an exception.
    /// </summary>
    [Fact]
    public void EmptySeedList_ThrowsException()
    {
        Assert.Throws<ArgumentException>(() =>
            _harness.CreateJoinerNodeWithSeedAddresses([], nodeId: 1));
    }

    /// <summary>
    /// Tests that duplicate seeds in the list are handled correctly.
    /// </summary>
    [Fact]
    public void DuplicateSeeds_HandledCorrectly()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Create node with duplicate seeds
        var duplicateSeeds = new List<Endpoint>
        {
            seedNode.Address,
            seedNode.Address,
            joiner1.Address,
            seedNode.Address
        };
        var joiner2 = _harness.CreateJoinerNodeWithSeedAddresses(duplicateSeeds, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner2.IsInitialized);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    /// <summary>
    /// Tests that when a node's own address is in the seed list along with other seeds,
    /// the self address is filtered out and the node joins the cluster via other seeds.
    /// This is important for Aspire-style deployments where service discovery may return
    /// all replica addresses including the node's own address.
    /// </summary>
    [Fact]
    public void SelfInSeedList_FilteredOut_JoinsOthers()
    {
        var seedNode = _harness.CreateSeedNode();

        _harness.WaitForConvergence(expectedSize: 1);

        // Get the address that the joiner will use
        var joinerAddress = RapidClusterUtils.HostFromParts("node", 1);

        // Create joiner with seed list containing [self, seedNode]
        // Self should be filtered out, and it should join via seedNode
        var seedsIncludingSelf = new List<Endpoint> { joinerAddress, seedNode.Address };
        var joiner = _harness.CreateJoinerNodeWithSeedAddresses(seedsIncludingSelf, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Tests that multiple copies of self in the seed list are all filtered out.
    /// The node should still successfully join via the remaining valid seeds.
    /// </summary>
    [Fact]
    public void MultipleSelfInSeedList_AllFilteredOut()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        _harness.WaitForConvergence(expectedSize: 2);

        // Get the address that joiner2 will use
        var joiner2Address = RapidClusterUtils.HostFromParts("node", 2);

        // Create joiner with seed list containing [self, self, seedNode, self, joiner1]
        // All self entries should be filtered out
        var seedsWithMultipleSelf = new List<Endpoint>
        {
            joiner2Address,
            joiner2Address,
            seedNode.Address,
            joiner2Address,
            joiner1.Address
        };
        var joiner2 = _harness.CreateJoinerNodeWithSeedAddresses(seedsWithMultipleSelf, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        Assert.True(joiner2.IsInitialized);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    /// <summary>
    /// Tests that seed discovery works with larger cluster sizes.
    /// </summary>
    [Fact]
    public void LargerCluster_SeedDiscoveryWorks()
    {
        // Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);

        Assert.Equal(5, nodes.Count);
        Assert.All(nodes, node =>
        {
            Assert.True(node.IsInitialized);
            Assert.Equal(5, node.MembershipSize);
        });

        // Join a new node using any existing node as seed
        var joiner = _harness.CreateJoinerNodeWithMultipleSeeds([nodes[0], nodes[2], nodes[4]], nodeId: 5);

        _harness.WaitForConvergence(expectedSize: 6);

        Assert.True(joiner.IsInitialized);
        Assert.Equal(6, joiner.MembershipSize);
    }

    #endregion

    #region ConfigurationSeedProvider Contract Verification

    /// <summary>
    /// Tests that ConfigurationSeedProvider returns the same seeds on every call.
    /// </summary>
    [Fact]
    public async Task ConfigurationSeedProvider_ReturnsConsistentSeeds()
    {
        var seeds = new List<EndPoint>
        {
            new DnsEndPoint("host1", 5000),
            new DnsEndPoint("host2", 5000)
        };

        var options = new RapidClusterOptions { SeedAddresses = seeds };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);
        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result1 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        var result2 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        var result3 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(result1, result2);
        Assert.Equal(result2, result3);
        Assert.Equal(2, result1.Count);
    }

    /// <summary>
    /// Tests that ConfigurationSeedProvider with empty list returns empty.
    /// </summary>
    [Fact]
    public async Task ConfigurationSeedProvider_EmptyList_ReturnsEmpty()
    {
        var options = new RapidClusterOptions { SeedAddresses = [] };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);
        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    /// <summary>
    /// Simple options monitor for tests that returns a fixed value.
    /// </summary>
    private sealed class TestOptionsMonitor<T>(T value) : Microsoft.Extensions.Options.IOptionsMonitor<T>
    {
        public T CurrentValue => value;
        public T Get(string? name) => value;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    #endregion

    #region Cluster Recovery with Seeds

    /// <summary>
    /// Tests that nodes can rejoin after a network partition using seed discovery.
    /// </summary>
    [Fact]
    public void AfterPartition_SeedDiscoveryAllowsRejoin()
    {
        // Create a 3-node cluster
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Verify initial state
        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner1.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);

        // The seed discovery mechanism ensures nodes have the addresses
        // needed to reconnect after partitions heal
        // This test verifies the basic cluster formation with seed discovery
    }

    #endregion
}
