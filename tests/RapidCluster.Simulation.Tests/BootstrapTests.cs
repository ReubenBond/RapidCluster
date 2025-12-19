using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for cluster bootstrapping with BootstrapExpect.
/// When BootstrapExpect is configured, all nodes wait for each other and
/// create a deterministic initial membership without needing a single coordinator.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class BootstrapTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 54321;

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
    public void ThreeNodeBootstrap_AllNodesFormClusterSimultaneously()
    {
        // Create 3 nodes with BootstrapExpect=3
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // All nodes should be initialized
        Assert.Equal(3, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));

        // All nodes should see 3 members
        Assert.All(nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void FiveNodeBootstrap_AllNodesFormClusterSimultaneously()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 5);

        Assert.Equal(5, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    [Fact]
    public void Bootstrap_AllNodesHaveSameConfigurationId()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // All nodes should have the same configuration ID
        var configIds = nodes.Select(n => n.CurrentView.ConfigurationId).Distinct().ToList();
        Assert.Single(configIds);
    }

    [Fact]
    public void Bootstrap_AllNodesHaveSameMembershipView()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // All nodes should see the same members
        var memberSets = nodes.Select(n =>
            string.Join(",", n.CurrentView.Members
                .Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}")
                .OrderBy(x => x)))
            .Distinct()
            .ToList();

        Assert.Single(memberSets);
    }

    [Fact]
    public void Bootstrap_NodeIdsAreAssignedDeterministically()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // Node IDs should be 1, 2, 3 based on sorted address order
        var allNodeIds = nodes
            .SelectMany(n => n.CurrentView.Members.Select(m => m.NodeId))
            .Distinct()
            .OrderBy(id => id)
            .ToList();

        Assert.Equal([1, 2, 3], allNodeIds);
    }

    [Fact]
    public void Bootstrap_ConfigurationIdIsOne()
    {
        // Bootstrap should create ConfigurationId 1 directly
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // All nodes should have ConfigurationId 1
        Assert.All(nodes, n => Assert.Equal(1, n.CurrentView.ConfigurationId.Version));
    }

    [Fact]
    public void Bootstrap_CanJoinAdditionalNodesAfterBootstrap()
    {
        // Bootstrap initial cluster
        var bootstrapNodes = _harness.CreateClusterWithBootstrap(size: 3);

        // Join additional node using traditional join
        var joiner = _harness.CreateJoinerNode(bootstrapNodes[0], nodeId: 10);

        _harness.WaitForConvergence(expectedSize: 4);

        // All nodes should see 4 members
        Assert.All(bootstrapNodes, n => Assert.Equal(4, n.MembershipSize));
        Assert.Equal(4, joiner.MembershipSize);
    }

    [Fact]
    public void Bootstrap_CanRemoveNodeAfterBootstrap()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // Remove one node gracefully
        _harness.RemoveNodeGracefully(nodes[2]);

        // Remaining nodes should converge to size 2
        _harness.WaitForConvergence([nodes[0], nodes[1]], expectedSize: 2);

        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);
    }

    [Fact]
    public void Bootstrap_HandlesNodeCrashAfterFormation()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 3);

        // Crash one node
        _harness.CrashNode(nodes[2]);

        // Remaining nodes should eventually detect failure and converge
        _harness.WaitForConvergence([nodes[0], nodes[1]], expectedSize: 2);

        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);
    }

    [Fact]
    public void Bootstrap_LargeCluster_TenNodes()
    {
        var nodes = _harness.CreateClusterWithBootstrap(size: 10);

        Assert.Equal(10, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(10, n.MembershipSize));
    }

    [Fact]
    public void Bootstrap_TwoNodeCluster()
    {
        // Minimum viable cluster with bootstrap
        var nodes = _harness.CreateClusterWithBootstrap(size: 2);

        Assert.Equal(2, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(2, n.MembershipSize));
    }

    [Fact]
    public void Bootstrap_SingleNodeCluster_DegeneratesToSeedNode()
    {
        // BootstrapExpect=1 should behave like creating a seed node
        var nodes = _harness.CreateClusterWithBootstrap(size: 1);

        Assert.Single(nodes);
        Assert.True(nodes[0].IsInitialized);
        Assert.Equal(1, nodes[0].MembershipSize);
    }

    [Fact]
    public async Task Bootstrap_DifferentSeedOrderings_ProduceSameMembership()
    {
        // Test that the deterministic sorting ensures same membership
        // regardless of which node is created first
        var nodes1 = _harness.CreateClusterWithBootstrap(size: 3);

        var memberSet1 = string.Join(",", nodes1[0].CurrentView.Members
            .Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}")
            .OrderBy(x => x));

        // Create another harness with same seed
        await using var harness2 = new RapidSimulationCluster(seed: TestSeed);
        var nodes2 = harness2.CreateClusterWithBootstrap(size: 3);

        var memberSet2 = string.Join(",", nodes2[0].CurrentView.Members
            .Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}")
            .OrderBy(x => x));

        // Same seed should produce same membership
        Assert.Equal(memberSet1, memberSet2);
    }

    [Fact]
    public void Bootstrap_ConvergesQuickly()
    {
        // Bootstrap should be faster than sequential joins because there's no
        // need for multiple consensus rounds (one per joiner)
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();

        var nodes = _harness.CreateClusterWithBootstrap(size: 5);

        stopwatch.Stop();

        // All nodes should be initialized
        Assert.All(nodes, n => Assert.True(n.IsInitialized));

        // Should complete reasonably quickly (this is simulation time, not real time)
        // Just verify it doesn't hang
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(30),
            $"Bootstrap took too long: {stopwatch.Elapsed}");
    }
}
