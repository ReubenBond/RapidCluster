using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for the seed gossip protocol that enables safe cluster bootstrapping
/// when seeds are discovered dynamically (DNS, service discovery, Kubernetes, etc.).
/// </summary>
/// <remarks>
/// <para>
/// When seeds are not guaranteed identical across nodes (IsStatic = false),
/// the seed gossip protocol ensures all bootstrap nodes agree on the same
/// seed set before forming the cluster, preventing split-brain scenarios.
/// </para>
/// <para>
/// The protocol works as follows:
/// 1. Each node starts with its initial discovered seeds
/// 2. Nodes gossip known seeds to other known seeds
/// 3. Receiving nodes merge seeds (union) and gossip back
/// 4. Seeds are normalized: sorted, truncated to BootstrapExpect
/// 5. Once all nodes report the same hash, agreement is reached
/// 6. Cluster forms with the agreed seed set
/// </para>
/// </remarks>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class DynamicSeedBootstrapTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 77777;

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
    public void DynamicSeeds_AllNodesKnowAllSeeds_FormCluster()
    {
        // Simplest case: dynamic seeds but all nodes happen to know all seeds
        // Should behave like static bootstrap
        var nodes = _harness.CreateClusterWithDynamicSeeds(size: 3);

        Assert.Equal(3, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void DynamicSeeds_OverlappingSeedSets_ConvergeAndFormCluster()
    {
        // Node 0 knows nodes 0, 1
        // Node 1 knows nodes 0, 1, 2
        // Node 2 knows nodes 1, 2
        // After gossip, all nodes should learn about all 3 seeds
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        Assert.Equal(3, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void DynamicSeeds_MinimalOverlap_ConvergeAndFormCluster()
    {
        // Five nodes with minimal overlap - each knows itself and one neighbor
        // Node 0: [0, 1]
        // Node 1: [1, 2]
        // Node 2: [2, 3]
        // Node 3: [3, 4]
        // Node 4: [4, 0]
        // The chain of gossip should propagate all seeds around
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 5,
            seedsPerNode: nodeIndex => [nodeIndex, (nodeIndex + 1) % 5]);

        Assert.Equal(5, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    [Fact]
    public void DynamicSeeds_AllNodesHaveSameConfigurationId()
    {
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        // All nodes should have the same configuration ID after gossip convergence
        var configIds = nodes.Select(n => n.CurrentView.ConfigurationId).Distinct().ToList();
        Assert.Single(configIds);
    }

    [Fact]
    public void DynamicSeeds_AllNodesHaveSameMembershipView()
    {
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

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
    public void DynamicSeeds_TwoDisjointGroupsWithBridge_ConvergeAndFormCluster()
    {
        // Two groups that would form separate clusters without the bridge node
        // Group A: nodes 0, 1, 2 (each knows 0, 1, 2)
        // Group B: nodes 3, 4 (each knows 3, 4)
        // Bridge: node 2 also knows node 3
        // This tests that the gossip protocol can merge disjoint seed sets
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 5,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1, 2],
                1 => [0, 1, 2],
                2 => [0, 1, 2, 3],  // Bridge node
                3 => [2, 3, 4],     // Knows the bridge
                4 => [3, 4],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        Assert.Equal(5, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    [Fact]
    public void DynamicSeeds_CanJoinAdditionalNodesAfterBootstrap()
    {
        // Bootstrap with dynamic seeds
        var bootstrapNodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        // Join additional node using traditional join
        var joiner = _harness.CreateJoinerNode(bootstrapNodes[0], nodeId: 10);

        _harness.WaitForConvergence(expectedSize: 4);

        // All nodes should see 4 members
        Assert.All(bootstrapNodes, n => Assert.Equal(4, n.MembershipSize));
        Assert.Equal(4, joiner.MembershipSize);
    }

    [Fact]
    public void DynamicSeeds_ConfigurationIdIsOne()
    {
        // Bootstrap should create ConfigurationId 1 directly
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        // All nodes should have ConfigurationId 1
        Assert.All(nodes, n => Assert.Equal(1, n.CurrentView.ConfigurationId.Version));
    }

    [Fact]
    public void DynamicSeeds_NodeIdsAreAssignedDeterministically()
    {
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        // Node IDs should be 1, 2, 3 based on sorted address order
        var allNodeIds = nodes
            .SelectMany(n => n.CurrentView.Members.Select(m => m.NodeId))
            .Distinct()
            .OrderBy(id => id)
            .ToList();

        Assert.Equal([1, 2, 3], allNodeIds);
    }

    [Fact]
    public void DynamicSeeds_HandlesNodeCrashAfterFormation()
    {
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        // Crash one node
        _harness.CrashNode(nodes[2]);

        // Remaining nodes should eventually detect failure and converge
        _harness.WaitForConvergence([nodes[0], nodes[1]], expectedSize: 2);

        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);
    }

    [Fact]
    public void DynamicSeeds_FiveNodeCluster_WithVariedSeedKnowledge()
    {
        // Test with larger cluster where seed knowledge varies
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 5,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1, 2],        // Knows first 3
                1 => [0, 1, 2, 3],     // Knows first 4
                2 => [1, 2, 3, 4],     // Knows middle 4
                3 => [2, 3, 4],        // Knows last 3
                4 => [0, 4],           // Knows only first and last
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        Assert.Equal(5, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    [Fact]
    public void DynamicSeeds_CanRemoveNodeAfterBootstrap()
    {
        var nodes = _harness.CreateClusterWithDynamicSeeds(
            size: 3,
            seedsPerNode: nodeIndex => nodeIndex switch
            {
                0 => [0, 1],
                1 => [0, 1, 2],
                2 => [1, 2],
                _ => throw new ArgumentOutOfRangeException(nameof(nodeIndex))
            });

        // Remove one node gracefully
        _harness.RemoveNodeGracefully(nodes[2]);

        // Remaining nodes should converge to size 2
        _harness.WaitForConvergence([nodes[0], nodes[1]], expectedSize: 2);

        Assert.Equal(2, nodes[0].MembershipSize);
        Assert.Equal(2, nodes[1].MembershipSize);
    }
}
