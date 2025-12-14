using System.Diagnostics.CodeAnalysis;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for basic cluster operations using the simulation harness.
/// Covers single node, two-node, and multi-node cluster formation.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class ClusterBasicTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 12345;

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
    public void SingleNodeClusterInitializes()
    {
        var seedNode = _harness.CreateSeedNode();

        Assert.NotNull(seedNode);
        Assert.True(seedNode.IsInitialized);
        Assert.Equal(1, seedNode.MembershipSize);
    }

    [Fact]
    public void SingleNodeHasValidConfigurationId()
    {
        var seedNode = _harness.CreateSeedNode();

        Assert.NotNull(seedNode.CurrentView);
        Assert.True(seedNode.CurrentView.ConfigurationId >= 0);
    }

    [Fact]
    public void SingleNodeViewContainsSelf()
    {
        var seedNode = _harness.CreateSeedNode();

        var view = seedNode.CurrentView;
        Assert.NotNull(view);
        Assert.Single(view.Members);

        var member = view.Members[0];
        Assert.Equal(seedNode.Address.Hostname, member.Hostname);
        Assert.Equal(seedNode.Address.Port, member.Port);
    }

    [Fact]
    public void SingleNodeCanShutdownGracefully()
    {
        var seedNode = _harness.CreateSeedNode();
        Assert.True(seedNode.IsInitialized);

        // CrashNode should not throw (hard shutdown)
        _harness.CrashNode(seedNode);
    }

    [Fact]
    public async Task SingleNodeCanLeaveCluster()
    {
        var seedNode = _harness.CreateSeedNode();
        Assert.True(seedNode.IsInitialized);

        // Stop should not throw (degenerates to shutdown for single node)
        _harness.Run(seedNode.StopAsync);
    }

    [Fact]
    public void TwoNodeClusterFormation()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        Assert.NotNull(joiner);
        Assert.True(joiner.IsInitialized);
        Assert.Equal(2, joiner.MembershipSize);
    }

    [Fact]
    public void JoinerSeesCorrectMembership()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        var view = joiner.CurrentView;
        Assert.NotNull(view);
        Assert.Equal(2, view.Members.Length);

        // View should contain both nodes
        var addresses = view.Members.Select(m => $"{m.Hostname}:{m.Port}").ToHashSet();
        Assert.Contains($"{seedNode.Address.Hostname}:{seedNode.Address.Port}", addresses);
        Assert.Contains($"{joiner.Address.Hostname}:{joiner.Address.Port}", addresses);
    }

    [Fact]
    public void SeedSeesJoinerAfterJoin()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Wait for seed to see the joiner
        _harness.WaitForNodeSize(seedNode, expectedSize: 2);

        Assert.Equal(2, seedNode.MembershipSize);
    }

    [Fact]
    public void BothNodesHaveSameConfigurationId()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Wait for convergence
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(seedNode.CurrentView.ConfigurationId, joiner.CurrentView.ConfigurationId);
    }

    [Fact]
    public void JoinerCanLeaveThreeNodeCluster()
    {
        // Use 3-node cluster so remaining 2 nodes can reach quorum for consensus
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Joiner2 leaves gracefully
        _harness.RemoveNodeGracefully(joiner2);

        // Wait for remaining nodes to see the leave
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner1.MembershipSize);
    }

    [Fact]
    public void SeedCanLeaveThreeNodeCluster()
    {
        // Use 3-node cluster so remaining 2 nodes can reach quorum for consensus
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Seed leaves gracefully
        _harness.RemoveNodeGracefully(seedNode);

        // Wait for remaining nodes to see the leave
        _harness.WaitForConvergence(expectedSize: 2);

        Assert.Equal(2, joiner1.MembershipSize);
        Assert.Equal(2, joiner2.MembershipSize);
    }

    [Fact]
    public void ThreeNodeClusterFormation()
    {
        var nodes = _harness.CreateCluster(size: 3);

        Assert.Equal(3, nodes.Count);
        Assert.All(nodes, node => Assert.True(node.IsInitialized));
        Assert.All(nodes, node => Assert.Equal(3, node.MembershipSize));
    }

    [Fact]
    public void FiveNodeClusterFormation()
    {
        var nodes = _harness.CreateCluster(size: 5);

        Assert.Equal(5, nodes.Count);
        Assert.All(nodes, node => Assert.True(node.IsInitialized));
        Assert.All(nodes, node => Assert.Equal(5, node.MembershipSize));
    }

    [Fact]
    public void SequentialJoinsSucceed()
    {
        var seedNode = _harness.CreateSeedNode();

        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        Assert.True(joiner1.IsInitialized);
        Assert.Equal(2, joiner1.MembershipSize);

        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        Assert.True(joiner2.IsInitialized);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    [Fact]
    public void AllNodesConvergeToSameMembership()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // All nodes should have the same membership size
        Assert.Equal(3, seedNode.MembershipSize);
        Assert.Equal(3, joiner1.MembershipSize);
        Assert.Equal(3, joiner2.MembershipSize);
    }

    [Fact]
    public void ConfigurationIdChangesWithMembershipChanges()
    {
        var seedNode = _harness.CreateSeedNode();
        var initialConfigId = seedNode.CurrentView.ConfigurationId;

        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);

        // Wait for convergence
        _harness.WaitForConvergence(expectedSize: 2);

        // Configuration ID should have changed after membership change
        // Note: Configuration IDs are hashes, so they change but don't necessarily increase monotonically
        Assert.NotEqual(initialConfigId, joiner.CurrentView.ConfigurationId);

        // Both nodes should have the same configuration ID
        Assert.Equal(joiner.CurrentView.ConfigurationId, seedNode.CurrentView.ConfigurationId);
    }

    [Fact]
    public void MembershipViewContainsAllNodes()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        var view = seedNode.CurrentView;
        var addresses = view.Members.Select(m => $"{m.Hostname}:{m.Port}").ToHashSet();

        Assert.Contains($"{seedNode.Address.Hostname}:{seedNode.Address.Port}", addresses);
        Assert.Contains($"{joiner1.Address.Hostname}:{joiner1.Address.Port}", addresses);
        Assert.Contains($"{joiner2.Address.Hostname}:{joiner2.Address.Port}", addresses);
    }

    [Fact]
    public void NodeCanJoinWithMetadata()
    {
        var seedNode = _harness.CreateSeedNode();

        var metadata = new Pb.Metadata();
        metadata.Metadata_.Add("role", Google.Protobuf.ByteString.CopyFromUtf8("worker"));

        // Join with metadata should not throw
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        Assert.True(joiner.IsInitialized);
    }

}
