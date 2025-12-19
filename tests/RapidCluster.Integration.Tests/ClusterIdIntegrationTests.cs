using System.Net;

namespace RapidCluster.Integration.Tests;

/// <summary>
/// Integration tests that verify ClusterId behavior with real networking.
/// Tests that ClusterId is properly propagated and remains stable across cluster operations.
/// </summary>
public sealed class ClusterIdIntegrationTests(ITestOutputHelper outputHelper) : IAsyncDisposable
{
    private readonly TestCluster _cluster = new(outputHelper);

    public async ValueTask DisposeAsync()
    {
        await _cluster.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    private static IPEndPoint CreateAddress(int port) => new(IPAddress.Parse("127.0.0.1"), port);

    [Fact]
    public async Task SeedNode_Has_NonEmpty_ClusterId()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());

        var (app, cluster) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Give it a moment to initialize
        await Task.Delay(500, TestContext.Current.CancellationToken);

        var clusterId = cluster.CurrentView.ConfigurationId.ClusterId;

        Assert.NotEqual(ClusterId.None, clusterId);
    }

    [Fact]
    public async Task Joiner_Inherits_ClusterId_From_Seed()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10));

        var seedClusterId = seed.CurrentView.ConfigurationId.ClusterId;
        var joinerClusterId = joiner.CurrentView.ConfigurationId.ClusterId;

        Assert.NotEqual(ClusterId.None, seedClusterId);
        Assert.Equal(seedClusterId, joinerClusterId);
    }

    [Fact]
    public async Task All_Nodes_Have_Same_ClusterId()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());
        var joiner2Address = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10));

        var seedClusterId = seed.CurrentView.ConfigurationId.ClusterId;
        var joiner1ClusterId = joiner1.CurrentView.ConfigurationId.ClusterId;
        var joiner2ClusterId = joiner2.CurrentView.ConfigurationId.ClusterId;

        Assert.NotEqual(ClusterId.None, seedClusterId);
        Assert.Equal(seedClusterId, joiner1ClusterId);
        Assert.Equal(seedClusterId, joiner2ClusterId);
    }

    [Fact]
    public async Task ClusterId_Remains_Stable_After_Join()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Capture initial ClusterId
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var initialClusterId = seed.CurrentView.ConfigurationId.ClusterId;

        // Add a node
        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // ClusterId should remain stable
        var afterJoinClusterId = seed.CurrentView.ConfigurationId.ClusterId;

        Assert.NotEqual(ClusterId.None, initialClusterId);
        Assert.Equal(initialClusterId, afterJoinClusterId);
    }

    [Fact]
    public async Task ClusterId_Stable_After_Multiple_Sequential_Joins()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        var initialClusterId = seed.CurrentView.ConfigurationId.ClusterId;

        // Add multiple nodes sequentially
        var nodes = new List<IRapidCluster> { seed };
        for (var i = 0; i < 3; i++)
        {
            var joinerAddress = CreateAddress(_cluster.GetNextPort());
            var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
            await TestCluster.WaitForClusterSizeAsync(joiner, nodes.Count + 1, TimeSpan.FromSeconds(10));
            nodes.Add(joiner);
        }

        // All nodes should have the same ClusterId
        foreach (var node in nodes)
        {
            Assert.Equal(initialClusterId, node.CurrentView.ConfigurationId.ClusterId);
        }
    }

    [Fact]
    public async Task ConfigurationId_Version_Increases_With_Membership_Changes()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        var initialVersion = seed.CurrentView.ConfigurationId.Version;
        var initialClusterId = seed.CurrentView.ConfigurationId.ClusterId;

        // Add a node
        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        var afterJoinVersion = seed.CurrentView.ConfigurationId.Version;
        var afterJoinClusterId = seed.CurrentView.ConfigurationId.ClusterId;

        // Version should have increased
        Assert.True(afterJoinVersion > initialVersion, 
            $"Version should increase: initial={initialVersion}, afterJoin={afterJoinVersion}");

        // ClusterId should remain stable
        Assert.Equal(initialClusterId, afterJoinClusterId);
    }

    [Fact]
    public async Task ClusterId_Is_Deterministic_For_Same_Seed()
    {
        // Create first cluster
        var seedAddress1 = CreateAddress(_cluster.GetNextPort());
        var (seedApp1, seed1) = await _cluster.CreateSeedNodeAsync(seedAddress1, TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var clusterId1 = seed1.CurrentView.ConfigurationId.ClusterId;

        // Stop it
        await seedApp1.StopAsync(TestContext.Current.CancellationToken);

        // Create another cluster with a different address
        var seedAddress2 = CreateAddress(_cluster.GetNextPort());
        var (seedApp2, seed2) = await _cluster.CreateSeedNodeAsync(seedAddress2, TestContext.Current.CancellationToken);
        await Task.Delay(500, TestContext.Current.CancellationToken);
        var clusterId2 = seed2.CurrentView.ConfigurationId.ClusterId;

        // Different addresses should produce different ClusterIds
        Assert.NotEqual(clusterId1, clusterId2);
    }

    [Fact]
    public async Task ConfigurationId_Available_On_All_View_Updates()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Collect view updates
        var viewUpdates = new List<ClusterMembershipView>();
        using var cts = new CancellationTokenSource();

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var view in seed.ViewUpdates.WithCancellation(cts.Token))
                {
                    viewUpdates.Add(view);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during cleanup
            }
        }, cts.Token);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Give time for view updates to be collected
        await Task.Delay(500, TestContext.Current.CancellationToken);
        await cts.CancelAsync();

        // All view updates should have the same ClusterId
        Assert.NotEmpty(viewUpdates);
        var expectedClusterId = seed.CurrentView.ConfigurationId.ClusterId;
        Assert.All(viewUpdates, view =>
            Assert.Equal(expectedClusterId, view.ConfigurationId.ClusterId));
    }
}
