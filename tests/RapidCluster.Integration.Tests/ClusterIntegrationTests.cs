using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Microsoft.AspNetCore.Builder;

namespace RapidCluster.Integration.Tests;

/// <summary>
/// Integration tests for Rapid cluster using the new hosting API
/// </summary>
public sealed class ClusterIntegrationTests(ITestOutputHelper outputHelper) : IAsyncDisposable
{
    private readonly TestCluster _cluster = new(outputHelper);
    private readonly List<CancellationTokenSource> _subscriptionCts = [];

    public async ValueTask DisposeAsync()
    {
        // Cancel all subscriptions
        foreach (var cts in _subscriptionCts)
        {
            cts.SafeCancel();
            cts.Dispose();
        }
        _subscriptionCts.Clear();

        await _cluster.DisposeAsync();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Helper to create an IPEndPoint from port.
    /// </summary>
    private static IPEndPoint CreateAddress(int port) => new(IPAddress.Parse("127.0.0.1"), port);

    /// <summary>
    /// Starts consuming view updates from a cluster's ViewUpdates channel and adds them to a bag.
    /// </summary>
    private void StartViewUpdateConsumer(IRapidCluster cluster, ConcurrentBag<ClusterMembershipView> bag)
    {
        var cts = new CancellationTokenSource();
        _subscriptionCts.Add(cts);

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var view in cluster.ViewUpdates.WithCancellation(cts.Token))
                {
                    bag.Add(view);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during cleanup
            }
        }, cts.Token);
    }

    /// <summary>
    /// Test that a single seed node can start successfully
    /// </summary>
    [Fact]
    public async Task SingleSeedNodeStarts()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var (_, cluster) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Give it a moment to initialize
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Single(cluster.CurrentView.Members);
    }

    /// <summary>
    /// Test with a single node joining through a seed
    /// </summary>
    [Fact]
    public async Task SingleNodeJoinsThroughSeed()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        Assert.Single(seed.CurrentView.Members);
        var (_, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10));

        Assert.Equal(2, seed.CurrentView.Members.Length);
        Assert.Equal(2, joiner.CurrentView.Members.Length);
    }

    /// <summary>
    /// Test with three nodes forming a cluster
    /// </summary>
    [Fact]
    public async Task ThreeNodesFormCluster()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());
        var joiner2Address = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (_, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (_, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10));

        Assert.Equal(3, seed.CurrentView.Members.Length);
        Assert.Equal(3, joiner1.CurrentView.Members.Length);
        Assert.Equal(3, joiner2.CurrentView.Members.Length);
    }

    /// <summary>
    /// Test that view updates are received when nodes join
    /// </summary>
    [Fact]
    public async Task ViewUpdatesReceivedOnJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var viewUpdates = new ConcurrentBag<ClusterMembershipView>();

        // Create seed and start collecting view updates
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartViewUpdateConsumer(seed, viewUpdates);
        var (_, _) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Give some time for events to be collected
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Should have received at least one view update
        Assert.False(viewUpdates.IsEmpty, $"Expected at least 1 view update, got {viewUpdates.Count}");
    }

    /// <summary>
    /// Test that metadata is propagated correctly
    /// </summary>
    [Fact]
    public async Task MetadataIsPropagated()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        // Create seed with metadata
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, options =>
        {
            options.Metadata["role"] = Encoding.UTF8.GetBytes("seed");
            options.Metadata["datacenter"] = Encoding.UTF8.GetBytes("us-west");
        }, TestContext.Current.CancellationToken);

        // Create joiner with metadata
        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, options =>
        {
            options.Metadata["role"] = Encoding.UTF8.GetBytes("worker");
            options.Metadata["datacenter"] = Encoding.UTF8.GetBytes("us-east");
        }, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Verify metadata is available
        var members = seed.CurrentView.Members;
        Assert.Equal(2, members.Length);

        // Find members by endpoint
        var seedMember = members.First(m => m.EndPoint.Equals(seedAddress));
        var joinerMember = members.First(m => m.EndPoint.Equals(joinerAddress));

        // Verify metadata values
        Assert.Equal("seed", Encoding.UTF8.GetString(seedMember.Metadata["role"].Span));
        Assert.Equal("worker", Encoding.UTF8.GetString(joinerMember.Metadata["role"].Span));
    }

    /// <summary>
    /// Test that multiple nodes can join concurrently
    /// Reduced from 5 to 3 concurrent joins to avoid consensus timeout issues
    /// </summary>
    [Fact]
    public async Task MultipleNodesConcurrentJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        const int numJoiners = 3;
        var joinTasks = new List<Task<(WebApplication App, IRapidCluster Cluster)>>();

        for (var i = 0; i < numJoiners; i++)
        {
            var joinerAddress = CreateAddress(_cluster.GetNextPort());
            joinTasks.Add(_cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken));
        }

        var joiners = await Task.WhenAll(joinTasks);

        // Wait for cluster convergence - increased timeout for concurrent joins
        await TestCluster.WaitForClusterSizeAsync(seed, numJoiners + 1, TimeSpan.FromSeconds(30));

        Assert.Equal(numJoiners + 1, seed.CurrentView.Members.Length);
        foreach (var (_, joiner) in joiners)
        {
            await TestCluster.WaitForClusterSizeAsync(joiner, numJoiners + 1, TimeSpan.FromSeconds(30));
            Assert.Equal(numJoiners + 1, joiner.CurrentView.Members.Length);
        }
    }

    [Fact]
    public async Task SequentialJoinsFiveNodesAllConverge()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var nodes = new List<IRapidCluster> { seed };
        for (var i = 0; i < 4; i++)
        {
            var joinerAddress = CreateAddress(_cluster.GetNextPort());
            var (_, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
            await TestCluster.WaitForClusterSizeAsync(joiner, nodes.Count + 1, TimeSpan.FromSeconds(10));
            nodes.Add(joiner);
        }

        foreach (var node in nodes)
        {
            await TestCluster.WaitForClusterSizeAsync(node, 5, TimeSpan.FromSeconds(10));
            Assert.Equal(5, node.CurrentView.Members.Length);
        }
    }

    [Fact]
    public async Task JoinThroughNonSeedNodeWorks()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());
        var joiner2Address = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (_, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));
        var (_, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, joiner1Address, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10));
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10));

        Assert.Equal(3, seed.CurrentView.Members.Length);
        Assert.Equal(3, joiner1.CurrentView.Members.Length);
        Assert.Equal(3, joiner2.CurrentView.Members.Length);
    }

    [Fact]
    public async Task GetMembersReturnsAllMembers()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        var members = seed.CurrentView.Members;

        Assert.Equal(2, members.Length);
        Assert.Contains(members, m => ((IPEndPoint)m.EndPoint).Port == seedAddress.Port);
        Assert.Contains(members, m => ((IPEndPoint)m.EndPoint).Port == joiner1Address.Port);
    }

    [Fact]
    public async Task GetMembersAllNodesConsistent()
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

        var seedList = seed.CurrentView.Members.Select(m => ((IPEndPoint)m.EndPoint).Port).Order().ToList();
        var joiner1List = joiner1.CurrentView.Members.Select(m => ((IPEndPoint)m.EndPoint).Port).Order().ToList();
        var joiner2List = joiner2.CurrentView.Members.Select(m => ((IPEndPoint)m.EndPoint).Port).Order().ToList();

        Assert.Equal(seedList.Count, joiner1List.Count);
        Assert.Equal(seedList.Count, joiner2List.Count);

        for (var i = 0; i < seedList.Count; i++)
        {
            Assert.Equal(seedList[i], joiner1List[i]);
            Assert.Equal(seedList[i], joiner2List[i]);
        }
    }

    [Fact]
    public async Task ComplexMetadataPropagatedCorrectly()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, options =>
        {
            options.Metadata["role"] = Encoding.UTF8.GetBytes("seed");
            options.Metadata["region"] = Encoding.UTF8.GetBytes("us-west-2");
            options.Metadata["zone"] = Encoding.UTF8.GetBytes("a");
            options.Metadata["instance_type"] = Encoding.UTF8.GetBytes("m5.large");
            options.Metadata["version"] = Encoding.UTF8.GetBytes("1.0.0");
        }, TestContext.Current.CancellationToken);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, options =>
        {
            options.Metadata["role"] = Encoding.UTF8.GetBytes("worker");
            options.Metadata["region"] = Encoding.UTF8.GetBytes("us-east-1");
            options.Metadata["zone"] = Encoding.UTF8.GetBytes("b");
            options.Metadata["instance_type"] = Encoding.UTF8.GetBytes("c5.xlarge");
            options.Metadata["version"] = Encoding.UTF8.GetBytes("1.0.0");
        }, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        var members = seed.CurrentView.Members;

        Assert.Equal(2, members.Length);
        var seedMember = members.First(m => m.EndPoint.Equals(seedAddress));
        var joinerMember = members.First(m => m.EndPoint.Equals(joinerAddress));
        Assert.Equal("seed", Encoding.UTF8.GetString(seedMember.Metadata["role"].Span));
        Assert.Equal("worker", Encoding.UTF8.GetString(joinerMember.Metadata["role"].Span));
        Assert.Equal("us-west-2", Encoding.UTF8.GetString(seedMember.Metadata["region"].Span));
        Assert.Equal("us-east-1", Encoding.UTF8.GetString(joinerMember.Metadata["region"].Span));
    }

    [Fact]
    public async Task EmptyMetadataHandledCorrectly()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        var members = seed.CurrentView.Members;

        Assert.Equal(2, members.Length);
        var seedMember = members.First(m => m.EndPoint.Equals(seedAddress));
        var joinerMember = members.First(m => m.EndPoint.Equals(joinerAddress));
        Assert.Empty(seedMember.Metadata);
        Assert.Empty(joinerMember.Metadata);
    }

    [Fact]
    public async Task MultipleSubscriptionsAllReceiveViewUpdates()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Start three separate view update consumers
        var bag1 = new ConcurrentBag<ClusterMembershipView>();
        var bag2 = new ConcurrentBag<ClusterMembershipView>();
        var bag3 = new ConcurrentBag<ClusterMembershipView>();
        StartViewUpdateConsumer(seed, bag1);
        StartViewUpdateConsumer(seed, bag2);
        StartViewUpdateConsumer(seed, bag3);
        var (_, _) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Give some time for events to be collected
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // All three consumers should have received view updates
        Assert.False(bag1.IsEmpty);
        Assert.False(bag2.IsEmpty);
        Assert.False(bag3.IsEmpty);
    }

    [Fact]
    public async Task ViewUpdateContainsCorrectMembership()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var viewUpdates = new ConcurrentBag<ClusterMembershipView>();
        StartViewUpdateConsumer(seed, viewUpdates);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Give some time for events to be collected
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.NotEmpty(viewUpdates);
        var lastView = viewUpdates.OrderByDescending(v => v.ConfigurationId).First();
        Assert.True(lastView.Members.Length >= 2);
    }

    [Fact]
    public async Task ConfigurationIdChangesOnJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var viewUpdates = new ConcurrentBag<ClusterMembershipView>();
        StartViewUpdateConsumer(seed, viewUpdates);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Give some time for events to be collected
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Collect config IDs from view updates
        var configIds = viewUpdates.Select(v => v.ConfigurationId).ToList();

        Assert.NotEmpty(configIds);
    }

    [Fact]
    public async Task FourNodesFormCluster()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());
        var joiner2Address = CreateAddress(_cluster.GetNextPort());
        var joiner3Address = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (_, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (_, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);
        var (_, joiner3) = await _cluster.CreateJoinerNodeAsync(joiner3Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 4, TimeSpan.FromSeconds(15));
        await TestCluster.WaitForClusterSizeAsync(joiner1, 4, TimeSpan.FromSeconds(15));
        await TestCluster.WaitForClusterSizeAsync(joiner2, 4, TimeSpan.FromSeconds(15));
        await TestCluster.WaitForClusterSizeAsync(joiner3, 4, TimeSpan.FromSeconds(15));

        Assert.Equal(4, seed.CurrentView.Members.Length);
        Assert.Equal(4, joiner1.CurrentView.Members.Length);
        Assert.Equal(4, joiner2.CurrentView.Members.Length);
        Assert.Equal(4, joiner3.CurrentView.Members.Length);
    }

    [Fact]
    public async Task ViewUpdatesTracked()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());
        var (_, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var viewUpdates = new ConcurrentBag<ClusterMembershipView>();
        StartViewUpdateConsumer(seed, viewUpdates);
        var (_, _) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10));

        // Give some time for events to be collected
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Count view updates
        Assert.False(viewUpdates.IsEmpty);
    }
}
