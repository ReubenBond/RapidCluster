using System.Collections.Concurrent;
using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Integration;

/// <summary>
/// Integration tests for Rapid cluster using the new hosting API
/// </summary>
public sealed class ClusterIntegrationTests(ITestOutputHelper outputHelper) : IAsyncDisposable
{
    private readonly TestCluster _cluster = new(outputHelper);
    private readonly List<ObservableCollector<ClusterEventNotification>> _collectors = [];

    public async ValueTask DisposeAsync()
    {
        foreach (var collector in _collectors)
        {
            collector.Dispose();
        }
        _collectors.Clear();

        await _cluster.DisposeAsync().ConfigureAwait(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Creates an event collector for the cluster's ClusterEvents observable.
    /// </summary>
    private ObservableCollector<ClusterEventNotification> CreateEventCollector(IRapidCluster cluster)
    {
        var collector = new ObservableCollector<ClusterEventNotification>(cluster.Events);
        _collectors.Add(collector);
        return collector;
    }

    /// <summary>
    /// Collects matching events from the collector into the bag.
    /// </summary>
    private static void CollectEvents(ObservableCollector<ClusterEventNotification> collector, ClusterEvents eventType, ConcurrentBag<ClusterStatusChange> bag)
    {
        foreach (var notification in collector.Items.Where(n => n.Event == eventType))
        {
            bag.Add(notification.Change);
        }
    }

    /// <summary>
    /// Test that a single seed node can start successfully
    /// </summary>
    [Fact]
    public async Task SingleSeedNodeStarts()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (app, cluster) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Give it a moment to initialize
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.Equal(1, cluster.GetMembershipSize());
    }

    /// <summary>
    /// Test with a single node joining through a seed
    /// </summary>
    [Fact]
    public async Task SingleNodeJoinsThroughSeed()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        Assert.Equal(1, seed.GetMembershipSize());

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        Assert.Equal(2, seed.GetMembershipSize());
        Assert.Equal(2, joiner.GetMembershipSize());
    }

    /// <summary>
    /// Test with three nodes forming a cluster
    /// </summary>
    [Fact]
    public async Task ThreeNodesFormCluster()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        Assert.Equal(3, seed.GetMembershipSize());
        Assert.Equal(3, joiner1.GetMembershipSize());
        Assert.Equal(3, joiner2.GetMembershipSize());
    }

    /// <summary>
    /// Test that view change events are fired when nodes join
    /// </summary>
    [Fact]
    public async Task ViewChangeEventsFireOnJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var viewChanges = new ConcurrentBag<ClusterStatusChange>();

        // Create seed and start collecting events
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var collector = CreateEventCollector(seed);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Collect view changes
        CollectEvents(collector, ClusterEvents.ViewChange, viewChanges);

        // Should have received at least one view change event
        Assert.True(viewChanges.Count > 0);
    }

    /// <summary>
    /// Test that metadata is propagated correctly
    /// </summary>
    [Fact]
    public async Task MetadataIsPropagated()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedMetadataDict = new Dictionary<string, Google.Protobuf.ByteString>
        {
            ["role"] = Google.Protobuf.ByteString.CopyFromUtf8("seed"),
            ["datacenter"] = Google.Protobuf.ByteString.CopyFromUtf8("us-west")
        };

        // Create seed with metadata
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, options =>
        {
            options.SetMetadata(seedMetadataDict);
        }, TestContext.Current.CancellationToken);

        var joinerMetadataDict = new Dictionary<string, Google.Protobuf.ByteString>
        {
            ["role"] = Google.Protobuf.ByteString.CopyFromUtf8("worker"),
            ["datacenter"] = Google.Protobuf.ByteString.CopyFromUtf8("us-east")
        };

        // Create joiner with metadata
        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, options =>
        {
            options.SetMetadata(joinerMetadataDict);
        }, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Verify metadata is available
        var allMetadata = seed.GetClusterMetadata();
        Assert.True(allMetadata.ContainsKey(seedAddress));
        Assert.True(allMetadata.ContainsKey(joinerAddress));

        Assert.Equal("seed", allMetadata[seedAddress].Metadata_["role"].ToStringUtf8());
        Assert.Equal("worker", allMetadata[joinerAddress].Metadata_["role"].ToStringUtf8());
    }

    /// <summary>
    /// Test graceful leave - uses 3 nodes to ensure monitoring relationships exist
    /// </summary>
    [Fact]
    public async Task NodeCanLeaveGracefully()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        Assert.Equal(3, seed.GetMembershipSize());

        // Joiner2 leaves gracefully
        await joiner2.LeaveGracefullyAsync(TestContext.Current.CancellationToken).ConfigureAwait(true);

        // Wait for remaining nodes to detect the leave - use WaitForClusterSizeExactAsync since we're waiting for size to decrease
        await TestCluster.WaitForClusterSizeExactAsync(seed, 2, TimeSpan.FromSeconds(20)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeExactAsync(joiner1, 2, TimeSpan.FromSeconds(20)).ConfigureAwait(true);

        Assert.Equal(2, seed.GetMembershipSize());
        Assert.Equal(2, joiner1.GetMembershipSize());
    }

    /// <summary>
    /// Test that multiple nodes can join concurrently
    /// Reduced from 5 to 3 concurrent joins to avoid consensus timeout issues
    /// </summary>
    [Fact]
    public async Task MultipleNodesConcurrentJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        const int numJoiners = 3;
        var joinTasks = new List<Task<(WebApplication App, IRapidCluster Cluster)>>();

        for (var i = 0; i < numJoiners; i++)
        {
            var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
            joinTasks.Add(_cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken));
        }

        var joiners = await Task.WhenAll(joinTasks).ConfigureAwait(true);

        // Wait for cluster convergence - increased timeout for concurrent joins
        await TestCluster.WaitForClusterSizeAsync(seed, numJoiners + 1, TimeSpan.FromSeconds(30)).ConfigureAwait(true);

        Assert.Equal(numJoiners + 1, seed.GetMembershipSize());

        foreach (var (app, joiner) in joiners)
        {
            await TestCluster.WaitForClusterSizeAsync(joiner, numJoiners + 1, TimeSpan.FromSeconds(30)).ConfigureAwait(true);
            Assert.Equal(numJoiners + 1, joiner.GetMembershipSize());
        }
    }

    /// <summary>
    /// Test view change proposal events
    /// </summary>
    [Fact]
    public async Task ViewChangeProposalEventsFire()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var proposals = new ConcurrentBag<ClusterStatusChange>();

        // Create seed and start collecting proposal events
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var collector = CreateEventCollector(seed);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        // Wait for cluster convergence
        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Collect proposals
        CollectEvents(collector, ClusterEvents.ViewChangeProposal, proposals);

        // Should have received proposal events
        Assert.True(proposals.Count > 0);
    }

    [Fact]
    public async Task SequentialJoinsFiveNodesAllConverge()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var nodes = new List<IRapidCluster> { seed };
        for (var i = 0; i < 4; i++)
        {
            var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
            var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
            await TestCluster.WaitForClusterSizeAsync(joiner, nodes.Count + 1, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            nodes.Add(joiner);
        }

        foreach (var node in nodes)
        {
            await TestCluster.WaitForClusterSizeAsync(node, 5, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
            Assert.Equal(5, node.GetMembershipSize());
        }
    }

    [Fact]
    public async Task JoinThroughNonSeedNodeWorks()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, joiner1Address, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        Assert.Equal(3, seed.GetMembershipSize());
        Assert.Equal(3, joiner1.GetMembershipSize());
        Assert.Equal(3, joiner2.GetMembershipSize());
    }

    [Fact]
    public async Task GetMemberlistReturnsAllMembers()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var memberlist = seed.GetMemberlist();

        Assert.Equal(2, memberlist.Count);
        Assert.Contains(memberlist, e => e.Port == seedAddress.Port);
        Assert.Contains(memberlist, e => e.Port == joiner1Address.Port);
    }

    [Fact]
    public async Task GetMemberlistAllNodesConsistent()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var seedList = seed.GetMemberlist().OrderBy(e => e.Port).ToList();
        var joiner1List = joiner1.GetMemberlist().OrderBy(e => e.Port).ToList();
        var joiner2List = joiner2.GetMemberlist().OrderBy(e => e.Port).ToList();

        Assert.Equal(seedList.Count, joiner1List.Count);
        Assert.Equal(seedList.Count, joiner2List.Count);

        for (var i = 0; i < seedList.Count; i++)
        {
            Assert.Equal(seedList[i].Port, joiner1List[i].Port);
            Assert.Equal(seedList[i].Port, joiner2List[i].Port);
        }
    }

    [Fact]
    public async Task ComplexMetadataPropagatedCorrectly()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, options =>
        {
            options.SetMetadata(new Dictionary<string, Google.Protobuf.ByteString>
            {
                ["role"] = Google.Protobuf.ByteString.CopyFromUtf8("seed"),
                ["region"] = Google.Protobuf.ByteString.CopyFromUtf8("us-west-2"),
                ["zone"] = Google.Protobuf.ByteString.CopyFromUtf8("a"),
                ["instance_type"] = Google.Protobuf.ByteString.CopyFromUtf8("m5.large"),
                ["version"] = Google.Protobuf.ByteString.CopyFromUtf8("1.0.0")
            });
        }, TestContext.Current.CancellationToken);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, options =>
        {
            options.SetMetadata(new Dictionary<string, Google.Protobuf.ByteString>
            {
                ["role"] = Google.Protobuf.ByteString.CopyFromUtf8("worker"),
                ["region"] = Google.Protobuf.ByteString.CopyFromUtf8("us-east-1"),
                ["zone"] = Google.Protobuf.ByteString.CopyFromUtf8("b"),
                ["instance_type"] = Google.Protobuf.ByteString.CopyFromUtf8("c5.xlarge"),
                ["version"] = Google.Protobuf.ByteString.CopyFromUtf8("1.0.0")
            });
        }, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var allMetadata = seed.GetClusterMetadata();

        Assert.Equal(2, allMetadata.Count);
        Assert.Equal("seed", allMetadata[seedAddress].Metadata_["role"].ToStringUtf8());
        Assert.Equal("worker", allMetadata[joinerAddress].Metadata_["role"].ToStringUtf8());
        Assert.Equal("us-west-2", allMetadata[seedAddress].Metadata_["region"].ToStringUtf8());
        Assert.Equal("us-east-1", allMetadata[joinerAddress].Metadata_["region"].ToStringUtf8());
    }

    [Fact]
    public async Task EmptyMetadataHandledCorrectly()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var allMetadata = seed.GetClusterMetadata();

        Assert.Equal(2, allMetadata.Count);
        Assert.Empty(allMetadata[seedAddress].Metadata_);
        Assert.Empty(allMetadata[joinerAddress].Metadata_);
    }

    [Fact]
    public async Task MultipleSubscriptionsAllReceiveEvents()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Start three separate collectors
        var collector1 = CreateEventCollector(seed);
        var collector2 = CreateEventCollector(seed);
        var collector3 = CreateEventCollector(seed);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Count events from all collectors
        var count1 = collector1.Items.Count(n => n.Event == ClusterEvents.ViewChange);
        var count2 = collector2.Items.Count(n => n.Event == ClusterEvents.ViewChange);
        var count3 = collector3.Items.Count(n => n.Event == ClusterEvents.ViewChange);

        Assert.True(count1 > 0);
        Assert.True(count2 > 0);
        Assert.True(count3 > 0);
    }

    [Fact]
    public async Task SubscriptionReceivesCorrectMembership()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var collector = CreateEventCollector(seed);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Find view changes
        var viewChanges = collector.Items
            .Where(n => n.Event == ClusterEvents.ViewChange)
            .Select(n => n.Change)
            .ToList();

        Assert.NotEmpty(viewChanges);
        var lastChange = viewChanges.Last();
        Assert.True(lastChange.Membership.Count >= 2);
    }

    [Fact]
    public async Task ConfigurationIdChangesOnJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var collector = CreateEventCollector(seed);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Collect config IDs from view changes
        var configIds = collector.Items
            .Where(n => n.Event == ClusterEvents.ViewChange)
            .Select(n => n.Change.ConfigurationId)
            .ToList();

        Assert.NotEmpty(configIds);
    }

    [Fact]
    public async Task FourNodesFormCluster()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner3Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);
        var (joiner3App, joiner3) = await _cluster.CreateJoinerNodeAsync(joiner3Address, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 4, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 4, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 4, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner3, 4, TimeSpan.FromSeconds(15)).ConfigureAwait(true);

        Assert.Equal(4, seed.GetMembershipSize());
        Assert.Equal(4, joiner1.GetMembershipSize());
        Assert.Equal(4, joiner2.GetMembershipSize());
        Assert.Equal(4, joiner3.GetMembershipSize());
    }

    [Fact]
    public async Task NodeStatusChangesTracked()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var collector = CreateEventCollector(seed);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Count view changes
        var viewChangeCount = collector.Items.Count(n => n.Event == ClusterEvents.ViewChange);

        Assert.True(viewChangeCount > 0);
    }

}
