using System.Collections.Concurrent;
using System.Net;
using System.Reactive.Linq;
using System.Text;

namespace RapidCluster.Integration.Tests;

/// <summary>
/// Tests for cluster view subscription functionality.
/// Updated to use the new ViewUpdates API that provides ClusterMembershipView updates.
/// </summary>
public sealed class SubscriptionsTests(ITestOutputHelper outputHelper) : IAsyncDisposable
{
    private readonly TestCluster _cluster = new(outputHelper);
    private readonly List<CancellationTokenSource> _subscriptionCts = [];
    private readonly List<IDisposable> _observableSubscriptions = [];

    public async ValueTask DisposeAsync()
    {
        // Dispose all observable subscriptions
        foreach (var subscription in _observableSubscriptions)
        {
            subscription.Dispose();
        }
        _observableSubscriptions.Clear();

        // Cancel all async enumerable subscriptions
        foreach (var cts in _subscriptionCts)
        {
            cts.SafeCancel();
            cts.Dispose();
        }
        _subscriptionCts.Clear();

        await _cluster.DisposeAsync().ConfigureAwait(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Helper to create an IPEndPoint from port.
    /// </summary>
    private static IPEndPoint CreateAddress(int port) => new(IPAddress.Parse("127.0.0.1"), port);

    /// <summary>
    /// Starts consuming view updates from a cluster's ViewUpdates (IAsyncEnumerable) and collects them into a callback.
    /// </summary>
    private void StartViewUpdateConsumer(IRapidCluster cluster, ViewCallback callback)
    {
        var cts = new CancellationTokenSource();
        _subscriptionCts.Add(cts);

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var view in cluster.ViewUpdates.WithCancellation(cts.Token))
                {
                    callback.Accept(view);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during cleanup
            }
        }, cts.Token);
    }

    /// <summary>
    /// Starts consuming view updates from a cluster's ViewUpdates (IAsyncEnumerable) and adds them to a bag.
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
    /// Starts consuming view updates from a cluster's ViewUpdates (IObservable) and collects them into a callback.
    /// Uses Rx.NET's Subscribe method.
    /// </summary>
    private void StartObservableConsumer(IRapidCluster cluster, ViewCallback callback)
    {
        var subscription = cluster.ViewUpdates.Subscribe(callback.Accept);
        _observableSubscriptions.Add(subscription);
    }

    /// <summary>
    /// Starts consuming view updates from a cluster's ViewUpdates (IObservable) and adds them to a bag.
    /// Uses Rx.NET's Subscribe method.
    /// </summary>
    private void StartObservableConsumer(IRapidCluster cluster, ConcurrentBag<ClusterMembershipView> bag)
    {
        var subscription = cluster.ViewUpdates.Subscribe(bag.Add);
        _observableSubscriptions.Add(subscription);
    }

    /// <summary>
    /// Two node cluster, one subscription each.
    /// With IAsyncEnumerable, subscribers only receive views published AFTER they subscribe.
    /// The seed's consumer is started before the join, so it receives the join view update.
    /// </summary>
    [Fact]
    public async Task SubscriptionOnJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var seedCb = new ViewCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartViewUpdateConsumer(seed, seedCb);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        // Note: Joiner's consumer is started AFTER join completes, so it may miss the initial view update
        var joinCb = new ViewCallback();
        StartViewUpdateConsumer(joiner, joinCb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed should receive at least 1 view update for the join (consumer started before joiner joined)
        Assert.True(seedCb.NumTimesCalled() >= 1, $"Expected seed to receive at least 1 view update, got {seedCb.NumTimesCalled()}");

        // Verify that the seed's final membership includes both nodes
        var seedMaxMembership = seedCb.GetViewLog().Max(v => v.Members.Length);
        Assert.True(seedMaxMembership >= 2, $"Seed max membership was {seedMaxMembership}, expected at least 2");
    }

    /// <summary>
    /// Two node cluster, two subscriptions each.
    /// With IAsyncEnumerable, subscribers only receive views published AFTER they subscribe.
    /// </summary>
    [Fact]
    public async Task MultipleSubscriptionsOnJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var seedCb1 = new ViewCallback();
        var seedCb2 = new ViewCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartViewUpdateConsumer(seed, seedCb1);
        StartViewUpdateConsumer(seed, seedCb2);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        // Note: Joiner's consumers are started AFTER join completes
        var joinCb1 = new ViewCallback();
        var joinCb2 = new ViewCallback();
        StartViewUpdateConsumer(joiner, joinCb1);
        StartViewUpdateConsumer(joiner, joinCb2);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Both seed callbacks should receive the same number of views (consumer started before join)
        Assert.True(seedCb1.NumTimesCalled() >= 1, $"Expected seedCb1 to receive at least 1 view update, got {seedCb1.NumTimesCalled()}");
        Assert.True(seedCb2.NumTimesCalled() >= 1, $"Expected seedCb2 to receive at least 1 view update, got {seedCb2.NumTimesCalled()}");
        // Both should receive the same views since they subscribed at the same time
        Assert.Equal(seedCb1.NumTimesCalled(), seedCb2.NumTimesCalled());
    }

    /// <summary>
    /// Tests that view updates include membership information.
    /// </summary>
    [Fact]
    public async Task ViewUpdateIncludesMembership()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var viewUpdates = new ConcurrentBag<ClusterMembershipView>();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartViewUpdateConsumer(seed, viewUpdates);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.True(viewUpdates.Count > 0);

        // At least one view should have 2 members
        var viewsWithTwoMembers = viewUpdates.Where(v => v.Members.Length >= 2).ToList();
        Assert.True(viewsWithTwoMembers.Count > 0, "Expected at least one view with 2 members");
    }

    /// <summary>
    /// Tests subscription with metadata propagation.
    /// </summary>
    [Fact]
    public async Task SubscriptionWithMetadata()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var viewUpdates = new ConcurrentBag<ClusterMembershipView>();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, options =>
        {
            options.Metadata["role"] = Encoding.UTF8.GetBytes("seed");
        }, TestContext.Current.CancellationToken);
        StartViewUpdateConsumer(seed, viewUpdates);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, options =>
        {
            options.Metadata["role"] = Encoding.UTF8.GetBytes("worker");
        }, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.True(viewUpdates.Count > 0);

        // Final view should include both nodes with metadata
        var lastView = viewUpdates.OrderByDescending(v => v.ConfigurationId).First();
        Assert.Equal(2, lastView.Members.Length);
        // Verify both members have metadata
        Assert.All(lastView.Members, m => Assert.True(m.Metadata.ContainsKey("role")));
    }

    /// <summary>
    /// Multi-node cluster with subscriptions.
    /// With IAsyncEnumerable, subscribers only receive views published AFTER they subscribe.
    /// Joiner1's consumer is started after it joins, so it can receive joiner2's join view.
    /// </summary>
    [Fact]
    public async Task SubscriptionWithMultipleNodes()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());
        var joiner2Address = CreateAddress(_cluster.GetNextPort());

        var seedCb = new ViewCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartViewUpdateConsumer(seed, seedCb);

        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        // Joiner1's consumer is started AFTER join, but BEFORE joiner2 joins
        var joiner1Cb = new ViewCallback();
        StartViewUpdateConsumer(joiner1, joiner1Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);
        // Joiner2's consumer is started AFTER join
        var joiner2Cb = new ViewCallback();
        StartViewUpdateConsumer(joiner2, joiner2Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed receives callbacks for both joins (consumer started before any joiner joined)
        Assert.True(seedCb.NumTimesCalled() >= 2, $"Expected seed to receive at least 2 views, got {seedCb.NumTimesCalled()}");

        // Joiner1's consumer was started after it joined but before joiner2 joined,
        // so it should receive at least the joiner2 join view
        Assert.True(joiner1Cb.NumTimesCalled() >= 1, $"Expected joiner1 to receive at least 1 view, got {joiner1Cb.NumTimesCalled()}");

        // Final membership should have 3 nodes (check max to handle timing issues)
        var seedMaxMembership = seedCb.GetViewLog().Max(v => v.Members.Length);
        var joiner1MaxMembership = joiner1Cb.GetViewLog().Max(v => v.Members.Length);
        Assert.True(seedMaxMembership >= 3, $"Seed max membership was {seedMaxMembership}, expected at least 3");
        Assert.True(joiner1MaxMembership >= 3, $"Joiner1 max membership was {joiner1MaxMembership}, expected at least 3");
    }

    /// <summary>
    /// Encapsulates a view update callback and counts the number of times it was invoked.
    /// </summary>
    private sealed class ViewCallback
    {
        private readonly ConcurrentBag<ClusterMembershipView> _viewLog = [];

        public int NumTimesCalled() => _viewLog.Count;

        public List<ClusterMembershipView> GetViewLog() => [.. _viewLog];

        public void Accept(ClusterMembershipView view) => _viewLog.Add(view);
    }

    /// <summary>
    /// Two node cluster using IObservable subscriptions.
    /// Observable subscribers receive views synchronously when published.
    /// </summary>
    [Fact]
    public async Task ObservableSubscriptionOnJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var seedCb = new ViewCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartObservableConsumer(seed, seedCb);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        var joinCb = new ViewCallback();
        StartObservableConsumer(joiner, joinCb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed should receive at least 1 view update for the join
        Assert.True(seedCb.NumTimesCalled() >= 1, $"Expected seed to receive at least 1 view, got {seedCb.NumTimesCalled()}");

        // Verify that the seed's final membership includes both nodes
        var seedMaxMembership = seedCb.GetViewLog().Max(v => v.Members.Length);
        Assert.True(seedMaxMembership >= 2, $"Seed max membership was {seedMaxMembership}, expected at least 2");
    }

    /// <summary>
    /// Two node cluster, two IObservable subscriptions each.
    /// Both observers should receive the same views since BroadcastChannel multicasts to all subscribers.
    /// </summary>
    [Fact]
    public async Task MultipleObservableSubscriptionsOnJoin()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var seedCb1 = new ViewCallback();
        var seedCb2 = new ViewCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartObservableConsumer(seed, seedCb1);
        StartObservableConsumer(seed, seedCb2);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        var joinCb1 = new ViewCallback();
        var joinCb2 = new ViewCallback();
        StartObservableConsumer(joiner, joinCb1);
        StartObservableConsumer(joiner, joinCb2);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Both seed callbacks should receive the same number of views
        Assert.True(seedCb1.NumTimesCalled() >= 1, $"Expected seedCb1 to receive at least 1 view, got {seedCb1.NumTimesCalled()}");
        Assert.True(seedCb2.NumTimesCalled() >= 1, $"Expected seedCb2 to receive at least 1 view, got {seedCb2.NumTimesCalled()}");
        // Both should receive the same views since they subscribed at the same time
        Assert.Equal(seedCb1.NumTimesCalled(), seedCb2.NumTimesCalled());

        // Both joiner callbacks should receive the same number of views
        Assert.Equal(joinCb1.NumTimesCalled(), joinCb2.NumTimesCalled());
    }

    /// <summary>
    /// Tests that IObservable subscriptions work with Rx.NET operators like Where, Select, etc.
    /// This demonstrates the composability of the observable API.
    /// </summary>
    [Fact]
    public async Task ObservableWithRxOperators()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joinerAddress = CreateAddress(_cluster.GetNextPort());

        var membershipCounts = new ConcurrentBag<int>();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Use Rx operators to transform the stream: extract membership count
        // Explicit cast needed because ViewUpdates implements both IObservable<T> and IAsyncEnumerable<T>
        var subscription = ((IObservable<ClusterMembershipView>)seed.ViewUpdates)
            .Select(v => v.Members.Length)
            .Subscribe(membershipCounts.Add);
        _observableSubscriptions.Add(subscription);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Should have received at least one membership count
        Assert.True(membershipCounts.Count > 0, "Expected at least one membership count");
        // The max membership count should be 2 (seed + joiner)
        Assert.True(membershipCounts.Max() >= 2, $"Expected max membership count >= 2, got {membershipCounts.Max()}");
    }

    /// <summary>
    /// Tests that IObservable subscriptions receive views published after subscription,
    /// demonstrating the multicast nature of BroadcastChannel.
    /// </summary>
    [Fact]
    public async Task ObservableMulticastBehavior()
    {
        var seedAddress = CreateAddress(_cluster.GetNextPort());
        var joiner1Address = CreateAddress(_cluster.GetNextPort());
        var joiner2Address = CreateAddress(_cluster.GetNextPort());

        var seedCb = new ViewCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartObservableConsumer(seed, seedCb);

        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        // Subscribe to joiner1 after it joins
        var joiner1Cb = new ViewCallback();
        StartObservableConsumer(joiner1, joiner1Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);
        var joiner2Cb = new ViewCallback();
        StartObservableConsumer(joiner2, joiner2Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed should receive callbacks for both joins
        Assert.True(seedCb.NumTimesCalled() >= 2, $"Expected seed to receive at least 2 views, got {seedCb.NumTimesCalled()}");

        // Joiner1's observer was subscribed after it joined but before joiner2 joined,
        // so it should receive at least the joiner2 join view
        Assert.True(joiner1Cb.NumTimesCalled() >= 1, $"Expected joiner1 to receive at least 1 view, got {joiner1Cb.NumTimesCalled()}");

        // Final membership should have 3 nodes
        var seedMaxMembership = seedCb.GetViewLog().Max(v => v.Members.Length);
        var joiner1MaxMembership = joiner1Cb.GetViewLog().Max(v => v.Members.Length);
        Assert.True(seedMaxMembership >= 3, $"Seed max membership was {seedMaxMembership}, expected at least 3");
        Assert.True(joiner1MaxMembership >= 3, $"Joiner1 max membership was {joiner1MaxMembership}, expected at least 3");
    }
}
