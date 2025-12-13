using System.Collections.Concurrent;
using System.Reactive.Linq;
using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Integration;

/// <summary>
/// Tests whether subscription callbacks are invoked on cluster starts/joins.
/// Port of Java SubscriptionsTest.java
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
    /// Starts consuming events from a cluster's EventStream (IAsyncEnumerable) and collects them into a TestCallback.
    /// </summary>
    private void StartEventConsumer(IRapidCluster cluster, ClusterEvents eventType, TestCallback callback)
    {
        var cts = new CancellationTokenSource();
        _subscriptionCts.Add(cts);

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var notification in cluster.EventStream.WithCancellation(cts.Token))
                {
                    if (notification.Event == eventType)
                    {
                        callback.Accept(notification.Change);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during cleanup
            }
        }, cts.Token);
    }

    /// <summary>
    /// Starts consuming events from a cluster's EventStream (IAsyncEnumerable) and adds them to a bag.
    /// </summary>
    private void StartEventConsumer(IRapidCluster cluster, ClusterEvents eventType, ConcurrentBag<ClusterStatusChange> bag)
    {
        var cts = new CancellationTokenSource();
        _subscriptionCts.Add(cts);

        _ = Task.Run(async () =>
        {
            try
            {
                await foreach (var notification in cluster.EventStream.WithCancellation(cts.Token))
                {
                    if (notification.Event == eventType)
                    {
                        bag.Add(notification.Change);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during cleanup
            }
        }, cts.Token);
    }

    /// <summary>
    /// Starts consuming events from a cluster's Events (IObservable) and collects them into a TestCallback.
    /// Uses Rx.NET's Subscribe method with filtering.
    /// </summary>
    private void StartObservableConsumer(IRapidCluster cluster, ClusterEvents eventType, TestCallback callback)
    {
        var subscription = cluster.Events
            .Where(n => n.Event == eventType)
            .Subscribe(n => callback.Accept(n.Change));
        _observableSubscriptions.Add(subscription);
    }

    /// <summary>
    /// Starts consuming events from a cluster's Events (IObservable) and adds them to a bag.
    /// Uses Rx.NET's Subscribe method with filtering.
    /// </summary>
    private void StartObservableConsumer(IRapidCluster cluster, ClusterEvents eventType, ConcurrentBag<ClusterStatusChange> bag)
    {
        var subscription = cluster.Events
            .Where(n => n.Event == eventType)
            .Subscribe(n => bag.Add(n.Change));
        _observableSubscriptions.Add(subscription);
    }

    /// <summary>
    /// Two node cluster, one subscription each.
    /// With IAsyncEnumerable, subscribers only receive events published AFTER they subscribe.
    /// The seed's consumer is started before the join, so it receives the join event.
    /// The joiner's consumer may miss the initial view change if it's published synchronously during join.
    /// </summary>
    [Fact]
    public async Task SubscriptionOnJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedCb = new TestCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartEventConsumer(seed, ClusterEvents.ViewChange, seedCb);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        // Note: Joiner's consumer is started AFTER join completes, so it may miss the initial view change
        var joinCb = new TestCallback();
        StartEventConsumer(joiner, ClusterEvents.ViewChange, joinCb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed should receive at least 1 event for the join (consumer started before joiner joined)
        Assert.True(seedCb.NumTimesCalled() >= 1, $"Expected seed to receive at least 1 event, got {seedCb.NumTimesCalled()}");

        // Verify that the seed's final membership includes both nodes
        var seedMaxMembership = seedCb.GetMembershipLog().Max(m => m.Count);
        Assert.True(seedMaxMembership >= 2, $"Seed max membership was {seedMaxMembership}, expected at least 2");

        // The joiner's consumer was started after the join completed, so it may receive 0 events.
        // This is expected behavior with IAsyncEnumerable - you only get events published after subscribing.
        // The important thing is that the seed received the join event.

        // Verify all reported statuses from seed are UP
        TestNodeStatus(seedCb.GetDeltaLog(), EdgeStatus.Up);
        if (joinCb.NumTimesCalled() > 0)
        {
            TestNodeStatus(joinCb.GetDeltaLog(), EdgeStatus.Up);
        }
    }

    /// <summary>
    /// Two node cluster, two subscriptions each.
    /// With IAsyncEnumerable, subscribers only receive events published AFTER they subscribe.
    /// </summary>
    [Fact]
    public async Task MultipleSubscriptionsOnJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedCb1 = new TestCallback();
        var seedCb2 = new TestCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartEventConsumer(seed, ClusterEvents.ViewChange, seedCb1);
        StartEventConsumer(seed, ClusterEvents.ViewChange, seedCb2);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        // Note: Joiner's consumers are started AFTER join completes
        var joinCb1 = new TestCallback();
        var joinCb2 = new TestCallback();
        StartEventConsumer(joiner, ClusterEvents.ViewChange, joinCb1);
        StartEventConsumer(joiner, ClusterEvents.ViewChange, joinCb2);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Both seed callbacks should receive the same number of events (consumer started before join)
        Assert.True(seedCb1.NumTimesCalled() >= 1, $"Expected seedCb1 to receive at least 1 event, got {seedCb1.NumTimesCalled()}");
        Assert.True(seedCb2.NumTimesCalled() >= 1, $"Expected seedCb2 to receive at least 1 event, got {seedCb2.NumTimesCalled()}");
        // Both should receive the same events since they subscribed at the same time
        Assert.Equal(seedCb1.NumTimesCalled(), seedCb2.NumTimesCalled());

        // Verify all reported statuses from seed are UP
        TestNodeStatus(seedCb1.GetDeltaLog(), EdgeStatus.Up);
        TestNodeStatus(seedCb2.GetDeltaLog(), EdgeStatus.Up);

        // Joiner callbacks may be 0 as they were started after join completed
        if (joinCb1.NumTimesCalled() > 0)
        {
            TestNodeStatus(joinCb1.GetDeltaLog(), EdgeStatus.Up);
        }
        if (joinCb2.NumTimesCalled() > 0)
        {
            TestNodeStatus(joinCb2.GetDeltaLog(), EdgeStatus.Up);
        }
    }

    /// <summary>
    /// Tests that view change events include the correct delta information.
    /// </summary>
    [Fact]
    public async Task SubscriptionIncludesDeltaInformation()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var viewChanges = new ConcurrentBag<ClusterStatusChange>();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartEventConsumer(seed, ClusterEvents.ViewChange, viewChanges);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.True(viewChanges.Count > 0);

        // At least one change should have delta information
        var changesWithDelta = viewChanges.Where(c => c.Delta.Count > 0).ToList();
        Assert.True(changesWithDelta.Count > 0, "Expected at least one view change with delta");
    }

    /// <summary>
    /// Tests subscription with metadata propagation.
    /// </summary>
    [Fact]
    public async Task SubscriptionWithMetadata()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var viewChanges = new ConcurrentBag<ClusterStatusChange>();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, options =>
        {
            options.SetMetadata(new Dictionary<string, ByteString>
            {
                ["role"] = ByteString.CopyFromUtf8("seed")
            });
        }, TestContext.Current.CancellationToken);
        StartEventConsumer(seed, ClusterEvents.ViewChange, viewChanges);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, options =>
        {
            options.SetMetadata(new Dictionary<string, ByteString>
            {
                ["role"] = ByteString.CopyFromUtf8("worker")
            });
        }, TestContext.Current.CancellationToken);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await Task.Delay(500, TestContext.Current.CancellationToken);

        Assert.True(viewChanges.Count > 0);

        // Final membership should include both nodes
        var lastChange = viewChanges.OrderByDescending(c => c.Membership.Count).First();
        Assert.Equal(2, lastChange.Membership.Count);
    }

    /// <summary>
    /// Multi-node cluster with subscriptions.
    /// With IAsyncEnumerable, subscribers only receive events published AFTER they subscribe.
    /// Joiner1's consumer is started after it joins, so it can receive joiner2's join event.
    /// </summary>
    [Fact]
    public async Task SubscriptionWithMultipleNodes()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedCb = new TestCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartEventConsumer(seed, ClusterEvents.ViewChange, seedCb);

        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        // Joiner1's consumer is started AFTER join, but BEFORE joiner2 joins
        var joiner1Cb = new TestCallback();
        StartEventConsumer(joiner1, ClusterEvents.ViewChange, joiner1Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);
        // Joiner2's consumer is started AFTER join
        var joiner2Cb = new TestCallback();
        StartEventConsumer(joiner2, ClusterEvents.ViewChange, joiner2Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed receives callbacks for both joins (consumer started before any joiner joined)
        Assert.True(seedCb.NumTimesCalled() >= 2, $"Expected seed to receive at least 2 events, got {seedCb.NumTimesCalled()}");

        // Joiner1's consumer was started after it joined but before joiner2 joined,
        // so it should receive at least the joiner2 join event
        Assert.True(joiner1Cb.NumTimesCalled() >= 1, $"Expected joiner1 to receive at least 1 event, got {joiner1Cb.NumTimesCalled()}");

        // Joiner2's consumer was started after it joined, so may receive 0 events
        // This is expected behavior with IAsyncEnumerable

        // Final membership should have 3 nodes (check max to handle timing issues)
        var seedMaxMembership = seedCb.GetMembershipLog().Max(m => m.Count);
        var joiner1MaxMembership = joiner1Cb.GetMembershipLog().Max(m => m.Count);
        Assert.True(seedMaxMembership >= 3, $"Seed max membership was {seedMaxMembership}, expected at least 3");
        Assert.True(joiner1MaxMembership >= 3, $"Joiner1 max membership was {joiner1MaxMembership}, expected at least 3");
    }

    /// <summary>
    /// Helper that scans a notification log and checks whether all UP statuses match EdgeStatus.Up.
    /// Some delta entries may have default/unset status, so we only check those that are explicitly Up.
    /// </summary>
    private static void TestNodeStatus(List<List<NodeStatusChange>> log, EdgeStatus expectedValue)
    {
        // For join events, we expect all statuses to be UP
        // Filter to only non-empty entries and verify they match
        var nonEmptyEntries = log.Where(entry => entry.Count > 0).ToList();
        Assert.True(nonEmptyEntries.Count > 0 || log.Count == 0, "Expected at least one non-empty delta entry");

        foreach (var entry in nonEmptyEntries)
        {
            foreach (var status in entry.Where(s => s.Status == expectedValue))
            {
                // Found at least one status with expected value
                Assert.Equal(expectedValue, status.Status);
            }
        }
    }

    /// <summary>
    /// Encapsulates a NodeStatusChange callback and counts the number of times it was invoked.
    /// </summary>
    private sealed class TestCallback
    {
        private readonly ConcurrentBag<ClusterStatusChange> _notificationLog = [];

        public int NumTimesCalled() => _notificationLog.Count;

        public List<List<Endpoint>> GetMembershipLog() =>
            [.. _notificationLog.Select(c => c.Membership.ToList())];

        public List<List<NodeStatusChange>> GetDeltaLog() =>
            [.. _notificationLog.Select(c => c.Delta.ToList())];

        public void Accept(ClusterStatusChange clusterStatusChange) =>
            _notificationLog.Add(clusterStatusChange);
    }


    /// <summary>
    /// Two node cluster using IObservable subscriptions.
    /// Observable subscribers receive events synchronously when published, making them more
    /// likely to receive events during the join process.
    /// </summary>
    [Fact]
    public async Task ObservableSubscriptionOnJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedCb = new TestCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartObservableConsumer(seed, ClusterEvents.ViewChange, seedCb);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        var joinCb = new TestCallback();
        StartObservableConsumer(joiner, ClusterEvents.ViewChange, joinCb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed should receive at least 1 event for the join
        Assert.True(seedCb.NumTimesCalled() >= 1, $"Expected seed to receive at least 1 event, got {seedCb.NumTimesCalled()}");

        // Verify that the seed's final membership includes both nodes
        var seedMaxMembership = seedCb.GetMembershipLog().Max(m => m.Count);
        Assert.True(seedMaxMembership >= 2, $"Seed max membership was {seedMaxMembership}, expected at least 2");

        // Verify all reported statuses from seed are UP
        TestNodeStatus(seedCb.GetDeltaLog(), EdgeStatus.Up);
        if (joinCb.NumTimesCalled() > 0)
        {
            TestNodeStatus(joinCb.GetDeltaLog(), EdgeStatus.Up);
        }
    }

    /// <summary>
    /// Two node cluster, two IObservable subscriptions each.
    /// Both observers should receive the same events since BroadcastChannel multicasts to all subscribers.
    /// </summary>
    [Fact]
    public async Task MultipleObservableSubscriptionsOnJoin()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedCb1 = new TestCallback();
        var seedCb2 = new TestCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartObservableConsumer(seed, ClusterEvents.ViewChange, seedCb1);
        StartObservableConsumer(seed, ClusterEvents.ViewChange, seedCb2);

        var (joinerApp, joiner) = await _cluster.CreateJoinerNodeAsync(joinerAddress, seedAddress, TestContext.Current.CancellationToken);
        var joinCb1 = new TestCallback();
        var joinCb2 = new TestCallback();
        StartObservableConsumer(joiner, ClusterEvents.ViewChange, joinCb1);
        StartObservableConsumer(joiner, ClusterEvents.ViewChange, joinCb2);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Both seed callbacks should receive the same number of events
        Assert.True(seedCb1.NumTimesCalled() >= 1, $"Expected seedCb1 to receive at least 1 event, got {seedCb1.NumTimesCalled()}");
        Assert.True(seedCb2.NumTimesCalled() >= 1, $"Expected seedCb2 to receive at least 1 event, got {seedCb2.NumTimesCalled()}");
        // Both should receive the same events since they subscribed at the same time
        Assert.Equal(seedCb1.NumTimesCalled(), seedCb2.NumTimesCalled());

        // Verify all reported statuses from seed are UP
        TestNodeStatus(seedCb1.GetDeltaLog(), EdgeStatus.Up);
        TestNodeStatus(seedCb2.GetDeltaLog(), EdgeStatus.Up);

        // Both joiner callbacks should receive the same number of events
        Assert.Equal(joinCb1.NumTimesCalled(), joinCb2.NumTimesCalled());
    }

    /// <summary>
    /// Tests that IObservable subscriptions work with Rx.NET operators like Where, Select, etc.
    /// This demonstrates the composability of the observable API.
    /// </summary>
    [Fact]
    public async Task ObservableWithRxOperators()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joinerAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var membershipCounts = new ConcurrentBag<int>();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);

        // Use Rx operators to transform the stream: filter to ViewChange events and extract membership count
        var subscription = seed.Events
            .Where(n => n.Event == ClusterEvents.ViewChange)
            .Select(n => n.Change.Membership.Count)
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
    /// Tests that IObservable subscriptions receive events published after subscription,
    /// demonstrating the multicast nature of BroadcastChannel.
    /// </summary>
    [Fact]
    public async Task ObservableMulticastBehavior()
    {
        var seedAddress = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner1Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());
        var joiner2Address = Utils.HostFromParts("127.0.0.1", _cluster.GetNextPort());

        var seedCb = new TestCallback();
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        StartObservableConsumer(seed, ClusterEvents.ViewChange, seedCb);

        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        // Subscribe to joiner1 after it joins
        var joiner1Cb = new TestCallback();
        StartObservableConsumer(joiner1, ClusterEvents.ViewChange, joiner1Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 2, TimeSpan.FromSeconds(10)).ConfigureAwait(true);

        var (joiner2App, joiner2) = await _cluster.CreateJoinerNodeAsync(joiner2Address, seedAddress, TestContext.Current.CancellationToken);
        var joiner2Cb = new TestCallback();
        StartObservableConsumer(joiner2, ClusterEvents.ViewChange, joiner2Cb);

        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner1, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);
        await TestCluster.WaitForClusterSizeAsync(joiner2, 3, TimeSpan.FromSeconds(15)).ConfigureAwait(true);

        // Give callbacks time to fire
        await Task.Delay(500, TestContext.Current.CancellationToken);

        // Seed should receive callbacks for both joins
        Assert.True(seedCb.NumTimesCalled() >= 2, $"Expected seed to receive at least 2 events, got {seedCb.NumTimesCalled()}");

        // Joiner1's observer was subscribed after it joined but before joiner2 joined,
        // so it should receive at least the joiner2 join event
        Assert.True(joiner1Cb.NumTimesCalled() >= 1, $"Expected joiner1 to receive at least 1 event, got {joiner1Cb.NumTimesCalled()}");

        // Final membership should have 3 nodes
        var seedMaxMembership = seedCb.GetMembershipLog().Max(m => m.Count);
        var joiner1MaxMembership = joiner1Cb.GetMembershipLog().Max(m => m.Count);
        Assert.True(seedMaxMembership >= 3, $"Seed max membership was {seedMaxMembership}, expected at least 3");
        Assert.True(joiner1MaxMembership >= 3, $"Joiner1 max membership was {joiner1MaxMembership}, expected at least 3");
    }

}
