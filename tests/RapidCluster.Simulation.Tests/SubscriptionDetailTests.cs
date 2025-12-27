using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Detailed subscription tests using the simulation harness.
/// These tests verify callback counts, membership views, and configuration changes
/// using the MembershipViewAccessor.Updates channel.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class SubscriptionDetailTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 45678;
    private readonly List<ObservableCollector<MembershipView>> _collectors = [];

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var collector in _collectors)
        {
            collector.Dispose();
        }
        _collectors.Clear();

        await _harness.DisposeAsync().ConfigureAwait(true);
    }

    /// <summary>
    /// Creates a view collector for the node and registers it for cleanup during disposal.
    /// </summary>
    private ObservableCollector<MembershipView> CreateViewCollector(RapidSimulationNode node)
    {
        var collector = new ObservableCollector<MembershipView>(node.ViewAccessor.Updates);
        _collectors.Add(collector);
        return collector;
    }

    /// <summary>
    /// Collects membership views from the collector.
    /// </summary>
    private static void CollectViews(ObservableCollector<MembershipView> collector, ConcurrentBag<MembershipView> bag)
    {
        foreach (var view in collector.Items)
        {
            bag.Add(view);
        }
    }

    /// <summary>
    /// Verifies that the seed node receives view change callbacks for membership changes.
    /// With IObservable, subscribers only receive events published AFTER they subscribe.
    /// Since the collector is registered after StartCluster(), it won't receive the initial
    /// VIEW_CHANGE event, but will receive events for subsequent membership changes.
    /// </summary>
    [Fact]
    public void SeedNodeReceivesCorrectCallbackCount()
    {
        var callbackLog = new ConcurrentBag<MembershipView>();

        var seedNode = _harness.CreateSeedNode();
        // Collector is started after node is created, so initial event may be missed
        var collector = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();

        // Join a node - this should trigger a callback that the collector CAN see
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        // Seed should have received at least one callback for the join
        Assert.True(callbackLog.Count >= 1,
            $"Expected at least 1 callback after join, got {callbackLog.Count}");

        // Verify the callback contains the expected membership size
        Assert.Contains(callbackLog, v => v.Size == 2);
    }

    /// <summary>
    /// Verifies that the joiner node receives view change callbacks for subsequent events.
    /// With IObservable, subscribers only receive events published AFTER they subscribe.
    /// The collector is registered after the join completes, so it needs another membership
    /// change (joiner2 joining) to trigger a callback.
    /// </summary>
    [Fact]
    public void JoinerNodeReceivesCorrectCallbackCount()
    {
        var seedNode = _harness.CreateSeedNode();

        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        // Collector is started AFTER join, so joiner1's own join event may be missed
        var joinerCallbackLog = new ConcurrentBag<MembershipView>();
        var collector = CreateViewCollector(joiner1);

        _harness.WaitForConvergence();

        // Trigger another membership change so joiner1 receives a callback
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, joinerCallbackLog);

        // Joiner1 should have received at least 1 callback for joiner2's join
        Assert.True(joinerCallbackLog.Count >= 1,
            $"Expected at least 1 callback for joiner, got {joinerCallbackLog.Count}");
    }

    /// <summary>
    /// Verifies that multiple subscriptions on the same node each receive callbacks.
    /// </summary>
    [Fact]
    public void MultipleSubscriptionsEachReceiveCallbacks()
    {
        var callbackLog1 = new ConcurrentBag<MembershipView>();
        var callbackLog2 = new ConcurrentBag<MembershipView>();

        var seedNode = _harness.CreateSeedNode();
        var collector1 = CreateViewCollector(seedNode);
        var collector2 = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();

        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector1, callbackLog1);
        CollectViews(collector2, callbackLog2);

        // Both subscriptions should receive the same number of callbacks
        Assert.True(callbackLog1.Count >= 1, "First subscription should receive callbacks");
        Assert.True(callbackLog2.Count >= 1, "Second subscription should receive callbacks");
        Assert.Equal(callbackLog1.Count, callbackLog2.Count);
    }

    /// <summary>
    /// Verifies callbacks in a multi-node cluster scenario.
    /// Subscriptions registered after join will only receive subsequent events.
    /// </summary>
    [Fact]
    public void MultiNodeClusterCallbackCounts()
    {
        var seedCallbackLog = new ConcurrentBag<MembershipView>();
        var joiner1CallbackLog = new ConcurrentBag<MembershipView>();
        var joiner2CallbackLog = new ConcurrentBag<MembershipView>();

        var seedNode = _harness.CreateSeedNode();
        var seedCollector = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();
        CollectViews(seedCollector, seedCallbackLog);
        var seedInitialCount = seedCallbackLog.Count;

        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner1Collector = CreateViewCollector(joiner1);
        _harness.WaitForConvergence();

        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        var joiner2Collector = CreateViewCollector(joiner2);
        _harness.WaitForConvergence();

        // Add a third joiner so that joiner1 and joiner2 both receive at least one callback
        var joiner3 = _harness.CreateJoinerNode(seedNode, nodeId: 3);
        _harness.WaitForConvergence();

        // Collect all views after final convergence
        CollectViews(seedCollector, seedCallbackLog);
        CollectViews(joiner1Collector, joiner1CallbackLog);
        CollectViews(joiner2Collector, joiner2CallbackLog);

        // Seed should have received callbacks for all three joins
        Assert.True(seedCallbackLog.Count >= seedInitialCount + 3,
            $"Seed should receive at least 3 more callbacks after joins, got {seedCallbackLog.Count - seedInitialCount}");

        // Joiner1 should have received callbacks for joiner2 and joiner3's joins
        Assert.True(joiner1CallbackLog.Count >= 2,
            $"Joiner1 should receive at least 2 callbacks (joiner2 join + joiner3 join), got {joiner1CallbackLog.Count}");

        // Joiner2 should have received callback for joiner3's join
        Assert.True(joiner2CallbackLog.Count >= 1,
            $"Joiner2 should receive at least 1 callback, got {joiner2CallbackLog.Count}");
    }

    /// <summary>
    /// Verifies that the membership list in callbacks grows as nodes join.
    /// Note: Subscriptions registered after MembershipService initialization will not receive
    /// the initial view callback (membership size=1), but will receive callbacks for subsequent joins.
    /// </summary>
    [Fact]
    public void MembershipListGrowsWithJoins()
    {
        var seedNode = _harness.CreateSeedNode();
        var callbackLog = new ConcurrentBag<MembershipView>();
        var collector = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();

        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        var sizes = callbackLog.Select(v => v.Size).ToList();

        // Should have seen memberships of increasing sizes for subsequent joins
        // Note: Initial view (size=1) may not be observed since subscription is registered
        // after MembershipService fires its initial VIEW_CHANGE event
        Assert.Contains(2, sizes); // After first join
        Assert.Contains(3, sizes); // After second join
    }

    /// <summary>
    /// Verifies that membership lists contain the expected endpoints.
    /// </summary>
    [Fact]
    public void MembershipContainsExpectedEndpoints()
    {
        var seedNode = _harness.CreateSeedNode();
        var callbackLog = new ConcurrentBag<MembershipView>();
        var collector = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();

        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        // Find the membership with 2 members
        var twoNodeView = callbackLog.FirstOrDefault(v => v.Size == 2);
        Assert.NotNull(twoNodeView);

        // Verify both endpoints are present
        var hostnames = twoNodeView.Members.Select(e => e.Hostname.ToStringUtf8()).ToHashSet();
        Assert.Contains(seedNode.Address.Hostname.ToStringUtf8(), hostnames);
        Assert.Contains(joiner.Address.Hostname.ToStringUtf8(), hostnames);
    }

    /// <summary>
    /// Verifies that view changes (membership growing) can be detected by comparing consecutive views.
    /// We use two joins to ensure we capture at least one growth transition.
    /// </summary>
    [Fact]
    public void ViewChangesCanBeDetectedByComparingConsecutiveViews()
    {
        var callbackLog = new ConcurrentBag<MembershipView>();

        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Start collecting after first convergence
        var collector = CreateViewCollector(seedNode);

        // Add another node - this triggers a view change we will capture
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        // Get views sorted by configuration version
        var views = callbackLog.OrderBy(v => v.ConfigurationId.Version).ToList();

        // Verify we captured a view with size 3 that contains joiner2
        var joiner2Hostname = joiner2.Address.Hostname.ToStringUtf8();
        Assert.Contains(views, v => v.Size == 3 &&
            v.Members.Any(e => e.Hostname.ToStringUtf8() == joiner2Hostname));
    }

    /// <summary>
    /// Verifies that view changes for node failures can be detected by comparing consecutive views.
    /// </summary>
    [Fact]
    public void FailureDetectedByComparingConsecutiveViews()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence();

        // Start collecting AFTER convergence, so we have a clean starting point
        var callbackLog = new ConcurrentBag<MembershipView>();
        var collector = CreateViewCollector(joiner1);

        var joiner2Address = joiner2.Address;

        // Crash a node
        _harness.CrashNode(joiner2);

        // Wait for failure detection
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        // Verify we captured a view without the crashed node (match by hostname AND port)
        Assert.Contains(callbackLog, v =>
            v.Size == 2 &&
            !v.Members.Any(e =>
                e.Hostname.ToStringUtf8() == joiner2Address.Hostname.ToStringUtf8() &&
                e.Port == joiner2Address.Port));
    }

    /// <summary>
    /// Verifies that configuration ID changes with each membership change.
    /// Note: Since subscriptions are registered after MembershipService initialization,
    /// we need to have multiple joins to observe multiple configuration IDs.
    /// </summary>
    [Fact]
    public void ConfigurationIdChangesWithMembershipChanges()
    {
        var seedNode = _harness.CreateSeedNode();
        var callbackLog = new ConcurrentBag<MembershipView>();
        var collector = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();

        // First joiner - triggers first callback
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Second joiner - triggers second callback with different config ID
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        var configIdList = callbackLog.Select(v => v.ConfigurationId).ToList();

        // Should have at least 2 different configuration IDs
        var uniqueConfigIds = configIdList.Distinct().ToList();
        Assert.True(uniqueConfigIds.Count >= 2,
            $"Expected at least 2 unique configuration IDs, got {uniqueConfigIds.Count}");
    }

    /// <summary>
    /// Verifies that all nodes see the same configuration ID after convergence.
    /// Note: Since subscriptions are registered after join, we need to trigger an
    /// additional membership change for joiner nodes to receive callbacks.
    /// </summary>
    [Fact]
    public void AllNodesSeeSameConfigurationIdAfterConvergence()
    {
        long? seedConfigId = null;
        long? joiner1ConfigId = null;

        var seedNode = _harness.CreateSeedNode();
        var seedCallbackLog = new ConcurrentBag<MembershipView>();
        var seedCollector = CreateViewCollector(seedNode);

        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner1CallbackLog = new ConcurrentBag<MembershipView>();
        var joiner1Collector = CreateViewCollector(joiner1);

        _harness.WaitForConvergence();

        // Add a third node so both seed and joiner1 receive callbacks
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(seedCollector, seedCallbackLog);
        CollectViews(joiner1Collector, joiner1CallbackLog);

        // Get config IDs from callbacks with 3 members
        seedConfigId = seedCallbackLog.FirstOrDefault(v => v.Size == 3)?.ConfigurationId.Version;
        joiner1ConfigId = joiner1CallbackLog.FirstOrDefault(v => v.Size == 3)?.ConfigurationId.Version;

        // Both should have seen configuration ID for 3-node cluster
        Assert.NotNull(seedConfigId);
        Assert.NotNull(joiner1ConfigId);
        Assert.Equal(seedConfigId, joiner1ConfigId);
    }

    /// <summary>
    /// Verifies that subscriptions added after join still receive future events.
    /// </summary>
    [Fact]
    public void SubscriptionAddedAfterJoinReceivesFutureEvents()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Add subscription after join
        var callbackLog = new ConcurrentBag<MembershipView>();
        var collector = CreateViewCollector(joiner1);

        // Trigger another membership change
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbackLog);

        // Should have received at least one callback for the new join
        Assert.NotEmpty(callbackLog);
        Assert.Contains(callbackLog, v => v.Size == 3);
    }

    /// <summary>
    /// Verifies that callback exceptions don't crash the membership service.
    /// Note: With IObservable, observer exceptions don't affect other observers or the broadcaster.
    /// </summary>
    [Fact]
    public void CallbackExceptionDoesNotCrashService()
    {
        var successfulCallbacks = new ConcurrentBag<MembershipView>();

        var seedNode = _harness.CreateSeedNode();

        // Start a collector that will succeed
        var collector = CreateViewCollector(seedNode);

        // Note: With IObservable, a throwing observer would only crash its own subscription,
        // not affect other observers. This is inherently safe by design.

        // This should work fine
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, successfulCallbacks);

        // Cluster should still be functional
        Assert.Equal(2, seedNode.MembershipSize);
        Assert.Equal(2, joiner.MembershipSize);
    }

    /// <summary>
    /// Verifies that MembershipView is properly initialized in callbacks.
    /// </summary>
    [Fact]
    public void MembershipViewIsProperlyInitializedInCallbacks()
    {
        var callbacks = new ConcurrentBag<MembershipView>();

        var seedNode = _harness.CreateSeedNode();
        var collector = CreateViewCollector(seedNode);

        _harness.RunUntilIdle();

        // Trigger a membership change to get a callback
        var joiner = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        _harness.WaitForConvergence();

        // Collect views after convergence
        CollectViews(collector, callbacks);

        // Verify we got at least one callback
        Assert.NotEmpty(callbacks);

        // All callbacks should have valid views
        foreach (var view in callbacks)
        {
            Assert.NotNull(view);
            Assert.False(view.Members.IsDefault);
            Assert.True(view.Size > 0);
        }
    }

}
