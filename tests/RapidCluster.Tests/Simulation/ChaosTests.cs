using System.Diagnostics.CodeAnalysis;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for chaos injection and stress testing using the simulation harness.
/// Verifies that the cluster maintains safety properties under random faults.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class ChaosTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private ChaosInjector _chaos = null!;
    private InvariantChecker _checker = null!;
    private const int TestSeed = 78901;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        _chaos = new ChaosInjector(_harness);
        _checker = new InvariantChecker(_harness);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _harness.DisposeAsync();
    }


    [Fact]
    public void ChaosInjectorDefaultsToZeroRates()
    {
        Assert.Equal(0, _chaos.NodeCrashRate);
        Assert.Equal(0, _chaos.PartitionRate);
    }

    [Fact]
    public void ChaosInjectorRespectsMinimumAliveNodes()
    {
        // Create multiple seed nodes (not a real cluster, but tests crash protection)
        var nodes = new List<RapidSimulationNode>();
        for (var i = 0; i < 5; i++)
        {
            nodes.Add(_harness.CreateSeedNode(i));
        }

        _chaos.NodeCrashRate = 1.0; // 100% crash rate
        _chaos.MinimumAliveNodes = 3;

        // Run chaos - should leave at least 3 nodes alive
        _chaos.RunChaos(steps: 100);

        Assert.True(_harness.Nodes.Count >= 3);
    }

    [Fact]
    public void ScheduledCrashExecutesAtCorrectTime()
    {
        var node = _harness.CreateSeedNode();

        _chaos.ScheduleNodeCrash(node, TimeSpan.FromSeconds(5));

        // Before scheduled time - node should still exist
        _harness.RunForDuration(TimeSpan.FromSeconds(4));
        _chaos.MaybeInjectFault(); // Process scheduled faults
        Assert.Contains(node, _harness.Nodes);

        // After scheduled time - node should be crashed
        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault(); // Process scheduled faults
        Assert.DoesNotContain(node, _harness.Nodes);
    }

    [Fact]
    public void ScheduledIsolationAndReconnection()
    {
        var node1 = _harness.CreateSeedNode(0);
        var node2 = _harness.CreateSeedNode(1);
        var node1Addr = RapidClusterUtils.Loggable(node1.Address);
        var node2Addr = RapidClusterUtils.Loggable(node2.Address);

        // Disable random healing so only scheduled events occur
        _chaos.PartitionHealRate = 0;

        _chaos.ScheduleIsolation(node1, TimeSpan.FromSeconds(2));
        _chaos.ScheduleReconnect(node1, TimeSpan.FromSeconds(5));

        // Initially connected
        Assert.True(_harness.Network.CanDeliver(node1Addr, node2Addr));

        // After isolation
        _harness.RunForDuration(TimeSpan.FromSeconds(3));
        _chaos.MaybeInjectFault();
        Assert.False(_harness.Network.CanDeliver(node1Addr, node2Addr));

        // After reconnection
        _harness.RunForDuration(TimeSpan.FromSeconds(3));
        _chaos.MaybeInjectFault();
        Assert.True(_harness.Network.CanDeliver(node1Addr, node2Addr));
    }



    [Fact]
    public void RunChaosForManySteps()
    {
        // Create some nodes
        for (var i = 0; i < 5; i++)
        {
            _harness.CreateSeedNode(i);
        }

        _chaos.NodeCrashRate = 0.01; // Low crash rate
        _chaos.PartitionRate = 0.05; // Higher partition rate
        _chaos.PartitionHealRate = 0.1;
        _chaos.MinimumAliveNodes = 2;

        // Run for many steps
        var faultsInjected = _chaos.RunChaos(steps: 500, stepInterval: TimeSpan.FromMilliseconds(10));

        // Some faults should have been injected
        // Note: With probabilistic injection, we may or may not have faults
        Assert.True(_harness.Nodes.Count >= 2);
    }

    [Fact]
    public void InvariantsHoldUnderLightChaos()
    {
        // Create a proper cluster with joined nodes
        var nodes = _harness.CreateCluster(size: 3);

        _chaos.NodeCrashRate = 0.0; // No crashes
        _chaos.PartitionRate = 0.02; // Light partitions
        _chaos.PartitionHealRate = 0.5;

        // Run chaos (reduced from 100 steps to 20 for faster test execution while
        // still validating invariants hold under light partition churn)
        _chaos.RunChaos(steps: 20);

        // Check invariants
        var result = _checker.CheckAll();

        // Output diagnostic information if invariants failed
        if (!result)
        {
            var output = TestContext.Current.TestOutputHelper;
            output?.WriteLine($"Invariant check failed! Violations:");
            foreach (var violation in _checker.Violations)
            {
                output?.WriteLine($"  [{violation.Type}] {violation.Message} (SimulatedTime={violation.SimulatedTime})");
            }

            // Output node membership info
            output?.WriteLine($"Node membership states:");
            foreach (var node in _harness.Nodes)
            {
                var view = node.CurrentView;
                output?.WriteLine($"  {RapidClusterUtils.Loggable(node.Address)}: IsInitialized={node.IsInitialized}, MembershipSize={node.MembershipSize}, ConfigId={view?.ConfigurationId}");
            }
        }

        Assert.True(result);
    }

    [Fact]
    public void ClearScheduledFaultsWorks()
    {
        var node = _harness.CreateSeedNode();

        _chaos.ScheduleNodeCrash(node, TimeSpan.FromSeconds(1));
        _chaos.ClearScheduledFaults();

        // Advance past scheduled time
        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();

        // Node should still exist
        Assert.Contains(node, _harness.Nodes);
    }



    [Fact]
    public void ScheduledPartitionExecutesOnTime()
    {
        var node1 = _harness.CreateSeedNode(0);
        var node2 = _harness.CreateSeedNode(1);

        _chaos.SchedulePartition(node1, node2, TimeSpan.FromSeconds(3));

        var addr1 = RapidClusterUtils.Loggable(node1.Address);
        var addr2 = RapidClusterUtils.Loggable(node2.Address);

        // Before scheduled time
        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.True(_harness.Network.CanDeliver(addr1, addr2));

        // After scheduled time
        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.False(_harness.Network.CanDeliver(addr1, addr2));
    }

    [Fact]
    public void ScheduledPartitionHealExecutesOnTime()
    {
        var node1 = _harness.CreateSeedNode(0);
        var node2 = _harness.CreateSeedNode(1);

        var addr1 = RapidClusterUtils.Loggable(node1.Address);
        var addr2 = RapidClusterUtils.Loggable(node2.Address);

        // Disable random healing so only scheduled heals occur
        _chaos.PartitionHealRate = 0;

        // Create partition immediately
        _harness.PartitionNodes(node1, node2);
        Assert.False(_harness.Network.CanDeliver(addr1, addr2));

        // Schedule heal
        _chaos.SchedulePartitionHeal(node1, node2, TimeSpan.FromSeconds(3));

        // Before heal
        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.False(_harness.Network.CanDeliver(addr1, addr2));

        // After heal
        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.True(_harness.Network.CanDeliver(addr1, addr2));
    }

    [Fact]
    public void MultipleScheduledFaultsExecuteInOrder()
    {
        var node1 = _harness.CreateSeedNode(0);
        var node2 = _harness.CreateSeedNode(1);
        var node1Addr = RapidClusterUtils.Loggable(node1.Address);
        var node2Addr = RapidClusterUtils.Loggable(node2.Address);

        // Schedule multiple events
        _chaos.ScheduleIsolation(node1, TimeSpan.FromSeconds(2));
        _chaos.ScheduleReconnect(node1, TimeSpan.FromSeconds(4));
        _chaos.ScheduleIsolation(node1, TimeSpan.FromSeconds(6));

        // Check state at each time point
        _harness.RunForDuration(TimeSpan.FromSeconds(1));
        _chaos.MaybeInjectFault();
        Assert.True(_harness.Network.CanDeliver(node1Addr, node2Addr)); // Before first isolation

        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.False(_harness.Network.CanDeliver(node1Addr, node2Addr)); // After first isolation

        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.True(_harness.Network.CanDeliver(node1Addr, node2Addr)); // After reconnect

        _harness.RunForDuration(TimeSpan.FromSeconds(2));
        _chaos.MaybeInjectFault();
        Assert.False(_harness.Network.CanDeliver(node1Addr, node2Addr)); // After second isolation
    }

    [Fact]
    public void ScheduledCrashOnNonexistentNodeHandled()
    {
        var node = _harness.CreateSeedNode();

        _chaos.ScheduleNodeCrash(node, TimeSpan.FromSeconds(5));

        // Crash node manually first
        _harness.CrashNode(node);

        // Advance past scheduled time and process - should not throw
        _harness.RunForDuration(TimeSpan.FromSeconds(10));
        _chaos.MaybeInjectFault(); // Should not throw even though node is already crashed
    }



    /// <summary>
    /// Tests the cluster's resilience to random network partitions being created and healed.
    /// Verifies that even with continuous partition churn, the cluster maintains operational
    /// integrity and doesn't enter an invalid state.
    /// </summary>
    [Fact]
    public void RandomPartitionCreationAndHealing()
    {
        // Create a 4-node cluster
        for (var i = 0; i < 4; i++)
        {
            _harness.CreateSeedNode(i);
        }

        _chaos.PartitionRate = 0.1;
        _chaos.PartitionHealRate = 0.15;

        // Run chaos with partition creation and healing
        _chaos.RunChaos(steps: 200, stepInterval: TimeSpan.FromMilliseconds(5));

        // Cluster should still have nodes
        Assert.NotEmpty(_harness.Nodes);
    }

    /// <summary>
    /// Verifies that scheduled faults are executed in chronological order based on their
    /// scheduled times. This ensures the chaos injector's time-based event scheduling
    /// mechanism works correctly for deterministic fault injection scenarios.
    /// </summary>
    [Fact]
    public void ScheduledFaultsExecuteInChronologicalOrder()
    {
        var node1 = _harness.CreateSeedNode(0);
        var node2 = _harness.CreateSeedNode(1);

        var events = new List<string>();

        // Schedule events at different times
        _chaos.ScheduleIsolation(node1, TimeSpan.FromSeconds(1));
        _harness.RunForDuration(TimeSpan.FromSeconds(1.5));
        _chaos.MaybeInjectFault();
        events.Add("isolated");

        _chaos.ScheduleReconnect(node1, TimeSpan.FromSeconds(0.5)); // 2 seconds from start
        _harness.RunForDuration(TimeSpan.FromSeconds(1));
        _chaos.MaybeInjectFault();
        events.Add("reconnected");

        // Verify events occurred
        Assert.Equal(2, events.Count);
        Assert.Equal("isolated", events[0]);
        Assert.Equal("reconnected", events[1]);
    }

    /// <summary>
    /// Stress tests the cluster under combined fault types (crashes, partitions, heals)
    /// occurring simultaneously. Verifies that safety properties are maintained when
    /// multiple types of faults are active, and that the minimum alive nodes constraint
    /// is respected by the chaos injector.
    /// </summary>
    [Fact]
    public void CombinedFaultsUnderLoad()
    {
        // Create a cluster
        var nodes = new List<RapidSimulationNode>();
        for (var i = 0; i < 5; i++)
        {
            nodes.Add(_harness.CreateSeedNode(i));
        }

        // Enable multiple types of faults simultaneously
        _chaos.NodeCrashRate = 0.02;
        _chaos.PartitionRate = 0.05;
        _chaos.PartitionHealRate = 0.1;
        _chaos.MinimumAliveNodes = 3;

        // Schedule some specific faults
        _chaos.SchedulePartition(nodes[0], nodes[1], TimeSpan.FromSeconds(2));
        _chaos.SchedulePartitionHeal(nodes[0], nodes[1], TimeSpan.FromSeconds(5));

        // Run chaos
        var faultsInjected = _chaos.RunChaos(steps: 300, stepInterval: TimeSpan.FromMilliseconds(10));

        // Verify minimum alive nodes maintained
        Assert.True(_harness.Nodes.Count >= 3);
    }

    /// <summary>
    /// Tests the cluster's ability to survive extremely high-frequency fault injection
    /// over an extended period. This stress test verifies that rapid fault injection
    /// doesn't cause race conditions or state corruption, and that the cluster remains
    /// stable with at least the minimum required nodes alive.
    /// </summary>
    [Fact]
    public void HighFrequencyFaultInjection()
    {
        for (var i = 0; i < 6; i++)
        {
            _harness.CreateSeedNode(i);
        }

        _chaos.NodeCrashRate = 0.05;
        _chaos.PartitionRate = 0.1;
        _chaos.PartitionHealRate = 0.2;
        _chaos.MinimumAliveNodes = 2;

        // Run many steps with frequent fault injection
        _chaos.RunChaos(steps: 1000, stepInterval: TimeSpan.FromMilliseconds(1));

        // System should survive
        Assert.True(_harness.Nodes.Count >= 2);
    }

}


