using System.Diagnostics.CodeAnalysis;
using Clockwork;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure;

namespace RapidCluster.Simulation.Tests;

/// <summary>
/// Tests for graceful leave scenarios using the simulation harness.
/// Graceful leave involves a node announcing its departure and waiting for consensus
/// before shutting down, ensuring the cluster smoothly transitions to the new membership.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class GracefulLeaveTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private SynchronizationContextScope _syncContextScope;
    private const int BaseSeed = 67890;
    private readonly List<ObservableCollector<MembershipView>> _collectors = [];

    // Track instance reuse - each test should get a fresh instance
    private readonly Guid _instanceId = Guid.NewGuid();
    private int _initializeCount;
    private string? _firstTestName;

    public ValueTask InitializeAsync()
    {
        var currentTestName = TestContext.Current.Test?.TestDisplayName ?? "unknown";
        var initCount = Interlocked.Increment(ref _initializeCount);

        if (initCount > 1)
        {
            throw new InvalidOperationException(
                $"INSTANCE REUSE DETECTED! Instance {_instanceId} was initialized {initCount} times. " +
                $"First test: '{_firstTestName}', Current test: '{currentTestName}'. " +
                $"xUnit should create a new instance for each test.");
        }

        _firstTestName = currentTestName;

        // Derive a unique seed from the test name to ensure test isolation
        // This makes tests deterministic (same seed for same test) but independent of execution order
        // Use GuidUtility.GetDeterministicHashCode instead of string.GetHashCode() because
        // .NET's string hash is randomized per-process for security
        var testSeed = BaseSeed ^ GuidUtility.GetDeterministicHashCode(currentTestName);

        _harness = new RapidSimulationCluster(seed: testSeed);

        // Install the simulation's synchronization context to capture async continuations
        // This ensures all async operations are routed through the simulation task queue
        _syncContextScope = _harness.SynchronizationContext.Install();

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

        // Restore the previous synchronization context AFTER harness disposal
        // to ensure any async cleanup operations are captured by the simulation
        _syncContextScope.Dispose();
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
    /// Counts view changes in the collector.
    /// </summary>
    private static int CountViewChanges(ObservableCollector<MembershipView> collector) => collector.Items.Count;

    /// <summary>
    /// Collects membership sizes from view changes in the collector.
    /// </summary>
    private static List<int> CollectViewChangeMembershipSizes(ObservableCollector<MembershipView> collector)
        => [.. collector.Items.Select(v => v.Size)];

    [Fact]
    public void GracefulLeave_SingleNode_RemainingNodesConverge()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence();

        var leavingNode = nodes[2];
        var remainingNodes = new[] { nodes[0], nodes[1] };

        // Act: Graceful leave
        _harness.RemoveNodeGracefully(leavingNode);

        // Assert: Remaining nodes converge to size 2
        Assert.All(remainingNodes, n => Assert.Equal(2, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_LeavingNodeRemovedFromHarness()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence();

        var leavingNode = nodes[2];

        // Act: Graceful leave
        _harness.RemoveNodeGracefully(leavingNode);

        // Assert: Leaving node is removed from harness
        Assert.DoesNotContain(leavingNode, _harness.Nodes);
        Assert.Equal(2, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_SeedNode_ClusterContinues()
    {
        // Arrange: Create a 4-node cluster (need 4 for quorum after seed leaves)
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var seedNode = nodes[0];
        var remainingNodes = nodes.Skip(1).ToList();

        // Act: Seed node gracefully leaves
        _harness.RemoveNodeGracefully(seedNode);

        // Assert: Remaining nodes converge
        _harness.WaitForConvergence();
        Assert.All(remainingNodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_MiddleNode_ClusterContinues()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var middleNode = nodes[2];
        var remainingNodes = nodes.Where(n => n != middleNode).ToList();

        // Act: Middle node gracefully leaves
        _harness.RemoveNodeGracefully(middleNode);

        // Assert: Remaining nodes converge
        _harness.WaitForConvergence();
        Assert.All(remainingNodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_LastJoiner_ClusterContinues()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var lastNode = nodes[3];
        var remainingNodes = nodes.Take(3).ToList();

        // Act: Last joiner gracefully leaves
        _harness.RemoveNodeGracefully(lastNode);

        // Assert: Remaining nodes converge
        _harness.WaitForConvergence();
        Assert.All(remainingNodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_TwoNodesSequentially()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Act: Two nodes leave sequentially
        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: Three nodes remain
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_ThreeNodesSequentially()
    {
        // Arrange: Create a 6-node cluster
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence();

        // Act: Three nodes leave sequentially
        _harness.RemoveNodeGracefully(nodes[5]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: Three nodes remain
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_AlternatingNodes()
    {
        // Arrange: Create a 6-node cluster
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence();

        // Act: Remove alternating nodes (1, 3, 5 - keeping 0, 2, 4)
        _harness.RemoveNodeGracefully(nodes[1]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[5]);
        _harness.WaitForConvergence();

        // Assert: Correct nodes remain
        Assert.Contains(nodes[0], _harness.Nodes);
        Assert.Contains(nodes[2], _harness.Nodes);
        Assert.Contains(nodes[4], _harness.Nodes);
        Assert.Equal(3, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_AllButOneNode()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var keepNode = nodes[0];

        // Act: All nodes except one leave
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.RemoveNodeGracefully(nodes[2]);
        _harness.RemoveNodeGracefully(nodes[1]);

        // Assert: One node remains (it's still "in the cluster" from its own perspective)
        Assert.Single(_harness.Nodes);
        Assert.Contains(keepNode, _harness.Nodes);
    }

    [Fact]
    public void GracefulLeave_SeedLeavesFirst_ThenOthers()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Act: Seed leaves first, then others
        _harness.RemoveNodeGracefully(nodes[0]); // seed
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();

        // Assert: Three nodes remain (1, 2, 3)
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.Contains(nodes[1], _harness.Nodes);
        Assert.Contains(nodes[2], _harness.Nodes);
        Assert.Contains(nodes[3], _harness.Nodes);
    }

    [Fact]
    public void GracefulLeave_TriggersSubscriptionCallback()
    {
        // Arrange: Create a 3-node cluster and set up view collector before any actions
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence();

        var collector = CreateViewCollector(nodes[0]);

        // Act: One node gracefully leaves
        _harness.RemoveNodeGracefully(nodes[2]);
        _harness.WaitForConvergence();

        // Assert: View changes were emitted
        var viewChangeCount = CountViewChanges(collector);
        Assert.True(viewChangeCount >= 1, "Membership changed callback should be invoked");
    }

    [Fact]
    public void GracefulLeave_AllRemainingNodesReceiveNotification()
    {
        // Arrange: Create a 4-node cluster and set up view collectors before any actions
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var collectors = new[]
        {
            CreateViewCollector(nodes[0]),
            CreateViewCollector(nodes[1]),
            CreateViewCollector(nodes[2])
        };

        // Act: Node 3 gracefully leaves
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: All remaining nodes received notification
        var viewChangeCounts = collectors.Select(CountViewChanges).ToArray();
        Assert.All(viewChangeCounts, count => Assert.True(count >= 1));
    }

    [Fact]
    public void GracefulLeave_CallbackIncludesCorrectMembershipSize()
    {
        // Arrange: Create a 4-node cluster and set up view collector before any actions
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var collector = CreateViewCollector(nodes[0]);

        // Act: Two nodes leave
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[2]);
        _harness.WaitForConvergence();

        // Assert: Observed sizes should include 3 and 2
        var observedSizes = CollectViewChangeMembershipSizes(collector);
        Assert.Contains(3, observedSizes);
        Assert.Contains(2, observedSizes);
    }

    [Fact]
    public void GracefulLeave_ConfigurationIdChanges()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var initialConfigId = nodes[0].CurrentView.ConfigurationId;

        // Act: Node gracefully leaves
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: Configuration ID changed and version increased
        var newConfigId = nodes[0].CurrentView.ConfigurationId;
        Assert.NotEqual(initialConfigId, newConfigId);
        Assert.True(newConfigId.Version > initialConfigId.Version,
            $"Version should increase after leave. Initial: {initialConfigId.Version}, Current: {newConfigId.Version}");
    }

    [Fact]
    public void GracefulLeave_MultipleLeaves_ConfigurationIdIncrementsEachTime()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        var configIds = new List<ConfigurationId> { nodes[0].CurrentView.ConfigurationId };

        // Act: Multiple nodes leave
        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();
        configIds.Add(nodes[0].CurrentView.ConfigurationId);

        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();
        configIds.Add(nodes[0].CurrentView.ConfigurationId);

        // Assert: Configuration IDs have strictly increasing versions
        for (var i = 1; i < configIds.Count; i++)
        {
            Assert.True(configIds[i].Version > configIds[i - 1].Version,
                $"Version at index {i} ({configIds[i].Version}) should be greater than at {i - 1} ({configIds[i - 1].Version})");
        }
    }

    [Fact]
    public void GracefulLeave_WithMessageDrops()
    {
        // Arrange: Create a 4-node cluster with message drops
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        // Enable some message drops
        _harness.Network.MessageDropRate = 0.1; // 10% drop rate

        // Act: Graceful leave with drops
        _harness.RemoveNodeGracefully(nodes[3]);

        // Wait for convergence with higher iteration count due to retries
        _harness.WaitForConvergence();

        // Clean up
        _harness.Network.MessageDropRate = 0;

        // Assert: Cluster converged despite drops
        Assert.Equal(3, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_WithNetworkDelay()
    {
        // Arrange: Create a 4-node cluster with network delays
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        // Enable delays
        _harness.Network.EnableDelays = true;
        _harness.Network.BaseMessageDelay = TimeSpan.FromMilliseconds(10);
        _harness.Network.MaxJitter = TimeSpan.FromMilliseconds(20);

        // Act: Graceful leave with delays
        _harness.RemoveNodeGracefully(nodes[3]);

        // Wait for convergence
        _harness.WaitForConvergence();

        // Clean up
        _harness.Network.EnableDelays = false;

        // Assert: Cluster converged
        Assert.Equal(3, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_PartitionDuringLeave_EventuallyConverges()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        var leavingNode = nodes[4];

        // Create a partition affecting the leaving node
        _harness.PartitionNodes(leavingNode, nodes[1]);

        // Act: Attempt graceful leave
        _harness.RemoveNodeGracefully(leavingNode);

        // Heal partition
        _harness.HealPartition(nodes[4], nodes[1]);

        // Wait for convergence
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Assert: Cluster should eventually converge (leaving node removed)
        Assert.DoesNotContain(leavingNode, _harness.Nodes);
    }

    [Fact]
    public void GracefulLeave_FollowedByJoin()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        // Act: One leaves, then new one joins
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        var newNode = _harness.CreateJoinerNode(nodes[0], nodeId: 10);
        _harness.WaitForConvergence();

        // Assert: New configuration is correct
        Assert.Equal(4, _harness.Nodes.Count);
        Assert.Contains(newNode, _harness.Nodes);
        Assert.DoesNotContain(nodes[3], _harness.Nodes);
    }

    [Fact]
    public void GracefulLeave_PrecededByJoin()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence();

        // Act: Join then leave
        var newNode = _harness.CreateJoinerNode(nodes[0], nodeId: 10);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[2]);
        _harness.WaitForConvergence();

        // Assert: Correct membership
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.Contains(newNode, _harness.Nodes);
        Assert.DoesNotContain(nodes[2], _harness.Nodes);
    }

    [Fact]
    public void GracefulLeave_MultipleLeavesAndJoins()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Act: Complex sequence of leaves and joins
        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();

        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: 10);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        var newNode2 = _harness.CreateJoinerNode(nodes[1], nodeId: 11);
        _harness.WaitForConvergence();

        // Assert: Final state correct
        Assert.Equal(5, _harness.Nodes.Count);
        Assert.Contains(newNode1, _harness.Nodes);
        Assert.Contains(newNode2, _harness.Nodes);
        Assert.DoesNotContain(nodes[3], _harness.Nodes);
        Assert.DoesNotContain(nodes[4], _harness.Nodes);
    }

    [Fact]
    public void GracefulLeave_AfterCrash()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Crash one node
        _harness.CrashNode(nodes[4]);
        _harness.WaitForConvergence();

        // Act: Graceful leave after crash
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: Cluster converged correctly
        Assert.Equal(3, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_ThenCrash()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Act: Graceful leave then crash
        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();

        _harness.CrashNode(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: Cluster converged correctly
        Assert.Equal(3, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_MinimumClusterSize()
    {
        // Arrange: Create minimum viable cluster (3 nodes for consensus)
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence();

        // Act: Reduce to 2 nodes (still functional but at limit)
        _harness.RemoveNodeGracefully(nodes[2]);
        _harness.WaitForConvergence();

        // Assert: Cluster still functional with 2 nodes
        Assert.Equal(2, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_LeavingNodeMetadataRemoved()
    {
        // Arrange: Create a cluster with metadata
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        var leavingNode = nodes[3];
        var leavingAddress = leavingNode.Address;

        // Act: Graceful leave
        _harness.RemoveNodeGracefully(leavingNode);
        _harness.WaitForConvergence();

        // Assert: Leaving node's address no longer in membership
        var remainingAddresses = nodes[0].CurrentView.Members;
        Assert.DoesNotContain(leavingAddress, remainingAddresses);
    }

    [Fact]
    public void GracefulLeave_RapidSuccessiveLeaves()
    {
        // Arrange: Create a 6-node cluster
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence();

        // Act: Rapid successive leaves with minimal delay between
        _harness.RemoveNodeGracefully(nodes[5]);
        // Don't wait for full convergence - immediately start next leave
        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.RemoveNodeGracefully(nodes[3]);

        // Now wait for convergence
        _harness.WaitForConvergence();

        // Assert: Final state correct
        Assert.Equal(3, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_VerifyMembershipConsistency()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        // Act: Graceful leave
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: All remaining nodes have identical membership views
        var membershipSets = _harness.Nodes
            .Select(n => new HashSet<Endpoint>(n.CurrentView.Members))
            .ToList();

        for (var i = 1; i < membershipSets.Count; i++)
        {
            Assert.True(membershipSets[0].SetEquals(membershipSets[i]),
                "All nodes should have identical membership view");
        }
    }

    [Fact]
    public void GracefulLeave_VerifyConfigurationConsistency()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        // Act: Graceful leave
        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: All remaining nodes have same configuration ID
        var configIds = _harness.Nodes.Select(n => n.CurrentView.ConfigurationId).Distinct().ToList();
        Assert.Single(configIds);
    }

    [Fact]
    public void GracefulLeave_ObservedNodeLeaves_ObserverUpdated()
    {
        // Arrange: Create a 4-node cluster where monitoring relationships exist
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence();

        // In the K=3 model, each node is observed by K other nodes
        // When a node leaves, its observers should update their monitoring

        var leavingNode = nodes[2];

        // Act: Node that's being observed leaves
        _harness.RemoveNodeGracefully(leavingNode);
        _harness.WaitForConvergence();

        // Assert: Cluster restructures and remains healthy
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_ObserverNodeLeaves_SubjectStillMonitored()
    {
        // Arrange: Create a 5-node cluster for more complex monitoring topology
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Remove a node - other nodes will take over monitoring duties
        _harness.RemoveNodeGracefully(nodes[1]);
        _harness.WaitForConvergence();

        // Advance time to trigger failure detection cycles
        _harness.RunForDuration(TimeSpan.FromSeconds(10));
        _harness.RunUntilIdle();

        // Assert: Remaining nodes are still monitored (cluster stable)
        Assert.Equal(4, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(4, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_MultipleObserversLeave()
    {
        // Arrange: Create a 6-node cluster
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence();

        // Act: Multiple nodes leave (some may be observers of remaining nodes)
        _harness.RemoveNodeGracefully(nodes[5]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[4]);
        _harness.WaitForConvergence();

        _harness.RemoveNodeGracefully(nodes[3]);
        _harness.WaitForConvergence();

        // Assert: Remaining nodes are healthy
        Assert.Equal(3, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(3, n.MembershipSize));
    }

    [Fact]
    public void GracefulLeave_WithSuspendedNode()
    {
        // Arrange: Create a 5-node cluster with long failure detector interval
        // This prevents the suspended node from being detected as failed during the test
        var options = new RapidClusterProtocolOptions
        {
            FailureDetectorInterval = TimeSpan.FromMinutes(10)
        };
        var nodes = _harness.CreateCluster(size: 5, options);
        _harness.WaitForConvergence();

        // Suspend one node (not the one leaving)
        nodes[3].Suspend();

        // Act: Different node leaves gracefully
        _harness.RemoveNodeGracefully(nodes[4]);

        // Resume suspended node
        nodes[3].Resume();

        // Wait for convergence
        _harness.WaitForConvergence();

        // Assert: Cluster converged to 4 nodes (only the leaving node removed)
        Assert.Equal(4, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_SuspendedNodeDetectedAsFailed()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        var suspendedNode = nodes[2];

        // A suspended node is effectively down - it won't respond to failure detector probes
        suspendedNode.Suspend();

        // Act: Another node leaves gracefully
        _harness.RemoveNodeGracefully(nodes[4]);

        // Resume suspended node - it will detect it was kicked and automatically rejoin
        suspendedNode.Resume();

        // Wait for convergence - expect size 4 because:
        // - Node 4 left gracefully
        // - Suspended node was detected as failed and kicked, but auto-rejoins after resume
        _harness.WaitForConvergence();

        // Assert: Suspended node should have rejoined after being kicked
        Assert.Contains(suspendedNode, _harness.Nodes);
        Assert.Equal(4, _harness.Nodes.Count);
    }

    [Fact]
    public void GracefulLeave_SuspendedNodeRemovedWithLeavingNode()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence();

        // Suspend a node - this makes it effectively down (won't respond to probes)
        var suspendedNode = nodes[1];
        suspendedNode.Suspend();

        // Act: Another node leaves while one is suspended
        _harness.RemoveNodeGracefully(nodes[4]);

        // Resume the node - it will detect it was kicked and automatically rejoin
        suspendedNode.Resume();

        // Wait for convergence - expect size 4 because:
        // - Node 4 left gracefully
        // - Suspended node was detected as failed and kicked, but auto-rejoins after resume
        _harness.WaitForConvergence();

        // Assert: Suspended node should have rejoined after being kicked
        Assert.Equal(4, _harness.Nodes.Count);
        Assert.Contains(suspendedNode, _harness.Nodes);
    }

}
