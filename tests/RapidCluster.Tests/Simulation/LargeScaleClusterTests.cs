using System.Diagnostics.CodeAnalysis;
using System.Text;
using RapidCluster.Tests.Simulation.Infrastructure;
using Xunit.v3;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for large-scale cluster operations using the simulation harness.
/// Covers cluster formation, sequential joins, and parallel joins at scale (10-50 nodes).
/// These tests verify that the consensus protocol and membership management can handle larger cluster sizes.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class LargeScaleClusterTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 56789;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _harness.DisposeAsync();
    }

    /// <summary>
    /// Tests formation of a cluster using sequential joins.
    /// Each node joins and waits for consensus before the next node joins.
    /// This is slower but guarantees deterministic, one-at-a-time behavior.
    /// </summary>
    [Theory]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    public void LargeClusterFormation_Sequential(int clusterSize)
    {
        var nodes = _harness.CreateCluster(size: clusterSize);

        _harness.WaitForConvergence(expectedSize: clusterSize);

        Assert.Equal(clusterSize, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(clusterSize, n.MembershipSize));
    }

    /// <summary>
    /// Tests formation of a cluster using 10 parallel joins.
    /// </summary>
    [Fact]
    public void LargeClusterFormation_Parallel_10() => LargeClusterFormation_Parallel(clusterSize: 10);

    /// <summary>
    /// Tests formation of a cluster using parallel joins.
    /// Multiple nodes join concurrently, allowing the multi-node cut detection
    /// to batch them into fewer consensus rounds (O(log N) instead of O(N)).
    /// This matches the Rapid paper's approach where 2000 nodes bootstrapped
    /// with only 8 configuration changes.
    /// </summary>
    [Theory]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    [InlineData(50)]
    [InlineData(80)]
    [InlineData(100)]
    [InlineData(200)]
    [InlineData(300)]
    [InlineData(400)]
    [InlineData(500)]
    public void LargeClusterFormation_Parallel(int clusterSize)
    {
        var nodes = _harness.CreateClusterParallel(size: clusterSize);

        _harness.WaitForConvergence(expectedSize: clusterSize);

        Assert.Equal(clusterSize, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(clusterSize, n.MembershipSize));
    }

    /// <summary>
    /// Tests formation of very large clusters (200-500 nodes) using parallel joins.
    /// This validates the O(log N) scaling described in the Rapid paper.
    /// 
    /// The paper achieved 2000 nodes with only 8 configuration changes, implying
    /// ~250 nodes per view change. For our test sizes:
    /// - 200 nodes: expect ~10-15 changes (at least 13-20 nodes per change)
    /// - 500 nodes: expect ~12-20 changes (at least 25-40 nodes per change)
    /// </summary>
    [Theory]
    [InlineData(200, 30)]  // 200 nodes should have at most 30 config changes
    [InlineData(500, 50)]  // 500 nodes should have at most 50 config changes
    public void VeryLargeClusterFormation_Parallel(int clusterSize, int maxExpectedChanges)
    {
        var nodes = _harness.CreateClusterParallel(size: clusterSize);

        // Use higher max iterations for very large clusters
        _harness.WaitForConvergence(expectedSize: clusterSize, maxIterations: 5000000);

        Assert.Equal(clusterSize, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(clusterSize, n.MembershipSize));

        // Verify batching occurred - should have significantly fewer config changes than nodes
        var finalConfigId = nodes[0].CurrentView.ConfigurationId;
        var configChanges = finalConfigId.Version - 1;
        var nodesJoined = clusterSize - 1;
        var avgNodesPerChange = (double)nodesJoined / configChanges;

        Assert.True(configChanges <= maxExpectedChanges,
            $"Expected at most {maxExpectedChanges} configuration changes for {clusterSize} nodes, " +
            $"but got {configChanges}. Average nodes per change: {avgNodesPerChange:F1}. " +
            $"Batching is not working correctly.");
    }

    /// <summary>
    /// Tests formation of a cluster using batched parallel joins.
    /// Nodes join in batches of the specified size, providing a middle ground
    /// between fully sequential and fully parallel joining.
    /// 
    /// NOTE: This test is currently skipped because batched parallel joins have a fundamental
    /// timing issue. When batch N completes their join tasks, the cluster configuration may
    /// still be advancing (processing alerts, consensus rounds). When batch N+1 starts joining,
    /// they receive "Configuration changed during join" errors because the config version they
    /// joined with is already stale. This causes a cascade of retries that eventually times out.
    /// 
    /// The fully parallel approach (batchSize=0) works correctly because all nodes join together
    /// and the consensus protocol batches them naturally. For large-scale cluster testing, use
    /// LargeClusterFormation_Parallel or VeryLargeClusterFormation_Parallel instead.
    /// </summary>
    [Theory(Skip = "Batched parallel joins have timing issues - use fully parallel (batchSize=0) instead")]
    [InlineData(50, 10)]  // 50 nodes in batches of 10
    [InlineData(80, 20)]  // 80 nodes in batches of 20
    [InlineData(100, 25)] // 100 nodes in batches of 25
    [InlineData(200, 25)] // 200 nodes in batches of 25
    public void LargeClusterFormation_BatchedParallel(int clusterSize, int batchSize)
    {
        // Use higher max iterations for larger clusters (100+ nodes need more time per batch)
        var maxIterationsPerBatch = clusterSize >= 100 ? 500000 : 100000;
        var nodes = _harness.CreateClusterParallel(size: clusterSize, batchSize: batchSize, maxIterationsPerBatch: maxIterationsPerBatch);

        // Use higher max iterations for larger clusters (200+ nodes need more time)
        var maxIterations = clusterSize >= 200 ? 5000000 : 100000;
        _harness.WaitForConvergence(expectedSize: clusterSize, maxIterations: maxIterations);

        Assert.Equal(clusterSize, nodes.Count);
        Assert.All(nodes, n => Assert.True(n.IsInitialized));
        Assert.All(nodes, n => Assert.Equal(clusterSize, n.MembershipSize));
    }

    /// <summary>
    /// Tests that parallel joins result in batched view changes.
    /// This validates the O(log N) scaling from the Rapid paper where 2000 nodes
    /// were bootstrapped with only 8 configuration changes.
    /// 
    /// For batching to work correctly:
    /// - Multiple alerts should be batched into single BatchedAlertMessage broadcasts
    /// - Multiple nodes should be added in a single view change (configuration change)
    /// - The total number of configuration changes should be significantly less than N-1
    /// 
    /// Expected behavior per the paper:
    /// - 2000 nodes bootstrapped with 8 configuration changes
    /// - This implies ~250 nodes per view change on average
    /// - For 50 nodes, we should see ~6-10 changes (allowing for smaller batches)
    /// - For 80 nodes, we should see ~8-12 changes
    /// </summary>
    [Theory]
    [InlineData(20, 10)]   // 20 nodes should have at most 10 config changes (at least 2 nodes per change on average)
    [InlineData(50, 15)]   // 50 nodes should have at most 15 config changes (at least 3-4 nodes per change on average)
    [InlineData(80, 20)]   // 80 nodes should have at most 20 config changes (at least 4 nodes per change on average)
    public void ParallelJoins_BatchMultipleNodesPerViewChange(int clusterSize, int maxExpectedChanges)
    {
        var nodes = _harness.CreateClusterParallel(size: clusterSize);
        _harness.WaitForConvergence(expectedSize: clusterSize);

        // Get the final configuration ID which represents the number of view changes
        var finalConfigId = nodes[0].CurrentView.ConfigurationId;

        // The configuration version starts at 1 for the seed node, so the number of
        // configuration changes (view changes) is version - 1
        var configChanges = finalConfigId.Version - 1;
        var nodesJoined = clusterSize - 1; // Excluding seed node
        var avgNodesPerChange = (double)nodesJoined / configChanges;

        // Log membership transitions summary
        LogBatchingSummary("ParallelJoins", clusterSize, configChanges, nodesJoined, avgNodesPerChange);

        Assert.True(configChanges <= maxExpectedChanges,
            $"Expected at most {maxExpectedChanges} configuration changes for {clusterSize} nodes, " +
            $"but got {configChanges}. Average nodes per change: {avgNodesPerChange:F1}. " +
            $"Batching is not working correctly - each view change should include multiple nodes.");
    }

    /// <summary>
    /// Tests parallel joins with detailed view transition tracking via MembershipViewAccessor.
    /// This test subscribes to the seed node's view changes to capture exactly what members
    /// are added in each configuration change, providing visibility into batching behavior.
    /// </summary>
    [Theory]
    [InlineData(20)]
    [InlineData(50)]
    public void ParallelJoins_WithDetailedViewTracking(int clusterSize)
    {
        // Create seed node first
        var seedNode = _harness.CreateSeedNode();
        var viewHistory = new List<MembershipView> { seedNode.CurrentView };
        var cancellationToken = TestContext.Current.CancellationToken;

        // Subscribe to view changes using IObservable pattern instead of async enumerable.
        // This avoids Task.Run which introduces real concurrency outside the simulation.
        // The observer's OnNext will be called synchronously during view publication,
        // which happens during simulation step execution.
        using var subscription = seedNode.ViewAccessor.Updates.Subscribe(new ViewHistoryObserver(viewHistory, clusterSize));

        // Create nodes first (without starting joins)
        var nodes = new List<RapidSimulationNode>();
        for (var i = 1; i < clusterSize; i++)
        {
            nodes.Add(_harness.CreateUninitializedNode(i, seedNode));
        }

        // Drive simulation until all joins complete (larger clusters need more iterations).
        // IMPORTANT: Join tasks must be started inside DriveToCompletion so they
        // capture the simulation's SynchronizationContext for their continuations.
        var maxIterations = clusterSize >= 50 ? 500000 : 100000;
        _harness.Run(() =>
        {
            var joinTasks = nodes.Select(n => n.InitializeAsync(cancellationToken));
            return Task.WhenAll(joinTasks);
        }, maxIterations);
        _harness.WaitForConvergence(expectedSize: clusterSize, maxIterations: maxIterations);

        // Compute and log detailed view transitions
        var transitions = ComputeViewTransitions(viewHistory);
        LogViewTransitions("ParallelJoins", transitions);

        // Verify batching occurred
        var configChanges = transitions.Count;
        var nodesJoined = clusterSize - 1;
        var avgNodesPerChange = configChanges > 0 ? (double)nodesJoined / configChanges : 0;

        LogBatchingSummary("ParallelJoins (detailed)", clusterSize, configChanges, nodesJoined, avgNodesPerChange);

        // Should have fewer config changes than nodes (indicating batching)
        Assert.True(configChanges < nodesJoined,
            $"Expected batching (fewer config changes than nodes joined). " +
            $"Got {configChanges} changes for {nodesJoined} nodes joined.");
    }

    /// <summary>
    /// Observer that collects view changes into a history list.
    /// Used instead of async enumerable to avoid Task.Run and real concurrency.
    /// </summary>
    private sealed class ViewHistoryObserver(List<MembershipView> viewHistory, int targetSize) : IObserver<MembershipView>
    {
        public void OnNext(MembershipView value)
        {
            // Only add if we haven't reached the target size yet
            if (viewHistory.Count == 0 || viewHistory[^1].Size < targetSize)
            {
                viewHistory.Add(value);
            }
        }

        public void OnError(Exception error) { }

        public void OnCompleted() { }
    }

    /// <summary>
    /// Tests that all nodes in a large cluster have consistent configuration IDs.
    /// </summary>
    [Fact]
    public void LargeClusterHasConsistentConfigurationIds()
    {
        var nodes = _harness.CreateCluster(size: 10);

        _harness.WaitForConvergence(expectedSize: 10);

        var configIds = nodes.Select(n => n.CurrentView.ConfigurationId).Distinct().ToList();

        // All nodes should have the same configuration ID
        Assert.Single(configIds);
    }

    /// <summary>
    /// Tests that membership views in a large cluster contain all nodes.
    /// </summary>
    [Fact]
    public void LargeClusterMembershipViewContainsAllNodes()
    {
        var nodes = _harness.CreateCluster(size: 10);

        _harness.WaitForConvergence(expectedSize: 10);

        var referenceView = nodes[0].CurrentView;
        var referenceAddresses = referenceView.Members
            .Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}")
            .ToHashSet();

        // Verify all nodes are in the membership
        foreach (var node in nodes)
        {
            var nodeAddress = $"{node.Address.Hostname.ToStringUtf8()}:{node.Address.Port}";
            Assert.Contains(nodeAddress, referenceAddresses);
        }
    }

    /// <summary>
    /// Tests that 10 nodes can join sequentially.
    /// </summary>
    [Fact]
    public void TenNodesJoinSequentially()
    {
        var seedNode = _harness.CreateSeedNode();

        for (var i = 1; i <= 9; i++)
        {
            var joiner = _harness.CreateJoinerNode(seedNode, nodeId: i);
            Assert.True(joiner.IsInitialized);
            Assert.Equal(i + 1, joiner.MembershipSize);
        }

        _harness.WaitForConvergence(expectedSize: 10);

        Assert.All(_harness.Nodes, n => Assert.Equal(10, n.MembershipSize));
    }

    /// <summary>
    /// Tests that nodes joining sequentially all converge to the same view.
    /// </summary>
    [Fact]
    public void SequentialJoinsConvergeToSameView()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiners = new List<RapidSimulationNode> { seedNode };

        for (var i = 1; i <= 7; i++)
        {
            var joiner = _harness.CreateJoinerNode(seedNode, nodeId: i);
            joiners.Add(joiner);
        }

        _harness.WaitForConvergence(expectedSize: 8);

        // All nodes should have the same view
        var configIds = joiners.Select(n => n.CurrentView.ConfigurationId).Distinct().ToList();
        Assert.Single(configIds);
    }

    /// <summary>
    /// Tests that joins can use different existing nodes as seeds.
    /// </summary>
    [Fact]
    public void JoinsUsingDifferentSeeds()
    {
        var seedNode = _harness.CreateSeedNode();
        var joiner1 = _harness.CreateJoinerNode(seedNode, nodeId: 1);
        var joiner2 = _harness.CreateJoinerNode(seedNode, nodeId: 2);

        _harness.WaitForConvergence(expectedSize: 3);

        // Join using different existing members as "seeds"
        var joiner3 = _harness.CreateJoinerNode(joiner1, nodeId: 3);
        _harness.WaitForConvergence(expectedSize: 4);

        var joiner4 = _harness.CreateJoinerNode(joiner2, nodeId: 4);
        _harness.WaitForConvergence(expectedSize: 5);

        var joiner5 = _harness.CreateJoinerNode(joiner3, nodeId: 5);
        _harness.WaitForConvergence(expectedSize: 6);

        Assert.All(_harness.Nodes, n => Assert.Equal(6, n.MembershipSize));
    }

    /// <summary>
    /// Tests that 5 nodes can join an existing 5-node cluster.
    /// </summary>
    [Fact]
    public void FiveNodesJoinFiveNodeCluster()
    {
        // Create initial 5-node cluster
        var initialNodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        // Join 5 more nodes
        for (var i = 5; i < 10; i++)
        {
            var joiner = _harness.CreateJoinerNode(initialNodes[0], nodeId: i);
            Assert.True(joiner.IsInitialized);
        }

        _harness.WaitForConvergence(expectedSize: 10);

        Assert.Equal(10, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(10, n.MembershipSize));
    }

    /// <summary>
    /// Tests that many nodes can join through different entry points.
    /// </summary>
    [Fact]
    public void ManyNodesJoinThroughDifferentEntryPoints()
    {
        // Create initial 3-node cluster
        var initialNodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        // Join 6 more nodes through different entry points (round-robin)
        for (var i = 3; i < 9; i++)
        {
            var entryPoint = initialNodes[i % 3];
            var joiner = _harness.CreateJoinerNode(entryPoint, nodeId: i);
            Assert.True(joiner.IsInitialized);
        }

        _harness.WaitForConvergence(expectedSize: 9);

        Assert.All(_harness.Nodes, n => Assert.Equal(9, n.MembershipSize));
    }

    /// <summary>
    /// Tests that a large cluster can handle single node failure.
    /// </summary>
    [Fact]
    public void LargeClusterHandlesSingleNodeFailure()
    {
        var nodes = _harness.CreateCluster(size: 10);
        _harness.WaitForConvergence(expectedSize: 10);

        // Crash one node
        _harness.CrashNode(nodes[9]);

        // Wait for failure detection
        _harness.WaitForConvergence(expectedSize: 9, maxIterations: 500000);

        Assert.All(_harness.Nodes, n => Assert.Equal(9, n.MembershipSize));
    }

    /// <summary>
    /// Tests that a large cluster can handle multiple node failures.
    /// </summary>
    [Fact]
    public void LargeClusterHandlesMultipleNodeFailures()
    {
        var nodes = _harness.CreateCluster(size: 10);
        _harness.WaitForConvergence(expectedSize: 10);

        // Crash 3 nodes (30% failure, still have 7/10 majority)
        _harness.CrashNode(nodes[7]);
        _harness.CrashNode(nodes[8]);
        _harness.CrashNode(nodes[9]);

        // Wait for failure detection
        _harness.WaitForConvergence(expectedSize: 7, maxIterations: 500000);

        Assert.All(_harness.Nodes, n => Assert.Equal(7, n.MembershipSize));
    }

    /// <summary>
    /// Tests that a large cluster can recover after failures by adding new nodes.
    /// </summary>
    [Fact]
    public void LargeClusterRecoversAfterFailures()
    {
        var nodes = _harness.CreateCluster(size: 10);
        _harness.WaitForConvergence(expectedSize: 10);

        // Crash 3 nodes
        _harness.CrashNode(nodes[7]);
        _harness.CrashNode(nodes[8]);
        _harness.CrashNode(nodes[9]);

        // Wait for failure detection
        _harness.WaitForConvergence(expectedSize: 7, maxIterations: 500000);

        // Add 3 new nodes to recover
        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: 10);
        var newNode2 = _harness.CreateJoinerNode(nodes[0], nodeId: 11);
        var newNode3 = _harness.CreateJoinerNode(nodes[0], nodeId: 12);

        _harness.WaitForConvergence(expectedSize: 10);

        Assert.All(_harness.Nodes, n => Assert.Equal(10, n.MembershipSize));
    }

    /// <summary>
    /// Tests that nodes can leave gracefully from a large cluster.
    /// </summary>
    [Fact]
    public void LargeClusterHandlesGracefulLeave()
    {
        var nodes = _harness.CreateCluster(size: 10);
        _harness.WaitForConvergence(expectedSize: 10);

        // Graceful leave of one node
        _harness.RemoveNodeGracefully(nodes[9]);
        _harness.WaitForConvergence(expectedSize: 9);

        Assert.All(_harness.Nodes, n => Assert.Equal(9, n.MembershipSize));
    }

    /// <summary>
    /// Tests that multiple nodes can leave gracefully from a large cluster.
    /// </summary>
    [Fact]
    public void LargeClusterHandlesMultipleGracefulLeaves()
    {
        var nodes = _harness.CreateCluster(size: 10);
        _harness.WaitForConvergence(expectedSize: 10);

        // Graceful leave of 3 nodes
        _harness.RemoveNodeGracefully(nodes[9]);
        _harness.WaitForConvergence(expectedSize: 9);

        _harness.RemoveNodeGracefully(nodes[8]);
        _harness.WaitForConvergence(expectedSize: 8);

        _harness.RemoveNodeGracefully(nodes[7]);
        _harness.WaitForConvergence(expectedSize: 7);

        Assert.All(_harness.Nodes, n => Assert.Equal(7, n.MembershipSize));
    }

    /// <summary>
    /// Tests that parallel graceful leaves result in batched view changes.
    /// Similar to parallel joins, multiple concurrent leaves should be batched
    /// into fewer configuration changes than sequential leaves.
    /// </summary>
    [Theory]
    [InlineData(20, 5, 4)]    // Remove 5 from 20 nodes, expect at most 4 config changes
    [InlineData(30, 10, 6)]   // Remove 10 from 30 nodes, expect at most 6 config changes
    public void ParallelLeaves_BatchMultipleNodesPerViewChange(int clusterSize, int nodesToRemove, int maxExpectedChanges)
    {
        // Create the cluster using parallel joins for speed (larger clusters need more iterations)
        var maxIterationsPerBatch = clusterSize >= 30 ? 500000 : 100000;
        var nodes = _harness.CreateClusterParallel(size: clusterSize, maxIterationsPerBatch: maxIterationsPerBatch);
        _harness.WaitForConvergence(expectedSize: clusterSize, maxIterations: maxIterationsPerBatch);

        var configVersionBeforeLeaves = nodes[0].CurrentView.ConfigurationId.Version;

        // Select nodes to remove (avoiding the seed node at index 0)
        var leavingNodes = nodes.Skip(clusterSize - nodesToRemove).Take(nodesToRemove).ToList();

        // Remove nodes in parallel and measure configuration changes
        var configChanges = _harness.RemoveNodesGracefullyParallel(leavingNodes);

        var expectedSize = clusterSize - nodesToRemove;
        _harness.WaitForConvergence(expectedSize: expectedSize, maxIterations: maxIterationsPerBatch);

        Assert.All(_harness.Nodes, n => Assert.Equal(expectedSize, n.MembershipSize));

        var avgNodesPerChange = (double)nodesToRemove / configChanges;

        // Log membership transitions summary
        LogBatchingSummary("ParallelLeaves", nodesToRemove, configChanges, nodesToRemove, avgNodesPerChange,
            $"(cluster: {clusterSize} -> {expectedSize})");

        Assert.True(configChanges <= maxExpectedChanges,
            $"Expected at most {maxExpectedChanges} configuration changes for removing {nodesToRemove} nodes, " +
            $"but got {configChanges}. Average nodes per change: {avgNodesPerChange:F1}. " +
            $"Leave batching is not working correctly.");
    }

    /// <summary>
    /// Tests mixed join and leave operations in a large cluster.
    /// </summary>
    [Fact]
    public void LargeClusterHandlesMixedJoinAndLeave()
    {
        var nodes = _harness.CreateCluster(size: 8);
        _harness.WaitForConvergence(expectedSize: 8);

        // Leave one, join two
        _harness.RemoveNodeGracefully(nodes[7]);
        _harness.WaitForConvergence(expectedSize: 7);

        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: 8);
        _harness.WaitForConvergence(expectedSize: 8);

        var newNode2 = _harness.CreateJoinerNode(nodes[0], nodeId: 9);
        _harness.WaitForConvergence(expectedSize: 9);

        Assert.All(_harness.Nodes, n => Assert.Equal(9, n.MembershipSize));
    }

    /// <summary>
    /// Tests that configuration changes are properly tracked as cluster grows and shrinks.
    /// </summary>
    [Fact]
    public void ConfigurationChangesTrackedThroughGrowthAndShrinkage()
    {
        var seedNode = _harness.CreateSeedNode();
        var configIds = new HashSet<long> { seedNode.CurrentView.ConfigurationId };

        // Grow the cluster
        for (var i = 1; i <= 5; i++)
        {
            _harness.CreateJoinerNode(seedNode, nodeId: i);
            _harness.WaitForConvergence();
            configIds.Add(seedNode.CurrentView.ConfigurationId);
        }

        // Shrink the cluster
        var nodesToRemove = _harness.Nodes.Skip(3).ToList();
        foreach (var node in nodesToRemove)
        {
            _harness.RemoveNodeGracefully(node);
            configIds.Add(seedNode.CurrentView.ConfigurationId);
        }

        // Each membership change should result in a different configuration ID
        // (At minimum, we should have more than 1 unique config ID)
        Assert.True(configIds.Count > 1,
            $"Expected multiple configuration IDs through cluster lifecycle, got {configIds.Count}");
    }

    /// <summary>
    /// Logs a summary of batching statistics to the test output.
    /// </summary>
    private static void LogBatchingSummary(
        string operationType,
        int totalNodes,
        long configChanges,
        int nodesChanged,
        double avgNodesPerChange,
        string? suffix = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine($"=== {operationType} Batching Summary ===");
        sb.AppendLine($"  Total nodes: {totalNodes}");
        sb.AppendLine($"  Nodes changed: {nodesChanged}");
        sb.AppendLine($"  Configuration changes: {configChanges}");
        sb.AppendLine($"  Avg nodes per change: {avgNodesPerChange:F1}");
        if (suffix != null)
        {
            sb.AppendLine($"  {suffix}");
        }
        sb.AppendLine($"================================");

        TestContext.Current.TestOutputHelper?.WriteLine(sb.ToString());
    }

    /// <summary>
    /// Records a view change with transition details.
    /// </summary>
    private sealed record ViewTransition(
        long Version,
        int PreviousSize,
        int NewSize,
        int MembersAdded,
        int MembersRemoved,
        IReadOnlyList<string> AddedMembers,
        IReadOnlyList<string> RemovedMembers);

    /// <summary>
    /// Logs detailed view transitions to the test output.
    /// </summary>
    private static void LogViewTransitions(string operationType, IReadOnlyList<ViewTransition> transitions)
    {
        var sb = new StringBuilder();
        sb.AppendLine();
        sb.AppendLine($"=== {operationType} View Transitions ({transitions.Count} changes) ===");

        foreach (var t in transitions)
        {
            sb.AppendLine($"  Version {t.Version}: {t.PreviousSize} -> {t.NewSize} members");
            if (t.MembersAdded > 0)
            {
                sb.AppendLine($"    + Added ({t.MembersAdded}): {string.Join(", ", t.AddedMembers.Take(5))}{(t.MembersAdded > 5 ? $"... (+{t.MembersAdded - 5} more)" : "")}");
            }
            if (t.MembersRemoved > 0)
            {
                sb.AppendLine($"    - Removed ({t.MembersRemoved}): {string.Join(", ", t.RemovedMembers.Take(5))}{(t.MembersRemoved > 5 ? $"... (+{t.MembersRemoved - 5} more)" : "")}");
            }
        }

        // Summary statistics
        var totalAdded = transitions.Sum(t => t.MembersAdded);
        var totalRemoved = transitions.Sum(t => t.MembersRemoved);
        var avgAddedPerChange = transitions.Count > 0 ? (double)totalAdded / transitions.Count : 0;
        var avgRemovedPerChange = transitions.Count > 0 ? (double)totalRemoved / transitions.Count : 0;

        sb.AppendLine();
        sb.AppendLine($"  Summary:");
        sb.AppendLine($"    Total members added: {totalAdded} (avg {avgAddedPerChange:F1} per change)");
        sb.AppendLine($"    Total members removed: {totalRemoved} (avg {avgRemovedPerChange:F1} per change)");
        sb.AppendLine($"================================");

        TestContext.Current.TestOutputHelper?.WriteLine(sb.ToString());
    }

    /// <summary>
    /// Computes view transitions from a list of membership views.
    /// </summary>
    private static List<ViewTransition> ComputeViewTransitions(List<MembershipView> views)
    {
        var transitions = new List<ViewTransition>();

        for (var i = 1; i < views.Count; i++)
        {
            var prev = views[i - 1];
            var curr = views[i];

            var prevMembers = prev.Members.Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}").ToHashSet();
            var currMembers = curr.Members.Select(m => $"{m.Hostname.ToStringUtf8()}:{m.Port}").ToHashSet();

            var added = currMembers.Except(prevMembers).ToList();
            var removed = prevMembers.Except(currMembers).ToList();

            transitions.Add(new ViewTransition(
                curr.ConfigurationId.Version,
                prev.Size,
                curr.Size,
                added.Count,
                removed.Count,
                added,
                removed));
        }

        return transitions;
    }

    /// <summary>
    /// Tests sustained join/leave churn in a cluster.
    /// </summary>
    [Fact]
    public void SustainedJoinLeaveChurn()
    {
        // Start with a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        var nodeIdCounter = 5;

        // Perform 5 rounds of churn
        for (var round = 0; round < 5; round++)
        {
            // Remove one node
            var nodeToRemove = _harness.Nodes.Last();
            _harness.RemoveNodeGracefully(nodeToRemove);

            var expectedSize = _harness.Nodes.Count;
            _harness.WaitForConvergence(expectedSize: expectedSize);

            // Add one node
            var newNode = _harness.CreateJoinerNode(_harness.Nodes[0], nodeId: nodeIdCounter++);
            _harness.WaitForConvergence(expectedSize: expectedSize + 1);
        }

        // Cluster should be stable with 5 nodes
        Assert.Equal(5, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(5, n.MembershipSize));
    }

    /// <summary>
    /// Tests rapid sequential joins.
    /// </summary>
    [Fact]
    public void RapidSequentialJoins()
    {
        var seedNode = _harness.CreateSeedNode();

        // Rapidly add 10 nodes
        for (var i = 1; i <= 10; i++)
        {
            var joiner = _harness.CreateJoinerNode(seedNode, nodeId: i);
            Assert.True(joiner.IsInitialized);
        }

        _harness.WaitForConvergence(expectedSize: 11);

        Assert.Equal(11, _harness.Nodes.Count);
        Assert.All(_harness.Nodes, n => Assert.Equal(11, n.MembershipSize));
    }

}
