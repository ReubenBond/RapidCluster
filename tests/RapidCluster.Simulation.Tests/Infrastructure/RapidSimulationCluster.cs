using System.Diagnostics;
using Clockwork;
using Microsoft.Extensions.Logging;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Unified simulation harness for fully deterministic testing of Rapid clusters.
/// Extends <see cref="SimulationCluster{TNode}"/> with RapidCluster-specific functionality.
/// 
/// Provides:
/// - Deterministic task scheduling via per-node <see cref="SimulationTaskScheduler"/> instances
/// - Controlled time via shared <see cref="SimulationClock"/>
/// - Seeded random number generation via <see cref="SimulationRandom"/>
/// - Simulated network with partition injection via <see cref="SimulationNetwork"/>
/// - Node lifecycle management (create, join, crash, leave)
/// - Per-node execution control (suspend, resume, step)
/// - Simulation driving APIs (Step, RunUntil, Run)
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed partial class RapidSimulationCluster : SimulationCluster<RapidSimulationNode>
{
    private readonly SimulationLogManager _logManager;
    private readonly ILogger<RapidSimulationCluster> _logger;
    private readonly SimulationHarnessLogger _log;
    private bool _disposed;

    /// <summary>
    /// Creates a new simulation harness with the specified seed.
    /// Logs are written to a unique file per simulation and attached to the test context.
    /// </summary>
    /// <param name="seed">The seed for deterministic random number generation.</param>
    public RapidSimulationCluster(int seed)
        : base(seed, startDateTime: DateTimeOffset.UtcNow, cancellationToken: TestContext.Current.CancellationToken)
    {
        Network = new SimulationNetwork(this, Random);

        // Create log manager for logger factory and log attachment
        _logManager = new SimulationLogManager(TimeProvider, seed);
        LoggerFactory = _logManager.LoggerFactory;

        _logger = LoggerFactory.CreateLogger<RapidSimulationCluster>();
        _log = new SimulationHarnessLogger(_logger);
        Network.SetLogger(LoggerFactory.CreateLogger<SimulationNetwork>());

        _log.HarnessCreated(seed);
    }

    /// <summary>
    /// Gets the logger factory.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; }

    /// <summary>
    /// Gets the simulation network.
    /// </summary>
    public SimulationNetwork Network { get; }

    /// <summary>
    /// Gets a node by its network address.
    /// </summary>
    /// <param name="address">The network address of the node.</param>
    /// <returns>The node with the specified address, or null if not found.</returns>
    public new RapidSimulationNode? GetNode(string address) => base.GetNode(address);

    /// <summary>
    /// Creates a node without initializing it (for testing edge cases).
    /// The node is registered but not started or joined to any cluster.
    /// </summary>
    /// <param name="nodeId">The node ID.</param>
    /// <param name="seedNode">Optional seed node for joining. If null, the node will start its own cluster when initialized.</param>
    /// <param name="options">Optional protocol options.</param>
    public RapidSimulationNode CreateUninitializedNode(int nodeId, RapidSimulationNode? seedNode = null, RapidClusterProtocolOptions? options = null)
    {
        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = new RapidSimulationNode(this, address, seedNode?.Address, metadata: null, options, LoggerFactory);
        RegisterNode(node);
        _log.UninitializedNodeCreated(nodeId);
        return node;
    }

    /// <summary>
    /// Creates and starts a new seed node.
    /// </summary>
    public RapidSimulationNode CreateSeedNode(int nodeId = 0, RapidClusterProtocolOptions? options = null)
    {
        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = new RapidSimulationNode(this, address, seedAddress: null, metadata: null, options, LoggerFactory);
        RegisterNode(node);

        // For seed nodes, initialization is synchronous (no network I/O needed),
        // but we still drive it through DriveToCompletion for consistency.
        Run(() => node.InitializeAsync());

        _log.SeedNodeCreated(nodeId);
        return node;
    }

    /// <summary>
    /// Creates and joins a new node to the cluster through the specified seed.
    /// Drives the simulation to complete the join.
    /// </summary>
    public RapidSimulationNode CreateJoinerNode(
        RapidSimulationNode seedNode,
        int nodeId,
        RapidClusterProtocolOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(seedNode);
        return CreateJoinerNodeWithMultipleSeeds([seedNode], nodeId, options);
    }

    /// <summary>
    /// Creates and joins a new node to the cluster with multiple seed addresses.
    /// The node will try seeds in round-robin order until join succeeds.
    /// Drives the simulation to complete the join.
    /// </summary>
    public RapidSimulationNode CreateJoinerNodeWithMultipleSeeds(
        IReadOnlyList<RapidSimulationNode> seedNodes,
        int nodeId,
        RapidClusterProtocolOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(seedNodes);
        if (seedNodes.Count == 0)
        {
            throw new ArgumentException("At least one seed node is required", nameof(seedNodes));
        }

        var seedAddresses = seedNodes.Select(n => n.Address).ToList();
        return CreateJoinerNodeWithSeedAddresses(seedAddresses, nodeId, options);
    }

    /// <summary>
    /// Creates and joins a new node to the cluster with raw seed addresses.
    /// Useful for testing duplicate seed handling and other edge cases.
    /// </summary>
    public RapidSimulationNode CreateJoinerNodeWithSeedAddresses(
        IList<Endpoint> seedAddresses,
        int nodeId,
        RapidClusterProtocolOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(seedAddresses);
        if (seedAddresses.Count == 0)
        {
            throw new ArgumentException("At least one seed address is required", nameof(seedAddresses));
        }

        var address = RapidClusterUtils.HostFromParts("node", nodeId);
        var node = new RapidSimulationNode(this, address, seedAddresses, metadata: null, options, LoggerFactory);
        RegisterNode(node);

        _log.NodeJoining(nodeId);

        // Drive the initialization to completion
        Run(() => node.InitializeAsync());

        _log.NodeJoined(nodeId);
        return node;
    }

    /// <summary>
    /// Creates a cluster of the specified size using sequential joins.
    /// Each node joins and waits for consensus before the next node joins.
    /// This results in O(N) consensus rounds but guarantees deterministic behavior.
    /// </summary>
    /// <remarks>
    /// For large clusters (50+ nodes), consider using <see cref="CreateClusterParallel"/>
    /// which batches joins together for O(log N) consensus rounds.
    /// </remarks>
    public IReadOnlyList<RapidSimulationNode> CreateCluster(int size, RapidClusterProtocolOptions? options = null)
    {
        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "Cluster size must be at least 1");
        }

        var result = new List<RapidSimulationNode>(size);

        // Create seed node
        var seedNode = CreateSeedNode(0, options);
        result.Add(seedNode);

        // Create joiner nodes
        for (var i = 1; i < size; i++)
        {
            var joiner = CreateJoinerNode(seedNode, i, options);
            result.Add(joiner);

            // Ensure the cluster catches up between sequential joins.
            // This avoids subsequent joins using stale configuration IDs.
            WaitForConvergence();
        }

        WaitForConvergence();
        return result;
    }

    /// <summary>
    /// Creates a cluster of the specified size using parallel joins.
    /// Multiple nodes join concurrently, allowing the multi-node cut detection
    /// to batch them into fewer consensus rounds (O(log N) instead of O(N)).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This matches the behavior described in the Rapid paper where 2000 nodes
    /// were bootstrapped with only 8 configuration changes. The multi-node cut
    /// detection aggregates pending JOIN alerts and proposes them together.
    /// </para>
    /// <para>
    /// For small clusters or when deterministic single-node-at-a-time behavior
    /// is needed, use <see cref="CreateCluster"/> instead.
    /// </para>
    /// </remarks>
    /// <param name="size">The number of nodes in the cluster.</param>
    /// <param name="options">Optional protocol options.</param>
    /// <param name="batchSize">
    /// Number of nodes to initiate joining simultaneously. Default is 0 which means all nodes.
    /// Smaller batch sizes provide more control over join ordering while still enabling batching.
    /// </param>
    /// <param name="maxIterationsPerBatch">
    /// Maximum simulation iterations to run for each batch of joins. Default is 100000.
    /// Larger clusters may need higher values (e.g., 500000 for 200+ nodes).
    /// </param>
    /// <returns>List of all nodes in the cluster.</returns>
    public IReadOnlyList<RapidSimulationNode> CreateClusterParallel(
        int size,
        RapidClusterProtocolOptions? options = null,
        int batchSize = 0,
        int maxIterationsPerBatch = 100000)
    {
        if (size < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(size), "Cluster size must be at least 1");
        }

        var result = new List<RapidSimulationNode>(size);

        // Create seed node first
        var seedNode = CreateSeedNode(0, options);
        result.Add(seedNode);

        if (size == 1)
        {
            return result;
        }

        // Use all remaining nodes as batch size if not specified or invalid
        var effectiveBatchSize = batchSize <= 0 ? size - 1 : batchSize;

        // Process nodes in batches
        var remainingNodes = size - 1;
        var nodeId = 1;

        while (remainingNodes > 0)
        {
            var currentBatchSize = Math.Min(effectiveBatchSize, remainingNodes);
            var batchNodes = new List<RapidSimulationNode>(currentBatchSize);

            // Create all nodes in this batch first (with seedAddress so MembershipService is ready)
            for (var i = 0; i < currentBatchSize; i++)
            {
                var address = RapidClusterUtils.HostFromParts("node", nodeId++);
                var node = new RapidSimulationNode(this, address, seedNode.Address, metadata: null, options, LoggerFactory);
                RegisterNode(node);

                _log.NodeJoiningParallel(RapidClusterUtils.Loggable(node.Address));

                batchNodes.Add(node);
            }

            // Drive the simulation until all joins in this batch complete.
            // IMPORTANT: Join tasks must be started inside DriveToCompletion so they
            // capture the simulation's SynchronizationContext for their continuations.
            Run(() => Task.WhenAll(batchNodes.Select(node => node.InitializeAsync())), maxIterationsPerBatch);

            // Log completion
            foreach (var node in batchNodes)
            {
                _log.NodeJoinedParallel(RapidClusterUtils.Loggable(node.Address));
            }

            result.AddRange(batchNodes);
            remainingNodes -= currentBatchSize;

            // IMPORTANT: Wait for ALL nodes (including previous batches) to converge to the
            // current cluster size before starting the next batch. Without this, nodes from
            // earlier batches may still be processing view changes when new nodes join,
            // causing them to get stuck with stale views.
            if (remainingNodes > 0)
            {
                var expectedSize = result.Count;
                var converged = RunUntilConverged(expectedSize, maxIterationsPerBatch);
                if (!converged)
                {
                    throw new TimeoutException(
                        $"Batch convergence failed. Expected size: {expectedSize}, " +
                        $"Actual sizes: [{string.Join(", ", result.Select(n => n.MembershipSize))}]");
                }
            }
        }

        return result;
    }

    /// <summary>
    /// Crashes a node (simulates sudden failure).
    /// Disposes and unregisters the node - no cleanup or leave messages are sent.
    /// The disposal cancels in-flight tasks and releases resources to prevent memory leaks.
    /// </summary>
    public void CrashNode(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);

        // Dispose the node while it's still registered so Run() can drive its task queue.
        // This cancels in-flight tasks and releases resources.
        Run(() => node.DisposeAsync().AsTask());

        // Unregister after disposal - no new messages will be delivered
        UnregisterNode(node);

        _log.NodeCrashed();
    }

    /// <summary>
    /// Gracefully removes a node from the cluster.
    /// The leaving node must participate in the consensus round that removes it,
    /// so we keep it alive until consensus completes and all remaining nodes converge.
    /// </summary>
    public void RemoveNodeGracefully(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        _log.NodeLeaving();

        var remainingNodes = Nodes.Where(n => n != node).ToList();
        var targetSize = remainingNodes.Count;

        // Drive the stop operation to completion (sends LeaveMessages to observers)
        // Node can still receive messages after this
        Run(node.StopAsync);

        // The leaving node must remain active to participate in consensus.
        // Run the simulation until all remaining nodes converge to the new size.
        var converged = RunUntil(
            () => remainingNodes.All(n => n.MembershipSize == targetSize),
            maxIterations: 100000);

        if (!converged)
        {
            // Log current state for debugging
            var sizes = string.Join(", ", remainingNodes.Select(n => n.MembershipSize));
            _logger.LogWarning(
                "RemoveNodeGracefully: remaining nodes did not converge to size {TargetSize}. Current sizes: [{Sizes}]",
                targetSize, sizes);
        }

        // Dispose the node's resources while it's still registered.
        // This ensures Run() can drive the node's task queue during disposal.
        Run(() => node.DisposeAsync().AsTask());

        // Unregister the node from the network (no more messages will be delivered)
        UnregisterNode(node);

        _log.NodeLeft();
    }

    /// <summary>
    /// Gracefully removes multiple nodes from the cluster in parallel.
    /// This allows the multi-node cut detection to batch multiple leaves into
    /// fewer consensus rounds, similar to how parallel joins work.
    /// 
    /// All leaving nodes initiate their leave concurrently, enabling the
    /// batching mechanism to combine their alerts into single view changes.
    /// </summary>
    /// <param name="nodesToRemove">The nodes to remove from the cluster.</param>
    /// <returns>The number of configuration changes that occurred during the parallel leave.</returns>
    public int RemoveNodesGracefullyParallel(IReadOnlyList<RapidSimulationNode> nodesToRemove)
    {
        ArgumentNullException.ThrowIfNull(nodesToRemove);
        if (nodesToRemove.Count == 0)
        {
            return 0;
        }

        // Get the starting configuration version to measure changes
        var remainingNodes = Nodes.Where(n => !nodesToRemove.Contains(n)).ToList();
        var startingConfigVersion = remainingNodes[0].CurrentView.ConfigurationId.Version;
        var targetSize = remainingNodes.Count;

        _log.NodesLeavingParallel(nodesToRemove.Count);

        foreach (var node in nodesToRemove)
        {
            _log.NodeLeavingParallel(RapidClusterUtils.Loggable(node.Address));
        }

        // Drive the simulation until all leave operations complete.
        // IMPORTANT: Leave tasks must be started inside DriveToCompletion so they
        // capture the simulation's SynchronizationContext for their continuations.
        // StopAsync sends leave messages but keeps nodes alive to participate in consensus.
        Run(() =>
        {
            var leaveTasks = nodesToRemove.Select(node => node.StopAsync());
            return Task.WhenAll(leaveTasks);
        });

        // Wait for remaining nodes to converge to the new size
        var converged = RunUntil(
            () => remainingNodes.All(n => n.MembershipSize == targetSize),
            maxIterations: 500000);

        if (!converged)
        {
            var sizes = string.Join(", ", remainingNodes.Select(n => n.MembershipSize));
            _logger.LogWarning(
                "RemoveNodesGracefullyParallel: remaining nodes did not converge to size {TargetSize}. Current sizes: [{Sizes}]",
                targetSize, sizes);
        }

        // Dispose all leaving nodes' resources while they're still registered.
        // This ensures Run() can drive each node's task queue during disposal.
        Run(() =>
        {
            var disposeTasks = nodesToRemove.Select(node => node.DisposeAsync().AsTask());
            return Task.WhenAll(disposeTasks);
        });

        // Unregister leaving nodes from the network (no more messages will be delivered)
        foreach (var node in nodesToRemove)
        {
            UnregisterNode(node);
            _log.NodeLeftParallel(RapidClusterUtils.Loggable(node.Address));
        }

        // Calculate configuration changes
        var endingConfigVersion = remainingNodes[0].CurrentView.ConfigurationId.Version;
        var configChanges = (int)(endingConfigVersion - startingConfigVersion);

        _log.ParallelLeaveCompleted(nodesToRemove.Count, configChanges);

        return configChanges;
    }

    /// <summary>
    /// Isolates a node from the cluster.
    /// </summary>
    public void IsolateNode(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        var addr = RapidClusterUtils.Loggable(node.Address);
        Network.IsolateNode(addr);
        _log.NodeIsolated();
    }

    /// <summary>
    /// Reconnects an isolated node.
    /// </summary>
    public void ReconnectNode(RapidSimulationNode node)
    {
        ArgumentNullException.ThrowIfNull(node);
        var addr = RapidClusterUtils.Loggable(node.Address);
        Network.ReconnectNode(addr);
        _log.NodeReconnected();
    }

    /// <summary>
    /// Creates a partition between two nodes.
    /// </summary>
    public void PartitionNodes(RapidSimulationNode node1, RapidSimulationNode node2)
    {
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentNullException.ThrowIfNull(node2);
        var addr1 = RapidClusterUtils.Loggable(node1.Address);
        var addr2 = RapidClusterUtils.Loggable(node2.Address);
        Network.CreateBidirectionalPartition(addr1, addr2);
        _log.PartitionCreated();
    }

    /// <summary>
    /// Heals a partition between two nodes.
    /// </summary>
    public void HealPartition(RapidSimulationNode node1, RapidSimulationNode node2)
    {
        using var _ = Guard.Enter();
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentNullException.ThrowIfNull(node2);
        var addr1 = RapidClusterUtils.Loggable(node1.Address);
        var addr2 = RapidClusterUtils.Loggable(node2.Address);
        Network.HealBidirectionalPartition(addr1, addr2);
        _log.PartitionHealed();
    }

    /// <summary>
    /// Runs until all non-suspended nodes have the expected membership size.
    /// Suspended nodes are excluded from the check since they cannot process messages.
    /// </summary>
    public bool RunUntilConverged(int expectedSize, int maxIterations = 100000) =>
        RunUntil(() => ActiveNodes.All(n => n.MembershipSize == expectedSize), maxIterations);

    /// <summary>
    /// Runs until the specified nodes have the expected membership size.
    /// Use this overload when some nodes (e.g., isolated/partitioned nodes) should be excluded from the check.
    /// </summary>
    public bool RunUntilConverged(IEnumerable<RapidSimulationNode> nodes, int expectedSize, int maxIterations = 100000)
    {
        ArgumentNullException.ThrowIfNull(nodes);
        var nodeList = nodes.ToList();
        return RunUntil(() => nodeList.All(n => n.MembershipSize == expectedSize), maxIterations);
    }

    /// <summary>
    /// Waits for all active (non-suspended) nodes to converge to a consistent view where
    /// each node sees exactly the number of active nodes in its membership.
    /// This is the preferred overload when nodes may be suspended during the test.
    /// </summary>
    public void WaitForConvergence(int maxIterations = 100000)
    {
        var converged = RunUntil(() =>
        {
            var activeCount = ActiveNodes.Count;
            return activeCount > 0 && ActiveNodes.All(n => n.MembershipSize == activeCount);
        }, maxIterations);

        if (!converged)
        {
            var suspendedNodes = Nodes.Where(n => n.IsSuspended).ToList();
            throw new TimeoutException($"Nodes did not converge. " +
                $"Active node count: {ActiveNodes.Count}, " +
                $"Active node sizes: [{string.Join(", ", ActiveNodes.Select(n => n.MembershipSize))}], " +
                $"Suspended nodes: {suspendedNodes.Count}");
        }
    }

    /// <summary>
    /// Waits for a specific node to reach the expected membership size.
    /// </summary>
    public void WaitForNodeSize(RapidSimulationNode node, int expectedSize, int maxIterations = 100000)
    {
        ArgumentNullException.ThrowIfNull(node);
        if (!RunUntil(() => node.MembershipSize == expectedSize, maxIterations))
        {
            throw new TimeoutException($"Node did not reach size {expectedSize}. Current size: {node.MembershipSize}");
        }
    }

    /// <summary>
    /// Logs the seed to the test output for reproduction.
    /// </summary>
    public void LogSeedForReproduction() => _logger.LogInformation("[SEED FOR REPRODUCTION] {Seed}", Seed);

    #region Override logging hooks from base class

    /// <inheritdoc />
    protected override void OnConditionMet(int iterations)
    {
        _log.ConditionMet(iterations);
    }

    /// <inheritdoc />
    protected override void OnSimulationIdleNoPendingWork(int iterations)
    {
        _log.SimulationIdleNoPendingWork(iterations, $"{TimeProvider.GetUtcNow():O}");
    }

    /// <inheritdoc />
    protected override void OnSimulationStuckMaxTime(TimeSpan timeDelta)
    {
        _log.SimulationStuckMaxTime(
            $"{MaxSimulatedTimeAdvance}",
            $"{StartDateTime:O}",
            $"{TimeProvider.GetUtcNow():O}",
            $"{timeDelta}");
    }

    /// <inheritdoc />
    protected override void OnSimulationStuckConsecutiveTimeAdvances(int count)
    {
        _log.SimulationStuckConsecutiveTimeAdvances(count);
    }

    /// <inheritdoc />
    protected override void OnMaxIterationsReached(int maxIterations)
    {
        _log.MaxIterationsReached(maxIterations);

        // Emit diagnostics at Information level so they show up
        // even when the full debug log gets truncated.
        var diagnostics = BuildMaxIterationDiagnostics(maxItemsPerQueue: 10);
        _log.MaxIterationsReachedWithDiagnostics(maxIterations, diagnostics);
    }

    private string BuildMaxIterationDiagnostics(int maxItemsPerQueue)
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine($"SimulatedTime={TimeProvider.GetUtcNow():O}");

        sb.AppendLine($"ClusterQueue: Items={TaskQueue.ScheduledItems.Count}, NextWaiting={TaskQueue.NextWaitingDueTime:O}");
        AppendQueueHead(sb, TaskQueue, maxItemsPerQueue);

        foreach (var node in Nodes)
        {
            var ctx = node.Context;
            sb.AppendLine($"Node={node.NetworkAddress} State={ctx.State} Items={ctx.TaskQueue.ScheduledItems.Count} HasReady={ctx.HasReadyTasks} NextWaiting={ctx.NextWaitingDueTime:O}");
            AppendQueueHead(sb, ctx.TaskQueue, maxItemsPerQueue);
        }

        return sb.ToString();
    }

    private static void AppendQueueHead(System.Text.StringBuilder sb, SimulationTaskQueue queue, int maxItems)
    {
        var count = 0;
        foreach (var item in queue.ScheduledItems)
        {
            if (count++ >= maxItems)
            {
                sb.AppendLine("  ...");
                break;
            }

            sb.AppendLine($"  {item.GetType().Name} Due={item.DueTime:O}");
        }
    }

    /// <inheritdoc />
    protected override void OnTeardownCancellationRequested()
    {
        _log.TeardownCancellationRequested();
    }

    /// <inheritdoc />
    protected override void OnSimulationReachedIdleState()
    {
        _log.SimulationReachedIdleState();
    }

    /// <inheritdoc />
    protected override void OnTimeAdvancing(TimeSpan delta)
    {
        _log.TimeAdvancing($"{delta}");
    }

    #endregion

    /// <inheritdoc />
    protected override async ValueTask DisposeAsyncCore()
    {
        if (_disposed) return;
        _disposed = true;

        // Dispose all nodes to cancel in-flight tasks and release resources.
        // This triggers cancellation of each node's _disposeCts, which propagates
        // to MembershipService and MessagingClient, allowing pending tasks to complete.
        var nodes = Nodes.ToList();
        foreach (var node in nodes)
        {
            await node.DisposeAsync().ConfigureAwait(true);
            UnregisterNode(node);
        }

        // Attach logs to test context BEFORE disposing the provider
        _logManager.AttachLogsToTestContext(TestContext.Current);

        // Dispose the log manager (disposes logger factory and provider)
        _logManager.Dispose();
    }

    private string DebuggerDisplay => $"SimulationHarness(Seed={Seed}, Nodes={Nodes.Count}, Time={Clock.CurrentTime:hh\\:mm\\:ss\\.fff})";
}
