namespace Clockwork;

/// <summary>
/// Specifies the type of fault to inject during chaos testing.
/// </summary>
public enum FaultType
{
    /// <summary>Crash a node (sudden failure).</summary>
    NodeCrash,
    /// <summary>Create a network partition between two nodes.</summary>
    Partition,
    /// <summary>Heal a network partition between two nodes.</summary>
    PartitionHeal,
    /// <summary>Isolate a node from all other nodes.</summary>
    Isolation,
    /// <summary>Reconnect an isolated node.</summary>
    Reconnect
}

/// <summary>
/// Represents a fault scheduled to occur at a future time.
/// </summary>
/// <typeparam name="TNode">The concrete simulation node type.</typeparam>
public readonly record struct ScheduledFault<TNode>(
    FaultType Type,
    DateTimeOffset ExecuteAt,
    TNode? Node1,
    TNode? Node2) where TNode : class;

/// <summary>
/// Generic chaos injector for injecting random faults into simulations.
/// Provides rate-based random fault injection and scheduled fault execution.
/// </summary>
/// <typeparam name="TNode">The concrete simulation node type.</typeparam>
/// <typeparam name="TCluster">The concrete simulation cluster type.</typeparam>
public abstract class ChaosInjector<TNode, TCluster>
    where TNode : SimulationNode
    where TCluster : SimulationCluster<TNode>
{
    private readonly TCluster _cluster;
    private readonly SimulationRandom _random;
    private readonly List<ScheduledFault<TNode>> _scheduledFaults = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// Creates a new chaos injector.
    /// </summary>
    /// <param name="cluster">The simulation cluster.</param>
    protected ChaosInjector(TCluster cluster)
    {
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _random = cluster.Random.Fork();
    }

    /// <summary>
    /// Gets the simulation cluster.
    /// </summary>
    protected TCluster Cluster => _cluster;

    /// <summary>
    /// Gets the deterministic random generator.
    /// </summary>
    protected SimulationRandom Random => _random;

    /// <summary>
    /// Gets or sets the probability of a random node crash per step (0.0 to 1.0).
    /// </summary>
    public double NodeCrashRate { get; set; }

    /// <summary>
    /// Gets or sets the probability of a random partition per step (0.0 to 1.0).
    /// </summary>
    public double PartitionRate { get; set; }

    /// <summary>
    /// Gets or sets the probability of healing a random partition per step (0.0 to 1.0).
    /// </summary>
    public double PartitionHealRate { get; set; } = 0.1;

    /// <summary>
    /// Gets or sets the minimum number of nodes to keep alive during chaos.
    /// </summary>
    public int MinimumAliveNodes { get; set; } = 1;

    /// <summary>
    /// Possibly injects a fault based on configured rates.
    /// Call this once per simulation step.
    /// </summary>
    /// <returns>True if a fault was injected.</returns>
    public bool MaybeInjectFault()
    {
        // Process scheduled faults first
        ProcessScheduledFaults();

        // Maybe crash a node
        if (NodeCrashRate > 0 && _random.Chance(NodeCrashRate))
        {
            if (TryCrashRandomNode())
            {
                return true;
            }
        }

        // Maybe create a partition
        if (PartitionRate > 0 && _random.Chance(PartitionRate))
        {
            if (TryCreateRandomPartition())
            {
                return true;
            }
        }

        // Maybe heal a partition
        if (PartitionHealRate > 0 && _random.Chance(PartitionHealRate))
        {
            HealAllPartitions();
        }

        return false;
    }

    /// <summary>
    /// Schedules a node crash at a future time.
    /// </summary>
    public void ScheduleNodeCrash(TNode node, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(node);
        var executeAt = _cluster.TimeProvider.GetUtcNow() + delay;

        lock (_lock)
        {
            _scheduledFaults.Add(new ScheduledFault<TNode>(FaultType.NodeCrash, executeAt, node, default));
        }
    }

    /// <summary>
    /// Schedules a network partition at a future time.
    /// </summary>
    public void SchedulePartition(TNode node1, TNode node2, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentNullException.ThrowIfNull(node2);
        var executeAt = _cluster.TimeProvider.GetUtcNow() + delay;

        lock (_lock)
        {
            _scheduledFaults.Add(new ScheduledFault<TNode>(FaultType.Partition, executeAt, node1, node2));
        }
    }

    /// <summary>
    /// Schedules healing of a network partition at a future time.
    /// </summary>
    public void SchedulePartitionHeal(TNode node1, TNode node2, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(node1);
        ArgumentNullException.ThrowIfNull(node2);
        var executeAt = _cluster.TimeProvider.GetUtcNow() + delay;

        lock (_lock)
        {
            _scheduledFaults.Add(new ScheduledFault<TNode>(FaultType.PartitionHeal, executeAt, node1, node2));
        }
    }

    /// <summary>
    /// Schedules node isolation at a future time.
    /// </summary>
    public void ScheduleIsolation(TNode node, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(node);
        var executeAt = _cluster.TimeProvider.GetUtcNow() + delay;

        lock (_lock)
        {
            _scheduledFaults.Add(new ScheduledFault<TNode>(FaultType.Isolation, executeAt, node, default));
        }
    }

    /// <summary>
    /// Schedules node reconnection at a future time.
    /// </summary>
    public void ScheduleReconnect(TNode node, TimeSpan delay)
    {
        ArgumentNullException.ThrowIfNull(node);
        var executeAt = _cluster.TimeProvider.GetUtcNow() + delay;

        lock (_lock)
        {
            _scheduledFaults.Add(new ScheduledFault<TNode>(FaultType.Reconnect, executeAt, node, default));
        }
    }

    /// <summary>
    /// Runs chaos for the specified number of steps.
    /// </summary>
    /// <param name="steps">Number of simulation steps to run.</param>
    /// <param name="stepInterval">Time to advance between steps.</param>
    /// <returns>The number of faults injected.</returns>
    public int RunChaos(int steps, TimeSpan? stepInterval = null)
    {
        var interval = stepInterval ?? TimeSpan.FromMilliseconds(100);
        var faultsInjected = 0;

        for (var i = 0; i < steps; i++)
        {
            // Execute pending tasks
            _cluster.TaskQueue.RunUntilIdle();

            // Maybe inject a fault
            if (MaybeInjectFault())
            {
                faultsInjected++;
            }

            // Advance time and run simulation until idle
            _cluster.RunForDuration(interval);
        }

        return faultsInjected;
    }

    /// <summary>
    /// Clears all scheduled faults.
    /// </summary>
    public void ClearScheduledFaults()
    {
        lock (_lock)
        {
            _scheduledFaults.Clear();
        }
    }

    private void ProcessScheduledFaults()
    {
        var now = _cluster.TimeProvider.GetUtcNow();
        List<ScheduledFault<TNode>>? toExecute = null;

        lock (_lock)
        {
            toExecute = [.. _scheduledFaults.Where(f => f.ExecuteAt <= now)];
            foreach (var fault in toExecute)
            {
                _scheduledFaults.Remove(fault);
            }
        }

        foreach (var fault in toExecute)
        {
            ExecuteFault(fault);
        }
    }

    private void ExecuteFault(ScheduledFault<TNode> fault)
    {
        switch (fault.Type)
        {
            case FaultType.NodeCrash:
                if (fault.Node1 != null && _cluster.Nodes.Contains(fault.Node1))
                {
                    CrashNode(fault.Node1);
                }
                break;

            case FaultType.Partition:
                if (fault.Node1 != null && fault.Node2 != null)
                {
                    PartitionNodes(fault.Node1, fault.Node2);
                }
                break;

            case FaultType.PartitionHeal:
                if (fault.Node1 != null && fault.Node2 != null)
                {
                    HealPartition(fault.Node1, fault.Node2);
                }
                break;

            case FaultType.Isolation:
                if (fault.Node1 != null && _cluster.Nodes.Contains(fault.Node1))
                {
                    IsolateNode(fault.Node1);
                }
                break;

            case FaultType.Reconnect:
                if (fault.Node1 != null && _cluster.Nodes.Contains(fault.Node1))
                {
                    ReconnectNode(fault.Node1);
                }
                break;
        }
    }

    private bool TryCrashRandomNode()
    {
        var nodes = _cluster.Nodes;
        if (nodes.Count <= MinimumAliveNodes)
        {
            return false;
        }

        var node = _random.Choose(nodes.ToList());
        CrashNode(node);
        return true;
    }

    private bool TryCreateRandomPartition()
    {
        var nodes = _cluster.Nodes;
        if (nodes.Count < 2)
        {
            return false;
        }

        var nodeList = nodes.ToList();
        var node1 = _random.Choose(nodeList);
        nodeList.Remove(node1);
        var node2 = _random.Choose(nodeList);

        PartitionNodes(node1, node2);
        return true;
    }

    /// <summary>
    /// Crashes a node. Override in derived classes to implement crash behavior.
    /// </summary>
    protected abstract void CrashNode(TNode node);

    /// <summary>
    /// Creates a partition between two nodes. Override in derived classes to implement partition behavior.
    /// </summary>
    protected abstract void PartitionNodes(TNode node1, TNode node2);

    /// <summary>
    /// Heals a partition between two nodes. Override in derived classes to implement heal behavior.
    /// </summary>
    protected abstract void HealPartition(TNode node1, TNode node2);

    /// <summary>
    /// Isolates a node. Override in derived classes to implement isolation behavior.
    /// </summary>
    protected abstract void IsolateNode(TNode node);

    /// <summary>
    /// Reconnects an isolated node. Override in derived classes to implement reconnection behavior.
    /// </summary>
    protected abstract void ReconnectNode(TNode node);

    /// <summary>
    /// Heals all partitions. Override in derived classes to implement heal-all behavior.
    /// </summary>
    protected abstract void HealAllPartitions();
}
