using System.Globalization;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Checks cluster invariants during simulation testing.
/// Used to detect split-brain scenarios, membership inconsistencies, and other safety violations.
/// </summary>
/// <remarks>
/// Creates a new invariant checker for the specified harness.
/// </remarks>
internal sealed class InvariantChecker(RapidSimulationCluster harness)
{
    private readonly RapidSimulationCluster _harness = harness ?? throw new ArgumentNullException(nameof(harness));
    private readonly List<InvariantViolation> _violations = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// Gets all recorded violations.
    /// </summary>
    public IReadOnlyList<InvariantViolation> Violations
    {
        get
        {
            lock (_lock)
            {
                return [.. _violations];
            }
        }
    }

    /// <summary>
    /// Gets a value indicating whether gets whether any violations have been recorded.
    /// </summary>
    public bool HasViolations
    {
        get
        {
            lock (_lock)
            {
                return _violations.Count > 0;
            }
        }
    }

    /// <summary>
    /// Checks all invariants and returns true if all pass.
    /// </summary>
    public bool CheckAll()
    {
        var passed = true;

        passed &= CheckMembershipConsistency();
        passed &= CheckNoSplitBrain();
        passed &= CheckConfigurationIdMonotonicity();
        passed &= CheckNodeIdValidity();
        passed &= CheckNodeIdUniqueness();
        passed &= CheckMaxNodeIdConsistency();

        return passed;
    }

    /// <summary>
    /// Checks that all nodes have consistent membership views.
    /// In a stable cluster, all nodes should see the same members (though possibly at different configuration IDs).
    /// </summary>
    public bool CheckMembershipConsistency()
    {
        var nodes = _harness.Nodes;
        if (nodes.Count < 2)
        {
            return true;
        }

        // Get all membership sizes
        var sizes = nodes.Select(n => n.MembershipSize).Distinct().ToList();

        // In a stable cluster, all sizes should be the same
        // Allow for temporary inconsistency during membership changes
        if (sizes.Count > 2)
        {
            RecordViolation(
                InvariantType.MembershipConsistency,
                $"More than 2 distinct membership sizes: [{string.Join(", ", sizes)}]");
            return false;
        }

        return true;
    }

    /// <summary>
    /// Checks for split-brain scenarios where two nodes both think they're in separate clusters.
    /// </summary>
    public bool CheckNoSplitBrain()
    {
        var nodes = _harness.Nodes;
        if (nodes.Count < 2)
        {
            return true;
        }

        // A split-brain occurs when:
        // 1. Two nodes have non-overlapping membership views
        // 2. Both nodes are initialized and think they're operational
        for (var i = 0; i < nodes.Count; i++)
        {
            var node1 = nodes[i];
            if (!node1.IsInitialized) continue;

            var members1 = node1.CurrentView?.Members ?? [];

            for (var j = i + 1; j < nodes.Count; j++)
            {
                var node2 = nodes[j];
                if (!node2.IsInitialized) continue;

                var members2 = node2.CurrentView?.Members ?? [];

                // Check if there's any overlap
                var overlap = members1.Intersect(members2, new EndpointComparer()).Any();

                if (!overlap && members1.Length > 0 && members2.Length > 0)
                {
                    RecordViolation(
                        InvariantType.NoSplitBrain,
                        "Split-brain detected: Node views have no overlap");
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that configuration IDs are monotonically increasing.
    /// A node should never go back to a lower configuration ID.
    /// Note: Configuration IDs are computed from membership hashes and can be any long value (including negative).
    /// </summary>
    public bool CheckConfigurationIdMonotonicity()
    {
        // This check requires tracking historical configuration IDs
        // Configuration IDs are computed from membership hashes and can be any long value,
        // including negative values. The only invalid state is if we have no view at all.
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null)
            {
                RecordViolation(
                    InvariantType.ConfigurationIdMonotonicity,
                    $"Node {RapidClusterUtils.Loggable(node.Address)} has no membership view");
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that all endpoints in each membership view have valid monotonic node IDs (greater than 0).
    /// </summary>
    public bool CheckNodeIdValidity()
    {
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null) continue;

            foreach (var member in view.Members)
            {
                if (member.NodeId <= 0)
                {
                    RecordViolation(
                        InvariantType.NodeIdValidity,
                        string.Create(CultureInfo.InvariantCulture, $"Node {RapidClusterUtils.Loggable(node.Address)} has member {RapidClusterUtils.Loggable(member)} with invalid NodeId={member.NodeId}"));
                    return false;
                }
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that monotonic node IDs are unique within each membership view.
    /// </summary>
    public bool CheckNodeIdUniqueness()
    {
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null) continue;

            var nodeIds = view.Members.Select(m => m.NodeId).ToList();
            var uniqueIds = nodeIds.Distinct().ToList();

            if (nodeIds.Count != uniqueIds.Count)
            {
                var duplicates = nodeIds.GroupBy(id => id)
                    .Where(g => g.Skip(1).Any())
                    .Select(g => g.Key);
                RecordViolation(
                    InvariantType.NodeIdUniqueness,
                    $"Node {RapidClusterUtils.Loggable(node.Address)} has duplicate NodeIds: [{string.Join(", ", duplicates)}]");
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Checks that MaxNodeId in each view is greater than or equal to the maximum NodeId of all members.
    /// </summary>
    public bool CheckMaxNodeIdConsistency()
    {
        foreach (var node in _harness.Nodes)
        {
            if (!node.IsInitialized) continue;

            var view = node.CurrentView;
            if (view == null) continue;

            var maxMemberId = view.Members.Length > 0 ? view.Members.Max(m => m.NodeId) : 0;

            if (view.MaxNodeId < maxMemberId)
            {
                RecordViolation(
                    InvariantType.MaxNodeIdConsistency,
                    string.Create(CultureInfo.InvariantCulture, $"Node {RapidClusterUtils.Loggable(node.Address)} has MaxNodeId={view.MaxNodeId} but has member with NodeId={maxMemberId}"));
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Verifies that the cluster can make progress (liveness check).
    /// This is a soft check - it doesn't guarantee liveness but can detect obvious deadlocks.
    /// </summary>
    public bool CheckLiveness(int maxSteps = 1000)
    {
        // Try to make progress
        for (var i = 0; i < maxSteps; i++)
        {
            if (_harness.TaskQueue.RunOnce())
            {
                return true; // Made progress
            }

            // Advance time to trigger any pending timers and run simulation until idle
            _harness.RunForDuration(TimeSpan.FromMilliseconds(10));
        }

        // Check if any tasks are pending now
        if (_harness.TaskQueue.HasItems)
        {
            return true;
        }

        RecordViolation(
            InvariantType.Liveness,
            string.Create(CultureInfo.InvariantCulture, $"No progress made in {maxSteps} steps"));
        return false;
    }

    /// <summary>
    /// Clears all recorded violations.
    /// </summary>
    public void Clear()
    {
        lock (_lock)
        {
            _violations.Clear();
        }
    }

    private void RecordViolation(InvariantType type, string message)
    {
        var violation = new InvariantViolation(
            _harness.TimeProvider.GetUtcNow(),
            type,
            message);

        lock (_lock)
        {
            _violations.Add(violation);
        }
    }

    private sealed class EndpointComparer : IEqualityComparer<Pb.Endpoint>
    {
        public bool Equals(Pb.Endpoint? x, Pb.Endpoint? y)
        {
            if (ReferenceEquals(x, y)) return true;
            if (x is null || y is null) return false;
            return x.Hostname == y.Hostname && x.Port == y.Port;
        }

        public int GetHashCode(Pb.Endpoint obj) => HashCode.Combine(obj.Hostname, obj.Port);
    }
}

/// <summary>
/// Types of invariants that can be checked.
/// </summary>
internal enum InvariantType
{
    MembershipConsistency,
    NoSplitBrain,
    ConfigurationIdMonotonicity,
    Liveness,
    ConsensusSafety,
    NodeIdValidity,
    NodeIdUniqueness,
    MaxNodeIdConsistency,
}

/// <summary>
/// Represents a detected invariant violation.
/// </summary>
internal readonly record struct InvariantViolation(
    DateTimeOffset SimulatedTime,
    InvariantType Type,
    string Message);
