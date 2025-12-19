using System.Collections.Immutable;
using System.Diagnostics;
using RapidCluster.Exceptions;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// An immutable snapshot of the cluster membership at a point in time.
/// Hosts K permutations of the memberlist that represent the monitoring relationship between nodes;
/// every node (an observer) observes its successor (a subject) on each ring.
/// This is an internal type; consumers should use <see cref="ClusterMembershipView"/> via <see cref="IRapidCluster"/>.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
[DebuggerTypeProxy(typeof(MembershipViewDebugView))]
public sealed class MembershipView
{
    /// <summary>
    /// An empty membership view with no members. Used as the initial state before the cluster is initialized.
    /// </summary>
    public static MembershipView Empty { get; } = new(0, ConfigurationId.Empty, [], 0);

    private readonly ImmutableArray<ImmutableArray<MemberInfo>> _rings;
    private readonly ImmutableSortedDictionary<Endpoint, MemberInfo> _memberInfoByEndpoint;

    /// <summary>
    /// Initializes a new immutable MembershipView instance.
    /// </summary>
    /// <param name="ringCount">Number of monitoring rings.</param>
    /// <param name="configurationId">The configuration identifier for this view.</param>
    /// <param name="rings">The rings of MemberInfo (each ring is sorted by its comparator).</param>
    /// <param name="maxNodeId">The highest node ID ever assigned.</param>
    internal MembershipView(int ringCount, ConfigurationId configurationId, ImmutableArray<ImmutableArray<MemberInfo>> rings, long maxNodeId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ringCount);
        ArgumentOutOfRangeException.ThrowIfNotEqual(rings.Length, ringCount, "Number of rings does not match ring count");
        RingCount = ringCount;
        ConfigurationId = configurationId;
        _rings = rings;
        MaxNodeId = maxNodeId;

        // Build endpoint -> MemberInfo lookup from the first ring (all rings have the same members)
        // Use EndpointAddressComparer to ignore NodeId when checking membership
        var builder = ImmutableSortedDictionary.CreateBuilder<Endpoint, MemberInfo>(EndpointAddressComparer.Instance);
        if (rings.Length > 0)
        {
            foreach (var memberInfo in rings[0])
            {
                builder[memberInfo.Endpoint] = memberInfo;
            }
        }
        _memberInfoByEndpoint = builder.ToImmutable();
    }

    /// <summary>
    /// Gets the number of monitoring rings (K value).
    /// </summary>
    public int RingCount { get; }

    /// <summary>
    /// Gets the configuration identifier for this view.
    /// This combines a monotonic version counter with a membership hash.
    /// </summary>
    public ConfigurationId ConfigurationId { get; }

    /// <summary>
    /// Gets the list of member endpoints in the cluster.
    /// </summary>
    public ImmutableArray<Endpoint> Members => _rings.Length > 0
        ? [.. _rings[0].Select(m => m.Endpoint)]
        : [];

    /// <summary>
    /// Gets the list of MemberInfo in the cluster (with endpoint and metadata).
    /// </summary>
    public ImmutableArray<MemberInfo> MemberInfos => _rings.Length > 0 ? _rings[0] : [];

    /// <summary>
    /// Gets the number of members in the cluster.
    /// </summary>
    public int Size => _rings.Length > 0 ? _rings[0].Length : 0;

    /// <summary>
    /// Gets the highest node ID ever assigned in this cluster.
    /// This value only ever increases, even when nodes leave.
    /// Used to assign unique IDs to new joiners for Paxos correctness.
    /// </summary>
    public long MaxNodeId { get; }

    /// <summary>
    /// Gets the configuration for this view, which can be used to bootstrap a new MembershipViewBuilder.
    /// </summary>
    public MembershipViewConfiguration Configuration => new(MemberInfos, MaxNodeId);

    /// <summary>
    /// Checks if a specific endpoint is a member of this view.
    /// </summary>
    /// <param name="endpoint">The endpoint to check.</param>
    /// <returns>True if the endpoint is a member, false otherwise.</returns>
    public bool IsMember(Endpoint endpoint) => _memberInfoByEndpoint.ContainsKey(endpoint);

    /// <summary>
    /// Query if a host is part of the current membership set.
    /// </summary>
    /// <param name="address">The host.</param>
    /// <returns>True if the node is present in the membership view and false otherwise.</returns>
    public bool IsHostPresent(Endpoint address) => _memberInfoByEndpoint.ContainsKey(address);

    /// <summary>
    /// Gets the MemberInfo for a specific endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The MemberInfo for the endpoint, or null if not found.</returns>
    public MemberInfo? GetMemberInfo(Endpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return _memberInfoByEndpoint.TryGetValue(endpoint, out var memberInfo) ? memberInfo : null;
    }

    /// <summary>
    /// Gets the metadata for a specific endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The metadata for the endpoint, or null if not found.</returns>
    public Metadata? GetMetadata(Endpoint endpoint)
    {
        return GetMemberInfo(endpoint)?.Metadata;
    }

    /// <summary>
    /// Gets all metadata as a dictionary keyed by endpoint.
    /// </summary>
    /// <returns>A dictionary mapping endpoints to their metadata.</returns>
    public IReadOnlyDictionary<Endpoint, Metadata> GetAllMetadata()
    {
        return _memberInfoByEndpoint.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value.Metadata,
            EndpointAddressComparer.Instance);
    }

    /// <summary>
    /// Gets the monotonic node ID for a specific endpoint.
    /// This ID is used for Paxos rank computation and is unique across node restarts.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The monotonic node ID for the endpoint.</returns>
    /// <exception cref="NodeNotInRingException">Thrown if the endpoint is not in the membership.</exception>
    public long GetNodeId(Endpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (!_memberInfoByEndpoint.TryGetValue(endpoint, out var memberInfo))
        {
            throw new NodeNotInRingException(endpoint);
        }
        return memberInfo.Endpoint.NodeId;
    }

    /// <summary>
    /// Queries if a host is safe to add to the network.
    /// </summary>
    /// <param name="node">The joining node.</param>
    /// <returns>
    /// HOSTNAME_ALREADY_IN_RING if the node is already in the ring.
    /// SAFE_TO_JOIN otherwise.
    /// </returns>
    public JoinStatusCode IsSafeToJoin(Endpoint node)
    {
        if (_memberInfoByEndpoint.ContainsKey(node))
        {
            return JoinStatusCode.HostnameAlreadyInRing;
        }

        return JoinStatusCode.SafeToJoin;
    }

    /// <summary>
    /// Get the list of endpoints in the k'th ring.
    /// </summary>
    /// <param name="ringIndex">The index of the ring to query.</param>
    /// <returns>The list of endpoints in the k'th ring.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if k is out of range.</exception>
    public ImmutableArray<Endpoint> GetRing(int ringIndex)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ringIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(ringIndex, RingCount);
        return [.. _rings[ringIndex].Select(m => m.Endpoint)];
    }

    /// <summary>
    /// Get the list of MemberInfo in the k'th ring.
    /// </summary>
    /// <param name="ringIndex">The index of the ring to query.</param>
    /// <returns>The list of MemberInfo in the k'th ring.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if k is out of range.</exception>
    public ImmutableArray<MemberInfo> GetRingMemberInfos(int ringIndex)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ringIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(ringIndex, RingCount);
        return _rings[ringIndex];
    }

    /// <summary>
    /// Returns the set of observers for the given node.
    /// </summary>
    /// <param name="node">Input node.</param>
    /// <returns>The set of observers for the node.</returns>
    /// <exception cref="NodeNotInRingException">Thrown if the node is not in the ring.</exception>
    public ImmutableArray<Endpoint> GetObserversOf(Endpoint node)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (!_memberInfoByEndpoint.ContainsKey(node))
        {
            throw new NodeNotInRingException(node);
        }

        if (Size <= 1)
        {
            return [];
        }

        var observers = ImmutableArray.CreateBuilder<Endpoint>(RingCount);
        for (var k = 0; k < RingCount; k++)
        {
            var ring = _rings[k];
            var index = FindIndex(ring, node);
            // Successor wraps around
            var successorIndex = (index + 1) % ring.Length;
            observers.Add(ring[successorIndex].Endpoint);
        }
        return observers.MoveToImmutable();
    }

    /// <summary>
    /// Returns the set of nodes monitored by the given node.
    /// </summary>
    /// <param name="node">Input node.</param>
    /// <returns>The set of nodes monitored by the node.</returns>
    /// <exception cref="NodeNotInRingException">Thrown if the node is not in the ring.</exception>
    public ImmutableArray<Endpoint> GetSubjectsOf(Endpoint node)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (!_memberInfoByEndpoint.ContainsKey(node))
        {
            throw new NodeNotInRingException(node);
        }

        if (Size <= 1)
        {
            return [];
        }

        return GetPredecessorsOf(node);
    }

    /// <summary>
    /// Returns the expected observers of the node, even before it is
    /// added to the ring. Used during the bootstrap protocol to identify
    /// the nodes responsible for gatekeeping a joining peer.
    /// </summary>
    /// <param name="node">Input node.</param>
    /// <returns>The list of expected observers. Empty list if the membership is empty.</returns>
    public ImmutableArray<Endpoint> GetExpectedObserversOf(Endpoint node)
    {
        ArgumentNullException.ThrowIfNull(node);

        if (Size == 0)
        {
            return [];
        }

        // For a joining node, find where it would be inserted and return predecessors
        var subjects = ImmutableArray.CreateBuilder<Endpoint>(RingCount);
        for (var k = 0; k < RingCount; k++)
        {
            var ring = _rings[k];
            var insertionPoint = FindInsertionPoint(ring, node, k);
            // Predecessor wraps around
            var predecessorIndex = (insertionPoint - 1 + ring.Length) % ring.Length;
            subjects.Add(ring[predecessorIndex].Endpoint);
        }
        return subjects.MoveToImmutable();
    }

    /// <summary>
    /// Get the ring numbers where an observer monitors a given subject.
    /// </summary>
    /// <param name="observer">The observer node.</param>
    /// <param name="subject">The subject node.</param>
    /// <returns>The indexes k such that observer is a successor of subject on ring[k].</returns>
    public ImmutableArray<int> GetRingNumbers(Endpoint observer, Endpoint subject)
    {
        var subjects = GetSubjectsOf(observer);
        if (subjects.Length == 0)
        {
            return [];
        }

        var ringIndexes = ImmutableArray.CreateBuilder<int>();
        for (var ringNumber = 0; ringNumber < subjects.Length; ringNumber++)
        {
            // Use EndpointAddressComparer to ignore NodeId when comparing
            if (EndpointAddressComparer.Instance.Equals(subjects[ringNumber], subject))
            {
                ringIndexes.Add(ringNumber);
            }
        }
        return ringIndexes.ToImmutable();
    }

    /// <summary>
    /// Creates a new MembershipViewBuilder initialized from this view.
    /// </summary>
    /// <returns>A new MembershipViewBuilder that can be used to create modified views.</returns>
    internal MembershipViewBuilder ToBuilder() => new(this);

    /// <summary>
    /// Creates a new MembershipViewBuilder initialized from this view with a specified max ring count.
    /// Use this overload when the cluster may grow and need more rings than currently exist.
    /// </summary>
    /// <param name="maxRingCount">Maximum number of monitoring rings to maintain.</param>
    /// <returns>A new MembershipViewBuilder that can be used to create modified views.</returns>
    internal MembershipViewBuilder ToBuilder(int maxRingCount) => new(this, maxRingCount);

    /// <summary>
    /// Gets the position of an endpoint in the specified ring.
    /// </summary>
    /// <param name="endpoint">The endpoint to find.</param>
    /// <param name="ringIndex">The ring index to search (default is ring 0).</param>
    /// <returns>The 0-based position in the ring, or -1 if not found.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if ringIndex is out of range.</exception>
    public int GetRingPosition(Endpoint endpoint, int ringIndex = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(ringIndex, 0);
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(ringIndex, RingCount);

        var ring = _rings[ringIndex];
        return FindIndex(ring, endpoint);
    }

    private string DebuggerDisplay => $"MembershipView(Size={Size}, Config={ConfigurationId}, Rings={RingCount})";

    private ImmutableArray<Endpoint> GetPredecessorsOf(Endpoint node)
    {
        var subjects = ImmutableArray.CreateBuilder<Endpoint>(RingCount);
        for (var k = 0; k < RingCount; k++)
        {
            var ring = _rings[k];
            var index = FindIndex(ring, node);
            // Predecessor wraps around
            var predecessorIndex = (index - 1 + ring.Length) % ring.Length;
            subjects.Add(ring[predecessorIndex].Endpoint);
        }
        return subjects.MoveToImmutable();
    }

    private static int FindIndex(ImmutableArray<MemberInfo> ring, Endpoint node)
    {
        for (var i = 0; i < ring.Length; i++)
        {
            // Use EndpointAddressComparer to ignore NodeId when comparing
            if (EndpointAddressComparer.Instance.Equals(ring[i].Endpoint, node))
            {
                return i;
            }
        }
        return -1;
    }

    private static int FindInsertionPoint(ImmutableArray<MemberInfo> ring, Endpoint node, int ringIndex)
    {
        // Compute hash for the new node
        var nodeHash = MembershipViewBuilder.ComputeEndpointHash(ringIndex, node);

        // Binary search for insertion point based on hash
        var left = 0;
        var right = ring.Length;
        while (left < right)
        {
            var mid = (left + right) / 2;
            var midHash = MembershipViewBuilder.ComputeEndpointHash(ringIndex, ring[mid].Endpoint);
            if (midHash < nodeHash)
            {
                left = mid + 1;
            }
            else
            {
                right = mid;
            }
        }
        return left;
    }
}

/// <summary>
/// The MembershipViewConfiguration object contains a list of nodes in the membership view.
/// An instance of this object created from one MembershipView object contains the necessary information
/// to bootstrap an identical MembershipView via MembershipViewBuilder.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MembershipViewConfiguration
{
    public MembershipViewConfiguration(IEnumerable<MemberInfo> members, long maxNodeId)
    {
        ArgumentNullException.ThrowIfNull(members);
        Members = [.. members];
        MaxNodeId = maxNodeId;
    }

    /// <summary>
    /// Gets the members in the view (with endpoint and metadata).
    /// </summary>
    public ImmutableArray<MemberInfo> Members { get; }

    /// <summary>
    /// Gets the endpoints in the view.
    /// </summary>
    public ImmutableArray<Endpoint> Endpoints => [.. Members.Select(m => m.Endpoint)];

    /// <summary>
    /// Gets the highest node ID ever assigned.
    /// </summary>
    public long MaxNodeId { get; }

    /// <summary>
    /// Gets the configuration ID for this configuration (version 0).
    /// </summary>
    public ConfigurationId ConfigurationId => new(ConfigurationId.Empty.ClusterId, 0);

    private string DebuggerDisplay => $"MembershipViewConfiguration(Members={Members.Length}, MaxNodeId={MaxNodeId})";
}

internal sealed class MembershipViewDebugView(MembershipView view)
{
    public int Size => view.Size;
    public ConfigurationId ConfigurationId => view.ConfigurationId;
    public int RingCount => view.RingCount;

    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public MemberInfo[] Members => [.. view.MemberInfos];
}
