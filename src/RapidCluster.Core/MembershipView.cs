using System.Collections.Immutable;
using System.Diagnostics;
using System.IO.Hashing;
using RapidCluster.Exceptions;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// An immutable snapshot of the cluster membership at a point in time.
/// Hosts K permutations of the memberlist that represent the monitoring relationship between nodes;
/// every node (an observer) observes its successor (a subject) on each ring.
/// Instances are obtained through <see cref="IRapidCluster.ViewAccessor"/>, or via <see cref="MembershipViewBuilder"/>.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
[DebuggerTypeProxy(typeof(MembershipViewDebugView))]
public sealed class MembershipView
{
    /// <summary>
    /// An empty membership view with no members. Used as the initial state before the cluster is initialized.
    /// </summary>
    public static MembershipView Empty { get; } = new(0, ConfigurationId.Empty, [], [], 0);

    private readonly ImmutableArray<ImmutableArray<Endpoint>> _rings;
    private readonly ImmutableSortedSet<Endpoint> _allNodes;
    private readonly ImmutableSortedSet<NodeId> _identifiersSeen;
    private readonly ImmutableDictionary<Endpoint, long> _monotonicIdByEndpoint;

    /// <summary>
    /// Initializes a new immutable MembershipView instance.
    /// </summary>
    /// <param name="ringCount">Number of monitoring rings.</param>
    /// <param name="configurationId">The configuration identifier for this view.</param>
    /// <param name="rings">The rings of endpoints (each ring is sorted by its comparator).</param>
    /// <param name="nodeIds">The set of node identifiers seen.</param>
    /// <param name="maxMonotonicId">The highest monotonic ID ever assigned.</param>
    internal MembershipView(int ringCount, ConfigurationId configurationId, ImmutableArray<ImmutableArray<Endpoint>> rings, ImmutableArray<NodeId> nodeIds, long maxMonotonicId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ringCount);
        ArgumentOutOfRangeException.ThrowIfNotEqual(rings.Length, ringCount, "Number of rings does not match ring count");
        RingCount = ringCount;
        ConfigurationId = configurationId;
        _rings = rings;
        NodeIds = nodeIds;
        MaxMonotonicId = maxMonotonicId;
        // Use EndpointComparer to ignore MonotonicNodeId when checking membership
        _allNodes = rings.Length > 0 ? rings[0].ToImmutableSortedSet(EndpointComparer.Instance) : ImmutableSortedSet<Endpoint>.Empty;
        _identifiersSeen = [.. nodeIds];

        // Build endpoint -> monotonic ID lookup from the first ring (all rings have the same endpoints)
        var builder = ImmutableDictionary.CreateBuilder<Endpoint, long>(EndpointAddressComparer.Instance);
        if (rings.Length > 0)
        {
            foreach (var endpoint in rings[0])
            {
                builder[endpoint] = endpoint.MonotonicNodeId;
            }
        }
        _monotonicIdByEndpoint = builder.ToImmutable();
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
    public ImmutableArray<Endpoint> Members => _rings.Length > 0 ? _rings[0] : [];

    /// <summary>
    /// Gets the number of members in the cluster.
    /// </summary>
    public int Size => Members.Length;

    /// <summary>
    /// Gets the list of node identifiers that have been seen (including those that have left).
    /// </summary>
    public ImmutableArray<NodeId> NodeIds { get; }

    /// <summary>
    /// Gets the highest monotonic node ID ever assigned in this cluster.
    /// This value only ever increases, even when nodes leave.
    /// Used to assign unique IDs to new joiners for Paxos correctness.
    /// </summary>
    public long MaxMonotonicId { get; }

    /// <summary>
    /// Gets the configuration for this view, which can be used to bootstrap a new MembershipViewBuilder.
    /// </summary>
    public MembershipViewConfiguration Configuration => new(NodeIds, Members, MaxMonotonicId);

    /// <summary>
    /// Checks if a specific endpoint is a member of this view.
    /// </summary>
    /// <param name="endpoint">The endpoint to check.</param>
    /// <returns>True if the endpoint is a member, false otherwise.</returns>
    public bool IsMember(Endpoint endpoint) => _allNodes.Contains(endpoint);

    /// <summary>
    /// Query if a host is part of the current membership set.
    /// </summary>
    /// <param name="address">The host.</param>
    /// <returns>True if the node is present in the membership view and false otherwise.</returns>
    public bool IsHostPresent(Endpoint address) => _allNodes.Contains(address);

    /// <summary>
    /// Query if an identifier has been used by a node already.
    /// </summary>
    /// <param name="identifier">The identifier to query for.</param>
    /// <returns>True if the identifier has been seen before and false otherwise.</returns>
    public bool IsIdentifierPresent(NodeId identifier) => _identifiersSeen.Contains(identifier);

    /// <summary>
    /// Gets the monotonic node ID for a specific endpoint.
    /// This ID is used for Paxos rank computation and is unique across node restarts.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The monotonic node ID for the endpoint.</returns>
    /// <exception cref="NodeNotInRingException">Thrown if the endpoint is not in the membership.</exception>
    public long GetMonotonicNodeId(Endpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (!_monotonicIdByEndpoint.TryGetValue(endpoint, out var monotonicId))
        {
            throw new NodeNotInRingException(endpoint);
        }
        return monotonicId;
    }

    /// <summary>
    /// Queries if a host with a logical identifier is safe to add to the network.
    /// </summary>
    /// <param name="node">The joining node.</param>
    /// <param name="uuid">The joining node's identifier.</param>
    /// <returns>
    /// HOSTNAME_ALREADY_IN_RING if the node is already in the ring.
    /// UUID_ALREADY_IN_RING if the uuid is already seen before.
    /// SAFE_TO_JOIN otherwise.
    /// </returns>
    public JoinStatusCode IsSafeToJoin(Endpoint node, NodeId uuid)
    {
        if (_allNodes.Contains(node))
        {
            return JoinStatusCode.HostnameAlreadyInRing;
        }

        if (_identifiersSeen.Contains(uuid))
        {
            return JoinStatusCode.UuidAlreadyInRing;
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

        if (!_allNodes.Contains(node))
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
            observers.Add(ring[successorIndex]);
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

        if (!_allNodes.Contains(node))
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
            subjects.Add(ring[predecessorIndex]);
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
            // Use EndpointAddressComparer to ignore MonotonicNodeId when comparing
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
            subjects.Add(ring[predecessorIndex]);
        }
        return subjects.MoveToImmutable();
    }

    private static int FindIndex(ImmutableArray<Endpoint> ring, Endpoint node)
    {
        for (var i = 0; i < ring.Length; i++)
        {
            // Use EndpointAddressComparer to ignore MonotonicNodeId when comparing
            if (EndpointAddressComparer.Instance.Equals(ring[i], node))
            {
                return i;
            }
        }
        return -1;
    }

    private static int FindInsertionPoint(ImmutableArray<Endpoint> ring, Endpoint node, int ringIndex)
    {
        // Compute hash for the new node
        var nodeHash = MembershipViewBuilder.ComputeEndpointHash(ringIndex, node);

        // Binary search for insertion point based on hash
        var left = 0;
        var right = ring.Length;
        while (left < right)
        {
            var mid = (left + right) / 2;
            var midHash = MembershipViewBuilder.ComputeEndpointHash(ringIndex, ring[mid]);
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
/// The MembershipViewConfiguration object contains a list of nodes in the membership view as well as a list of UUIDs.
/// An instance of this object created from one MembershipView object contains the necessary information
/// to bootstrap an identical MembershipView via MembershipViewBuilder.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MembershipViewConfiguration
{
    public MembershipViewConfiguration(IEnumerable<NodeId> nodeIds, IEnumerable<Endpoint> endpoints, long maxMonotonicId)
    {
        ArgumentNullException.ThrowIfNull(nodeIds);
        ArgumentNullException.ThrowIfNull(endpoints);
        NodeIds = [.. nodeIds];
        Endpoints = [.. endpoints];
        MaxMonotonicId = maxMonotonicId;
    }

    public ImmutableArray<NodeId> NodeIds { get; }
    public ImmutableArray<Endpoint> Endpoints { get; }

    /// <summary>
    /// Gets the highest monotonic node ID ever assigned.
    /// </summary>
    public long MaxMonotonicId { get; }

    /// <summary>
    /// Gets the configuration ID for this configuration (version 0).
    /// </summary>
    public ConfigurationId ConfigurationId => new(0);

    private string DebuggerDisplay => $"MembershipViewConfiguration(Endpoints={Endpoints.Length}, NodeIds={NodeIds.Length}, MaxMonotonicId={MaxMonotonicId})";
}

internal sealed class MembershipViewDebugView(MembershipView view)
{
    public int Size => view.Size;
    public ConfigurationId ConfigurationId => view.ConfigurationId;
    public int RingCount => view.RingCount;

    [DebuggerBrowsable(DebuggerBrowsableState.RootHidden)]
    public Endpoint[] Members => [.. view.Members];
}
