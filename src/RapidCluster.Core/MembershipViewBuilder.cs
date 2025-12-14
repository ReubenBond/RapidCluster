using System.Collections.Immutable;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using RapidCluster.Exceptions;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// A mutable builder for creating <see cref="MembershipView"/> instances.
/// Hosts K permutations of the memberlist that represent the monitoring relationship between nodes;
/// every node (an observer) observes its successor (a subject) on each ring.
/// Once a Build method is called, the builder becomes sealed and cannot be used again.
/// </summary>
internal sealed class MembershipViewBuilder
{
    private readonly int _maxRingCount;
    private readonly List<AddressComparator> _addressComparators;
    private readonly List<SortedSet<Endpoint>> _rings;
    private readonly SortedSet<NodeId> _identifiersSeen;
    private readonly HashSet<Endpoint> _allNodes = new(EndpointAddressComparer.Instance);
    private long _maxNodeId;
    private bool _isSealed;

    /// <summary>
    /// Initializes a new instance of the MembershipViewBuilder class with the specified maximum number of rings.
    /// </summary>
    /// <param name="maxRingCount">Maximum number of monitoring rings to maintain. Must be positive.
    /// The actual ring count in the built view may be less if there are insufficient nodes
    /// (at most nodes-1 rings, minimum 1).</param>
    /// <exception cref="ArgumentException">Thrown when maxRingCount is not positive.</exception>
    public MembershipViewBuilder(int maxRingCount)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRingCount);

        _maxRingCount = maxRingCount;
        _rings = new List<SortedSet<Endpoint>>(maxRingCount);
        _addressComparators = new List<AddressComparator>(maxRingCount);
        _identifiersSeen = new SortedSet<NodeId>(NodeIdComparer.Instance);
        _maxNodeId = 0;

        for (var i = 0; i < maxRingCount; i++)
        {
            var comparator = new AddressComparator(i);
            _addressComparators.Add(comparator);
            _rings.Add(new SortedSet<Endpoint>(comparator));
        }
    }

    /// <summary>
    /// Initializes a new MembershipViewBuilder from an existing MembershipView.
    /// Uses the view's current ring count as the maximum.
    /// </summary>
    /// <param name="view">The existing view to initialize from.</param>
    public MembershipViewBuilder(MembershipView view)
        : this(view, view.RingCount)
    {
    }

    /// <summary>
    /// Initializes a new MembershipViewBuilder from an existing MembershipView with a specified max ring count.
    /// Use this overload when the cluster may grow and need more rings than currently exist.
    /// </summary>
    /// <param name="view">The existing view to initialize from.</param>
    /// <param name="maxRingCount">Maximum number of monitoring rings to maintain. Must be positive.
    /// The actual ring count in the built view may be less if there are insufficient nodes.</param>
    public MembershipViewBuilder(MembershipView view, int maxRingCount)
    {
        ArgumentNullException.ThrowIfNull(view);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRingCount);

        _maxRingCount = maxRingCount;
        _rings = new List<SortedSet<Endpoint>>(_maxRingCount);
        _addressComparators = new List<AddressComparator>(_maxRingCount);
        _identifiersSeen = new SortedSet<NodeId>(NodeIdComparer.Instance);
        _maxNodeId = view.MaxNodeId;

        for (var i = 0; i < _maxRingCount; i++)
        {
            var comparator = new AddressComparator(i);
            _addressComparators.Add(comparator);
            var set = new SortedSet<Endpoint>(comparator);
            foreach (var endpoint in view.Members)
            {
                set.Add(endpoint);
                _allNodes.Add(endpoint);
            }
            _rings.Add(set);
        }

        foreach (var nodeId in view.NodeIds)
        {
            _identifiersSeen.Add(nodeId);
        }
    }

    /// <summary>
    /// Used to bootstrap a membership view from the fields of a MembershipView.Configuration object.
    /// </summary>
    /// <param name="maxRingCount">Maximum number of monitoring rings to maintain. Must be positive.
    /// The actual ring count in the built view may be less if there are insufficient nodes
    /// (at most nodes-1 rings, minimum 1).</param>
    /// <param name="nodeIds">Collection of node identifiers to add.</param>
    /// <param name="endpoints">Collection of endpoints corresponding to the node IDs.</param>
    /// <param name="maxNodeId">The highest node ID ever assigned. If not provided, computed from endpoints.</param>
    public MembershipViewBuilder(int maxRingCount, ICollection<NodeId> nodeIds, ICollection<Endpoint> endpoints, long? maxNodeId = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRingCount);

        _maxRingCount = maxRingCount;
        _rings = new List<SortedSet<Endpoint>>(maxRingCount);
        _addressComparators = new List<AddressComparator>(maxRingCount);
        _identifiersSeen = new SortedSet<NodeId>(NodeIdComparer.Instance);

        // Compute maxNodeId from endpoints if not provided
        _maxNodeId = maxNodeId ?? endpoints.Select(e => e.NodeId).DefaultIfEmpty(0).Max();

        for (var i = 0; i < maxRingCount; i++)
        {
            var comparator = new AddressComparator(i);
            _addressComparators.Add(comparator);
            var set = new SortedSet<Endpoint>(comparator);
            foreach (var endpoint in endpoints)
            {
                set.Add(endpoint);
                _allNodes.Add(endpoint);
            }
            _rings.Add(set);
        }

        foreach (var nodeId in nodeIds)
        {
            _identifiersSeen.Add(nodeId);
        }
    }

    private static int ComputeRingCount(int ringCount, int nodeIdCount)
    {
        // There is no reason to have more rings than there are nodes.
        // For one or two nodes, there should be one ring.
        // For more nodes, there should be at most one less ring than there are nodes.
        ringCount = Math.Clamp(ringCount, 1, Math.Max(1, nodeIdCount - 1));
        return ringCount;
    }

    /// <summary>
    /// Gets the maximum number of rings (K value) that could be in the built view.
    /// The actual ring count in the built MembershipView may be less if there are insufficient nodes.
    /// </summary>
    public int MaxRingCount
    {
        get
        {
            ThrowIfSealed();
            return _maxRingCount;
        }
    }

    /// <summary>
    /// Gets the highest node ID ever assigned.
    /// </summary>
    public long MaxNodeId
    {
        get
        {
            ThrowIfSealed();
            return _maxNodeId;
        }
    }

    /// <summary>
    /// Gets the next node ID to assign and increments the counter.
    /// Call this when adding a new node to get its unique node ID.
    /// </summary>
    /// <returns>The next available node ID.</returns>
    public long GetNextNodeId()
    {
        ThrowIfSealed();
        return ++_maxNodeId;
    }

    /// <summary>
    /// Sets the max node ID. Used when applying a consensus decision that includes
    /// the max_node_id from the proposal.
    /// </summary>
    /// <param name="maxNodeId">The new max node ID (must be >= current).</param>
    public void SetMaxNodeId(long maxNodeId)
    {
        ThrowIfSealed();
        if (maxNodeId > _maxNodeId)
        {
            _maxNodeId = maxNodeId;
        }
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
        ThrowIfSealed();

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
    /// Add a node to all K rings and records its unique identifier.
    /// The endpoint must have its NodeId already set.
    /// </summary>
    /// <param name="node">The node to be added (must have NodeId set).</param>
    /// <param name="nodeId">The logical identifier of the node being added.</param>
    /// <exception cref="NodeAlreadyInRingException">Thrown if the node is already in the ring.</exception>
    /// <exception cref="UuidAlreadySeenException">Thrown if the node ID has been seen before.</exception>
    /// <returns>This builder for method chaining.</returns>
    public MembershipViewBuilder RingAdd(Endpoint node, NodeId nodeId)
    {
        ThrowIfSealed();
        ArgumentNullException.ThrowIfNull(node);
        ArgumentNullException.ThrowIfNull(nodeId);

        if (_identifiersSeen.Contains(nodeId))
        {
            throw new UuidAlreadySeenException(node, nodeId);
        }

        if (_rings[0].Contains(node))
        {
            throw new NodeAlreadyInRingException(node);
        }

        for (var k = 0; k < _maxRingCount; k++)
        {
            _rings[k].Add(node);
        }
        _allNodes.Add(node);
        _identifiersSeen.Add(nodeId);

        // Update max node ID if this node's ID is higher
        if (node.NodeId > _maxNodeId)
        {
            _maxNodeId = node.NodeId;
        }

        return this;
    }

    /// <summary>
    /// Delete a host from all K rings.
    /// </summary>
    /// <param name="node">The host to be removed.</param>
    /// <exception cref="NodeNotInRingException">Thrown if the node is not in the ring.</exception>
    /// <returns>This builder for method chaining.</returns>
    public MembershipViewBuilder RingDelete(Endpoint node)
    {
        ThrowIfSealed();
        ArgumentNullException.ThrowIfNull(node);

        if (!_rings[0].Contains(node))
        {
            throw new NodeNotInRingException(node);
        }

        for (var k = 0; k < _maxRingCount; k++)
        {
            _rings[k].Remove(node);
            _addressComparators[k].RemoveEndpoint(node);
        }
        _allNodes.Remove(node);

        return this;
    }

    /// <summary>
    /// Query if a host is part of the current membership set.
    /// </summary>
    /// <param name="address">The host.</param>
    /// <returns>True if the node is present in the membership view and false otherwise.</returns>
    public bool IsHostPresent(Endpoint address)
    {
        ThrowIfSealed();
        return _allNodes.Contains(address);
    }

    /// <summary>
    /// Query if an identifier has been used by a node already.
    /// </summary>
    /// <param name="identifier">The identifier to query for.</param>
    /// <returns>True if the identifier has been seen before and false otherwise.</returns>
    public bool IsIdentifierPresent(NodeId identifier)
    {
        ThrowIfSealed();
        return _identifiersSeen.Contains(identifier);
    }

    /// <summary>
    /// Get the list of endpoints in the k'th ring.
    /// </summary>
    /// <param name="k">The index of the ring to query.</param>
    /// <returns>The list of endpoints in the k'th ring.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if k is out of range.</exception>
    public List<Endpoint> GetRing(int k)
    {
        ThrowIfSealed();
        if (k < 0 || k >= _maxRingCount) throw new ArgumentOutOfRangeException(nameof(k));
        return [.. _rings[k]];
    }

    /// <summary>
    /// Get the number of nodes currently in the membership.
    /// </summary>
    /// <returns>The number of nodes in the membership.</returns>
    public int GetMembershipSize()
    {
        ThrowIfSealed();
        return _rings[0].Count;
    }

    /// <summary>
    /// Builds and returns the immutable MembershipView with a new configuration ID
    /// that has an incremented version from the previous configuration.
    /// After this method is called, the builder becomes sealed and cannot be used again.
    /// </summary>
    /// <param name="previousConfigurationId">The previous configuration ID to increment from.</param>
    /// <returns>An immutable MembershipView instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the builder has already been sealed.</exception>
    public MembershipView Build(ConfigurationId previousConfigurationId)
    {
        return BuildWithConfigurationId(previousConfigurationId.Next());
    }

    /// <summary>
    /// Builds and returns the immutable MembershipView with the exact configuration ID provided.
    /// Use this overload when joining an existing cluster with a known configuration ID.
    /// After this method is called, the builder becomes sealed and cannot be used again.
    /// </summary>
    /// <param name="configurationId">The exact configuration ID to use.</param>
    /// <returns>An immutable MembershipView instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the builder has already been sealed.</exception>
    internal MembershipView BuildWithConfigurationId(ConfigurationId configurationId)
    {
        ThrowIfSealed();
        _isSealed = true;

        // Create immutable ring copies
        var ringCount = ComputeRingCount(_maxRingCount, _allNodes.Count);
        var ringsBuilder = ImmutableArray.CreateBuilder<ImmutableArray<Endpoint>>(ringCount);
        for (var i = 0; i < ringCount; i++)
        {
            ringsBuilder.Add([.. _rings[i]]);
        }

        return new MembershipView(ringCount, configurationId, ringsBuilder.MoveToImmutable(), [.. _identifiersSeen], _maxNodeId);
    }

    /// <summary>
    /// Builds and returns the immutable MembershipView with a configuration ID starting at version 0.
    /// Use this overload only for initial cluster creation.
    /// After this method is called, the builder becomes sealed and cannot be used again.
    /// </summary>
    /// <returns>An immutable MembershipView instance.</returns>
    /// <exception cref="InvalidOperationException">Thrown if the builder has already been sealed.</exception>
    public MembershipView Build()
    {
        return Build(ConfigurationId.Empty);
    }

    /// <summary>
    /// Computes the hash for an endpoint on a specific ring.
    /// Used for determining ring ordering.
    /// </summary>
    internal static long ComputeEndpointHash(int seed, Endpoint endpoint)
    {
        var hostnameHash = (long)XxHash64.HashToUInt64(endpoint.Hostname.Span, seed);
        var portHash = (long)XxHash64.HashToUInt64(BitConverter.GetBytes(endpoint.Port), seed);
        return hostnameHash * 31 + portHash;
    }

    private void ThrowIfSealed()
    {
        if (_isSealed)
        {
            throw new InvalidOperationException("This MembershipViewBuilder has been sealed and cannot be used again. Call ToBuilder() on the MembershipView to create a new builder.");
        }
    }

    /// <summary>
    /// Used to order endpoints in the different rings.
    /// </summary>
    private sealed class AddressComparator(int seed) : IComparer<Endpoint>
    {
        private readonly int _seed = seed;
        private readonly Dictionary<Endpoint, long> _hashCache = new(EndpointAddressComparer.Instance);

        public int Compare(Endpoint? x, Endpoint? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1;
            if (y == null) return 1;

            var hash1 = GetCachedHash(x);
            var hash2 = GetCachedHash(y);
            return hash1.CompareTo(hash2);
        }

        public void RemoveEndpoint(Endpoint endpoint) => _hashCache.Remove(endpoint, out _);

        private long GetCachedHash(Endpoint endpoint)
        {
            ref var hash = ref CollectionsMarshal.GetValueRefOrAddDefault(_hashCache, endpoint, out var exists);
            if (!exists)
            {
                hash = ComputeEndpointHash(_seed, endpoint);
            }
            return hash;
        }
    }

}
