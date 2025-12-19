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
    private readonly ImmutableSortedSet<MemberInfo> _allNodes;
    private readonly ImmutableDictionary<Endpoint, MemberInfo> _memberByEndpoint;

    /// <summary>
    /// Initializes a new immutable MembershipView instance.
    /// </summary>
    /// <param name="ringCount">Number of monitoring rings.</param>
    /// <param name="configurationId">The configuration identifier for this view.</param>
    /// <param name="rings">The rings of members (each ring is sorted by its comparator).</param>
    /// <param name="maxNodeId">The highest node ID ever assigned.</param>
    internal MembershipView(int ringCount, ConfigurationId configurationId, ImmutableArray<ImmutableArray<MemberInfo>> rings, long maxNodeId)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(ringCount);
        ArgumentOutOfRangeException.ThrowIfNotEqual(rings.Length, ringCount, "Number of rings does not match ring count");
        RingCount = ringCount;
        ConfigurationId = configurationId;
        _rings = rings;
        MaxNodeId = maxNodeId;

        // Use MemberInfoComparer for consistent ordering
        _allNodes = rings.Length > 0 ? rings[0].ToImmutableSortedSet(MemberInfoComparer.Instance) : ImmutableSortedSet<MemberInfo>.Empty;

        // Build endpoint -> MemberInfo lookup from the first ring (all rings have the same members)
        var builder = ImmutableDictionary.CreateBuilder<Endpoint, MemberInfo>(EndpointAddressComparer.Instance);
        if (rings.Length > 0)
        {
            foreach (var member in rings[0])
            {
                builder[member.Endpoint] = member;
            }
        }
        _memberByEndpoint = builder.ToImmutable();
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
    /// Gets the list of members (with metadata) in the cluster.
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
    public bool IsMember(Endpoint endpoint) => _memberByEndpoint.ContainsKey(endpoint);

    /// <summary>
    /// Query if a host is part of the current membership set.
    /// </summary>
    /// <param name="address">The host.</param>
    /// <returns>True if the node is present in the membership view and false otherwise.</returns>
    public bool IsHostPresent(Endpoint address) => _memberByEndpoint.ContainsKey(address);

    /// <summary>
    /// Gets the MemberInfo for a specific endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The MemberInfo for the endpoint, or null if not found.</returns>
    public MemberInfo? GetMember(Endpoint endpoint)
    {
        return _memberByEndpoint.GetValueOrDefault(endpoint);
    }

    /// <summary>
    /// Gets the metadata for a specific endpoint.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The metadata for the endpoint, or null if not found.</returns>
    public Metadata? GetMetadata(Endpoint endpoint)
    {
        return _memberByEndpoint.TryGetValue(endpoint, out var member) ? member.Metadata : null;
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
        if (!_memberByEndpoint.TryGetValue(endpoint, out var member))
        {
            throw new NodeNotInRingException(endpoint);
        }
        return member.NodeId;
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
        if (_memberByEndpoint.ContainsKey(node))
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
    /// Get the list of members (with metadata) in the k'th ring.
    /// </summary>
    /// <param name="ringIndex">The index of the ring to query.</param>
    /// <returns>The list of members in the k'th ring.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if k is out of range.</exception>
    public ImmutableArray<MemberInfo> GetRingMembers(int ringIndex)
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

        if (!_memberByEndpoint.ContainsKey(node))
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

        if (!_memberByEndpoint.ContainsKey(node))
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

    /// <summary>
    /// Finds the insertion point for a node in a sorted ring to maintain sort order.
    /// </summary>
    private static int FindInsertionPoint(ImmutableArray<MemberInfo> ring, Endpoint node, int ringIndex)
    {
        // Rings are sorted by the hash of the endpoint with the ring index as seed.
        // We replicate this logic to find where a new node would fit.
        
        var nodeHash = MembershipViewBuilder.ComputeEndpointHash(ringIndex, node);
        
        // Binary search
        int low = 0;
        int high = ring.Length - 1;

        while (low <= high)
        {
            int mid = low + ((high - low) >> 1);
            var midMember = ring[mid];
            var midHash = MembershipViewBuilder.ComputeEndpointHash(ringIndex, midMember.Endpoint);
            
            int comparison = midHash.CompareTo(nodeHash);
            
            if (comparison == 0)
            {
                return mid;
            }
            if (comparison < 0)
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }
        
        return low;
    }

    /// <summary>
    /// Applies a membership proposal to this view, creating a new view.
    /// </summary>
    /// <param name="proposal">The proposal containing the new membership state.</param>
    /// <param name="joinerMetadata">Metadata for nodes that might be joining and aren't in the current view.</param>
    /// <param name="maxRingCount">The maximum number of rings to use.</param>
    /// <param name="log">Logger for recording view changes.</param>
    /// <returns>The new membership view.</returns>
    internal MembershipView ApplyProposal(
        MembershipProposal proposal, 
        IReadOnlyDictionary<Endpoint, Metadata> joinerMetadata, 
        int maxRingCount,
        RapidCluster.Logging.MembershipServiceLogger log)
    {
        // Create a builder from the current view to make modifications
        var builder = ToBuilder(maxRingCount);

        // Build a lookup from the proposal for fast access to endpoints (which contain NodeId)
        var proposalMemberLookup = proposal.Members.ToDictionary(
            m => m,
            m => m,
            EndpointAddressComparer.Instance);
        
        var proposalMetadataLookup = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
        for (var i = 0; i < proposal.Members.Count; i++)
        {
            if (i < proposal.MemberMetadata.Count)
            {
                proposalMetadataLookup[proposal.Members[i]] = proposal.MemberMetadata[i];
            }
        }

        // Update the builder's max node ID from the proposal
        builder.SetMaxNodeId(proposal.MaxNodeId);

        // Determine which nodes are being added and which are being removed
        var currentMembers = new HashSet<Endpoint>(Members, EndpointAddressComparer.Instance);
        var proposedMembers = new HashSet<Endpoint>(proposal.Members, EndpointAddressComparer.Instance);

        // Nodes to remove: in current but not in proposed
        foreach (var node in currentMembers.Except(proposedMembers, EndpointAddressComparer.Instance))
        {
            log.RemovingNode(node);
            builder.RingDelete(node);
        }

        // Nodes to add: in proposed but not in current
        foreach (var node in proposedMembers.Except(currentMembers, EndpointAddressComparer.Instance))
        {
            if (!proposalMemberLookup.TryGetValue(node, out var endpointWithNodeId))
            {
                // This should never happen - the proposal should always have the endpoint
                log.DecidedNodeWithoutUuid(node);
                continue;
            }

            var metadata = proposalMetadataLookup.GetValueOrDefault(node) ?? joinerMetadata.GetValueOrDefault(node, new Metadata());

            log.AddingNode(endpointWithNodeId);
            // Create MemberInfo with endpoint and metadata - this stores metadata directly in the view
            var memberInfo = new MemberInfo(endpointWithNodeId, metadata);
            builder.RingAdd(memberInfo);
        }

        // Build the new immutable view with incremented version
        return builder.Build(ConfigurationId);
    }

    /// <summary>
    /// Computes the difference between this view and another view.
    /// </summary>
    public MembershipDiff Diff(MembershipView other)
    {
        ArgumentNullException.ThrowIfNull(other);
        
        var oldMembers = new HashSet<Endpoint>(Members, EndpointAddressComparer.Instance);
        var newMembers = new HashSet<Endpoint>(other.Members, EndpointAddressComparer.Instance);

        var addedNodes = new List<Endpoint>();
        var removedNodes = new List<Endpoint>();
        
        // Nodes that joined (in new but not in old)
        foreach (var node in newMembers)
        {
            if (!oldMembers.Contains(node))
            {
                addedNodes.Add(node);
            }
        }

        // Nodes that left (in old but not in new)
        foreach (var node in oldMembers)
        {
            if (!newMembers.Contains(node))
            {
                removedNodes.Add(node);
            }
        }

        return new MembershipDiff(addedNodes, removedNodes.Count, removedNodes);
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
    /// Gets the members (with metadata) in this configuration.
    /// </summary>
    public ImmutableArray<MemberInfo> Members { get; }

    /// <summary>
    /// Gets the endpoints in this configuration.
    /// </summary>
    public ImmutableArray<Endpoint> Endpoints => [.. Members.Select(m => m.Endpoint)];

    /// <summary>
    /// Gets the highest node ID ever assigned.
    /// </summary>
    public long MaxNodeId { get; }

    /// <summary>
    /// Gets the configuration ID for this configuration (version 0).
    /// </summary>
    public ConfigurationId ConfigurationId => new(0);

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
