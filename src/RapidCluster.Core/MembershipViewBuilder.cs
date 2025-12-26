using System.Buffers.Binary;
using System.Collections.Immutable;
using System.IO.Hashing;
using System.Numerics;
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
    private readonly List<List<MemberInfo>> _rings;
    private readonly Dictionary<Endpoint, MemberInfo> _memberInfoByEndpoint = new(EndpointAddressComparer.Instance);
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
        _rings = new List<List<MemberInfo>>(maxRingCount);
        _addressComparators = new List<AddressComparator>(maxRingCount);
        _maxNodeId = 0;

        for (var i = 0; i < maxRingCount; i++)
        {
            var comparator = new AddressComparator(i);
            _addressComparators.Add(comparator);
            _rings.Add(new List<MemberInfo>());
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
        _rings = new List<List<MemberInfo>>(_maxRingCount);
        _addressComparators = new List<AddressComparator>(_maxRingCount);
        _maxNodeId = view.MaxNodeId;

        foreach (var memberInfo in view.MemberInfos)
        {
            _memberInfoByEndpoint[memberInfo.Endpoint] = memberInfo;
        }

        for (var i = 0; i < _maxRingCount; i++)
        {
            var comparator = new AddressComparator(i);
            _addressComparators.Add(comparator);

            var ring = new List<MemberInfo>(view.MemberInfos);
            ring.Sort(comparator);
            _rings.Add(ring);
        }
    }

    /// <summary>
    /// Used to bootstrap a membership view from a collection of MemberInfo objects.
    /// </summary>
    /// <param name="maxRingCount">Maximum number of monitoring rings to maintain. Must be positive.
    /// The actual ring count in the built view may be less if there are insufficient nodes
    /// (at most nodes-1 rings, minimum 1).</param>
    /// <param name="members">Collection of MemberInfo to add.</param>
    /// <param name="maxNodeId">The highest node ID ever assigned. If not provided, computed from members.</param>
    public MembershipViewBuilder(int maxRingCount, ICollection<MemberInfo> members, long? maxNodeId = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxRingCount);

        _maxRingCount = maxRingCount;
        _rings = new List<List<MemberInfo>>(maxRingCount);
        _addressComparators = new List<AddressComparator>(maxRingCount);

        // Compute maxNodeId from members if not provided
        _maxNodeId = maxNodeId ?? members.Select(m => m.Endpoint.NodeId).DefaultIfEmpty(0).Max();

        foreach (var memberInfo in members)
        {
            _memberInfoByEndpoint[memberInfo.Endpoint] = memberInfo;
        }

        for (var i = 0; i < maxRingCount; i++)
        {
            var comparator = new AddressComparator(i);
            _addressComparators.Add(comparator);

            var ring = new List<MemberInfo>(members);
            ring.Sort(comparator);
            _rings.Add(ring);
        }
    }

    /// <summary>
    /// Used to bootstrap a membership view from the fields of a MembershipView.Configuration object.
    /// </summary>
    /// <param name="maxRingCount">Maximum number of monitoring rings to maintain. Must be positive.
    /// The actual ring count in the built view may be less if there are insufficient nodes
    /// (at most nodes-1 rings, minimum 1).</param>
    /// <param name="endpoints">Collection of endpoints to add (without metadata).</param>
    /// <param name="maxNodeId">The highest node ID ever assigned. If not provided, computed from endpoints.</param>
    public MembershipViewBuilder(int maxRingCount, ICollection<Endpoint> endpoints, long? maxNodeId = null)
        : this(maxRingCount, endpoints.Select(e => new MemberInfo(e)).ToList(), maxNodeId)
    {
    }

    private static int ComputeRingCount(int ringCount, int nodeCount)
    {
        // There is no reason to have more rings than there are nodes.
        // For one or two nodes, there should be one ring.
        // For more nodes, there should be at most one less ring than there are nodes.
        ringCount = Math.Clamp(ringCount, 1, Math.Max(1, nodeCount - 1));
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
    /// Queries if a host is safe to add to the network.
    /// </summary>
    /// <param name="node">The joining node.</param>
    /// <returns>
    /// HOSTNAME_ALREADY_IN_RING if the node is already in the ring.
    /// SAFE_TO_JOIN otherwise.
    /// </returns>
    public JoinStatusCode IsSafeToJoin(Endpoint node)
    {
        ThrowIfSealed();

        if (_memberInfoByEndpoint.ContainsKey(node))
        {
            return JoinStatusCode.HostnameAlreadyInRing;
        }

        return JoinStatusCode.SafeToJoin;
    }

    /// <summary>
    /// Add a member to all K rings.
    /// The endpoint must have its NodeId already set.
    /// </summary>
    /// <param name="memberInfo">The MemberInfo to be added (must have NodeId set in Endpoint).</param>
    /// <exception cref="NodeAlreadyInRingException">Thrown if the node is already in the ring.</exception>
    /// <returns>This builder for method chaining.</returns>
    public MembershipViewBuilder RingAdd(MemberInfo memberInfo)
    {
        ThrowIfSealed();
        ArgumentNullException.ThrowIfNull(memberInfo);

        if (_memberInfoByEndpoint.ContainsKey(memberInfo.Endpoint))
        {
            throw new NodeAlreadyInRingException(memberInfo.Endpoint);
        }

        for (var ringIndex = 0; ringIndex < _maxRingCount; ringIndex++)
        {
            var ring = _rings[ringIndex];
            var comparator = _addressComparators[ringIndex];
            ring.Insert(FindInsertionPoint(ring, memberInfo, comparator), memberInfo);
        }

        _memberInfoByEndpoint[memberInfo.Endpoint] = memberInfo;

        // Update max node ID if this node's ID is higher
        if (memberInfo.Endpoint.NodeId > _maxNodeId)
        {
            _maxNodeId = memberInfo.Endpoint.NodeId;
        }

        return this;
    }

    /// <summary>
    /// Add a node to all K rings with empty metadata.
    /// The endpoint must have its NodeId already set.
    /// </summary>
    /// <param name="node">The node to be added (must have NodeId set).</param>
    /// <exception cref="NodeAlreadyInRingException">Thrown if the node is already in the ring.</exception>
    /// <returns>This builder for method chaining.</returns>
    public MembershipViewBuilder RingAdd(Endpoint node)
    {
        return RingAdd(new MemberInfo(node));
    }

    /// <summary>
    /// Add a node to all K rings with the specified metadata.
    /// The endpoint must have its NodeId already set.
    /// </summary>
    /// <param name="node">The node to be added (must have NodeId set).</param>
    /// <param name="metadata">The metadata for the node.</param>
    /// <exception cref="NodeAlreadyInRingException">Thrown if the node is already in the ring.</exception>
    /// <returns>This builder for method chaining.</returns>
    public MembershipViewBuilder RingAdd(Endpoint node, Metadata metadata)
    {
        return RingAdd(new MemberInfo(node, metadata));
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

        if (!_memberInfoByEndpoint.TryGetValue(node, out var memberInfo))
        {
            throw new NodeNotInRingException(node);
        }

        for (var ringIndex = 0; ringIndex < _maxRingCount; ringIndex++)
        {
            var ring = _rings[ringIndex];
            var comparator = _addressComparators[ringIndex];

            var index = ring.BinarySearch(memberInfo, comparator);
            if (index >= 0)
            {
                ring.RemoveAt(index);
            }
            else
            {
                for (var i = 0; i < ring.Count; i++)
                {
                    if (EndpointAddressComparer.Instance.Equals(ring[i].Endpoint, memberInfo.Endpoint))
                    {
                        ring.RemoveAt(i);
                        break;
                    }
                }
            }

            comparator.RemoveMemberInfo(memberInfo);
        }
        _memberInfoByEndpoint.Remove(node);

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
        return _memberInfoByEndpoint.ContainsKey(address);
    }

    /// <summary>
    /// Get the MemberInfo for an endpoint, if present.
    /// </summary>
    /// <param name="endpoint">The endpoint to look up.</param>
    /// <returns>The MemberInfo, or null if not found.</returns>
    public MemberInfo? GetMemberInfo(Endpoint endpoint)
    {
        ThrowIfSealed();
        return _memberInfoByEndpoint.TryGetValue(endpoint, out var memberInfo) ? memberInfo : null;
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
        return [.. _rings[k].Select(m => m.Endpoint)];
    }

    /// <summary>
    /// Get the list of MemberInfo in the k'th ring.
    /// </summary>
    /// <param name="k">The index of the ring to query.</param>
    /// <returns>The list of MemberInfo in the k'th ring.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if k is out of range.</exception>
    public List<MemberInfo> GetRingMemberInfos(int k)
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

        var desiredRingCount = ComputeRingCount(_maxRingCount, _memberInfoByEndpoint.Count);

        // Build the base order and select offsets for the monitoring rings.
        // The monitoring relationship is defined by offsets on the base order:
        // successor_r(baseOrder[i]) = baseOrder[(i + offset_r) mod N]
        var (rings, offsets) = BuildUniqueMonitoringRings(desiredRingCount);
        return new MembershipView(rings.Length, configurationId, rings, offsets, _maxNodeId);
    }

    /// <summary>
    /// Builds K rings where each node has K unique successors (subjects) and K unique predecessors (observers).
    /// 
    /// Algorithm: Deterministic k-ring monitoring graph with offset-based construction
    /// 1. Create a single base permutation O of all nodes by sorting on xxhash scores.
    /// 2. Select K distinct offsets d_0, d_1, ..., d_{K-1} from {1, 2, ..., N-1}.
    ///    - Prioritize offsets that are coprime to N (gives single-cycle rings with better expansion).
    ///    - Use xxhash ranking to deterministically select among candidates.
    /// 3. For each ring r with offset d_r:
    ///    - successor_r(O[i]) = O[(i + d_r) mod N]
    ///    - predecessor_r(O[i]) = O[(i - d_r + N) mod N]
    /// 
    /// This guarantees:
    /// - Each node has exactly K unique successors (monitors K unique nodes)
    /// - Each node has exactly K unique predecessors (is monitored by K unique nodes)
    /// - The construction is deterministic (same on all nodes)
    /// - No algorithmic deadlock (pure computation)
    /// </summary>
    private (ImmutableArray<ImmutableArray<MemberInfo>> rings, ImmutableArray<int> offsets) BuildUniqueMonitoringRings(int ringCount)
    {
        var n = _memberInfoByEndpoint.Count;

        // Build base order O by sorting on xxhash scores (using ring 0's comparator)
        var baseOrder = new List<MemberInfo>(_rings[0]);
        baseOrder.Sort(_addressComparators[0]);
        var baseOrderArray = ImmutableArray.Create(baseOrder.ToArray());

        if (n <= 1 || ringCount <= 1)
        {
            // Single ring with offset 1
            return ([baseOrderArray], [1]);
        }

        // Select K distinct offsets, prioritizing coprimes for better expansion
        var offsets = SelectOffsets(n, ringCount);

        // We store the base order K times (all rings reference the same base order).
        // The actual successor relationship is computed at query time using offsets.
        var resultRings = ImmutableArray.CreateBuilder<ImmutableArray<MemberInfo>>(ringCount);
        for (var r = 0; r < ringCount; r++)
        {
            resultRings.Add(baseOrderArray);
        }

        return (resultRings.MoveToImmutable(), ImmutableArray.Create(offsets));
    }

    /// <summary>
    /// Selects K distinct offsets from {1, 2, ..., N-1}, prioritizing coprimes to N.
    /// Uses xxhash ranking to deterministically break ties.
    /// </summary>
    private static int[] SelectOffsets(int n, int k)
    {
        // Build candidate offsets 1..N-1
        var candidates = new List<(int offset, bool isCoprime, ulong rank)>(n - 1);

        for (var d = 1; d < n; d++)
        {
            var isCoprime = Gcd(d, n) == 1;
            // Use xxhash to rank offsets deterministically
            Span<byte> rankInput = stackalloc byte[8];
            BinaryPrimitives.WriteInt32LittleEndian(rankInput, d);
            BinaryPrimitives.WriteInt32LittleEndian(rankInput[4..], n);
            var rank = XxHash64.HashToUInt64(rankInput);
            candidates.Add((d, isCoprime, rank));
        }

        // Sort: coprimes first, then by xxhash rank
        candidates.Sort((a, b) =>
        {
            // Coprimes come first (false < true in bool comparison, so negate)
            var coprimeCompare = b.isCoprime.CompareTo(a.isCoprime);
            if (coprimeCompare != 0) return coprimeCompare;
            return a.rank.CompareTo(b.rank);
        });

        // Take first K offsets
        var result = new int[k];
        for (var i = 0; i < k; i++)
        {
            result[i] = candidates[i].offset;
        }

        return result;
    }

    private static int Gcd(int a, int b)
    {
        while (b != 0)
        {
            var t = b;
            b = a % b;
            a = t;
        }
        return a;
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
        // Use a combined input and a mixed per-ring seed to generate independent permutations.
        // This is particularly important for simulation-style addresses with identical hostnames
        // and sequential ports.
        var ringSeed = MixRingSeed(seed);

        Span<byte> portBytes = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(portBytes, endpoint.Port);

        Span<byte> input = stackalloc byte[endpoint.Hostname.Length + portBytes.Length];
        endpoint.Hostname.Span.CopyTo(input);
        portBytes.CopyTo(input[endpoint.Hostname.Length..]);

        var hash = XxHash64.HashToUInt64(input, ringSeed);

        // Further mix the ring seed into the final 64-bit value. This prevents different ring seeds
        // from producing orderings that are identical up to a cyclic rotation.
        var mixed = hash ^ BitOperations.RotateLeft((ulong)(uint)ringSeed, 17);
        mixed *= 0x9E3779B97F4A7C15UL;

        return (long)mixed;
    }

    private static int MixRingSeed(int ringIndex)
    {
        // Basic seed mixing to avoid consecutive integers producing correlated results.
        // We keep this deterministic and stable across versions/platforms.
        unchecked
        {
            var x = (uint)ringIndex;
            x ^= x >> 16;
            x *= 0x7FEB352Du;
            x ^= x >> 15;
            x *= 0x846CA68Bu;
            x ^= x >> 16;
            return (int)x;
        }
    }

    private static int FindInsertionPoint(List<MemberInfo> ring, MemberInfo memberInfo, IComparer<MemberInfo> comparator)
    {
        var left = 0;
        var right = ring.Count;
        while (left < right)
        {
            var mid = (left + right) / 2;
            if (comparator.Compare(ring[mid], memberInfo) < 0)
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

    private void ThrowIfSealed()
    {
        if (_isSealed)
        {
            throw new InvalidOperationException("This MembershipViewBuilder has been sealed and cannot be used again. Call ToBuilder() on the MembershipView to create a new builder.");
        }
    }

    /// <summary>
    /// Used to order MemberInfo instances in the different rings by their endpoint hash.
    /// </summary>
    private sealed class AddressComparator(int seed) : IComparer<MemberInfo>
    {
        private readonly int _seed = seed;
        private readonly Dictionary<Endpoint, long> _hashCache = new(EndpointAddressComparer.Instance);

        public int Compare(MemberInfo? x, MemberInfo? y)
        {
            if (x == null && y == null) return 0;
            if (x == null) return -1;
            if (y == null) return 1;

            var hash1 = GetCachedHash(x.Endpoint);
            var hash2 = GetCachedHash(y.Endpoint);
            var hashCompare = hash1.CompareTo(hash2);
            if (hashCompare != 0)
            {
                return hashCompare;
            }

            // Ensure a total ordering for SortedSet.
            // If two endpoints collide on a ring's hash, fall back to a deterministic compare.
            return EndpointAddressComparer.Instance.Compare(x.Endpoint, y.Endpoint);
        }

        public void RemoveMemberInfo(MemberInfo memberInfo) => _hashCache.Remove(memberInfo.Endpoint, out _);

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
