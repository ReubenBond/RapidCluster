using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Comparer for individual protobuf Endpoint instances that delegates to Endpoint.CompareTo.
/// Used with SortedSet to ensure consistent ordering across nodes.
/// </summary>
internal sealed class ProtobufEndpointComparer : IComparer<Endpoint>
{
    public static readonly ProtobufEndpointComparer Instance = new();

    private ProtobufEndpointComparer() { }

    public int Compare(Endpoint? x, Endpoint? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.CompareTo(y);
    }
}

/// <summary>
/// Compares Endpoint instances by address (hostname and port) only.
/// Ignores NodeId, which is used for Paxos rank computation but not for node identity.
/// Used with HashSet, Dictionary, and SortedDictionary to ensure proper equality checking by network address.
/// </summary>
internal sealed class EndpointAddressComparer : IEqualityComparer<Endpoint>, IComparer<Endpoint>
{
    public static readonly EndpointAddressComparer Instance = new();

    private EndpointAddressComparer() { }

    public bool Equals(Endpoint? x, Endpoint? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return x.Hostname == y.Hostname && x.Port == y.Port;
    }

    public int GetHashCode(Endpoint obj)
    {
        return HashCode.Combine(obj.Hostname, obj.Port);
    }

    public int Compare(Endpoint? x, Endpoint? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        // Compare by hostname first (byte-by-byte comparison)
        var hostnameCompare = x.Hostname.Span.SequenceCompareTo(y.Hostname.Span);
        if (hostnameCompare != 0) return hostnameCompare;

        // Then by port
        return x.Port.CompareTo(y.Port);
    }
}

internal sealed class ListEndpointComparer : IEqualityComparer<List<Endpoint>>
{
    public static readonly ListEndpointComparer Instance = new();

    private ListEndpointComparer() { }

    public bool Equals(List<Endpoint>? x, List<Endpoint>? y)
    {
        if (x == null && y == null) return true;
        if (x == null || y == null) return false;
        return x.SequenceEqual(y);
    }

    public int GetHashCode(List<Endpoint> obj)
    {
        var hash = new HashCode();
        foreach (var endpoint in obj)
        {
            hash.Add(endpoint.GetHashCode());
        }
        return hash.ToHashCode();
    }
}

/// <summary>
/// Comparer for MembershipProposal instances.
/// Two proposals are equal if they have the same configurationId and the same members (by endpoint).
/// </summary>
internal sealed class MembershipProposalComparer : IEqualityComparer<MembershipProposal>, IComparer<MembershipProposal>
{
    public static readonly MembershipProposalComparer Instance = new();

    private MembershipProposalComparer() { }

    public bool Equals(MembershipProposal? x, MembershipProposal? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;

        // Compare by configurationId and member endpoints
        if (x.ConfigurationId.ToConfigurationId() != y.ConfigurationId.ToConfigurationId()) return false;
        if (x.Members.Count != y.Members.Count) return false;

        for (var i = 0; i < x.Members.Count; i++)
        {
            if (ProtobufEndpointComparer.Instance.Compare(x.Members[i], y.Members[i]) != 0)
            {
                return false;
            }
        }
        return true;
    }

    public int GetHashCode(MembershipProposal obj)
    {
        var hash = new HashCode();
        hash.Add(obj.ConfigurationId);
        foreach (var member in obj.Members)
        {
            hash.Add(member?.GetHashCode() ?? 0);
        }
        return hash.ToHashCode();
    }

    /// <summary>
    /// Compares two proposals for ordering. Used to provide deterministic selection when
    /// the Paxos coordinator needs to pick a value and multiple values are possible.
    /// </summary>
    public int Compare(MembershipProposal? x, MembershipProposal? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;

        // First compare by configurationId
        var xConfigId = x.ConfigurationId.ToConfigurationId();
        var yConfigId = y.ConfigurationId.ToConfigurationId();

        // If ClusterIds differ, compare them first for deterministic ordering
        if (xConfigId.ClusterId != yConfigId.ClusterId)
        {
            return xConfigId.ClusterId.Value.CompareTo(yConfigId.ClusterId.Value);
        }

        // Same ClusterId, compare by version
        var versionCompare = xConfigId.Version.CompareTo(yConfigId.Version);
        if (versionCompare != 0) return versionCompare;

        // Then by member count
        var countCompare = x.Members.Count.CompareTo(y.Members.Count);
        if (countCompare != 0) return countCompare;

        // Then compare members lexicographically
        for (var i = 0; i < x.Members.Count; i++)
        {
            var endpointCompare = ProtobufEndpointComparer.Instance.Compare(x.Members[i], y.Members[i]);
            if (endpointCompare != 0) return endpointCompare;
        }

        return 0;
    }
}
