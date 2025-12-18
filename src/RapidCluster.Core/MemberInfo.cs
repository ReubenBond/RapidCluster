using System.Diagnostics;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Represents a cluster member with its endpoint and metadata.
/// This type combines the network identity (<see cref="Endpoint"/>) with node metadata
/// into a single cohesive type that can be stored in <see cref="MembershipView"/>.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="MemberInfo"/> implements <see cref="IComparable{T}"/> through delegation to
/// <see cref="Endpoint"/>, which compares by hostname and port (ignoring NodeId). This ensures
/// consistent ring ordering across all nodes in the cluster.
/// </para>
/// <para>
/// Equality is based on endpoint address only (hostname and port), ignoring both NodeId and Metadata.
/// This allows <see cref="MemberInfo"/> instances to be used as dictionary keys where node identity
/// is based on network address.
/// </para>
/// </remarks>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MemberInfo : IComparable<MemberInfo>, IEquatable<MemberInfo>
{
    /// <summary>
    /// Gets the network endpoint of this member.
    /// </summary>
    public Endpoint Endpoint { get; }

    /// <summary>
    /// Gets the metadata associated with this member.
    /// </summary>
    public Metadata Metadata { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MemberInfo"/> class.
    /// </summary>
    /// <param name="endpoint">The network endpoint of the member.</param>
    /// <param name="metadata">The metadata associated with the member.</param>
    public MemberInfo(Endpoint endpoint, Metadata metadata)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(metadata);
        Endpoint = endpoint;
        Metadata = metadata;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="MemberInfo"/> class with empty metadata.
    /// </summary>
    /// <param name="endpoint">The network endpoint of the member.</param>
    public MemberInfo(Endpoint endpoint)
        : this(endpoint, new Metadata())
    {
    }

    /// <summary>
    /// Gets the monotonically increasing node ID assigned during join.
    /// This ID is used for Paxos rank computation and is unique across node restarts.
    /// </summary>
    public long NodeId => Endpoint.NodeId;

    /// <summary>
    /// Compares this member to another by endpoint (hostname and port only).
    /// This provides consistent ordering for ring membership.
    /// </summary>
    /// <param name="other">The other member to compare to.</param>
    /// <returns>
    /// A negative value if this member is ordered before the other,
    /// zero if they are equal (same hostname and port),
    /// a positive value if this member is ordered after the other.
    /// </returns>
    public int CompareTo(MemberInfo? other)
    {
        if (other is null) return 1;
        return Endpoint.CompareTo(other.Endpoint);
    }

    /// <summary>
    /// Determines whether this member is equal to another based on endpoint address only.
    /// NodeId and Metadata are ignored for equality comparison.
    /// </summary>
    /// <param name="other">The other member to compare to.</param>
    /// <returns>True if the endpoints have the same hostname and port.</returns>
    public bool Equals(MemberInfo? other)
    {
        if (other is null) return false;
        return EndpointAddressComparer.Instance.Equals(Endpoint, other.Endpoint);
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as MemberInfo);

    /// <inheritdoc/>
    public override int GetHashCode() => EndpointAddressComparer.Instance.GetHashCode(Endpoint);

    /// <inheritdoc/>
    public override string ToString() => $"{Endpoint.GetNetworkAddressString()} (id={NodeId})";

    private string DebuggerDisplay => $"{Endpoint.GetNetworkAddressString()} (id={NodeId}, metadata={Metadata.Metadata_.Count} keys)";

    /// <summary>
    /// Equality operator based on endpoint address (hostname and port).
    /// </summary>
    public static bool operator ==(MemberInfo? left, MemberInfo? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    /// <summary>
    /// Inequality operator based on endpoint address (hostname and port).
    /// </summary>
    public static bool operator !=(MemberInfo? left, MemberInfo? right) => !(left == right);

    /// <summary>
    /// Less than operator based on endpoint comparison.
    /// </summary>
    public static bool operator <(MemberInfo? left, MemberInfo? right)
    {
        if (left is null) return right is not null;
        return left.CompareTo(right) < 0;
    }

    /// <summary>
    /// Less than or equal operator based on endpoint comparison.
    /// </summary>
    public static bool operator <=(MemberInfo? left, MemberInfo? right)
    {
        if (left is null) return true;
        return left.CompareTo(right) <= 0;
    }

    /// <summary>
    /// Greater than operator based on endpoint comparison.
    /// </summary>
    public static bool operator >(MemberInfo? left, MemberInfo? right)
    {
        if (left is null) return false;
        return left.CompareTo(right) > 0;
    }

    /// <summary>
    /// Greater than or equal operator based on endpoint comparison.
    /// </summary>
    public static bool operator >=(MemberInfo? left, MemberInfo? right)
    {
        if (left is null) return right is null;
        return left.CompareTo(right) >= 0;
    }
}

/// <summary>
/// Compares <see cref="MemberInfo"/> instances by endpoint address only.
/// Used with HashSet and Dictionary to ensure proper equality checking by network address.
/// </summary>
internal sealed class MemberInfoAddressComparer : IEqualityComparer<MemberInfo>
{
    public static readonly MemberInfoAddressComparer Instance = new();

    private MemberInfoAddressComparer() { }

    public bool Equals(MemberInfo? x, MemberInfo? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return EndpointAddressComparer.Instance.Equals(x.Endpoint, y.Endpoint);
    }

    public int GetHashCode(MemberInfo obj)
    {
        return EndpointAddressComparer.Instance.GetHashCode(obj.Endpoint);
    }
}

/// <summary>
/// Compares <see cref="MemberInfo"/> instances using full Endpoint comparison (including NodeId).
/// Used with SortedSet to ensure consistent ordering across nodes.
/// </summary>
internal sealed class MemberInfoComparer : IComparer<MemberInfo>
{
    public static readonly MemberInfoComparer Instance = new();

    private MemberInfoComparer() { }

    public int Compare(MemberInfo? x, MemberInfo? y)
    {
        if (ReferenceEquals(x, y)) return 0;
        if (x is null) return -1;
        if (y is null) return 1;
        return x.CompareTo(y);
    }
}
