using System.Diagnostics;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Represents a member of the cluster with its endpoint and metadata.
/// This is an internal type used by <see cref="MembershipView"/> to track
/// membership with associated metadata in a unified structure.
/// </summary>
/// <remarks>
/// Equality is based on the endpoint address only (hostname + port), ignoring metadata.
/// This ensures consistent membership semantics where a node's identity is determined
/// by its network address, not its metadata.
/// </remarks>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MemberInfo : IEquatable<MemberInfo>
{
    /// <summary>
    /// Gets the endpoint of this member.
    /// </summary>
    public Endpoint Endpoint { get; }

    /// <summary>
    /// Gets the metadata associated with this member.
    /// </summary>
    public Metadata Metadata { get; }

    /// <summary>
    /// Creates a new MemberInfo instance.
    /// </summary>
    /// <param name="endpoint">The endpoint of the member.</param>
    /// <param name="metadata">The metadata associated with the member.</param>
    public MemberInfo(Endpoint endpoint, Metadata metadata)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        ArgumentNullException.ThrowIfNull(metadata);
        Endpoint = endpoint;
        Metadata = metadata;
    }

    /// <summary>
    /// Creates a new MemberInfo instance with empty metadata.
    /// </summary>
    /// <param name="endpoint">The endpoint of the member.</param>
    public MemberInfo(Endpoint endpoint)
        : this(endpoint, new Metadata())
    {
    }

    /// <summary>
    /// Creates a new MemberInfo with the same endpoint but different metadata.
    /// </summary>
    /// <param name="metadata">The new metadata.</param>
    /// <returns>A new MemberInfo with the updated metadata.</returns>
    public MemberInfo WithMetadata(Metadata metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        return new MemberInfo(Endpoint, metadata);
    }

    /// <summary>
    /// Determines whether this MemberInfo is equal to another.
    /// Equality is based on the endpoint address only (hostname + port), ignoring metadata and NodeId.
    /// </summary>
    public bool Equals(MemberInfo? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        return EndpointAddressComparer.Instance.Equals(Endpoint, other.Endpoint);
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as MemberInfo);

    /// <inheritdoc/>
    public override int GetHashCode() => EndpointAddressComparer.Instance.GetHashCode(Endpoint);

    /// <summary>
    /// Determines whether two MemberInfo instances are equal.
    /// </summary>
    public static bool operator ==(MemberInfo? left, MemberInfo? right) =>
        left is null ? right is null : left.Equals(right);

    /// <summary>
    /// Determines whether two MemberInfo instances are not equal.
    /// </summary>
    public static bool operator !=(MemberInfo? left, MemberInfo? right) => !(left == right);

    private string DebuggerDisplay => $"MemberInfo({Endpoint.Hostname.ToStringUtf8()}:{Endpoint.Port}, NodeId={Endpoint.NodeId})";
}

/// <summary>
/// Comparer for MemberInfo that compares by endpoint address only (hostname + port),
/// ignoring metadata and NodeId. Used for consistent membership operations.
/// </summary>
internal sealed class MemberInfoAddressComparer : IEqualityComparer<MemberInfo>
{
    /// <summary>
    /// Gets the singleton instance of this comparer.
    /// </summary>
    public static MemberInfoAddressComparer Instance { get; } = new();

    private MemberInfoAddressComparer() { }

    /// <inheritdoc/>
    public bool Equals(MemberInfo? x, MemberInfo? y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null || y is null)
        {
            return false;
        }

        return EndpointAddressComparer.Instance.Equals(x.Endpoint, y.Endpoint);
    }

    /// <inheritdoc/>
    public int GetHashCode(MemberInfo obj)
    {
        ArgumentNullException.ThrowIfNull(obj);
        return EndpointAddressComparer.Instance.GetHashCode(obj.Endpoint);
    }
}
