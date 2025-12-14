using System.Collections.Immutable;
using System.Diagnostics;
using System.Net;

namespace RapidCluster;

/// <summary>
/// An immutable snapshot of the cluster membership at a point in time.
/// This is the public-facing view type that uses standard .NET types.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ClusterMembershipView : IEquatable<ClusterMembershipView>, IComparable<ClusterMembershipView>
{
    private static readonly ImmutableHashSet<EndPoint> EmptyMembers = ImmutableHashSet.Create<EndPoint>(EndPointComparer.Instance);
    private static readonly IReadOnlyDictionary<EndPoint, ClusterMetadata> EmptyMetadata = ImmutableDictionary.Create<EndPoint, ClusterMetadata>(EndPointComparer.Instance);

    /// <summary>
    /// An empty membership view with no members.
    /// </summary>
    public static ClusterMembershipView Empty { get; } = new(ConfigurationId.Empty, EmptyMembers, EmptyMetadata);

    /// <summary>
    /// Gets the set of member endpoints in the cluster.
    /// </summary>
    public ImmutableHashSet<EndPoint> Members { get; }

    /// <summary>
    /// Gets the configuration identifier for this view.
    /// </summary>
    public ConfigurationId ConfigurationId { get; }

    /// <summary>
    /// Gets the metadata for all members in the cluster.
    /// </summary>
    public IReadOnlyDictionary<EndPoint, ClusterMetadata> Metadata { get; }

    /// <summary>
    /// Creates a new ClusterMembershipView instance.
    /// </summary>
    /// <param name="configurationId">The configuration identifier.</param>
    /// <param name="members">The set of member endpoints.</param>
    /// <param name="metadata">The metadata for each member.</param>
    internal ClusterMembershipView(
        ConfigurationId configurationId,
        ImmutableHashSet<EndPoint> members,
        IReadOnlyDictionary<EndPoint, ClusterMetadata> metadata)
    {
        ConfigurationId = configurationId;
        Members = members;
        Metadata = metadata;
    }

    /// <inheritdoc/>
    public int CompareTo(ClusterMembershipView? other)
    {
        return other is null ? 1 : ConfigurationId.CompareTo(other.ConfigurationId);
    }

    /// <inheritdoc/>
    public bool Equals(ClusterMembershipView? other)
    {
        if (other is null)
        {
            return false;
        }

        return ReferenceEquals(this, other) ? true : ConfigurationId.Equals(other.ConfigurationId);
    }

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as ClusterMembershipView);

    /// <inheritdoc/>
    public override int GetHashCode() => ConfigurationId.GetHashCode();

    /// <summary>
    /// Determines whether two <see cref="ClusterMembershipView"/> instances are equal.
    /// </summary>
    public static bool operator ==(ClusterMembershipView? left, ClusterMembershipView? right) => left is null ? right is null : left.Equals(right);

    /// <summary>
    /// Determines whether two <see cref="ClusterMembershipView"/> instances are not equal.
    /// </summary>
    public static bool operator !=(ClusterMembershipView? left, ClusterMembershipView? right) => !(left == right);

    /// <summary>
    /// Determines whether one <see cref="ClusterMembershipView"/> is less than another.
    /// </summary>
    public static bool operator <(ClusterMembershipView? left, ClusterMembershipView? right) => left is null ? right is not null : left.CompareTo(right) < 0;

    /// <summary>
    /// Determines whether one <see cref="ClusterMembershipView"/> is less than or equal to another.
    /// </summary>
    public static bool operator <=(ClusterMembershipView? left, ClusterMembershipView? right) => left is null ? true : left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one <see cref="ClusterMembershipView"/> is greater than another.
    /// </summary>
    public static bool operator >(ClusterMembershipView? left, ClusterMembershipView? right) => left is null ? false : left.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one <see cref="ClusterMembershipView"/> is greater than or equal to another.
    /// </summary>
    public static bool operator >=(ClusterMembershipView? left, ClusterMembershipView? right) => left is null ? right is null : left.CompareTo(right) >= 0;

    private string DebuggerDisplay => $"ClusterMembershipView(Config={ConfigurationId}, Members={Members.Count})";
}
