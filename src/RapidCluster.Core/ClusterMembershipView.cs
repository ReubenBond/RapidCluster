using System.Collections.Immutable;
using System.Diagnostics;

namespace RapidCluster;

/// <summary>
/// An immutable snapshot of the cluster membership at a point in time.
/// This is the public-facing view type that uses standard .NET types.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ClusterMembershipView : IEquatable<ClusterMembershipView>, IComparable<ClusterMembershipView>
{
    /// <summary>
    /// Gets an empty membership view with no members.
    /// </summary>
    public static ClusterMembershipView Empty { get; } = new(ConfigurationId.Empty, []);

    /// <summary>
    /// Gets the members in the cluster.
    /// </summary>
    public ImmutableArray<ClusterMember> Members { get; }

    /// <summary>
    /// Gets the configuration identifier for this view.
    /// </summary>
    public ConfigurationId ConfigurationId { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="ClusterMembershipView"/> class.
    /// Creates a new ClusterMembershipView instance.
    /// </summary>
    /// <param name="configurationId">The configuration identifier.</param>
    /// <param name="members">The cluster members.</param>
    internal ClusterMembershipView(
        ConfigurationId configurationId,
        ImmutableArray<ClusterMember> members)
    {
        ConfigurationId = configurationId;
        Members = members;
    }

    /// <inheritdoc/>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the two views have different <see cref="ClusterId"/>s.
    /// </exception>
    public int CompareTo(ClusterMembershipView? other) => other is null ? 1 : ConfigurationId.CompareTo(other.ConfigurationId);

    /// <inheritdoc/>
    public bool Equals(ClusterMembershipView? other) => other is not null && (ReferenceEquals(this, other) || ConfigurationId.Equals(other.ConfigurationId));

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
    public static bool operator <=(ClusterMembershipView? left, ClusterMembershipView? right) => left is null || left.CompareTo(right) <= 0;

    /// <summary>
    /// Determines whether one <see cref="ClusterMembershipView"/> is greater than another.
    /// </summary>
    public static bool operator >(ClusterMembershipView? left, ClusterMembershipView? right) => left?.CompareTo(right) > 0;

    /// <summary>
    /// Determines whether one <see cref="ClusterMembershipView"/> is greater than or equal to another.
    /// </summary>
    public static bool operator >=(ClusterMembershipView? left, ClusterMembershipView? right) => left is null ? right is null : left.CompareTo(right) >= 0;

    private string DebuggerDisplay => $"ClusterMembershipView(Config={ConfigurationId}, Members={Members.Length})";
}
