using System.Diagnostics;

namespace RapidCluster;

/// <summary>
/// Represents a configuration identifier that combines a stable <see cref="ClusterId"/>
/// with a monotonically incrementing version counter.
/// </summary>
/// <remarks>
/// Initializes a new ConfigurationId with the specified cluster ID and version.
/// </remarks>
[DebuggerDisplay("{ToString(),nq}")]
public readonly struct ConfigurationId(ClusterId clusterId, long version) : IEquatable<ConfigurationId>, IComparable<ConfigurationId>
{
    /// <summary>
    /// The default/empty configuration ID with ClusterId.None and version 0.
    /// </summary>
    public static readonly ConfigurationId Empty = new(ClusterId.None, 0);

    /// <summary>
    /// Gets the unique cluster identifier.
    /// </summary>
    public ClusterId ClusterId { get; } = clusterId;

    /// <summary>
    /// Gets the monotonic version counter that increments with each configuration change.
    /// </summary>
    public long Version { get; } = version;

    /// <summary>
    /// Creates a new ConfigurationId with an incremented version, preserving the ClusterId.
    /// </summary>
    /// <returns>A new ConfigurationId with version incremented by 1.</returns>
    public ConfigurationId Next() => new(ClusterId, Version + 1);

    /// <summary>
    /// Compares this ConfigurationId to another for ordering.
    /// Comparisons are only valid when ClusterIds match exactly.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if ClusterIds do not match.</exception>
    public int CompareTo(ConfigurationId other)
    {
        if (ClusterId != other.ClusterId)
        {
            throw new InvalidOperationException(
                $"Cannot compare ConfigurationIds with different ClusterIds: {ClusterId} vs {other.ClusterId}");
        }

        return Version.CompareTo(other.Version);
    }

    /// <summary>
    /// Checks equality with another ConfigurationId.
    /// Both ClusterId and Version must match.
    /// </summary>
    public bool Equals(ConfigurationId other) => ClusterId == other.ClusterId && Version == other.Version;

    public override bool Equals(object? obj) => obj is ConfigurationId other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(ClusterId, Version);

    public override string ToString() => $"ConfigurationId({ClusterId}, v{Version})";

    public static bool operator ==(ConfigurationId left, ConfigurationId right) => left.Equals(right);

    public static bool operator !=(ConfigurationId left, ConfigurationId right) => !left.Equals(right);

    public static bool operator <(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) < 0;

    public static bool operator <=(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) <= 0;

    public static bool operator >(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) > 0;

    public static bool operator >=(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Converts to the protobuf message format.
    /// </summary>
    public Pb.ConfigurationId ToProtobuf() => new()
    {
        ClusterId = (ulong)ClusterId.Value,
        Version = Version,
    };

    /// <summary>
    /// Creates from the protobuf message format.
    /// </summary>
    public static ConfigurationId FromProtobuf(Pb.ConfigurationId? proto)
    {
        if (proto is null) return Empty;
        return new(new ClusterId((long)proto.ClusterId), proto.Version);
    }
}

/// <summary>
/// Extension methods for converting between ConfigurationId and its protobuf representation.
/// </summary>
internal static class ConfigurationIdExtensions
{
    public static ConfigurationId ToConfigurationId(this Pb.ConfigurationId? proto) => ConfigurationId.FromProtobuf(proto);
}
