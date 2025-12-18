using System.Diagnostics;
using System.IO.Hashing;
using System.Text;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Represents a configuration identifier combining a monotonically incrementing version counter
/// with a cluster identifier. The cluster ID is a hash of the initial seed configuration,
/// ensuring that configurations from different clusters can be distinguished.
/// </summary>
/// <remarks>
/// <para>
/// Each configuration change (node join or leave) results in a new ConfigurationId with
/// an incremented version number, while the cluster ID remains constant throughout the
/// cluster's lifetime.
/// </para>
/// <para>
/// Comparison operators (&lt;, &gt;, etc.) throw if the cluster IDs don't match, as comparing
/// configurations from different clusters is not meaningful. Equality checks return false
/// for different cluster IDs without throwing.
/// </para>
/// </remarks>
[DebuggerDisplay("{ToString(),nq}")]
public readonly struct ConfigurationId : IEquatable<ConfigurationId>, IComparable<ConfigurationId>
{
    /// <summary>
    /// The default/empty configuration ID with version 0 and no cluster ID.
    /// </summary>
    public static readonly ConfigurationId Empty = new(0, 0);

    /// <summary>
    /// Gets the monotonic version counter that increments with each configuration change.
    /// </summary>
    public long Version { get; }

    /// <summary>
    /// Gets the cluster identifier, which is a hash of the initial seed configuration.
    /// This value is computed once during bootstrap and remains constant throughout
    /// the cluster's lifetime.
    /// </summary>
    public long ClusterId { get; }

    /// <summary>
    /// Initializes a new ConfigurationId with the specified version and cluster ID.
    /// </summary>
    /// <param name="version">The monotonic version counter.</param>
    /// <param name="clusterId">The cluster identifier hash.</param>
    public ConfigurationId(long version, long clusterId = 0)
    {
        Version = version;
        ClusterId = clusterId;
    }

    /// <summary>
    /// Creates a new ConfigurationId with an incremented version, preserving the cluster ID.
    /// </summary>
    /// <returns>A new ConfigurationId with version incremented by 1.</returns>
    public ConfigurationId Next() => new(Version + 1, ClusterId);

    /// <summary>
    /// Creates the initial configuration ID (version 1) with a cluster ID computed
    /// from the sorted seed endpoints.
    /// </summary>
    /// <param name="sortedSeedEndpoints">The seed endpoints, sorted deterministically.
    /// Only hostname and port are used; node IDs are ignored.</param>
    /// <returns>A ConfigurationId with version 1 and the computed cluster ID.</returns>
    public static ConfigurationId CreateInitial(IEnumerable<Endpoint> sortedSeedEndpoints)
    {
        ArgumentNullException.ThrowIfNull(sortedSeedEndpoints);
        var clusterId = ComputeClusterId(sortedSeedEndpoints);
        return new ConfigurationId(1, clusterId);
    }

    /// <summary>
    /// Computes the cluster ID by hashing the concatenated endpoint addresses.
    /// </summary>
    private static long ComputeClusterId(IEnumerable<Endpoint> endpoints)
    {
        // Build a deterministic string representation of all endpoints
        var sb = new StringBuilder();
        foreach (var endpoint in endpoints)
        {
            if (sb.Length > 0)
            {
                sb.Append('|');
            }
            sb.Append(endpoint.Hostname.ToStringUtf8());
            sb.Append(':');
            sb.Append(endpoint.Port);
        }

        // Hash the string using XxHash64
        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        return (long)XxHash64.HashToUInt64(bytes);
    }

    /// <summary>
    /// Converts this ConfigurationId to its protobuf representation.
    /// </summary>
    /// <returns>A protobuf ConfigurationId message.</returns>
    public Pb.ConfigurationId ToProtobuf() => new() { Version = Version, ClusterId = ClusterId };

    /// <summary>
    /// Creates a ConfigurationId from its protobuf representation.
    /// </summary>
    /// <param name="pb">The protobuf ConfigurationId message.</param>
    /// <returns>A ConfigurationId struct.</returns>
    public static ConfigurationId FromProtobuf(Pb.ConfigurationId? pb) =>
        pb is null ? Empty : new ConfigurationId(pb.Version, pb.ClusterId);

    /// <summary>
    /// Compares this ConfigurationId to another for ordering.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the cluster IDs don't match, as configurations from different clusters
    /// cannot be meaningfully compared.
    /// </exception>
    public int CompareTo(ConfigurationId other)
    {
        if (ClusterId != other.ClusterId)
        {
            throw new InvalidOperationException(
                $"Cannot compare ConfigurationIds from different clusters: 0x{ClusterId:X} vs 0x{other.ClusterId:X}");
        }
        return Version.CompareTo(other.Version);
    }

    /// <summary>
    /// Checks equality with another ConfigurationId.
    /// Both version and cluster ID must match for equality.
    /// </summary>
    public bool Equals(ConfigurationId other) => Version == other.Version && ClusterId == other.ClusterId;

    public override bool Equals(object? obj) => obj is ConfigurationId other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Version, ClusterId);

    public override string ToString() => ClusterId == 0
        ? $"ConfigurationId(v{Version})"
        : $"ConfigurationId(v{Version}, c{ClusterId:X8})";

    public static bool operator ==(ConfigurationId left, ConfigurationId right) => left.Equals(right);
    public static bool operator !=(ConfigurationId left, ConfigurationId right) => !left.Equals(right);

#pragma warning disable CA1065 // Do not raise exceptions in unexpected locations - intentional for cross-cluster comparison

    /// <summary>
    /// Compares two ConfigurationIds for ordering.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the cluster IDs don't match.
    /// </exception>
    public static bool operator <(ConfigurationId left, ConfigurationId right)
    {
        if (left.ClusterId != right.ClusterId)
        {
            throw new InvalidOperationException(
                $"Cannot compare ConfigurationIds from different clusters: 0x{left.ClusterId:X} vs 0x{right.ClusterId:X}");
        }
        return left.Version < right.Version;
    }

    /// <summary>
    /// Compares two ConfigurationIds for ordering.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the cluster IDs don't match.
    /// </exception>
    public static bool operator <=(ConfigurationId left, ConfigurationId right)
    {
        if (left.ClusterId != right.ClusterId)
        {
            throw new InvalidOperationException(
                $"Cannot compare ConfigurationIds from different clusters: 0x{left.ClusterId:X} vs 0x{right.ClusterId:X}");
        }
        return left.Version <= right.Version;
    }

    /// <summary>
    /// Compares two ConfigurationIds for ordering.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the cluster IDs don't match.
    /// </exception>
    public static bool operator >(ConfigurationId left, ConfigurationId right)
    {
        if (left.ClusterId != right.ClusterId)
        {
            throw new InvalidOperationException(
                $"Cannot compare ConfigurationIds from different clusters: 0x{left.ClusterId:X} vs 0x{right.ClusterId:X}");
        }
        return left.Version > right.Version;
    }

    /// <summary>
    /// Compares two ConfigurationIds for ordering.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// Thrown if the cluster IDs don't match.
    /// </exception>
    public static bool operator >=(ConfigurationId left, ConfigurationId right)
    {
        if (left.ClusterId != right.ClusterId)
        {
            throw new InvalidOperationException(
                $"Cannot compare ConfigurationIds from different clusters: 0x{left.ClusterId:X} vs 0x{right.ClusterId:X}");
        }
        return left.Version >= right.Version;
    }

#pragma warning restore CA1065
}
