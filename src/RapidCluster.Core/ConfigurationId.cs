using System.Diagnostics;

namespace RapidCluster;

/// <summary>
/// Represents a configuration identifier as a monotonically incrementing version counter.
/// Each configuration change (node join or leave) results in a new ConfigurationId with
/// an incremented version number.
/// </summary>
/// <remarks>
/// The struct is designed to be wire-compatible with the int64 configurationId in protobuf messages.
/// </remarks>
[DebuggerDisplay("{ToString(),nq}")]
public readonly struct ConfigurationId : IEquatable<ConfigurationId>, IComparable<ConfigurationId>
{
    /// <summary>
    /// The default/empty configuration ID with version 0.
    /// </summary>
    public static readonly ConfigurationId Empty = new(0);

    /// <summary>
    /// Gets the monotonic version counter that increments with each configuration change.
    /// </summary>
    public long Version { get; }

    /// <summary>
    /// Initializes a new ConfigurationId with the specified version.
    /// </summary>
    /// <param name="version">The monotonic version counter.</param>
    public ConfigurationId(long version)
    {
        Version = version;
    }

    /// <summary>
    /// Creates a new ConfigurationId with an incremented version.
    /// </summary>
    /// <returns>A new ConfigurationId with version incremented by 1.</returns>
    public ConfigurationId Next() => new(Version + 1);

    /// <summary>
    /// Converts the ConfigurationId to a 64-bit integer for wire transmission.
    /// </summary>
    /// <returns>A 64-bit integer representation.</returns>
    public long ToInt64() => Version;

    /// <summary>
    /// Creates a ConfigurationId from a 64-bit integer (wire format).
    /// </summary>
    /// <param name="value">The 64-bit integer representation.</param>
    /// <returns>A ConfigurationId.</returns>
    public static ConfigurationId FromInt64(long value) => new(value);

    /// <summary>
    /// Compares this ConfigurationId to another for ordering.
    /// </summary>
    public int CompareTo(ConfigurationId other) => Version.CompareTo(other.Version);

    /// <summary>
    /// Checks equality with another ConfigurationId.
    /// </summary>
    public bool Equals(ConfigurationId other) => Version == other.Version;

    public override bool Equals(object? obj) => obj is ConfigurationId other && Equals(other);

    public override int GetHashCode() => Version.GetHashCode();

    public override string ToString() => $"ConfigurationId(v{Version})";

    public static bool operator ==(ConfigurationId left, ConfigurationId right) => left.Equals(right);
    public static bool operator !=(ConfigurationId left, ConfigurationId right) => !left.Equals(right);
    public static bool operator <(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) < 0;
    public static bool operator <=(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) <= 0;
    public static bool operator >(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) > 0;
    public static bool operator >=(ConfigurationId left, ConfigurationId right) => left.CompareTo(right) >= 0;

    /// <summary>
    /// Implicit conversion to long for backward compatibility.
    /// </summary>
    public static implicit operator long(ConfigurationId configId) => configId.ToInt64();

    /// <summary>
    /// Explicit conversion from long.
    /// </summary>
    public static explicit operator ConfigurationId(long value) => FromInt64(value);
}
