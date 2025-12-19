using System.Diagnostics;

namespace RapidCluster;

/// <summary>
/// Uniquely identifies a Rapid cluster instance. 
/// Computed exactly once during cluster bootstrapping and stays constant for the life of the cluster.
/// </summary>
/// <remarks>
/// Initializes a new ClusterId with the specified value.
/// </remarks>
[DebuggerDisplay("{ToString(),nq}")]
public readonly struct ClusterId(long value) : IEquatable<ClusterId>
{
    /// <summary>
    /// The initial cluster ID (0).
    /// </summary>
    public static readonly ClusterId None = new(0);

    /// <summary>
    /// Gets the underlying 64-bit value.
    /// </summary>
    public long Value { get; } = value;

    public bool Equals(ClusterId other) => Value == other.Value;
    public override bool Equals(object? obj) => obj is ClusterId other && Equals(other);
    public override int GetHashCode() => Value.GetHashCode();
    public override string ToString() => $"ClusterId({Value:X16})";

    public static bool operator ==(ClusterId left, ClusterId right) => left.Equals(right);
    public static bool operator !=(ClusterId left, ClusterId right) => !left.Equals(right);
}
