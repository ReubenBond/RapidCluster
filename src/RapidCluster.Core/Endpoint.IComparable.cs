using System.Diagnostics;

namespace RapidCluster.Pb;

/// <summary>
/// Implements IComparable for Endpoint to enable consistent ordering across processes.
/// This is critical for consensus protocols where all nodes must propose the same
/// ordered set of endpoints.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public partial class Endpoint : IComparable<Endpoint>
{
    public int CompareTo(Endpoint? other)
    {
        if (other is null) return 1;

        // Compare by hostname first (lexicographic byte comparison)
        var hostnameComparison = CompareByteStrings(Hostname, other.Hostname);
        if (hostnameComparison != 0) return hostnameComparison;

        // Then by port
        return Port.CompareTo(other.Port);
    }

    private static int CompareByteStrings(Google.Protobuf.ByteString x, Google.Protobuf.ByteString y)
    {
        var minLength = Math.Min(x.Length, y.Length);
        for (var i = 0; i < minLength; i++)
        {
            var cmp = x[i].CompareTo(y[i]);
            if (cmp != 0) return cmp;
        }
        return x.Length.CompareTo(y.Length);
    }

    // Note: == and != operators are intentionally not defined here because
    // protobuf generates Equals() which handles equality semantics.
    // These operators use the protobuf-generated Equals for consistency.
    public static bool operator ==(Endpoint? left, Endpoint? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    public static bool operator !=(Endpoint? left, Endpoint? right) => !(left == right);

    public static bool operator <(Endpoint? left, Endpoint? right)
    {
        if (left is null) return right is not null;
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(Endpoint? left, Endpoint? right)
    {
        if (left is null) return true;
        return left.CompareTo(right) <= 0;
    }

    public static bool operator >(Endpoint? left, Endpoint? right)
    {
        if (left is null) return false;
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(Endpoint? left, Endpoint? right)
    {
        if (left is null) return right is null;
        return left.CompareTo(right) >= 0;
    }

    /// <summary>
    /// Returns the network address as a string in "hostname:port" format.
    /// </summary>
    public string GetNetworkAddressString() => $"{Hostname.ToStringUtf8()}:{Port}";

    private string DebuggerDisplay => $"{GetNetworkAddressString()} (id={NodeId})";
}
