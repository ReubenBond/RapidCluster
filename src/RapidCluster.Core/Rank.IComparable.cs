using System.Diagnostics;
using System.Globalization;

namespace RapidCluster.Pb;

[DebuggerDisplay("{DebuggerDisplay,nq}")]
public partial class Rank : IComparable<Rank>
{
    public int CompareTo(Rank? other)
    {
        if (other == null) return 1;

        var roundCmp = Round.CompareTo(other.Round);
        if (roundCmp != 0) return roundCmp;
        return NodeIndex.CompareTo(other.NodeIndex);
    }

    public static bool operator ==(Rank? left, Rank? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    public static bool operator !=(Rank? left, Rank? right) => !(left == right);

    public static bool operator <(Rank? left, Rank? right)
    {
        if (left is null) return right is not null;
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(Rank? left, Rank? right)
    {
        if (left is null) return true;
        return left.CompareTo(right) <= 0;
    }

    public static bool operator >(Rank? left, Rank? right)
    {
        if (left is null) return false;
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(Rank? left, Rank? right)
    {
        if (left is null) return right is null;
        return left.CompareTo(right) >= 0;
    }

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"Rank({Round}:{NodeIndex})");
}
