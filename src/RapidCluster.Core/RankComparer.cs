using RapidCluster.Pb;

namespace RapidCluster;

internal sealed class RankComparer : IEqualityComparer<Rank>, IComparer<Rank>
{
    public static readonly RankComparer Instance = new();

    private RankComparer() { }

    public bool Equals(Rank? x, Rank? y)
    {
        if (x == null && y == null) return true;
        if (x == null || y == null) return false;
        return x.Equals(y);
    }

    public int GetHashCode(Rank obj) => HashCode.Combine(obj.Round, obj.NodeIndex);

    public int Compare(Rank? x, Rank? y)
    {
        if (x == null && y == null) return 0;
        if (x == null) return -1;
        if (y == null) return 1;
        return x.CompareTo(y);
    }
}

