using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for RankComparer equality, comparison, and hashing functionality.
/// </summary>
public class RankComparerTests
{
    private static readonly RankComparer Comparer = RankComparer.Instance;

    [Fact]
    public void EqualsBothNullReturnsTrue() => Assert.True(Comparer.Equals(x: null, y: null));

    [Fact]
    public void EqualsFirstNullReturnsFalse()
    {
        var rank = new Rank { Round = 1, NodeIndex = 1 };
        Assert.False(Comparer.Equals(x: null, rank));
    }

    [Fact]
    public void EqualsSecondNullReturnsFalse()
    {
        var rank = new Rank { Round = 1, NodeIndex = 1 };
        Assert.False(Comparer.Equals(rank, y: null));
    }

    [Fact]
    public void EqualsSameValuesReturnsTrue()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.True(Comparer.Equals(rank1, rank2));
    }

    [Fact]
    public void EqualsDifferentRoundReturnsFalse()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 6, NodeIndex = 10 };

        Assert.False(Comparer.Equals(rank1, rank2));
    }

    [Fact]
    public void EqualsDifferentNodeIndexReturnsFalse()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 11 };

        Assert.False(Comparer.Equals(rank1, rank2));
    }

    [Fact]
    public void EqualsSameInstanceReturnsTrue()
    {
        var rank = new Rank { Round = 5, NodeIndex = 10 };
        Assert.True(Comparer.Equals(rank, rank));
    }

    [Fact]
    public void EqualsZeroRanksReturnsTrue()
    {
        var rank1 = new Rank { Round = 0, NodeIndex = 0 };
        var rank2 = new Rank { Round = 0, NodeIndex = 0 };

        Assert.True(Comparer.Equals(rank1, rank2));
    }

    [Fact]
    public void GetHashCodeEqualRanksReturnsSameHashCode()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.Equal(Comparer.GetHashCode(rank1), Comparer.GetHashCode(rank2));
    }

    [Fact]
    public void GetHashCodeDifferentRanksLikelyDifferentHashCodes()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 11 };

        Assert.NotEqual(Comparer.GetHashCode(rank1), Comparer.GetHashCode(rank2));
    }

    [Fact]
    public void GetHashCodeConsistentForSameRank()
    {
        var rank = new Rank { Round = 100, NodeIndex = 200 };

        var hash1 = Comparer.GetHashCode(rank);
        var hash2 = Comparer.GetHashCode(rank);

        Assert.Equal(hash1, hash2);
    }

    [Fact]
    public void CompareBothNullReturnsZero() => Assert.Equal(0, Comparer.Compare(x: null, y: null));

    [Fact]
    public void CompareFirstNullReturnsNegative()
    {
        var rank = new Rank { Round = 1, NodeIndex = 1 };
        Assert.True(Comparer.Compare(x: null, rank) < 0);
    }

    [Fact]
    public void CompareSecondNullReturnsPositive()
    {
        var rank = new Rank { Round = 1, NodeIndex = 1 };
        Assert.True(Comparer.Compare(rank, y: null) > 0);
    }

    [Fact]
    public void CompareEqualRanksReturnsZero()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.Equal(0, Comparer.Compare(rank1, rank2));
    }

    [Fact]
    public void CompareHigherRoundReturnsPositive()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 4, NodeIndex = 10 };

        Assert.True(Comparer.Compare(rank1, rank2) > 0);
    }

    [Fact]
    public void CompareLowerRoundReturnsNegative()
    {
        var rank1 = new Rank { Round = 4, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.True(Comparer.Compare(rank1, rank2) < 0);
    }

    [Fact]
    public void CompareSameRoundHigherNodeIndexReturnsPositive()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 11 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.True(Comparer.Compare(rank1, rank2) > 0);
    }

    [Fact]
    public void CompareSameRoundLowerNodeIndexReturnsNegative()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 11 };

        Assert.True(Comparer.Compare(rank1, rank2) < 0);
    }

    [Fact]
    public void DictionaryCanUseRankAsKey()
    {
        var dict = new Dictionary<Rank, string>(Comparer);

        var key1 = new Rank { Round = 1, NodeIndex = 1 };
        dict[key1] = "value1";

        var key2 = new Rank { Round = 1, NodeIndex = 1 };
        Assert.True(dict.TryGetValue(key2, out var value));
        Assert.Equal("value1", value);
    }

    [Fact]
    public void DictionaryDifferentKeysStored()
    {
        var dict = new Dictionary<Rank, int>(Comparer);

        var key1 = new Rank { Round = 1, NodeIndex = 1 };
        var key2 = new Rank { Round = 1, NodeIndex = 2 };

        dict[key1] = 100;
        dict[key2] = 200;

        Assert.Equal(2, dict.Count);
        Assert.Equal(100, dict[key1]);
        Assert.Equal(200, dict[key2]);
    }

    [Fact]
    public void SortOrdersCorrectly()
    {
        var ranks = new List<Rank>
        {
            new() { Round = 3, NodeIndex = 1 },
            new() { Round = 1, NodeIndex = 5 },
            new() { Round = 2, NodeIndex = 3 },
            new() { Round = 1, NodeIndex = 2 },
        };

        ranks.Sort(Comparer);

        Assert.Equal(1, ranks[0].Round);
        Assert.Equal(2, ranks[0].NodeIndex);
        Assert.Equal(1, ranks[1].Round);
        Assert.Equal(5, ranks[1].NodeIndex);
        Assert.Equal(2, ranks[2].Round);
        Assert.Equal(3, ranks[2].NodeIndex);
        Assert.Equal(3, ranks[3].Round);
        Assert.Equal(1, ranks[3].NodeIndex);
    }

    [Fact]
    public void OrderByDescendingOrdersCorrectly()
    {
        var ranks = new List<Rank>
        {
            new() { Round = 1, NodeIndex = 1 },
            new() { Round = 3, NodeIndex = 1 },
            new() { Round = 2, NodeIndex = 1 },
        };

        var ordered = ranks.OrderDescending(Comparer).ToList();

        Assert.Equal(3, ordered[0].Round);
        Assert.Equal(2, ordered[1].Round);
        Assert.Equal(1, ordered[2].Round);
    }

    [Fact]
    public void InstanceReturnsSameInstance()
    {
        var instance1 = RankComparer.Instance;
        var instance2 = RankComparer.Instance;

        Assert.Same(instance1, instance2);
    }
}
