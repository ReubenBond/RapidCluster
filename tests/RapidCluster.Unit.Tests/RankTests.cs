using CsCheck;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Comprehensive tests for Rank comparison operators and IComparable implementation.
/// </summary>
public class RankTests
{
    [Fact]
    public void CompareToHigherRoundReturnsPositive()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 2, NodeIndex = 0 };

        Assert.True(rank2.CompareTo(rank1) > 0);
    }

    [Fact]
    public void CompareToLowerRoundReturnsNegative()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 2, NodeIndex = 0 };

        Assert.True(rank1.CompareTo(rank2) < 0);
    }

    [Fact]
    public void CompareToSameRoundHigherNodeIndexReturnsPositive()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 20 };

        Assert.True(rank2.CompareTo(rank1) > 0);
    }

    [Fact]
    public void CompareToSameRoundLowerNodeIndexReturnsNegative()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 20 };

        Assert.True(rank1.CompareTo(rank2) < 0);
    }

    [Fact]
    public void CompareToEqualRanksReturnsZero()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.Equal(0, rank1.CompareTo(rank2));
    }

    [Fact]
    public void CompareToNullRankReturnsPositive()
    {
        var rank = new Rank { Round = 1, NodeIndex = 1 };

        Assert.True(rank.CompareTo(other: null) > 0);
    }

    [Fact]
    public void CompareToZeroRankComparedCorrectly()
    {
        var zeroRank = new Rank { Round = 0, NodeIndex = 0 };
        var nonZeroRank = new Rank { Round = 1, NodeIndex = 0 };

        Assert.True(nonZeroRank.CompareTo(zeroRank) > 0);
        Assert.True(zeroRank.CompareTo(nonZeroRank) < 0);
    }

    [Fact]
    public void CompareToNegativeNodeIndexHandledCorrectly()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = -5 };
        var rank2 = new Rank { Round = 1, NodeIndex = 5 };

        Assert.True(rank1.CompareTo(rank2) < 0);
    }

    [Fact]
    public void CompareToLargeValuesHandledCorrectly()
    {
        var rank1 = new Rank { Round = int.MaxValue, NodeIndex = int.MaxValue };
        var rank2 = new Rank { Round = int.MaxValue, NodeIndex = int.MaxValue - 1 };

        Assert.True(rank1.CompareTo(rank2) > 0);
    }

    [Fact]
    public void EqualityOperatorEqualRanksReturnsTrue()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.True(rank1.Equals(rank2));
    }

    [Fact]
    public void EqualityOperatorDifferentRoundReturnsFalse()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 6, NodeIndex = 10 };

        Assert.False(rank1.Equals(rank2));
    }

    [Fact]
    public void EqualityOperatorDifferentNodeIndexReturnsFalse()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 11 };

        Assert.False(rank1.Equals(rank2));
    }

    [Fact]
    public void EqualityOperatorBothNullReturnsTrue()
    {
        Rank? rank1 = null;
        Rank? rank2 = null;

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.True(rank1 == rank2);
#pragma warning restore CA1508
    }

    [Fact]
    public void EqualityOperatorLeftNullReturnsFalse()
    {
        Rank? rank1 = null;
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.False(rank1 == rank2);
#pragma warning restore CA1508
    }

    [Fact]
    public void EqualityOperatorRightNullReturnsFalse()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 1 };
        Rank? rank2 = null;

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.False(rank1 == rank2);
#pragma warning restore CA1508
    }

    [Fact]
    public void InequalityOperatorEqualRanksReturnsFalse()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 5, NodeIndex = 10 };

        Assert.False(rank1 != rank2);
    }

    [Fact]
    public void InequalityOperatorDifferentRanksReturnsTrue()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 10 };
        var rank2 = new Rank { Round = 6, NodeIndex = 10 };

        Assert.True(rank1 != rank2);
    }

    [Fact]
    public void LessThanOperatorSmallerRankReturnsTrue()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 2, NodeIndex = 0 };

        Assert.True(rank1 < rank2);
    }

    [Fact]
    public void LessThanOperatorLargerRankReturnsFalse()
    {
        var rank1 = new Rank { Round = 2, NodeIndex = 0 };
        var rank2 = new Rank { Round = 1, NodeIndex = 0 };

        Assert.False(rank1 < rank2);
    }

    [Fact]
    public void LessThanOperatorEqualRanksReturnsFalse()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 1 };
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

        Assert.False(rank1 < rank2);
    }

    [Fact]
    public void LessThanOperatorLeftNullReturnsTrue()
    {
        Rank? rank1 = null;
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

        Assert.True(rank1 < rank2);
    }

    [Fact]
    public void LessThanOperatorRightNullReturnsFalse()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 1 };
        Rank? rank2 = null;

        Assert.False(rank1 < rank2);
    }

    [Fact]
    public void LessThanOperatorBothNullReturnsFalse()
    {
        Rank? rank1 = null;
        Rank? rank2 = null;

        Assert.False(rank1 < rank2);
    }

    [Fact]
    public void GreaterThanOperatorLargerRankReturnsTrue()
    {
        var rank1 = new Rank { Round = 2, NodeIndex = 0 };
        var rank2 = new Rank { Round = 1, NodeIndex = 0 };

        Assert.True(rank1 > rank2);
    }

    [Fact]
    public void GreaterThanOperatorSmallerRankReturnsFalse()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 2, NodeIndex = 0 };

        Assert.False(rank1 > rank2);
    }

    [Fact]
    public void GreaterThanOperatorEqualRanksReturnsFalse()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 1 };
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

        Assert.False(rank1 > rank2);
    }

    [Fact]
    public void LessThanOrEqualOperatorSmallerRankReturnsTrue()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 2, NodeIndex = 0 };

        Assert.True(rank1 <= rank2);
    }

    [Fact]
    public void LessThanOrEqualOperatorEqualRanksReturnsTrue()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 1 };
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

        Assert.True(rank1 <= rank2);
    }

    [Fact]
    public void GreaterThanOrEqualOperatorLargerRankReturnsTrue()
    {
        var rank1 = new Rank { Round = 2, NodeIndex = 0 };
        var rank2 = new Rank { Round = 1, NodeIndex = 0 };

        Assert.True(rank1 >= rank2);
    }

    [Fact]
    public void GreaterThanOrEqualOperatorEqualRanksReturnsTrue()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 1 };
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

        Assert.True(rank1 >= rank2);
    }

    [Fact]
    public void RankRoundDifferenceOverridesNodeIndex()
    {
        // Higher round should win even if node index is smaller
        var rank1 = new Rank { Round = 2, NodeIndex = 0 };
        var rank2 = new Rank { Round = 1, NodeIndex = 100 };

        Assert.True(rank1 > rank2);
        Assert.True(rank1.CompareTo(rank2) > 0);
    }

    [Fact]
    public void RankSortingOrdersCorrectly()
    {
        var ranks = new List<Rank>
        {
            new() { Round = 3, NodeIndex = 1 },
            new() { Round = 1, NodeIndex = 5 },
            new() { Round = 2, NodeIndex = 3 },
            new() { Round = 1, NodeIndex = 2 },
            new() { Round = 2, NodeIndex = 1 },
        };

        ranks.Sort();

        Assert.Equal(1, ranks[0].Round);
        Assert.Equal(2, ranks[0].NodeIndex);
        Assert.Equal(1, ranks[1].Round);
        Assert.Equal(5, ranks[1].NodeIndex);
        Assert.Equal(2, ranks[2].Round);
        Assert.Equal(1, ranks[2].NodeIndex);
    }

    [Fact]
    public void Property_CompareTo_Is_Transitive()
    {
        Gen.Int[0, 100].Select(Gen.Int[0, 100], Gen.Int[0, 100],
                   Gen.Int[0, 100], Gen.Int[0, 100], Gen.Int[0, 100])
            .Sample((r1, n1, r2, n2, r3, n3) =>
            {
                var rank1 = new Rank { Round = r1, NodeIndex = n1 };
                var rank2 = new Rank { Round = r2, NodeIndex = n2 };
                var rank3 = new Rank { Round = r3, NodeIndex = n3 };

                // If rank1 <= rank2 and rank2 <= rank3, then rank1 <= rank3
                if (rank1.CompareTo(rank2) <= 0 && rank2.CompareTo(rank3) <= 0)
                {
                    return rank1.CompareTo(rank3) <= 0;
                }

                return true;
            });
    }

    [Fact]
    public void Property_CompareTo_Is_Antisymmetric()
    {
        Gen.Int[0, 100].Select(Gen.Int[0, 100], Gen.Int[0, 100], Gen.Int[0, 100])
            .Sample((r1, n1, r2, n2) =>
            {
                var rank1 = new Rank { Round = r1, NodeIndex = n1 };
                var rank2 = new Rank { Round = r2, NodeIndex = n2 };

                // If rank1 <= rank2 and rank2 <= rank1, then rank1 == rank2
                return rank1.CompareTo(rank2) > 0 || rank2.CompareTo(rank1) > 0 || rank1.CompareTo(rank2) == 0;
            });
    }

    [Fact]
    public void Property_Higher_Round_Means_Higher_Rank()
    {
        Gen.Int[0, 100].Select(Gen.Int[0, 100], Gen.Int[0, 100])
            .Where(t => t.Item1 != t.Item2)
            .Sample((round1, round2, nodeIndex) =>
            {
                var rank1 = new Rank { Round = round1, NodeIndex = nodeIndex };
                var rank2 = new Rank { Round = round2, NodeIndex = nodeIndex };

                // Higher round number should mean higher rank
                if (round1 > round2)
                {
                    return rank1.CompareTo(rank2) > 0;
                }

                return rank1.CompareTo(rank2) < 0;
            });
    }
}
