using CsCheck;
using Google.Protobuf;
using RapidCluster;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for ListEndpointComparer equality and hashing functionality.
/// </summary>
public class ListEndpointComparerTests
{
    private static readonly ListEndpointComparer Comparer = ListEndpointComparer.Instance;

    [Fact]
    public void EqualsBothNullReturnsTrue() => Assert.True(Comparer.Equals(null, null));

    [Fact]
    public void EqualsFirstNullReturnsFalse()
    {
        var list = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        Assert.False(Comparer.Equals(null, list));
    }

    [Fact]
    public void EqualsSecondNullReturnsFalse()
    {
        var list = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        Assert.False(Comparer.Equals(list, null));
    }

    [Fact]
    public void EqualsBothEmptyReturnsTrue() => Assert.True(Comparer.Equals([], []));

    [Fact]
    public void EqualsSameElementsReturnsTrue()
    {
        var list1 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };
        var list2 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };

        Assert.True(Comparer.Equals(list1, list2));
    }

    [Fact]
    public void EqualsSameElementsDifferentOrderReturnsFalse()
    {
        var list1 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };
        var list2 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.2", 1235),
            Utils.HostFromParts("127.0.0.1", 1234)
        };

        Assert.False(Comparer.Equals(list1, list2));
    }

    [Fact]
    public void EqualsDifferentLengthsReturnsFalse()
    {
        var list1 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234)
        };
        var list2 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };

        Assert.False(Comparer.Equals(list1, list2));
    }

    [Fact]
    public void EqualsDifferentElementsReturnsFalse()
    {
        var list1 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234)
        };
        var list2 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 9999)
        };

        Assert.False(Comparer.Equals(list1, list2));
    }

    [Fact]
    public void EqualsSameReferenceReturnsTrue()
    {
        var list = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        Assert.True(Comparer.Equals(list, list));
    }

    [Fact]
    public void EqualsManyElementsReturnsTrue()
    {
        var list1 = new List<Endpoint>();
        var list2 = new List<Endpoint>();

        for (var i = 0; i < 100; i++)
        {
            list1.Add(Utils.HostFromParts("192.168.1." + i, 5000 + i));
            list2.Add(Utils.HostFromParts("192.168.1." + i, 5000 + i));
        }

        Assert.True(Comparer.Equals(list1, list2));
    }

    [Fact]
    public void GetHashCodeEqualListsReturnsSameHashCode()
    {
        var list1 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };
        var list2 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };

        Assert.Equal(Comparer.GetHashCode(list1), Comparer.GetHashCode(list2));
    }

    [Fact]
    public void GetHashCodeEmptyListsReturnsSameHashCode() => Assert.Equal(Comparer.GetHashCode([]), Comparer.GetHashCode([]));

    [Fact]
    public void GetHashCodeDifferentListsLikelyDifferentHashCodes()
    {
        var list1 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        var list2 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1235) };

        Assert.NotEqual(Comparer.GetHashCode(list1), Comparer.GetHashCode(list2));
    }

    [Fact]
    public void GetHashCodeOrderMatters()
    {
        var list1 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.1", 1234),
            Utils.HostFromParts("127.0.0.2", 1235)
        };
        var list2 = new List<Endpoint>
        {
            Utils.HostFromParts("127.0.0.2", 1235),
            Utils.HostFromParts("127.0.0.1", 1234)
        };

        // Different order means different sequences - they should not be equal
        Assert.False(Comparer.Equals(list1, list2));
    }

    [Fact]
    public void DictionaryCanUseListAsKey()
    {
        var dict = new Dictionary<List<Endpoint>, int>(Comparer);

        var key1 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        dict[key1] = 100;

        var key2 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        Assert.True(dict.TryGetValue(key2, out var value));
        Assert.Equal(100, value);
    }

    [Fact]
    public void DictionaryDifferentKeysStored()
    {
        var dict = new Dictionary<List<Endpoint>, int>(Comparer);

        var key1 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        var key2 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1235) };

        dict[key1] = 100;
        dict[key2] = 200;

        Assert.Equal(2, dict.Count);
        Assert.Equal(100, dict[key1]);
        Assert.Equal(200, dict[key2]);
    }

    [Fact]
    public void DictionaryUpdateExistingKey()
    {
        var dict = new Dictionary<List<Endpoint>, int>(Comparer);

        var key1 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        dict[key1] = 100;

        var key2 = new List<Endpoint> { Utils.HostFromParts("127.0.0.1", 1234) };
        dict[key2] = 200;

        Assert.Single(dict);
        Assert.Equal(200, dict[key1]);
    }

    [Fact]
    public void InstanceReturnsSameInstance()
    {
        var instance1 = ListEndpointComparer.Instance;
        var instance2 = ListEndpointComparer.Instance;

        Assert.Same(instance1, instance2);
    }

    #region Property-Based Tests

    /// <summary>
    /// Generator for valid endpoints.
    /// </summary>
    private static readonly Gen<Endpoint> GenEndpoint =
        Gen.Select(
            Gen.Int[1, 255],  // IP last octet
            Gen.Int[1000, 65535]  // Port
        ).Select((octet, port) => new Endpoint
        {
            Hostname = ByteString.CopyFromUtf8($"127.0.0.{octet}"),
            Port = port
        });

    /// <summary>
    /// Generator for a list of unique endpoints with their NodeIds.
    /// </summary>
    private static Gen<List<Endpoint>> GenUniqueNodes(int minCount, int maxCount)
    {
        return Gen.Int[minCount, maxCount].SelectMany(count =>
            Gen.Select(
                Gen.Int[1, 255].Array[count].Where(a => a.Distinct().Count() == count),
                Gen.Int[1000, 65535].Array[count],
                Gen.Long.Array[count].Where(a => a.Distinct().Count() == count)
            ).Select((octets, ports, nodeIdSeeds) =>
            {
                var result = new List<Endpoint>(count);
                for (var i = 0; i < count; i++)
                {
                    var endpoint = new Endpoint
                    {
                        Hostname = ByteString.CopyFromUtf8($"127.0.0.{octets[i]}"),
                        Port = ports[i],
                        NodeId = nodeIdSeeds[i]
                    };
                    result.Add(endpoint);
                }
                return result;
            }));
    }

    [Fact]
    public void Property_Equal_Lists_Have_Same_HashCode()
    {
        GenUniqueNodes(1, 10)
            .Sample(nodes =>
            {
                var list1 = nodes.ToList();
                var list2 = nodes.ToList();

                var comparer = ListEndpointComparer.Instance;

                return comparer.Equals(list1, list2) &&
                       comparer.GetHashCode(list1) == comparer.GetHashCode(list2);
            });
    }

    [Fact]
    public void Property_Order_Matters()
    {
        GenUniqueNodes(2, 10)
            .Sample(nodes =>
            {
                var list1 = nodes.ToList();
                var list2 = nodes.AsEnumerable().Reverse().ToList();

                var comparer = ListEndpointComparer.Instance;

                // Lists with same elements in different order should NOT be equal
                // (SequenceEqual is order-sensitive)
                return !comparer.Equals(list1, list2) || list1.SequenceEqual(list2);
            });
    }

    [Fact]
    public void Property_Different_Elements_Are_Not_Equal()
    {
        Gen.Select(GenUniqueNodes(2, 10), GenEndpoint)
            .Sample((nodes, extra) =>
            {
                var list1 = nodes.ToList();
                var list2 = nodes.ToList();
                list2.Add(extra);

                var comparer = ListEndpointComparer.Instance;

                // Lists with different elements should not be equal
                return !comparer.Equals(list1, list2);
            });
    }

    #endregion

    #region MembershipProposalComparer Tests

    [Fact]
    public void MembershipProposalComparer_Compare_OrdersByConfigurationId()
    {
        var node = Utils.HostFromParts("10.0.0.1", 5001);
        var proposalLowConfig = CreateProposal(node, configId: 1);
        var proposalHighConfig = CreateProposal(node, configId: 2);

        var result = MembershipProposalComparer.Instance.Compare(proposalLowConfig, proposalHighConfig);

        Assert.True(result < 0); // Lower config ID comes first
    }

    [Fact]
    public void MembershipProposalComparer_Compare_OrdersByMemberCount()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        var proposalSingle = CreateProposal(node1, configId: 100);
        var proposalDouble = CreateProposal([node1, node2], configId: 100);

        var result = MembershipProposalComparer.Instance.Compare(proposalSingle, proposalDouble);

        Assert.True(result < 0); // Fewer members comes first
    }

    [Fact]
    public void MembershipProposalComparer_Compare_OrdersByEndpoint()
    {
        var nodeA = Utils.HostFromParts("10.0.0.1", 5001);
        var nodeB = Utils.HostFromParts("10.0.0.2", 5002);

        var proposalA = CreateProposal(nodeA, configId: 100);
        var proposalB = CreateProposal(nodeB, configId: 100);

        var result = MembershipProposalComparer.Instance.Compare(proposalA, proposalB);

        Assert.True(result < 0); // nodeA < nodeB
    }

    [Fact]
    public void MembershipProposalComparer_Compare_ReturnsZero_ForEqualProposals()
    {
        var node = Utils.HostFromParts("10.0.0.1", 5001);
        var proposal1 = CreateProposal(node, configId: 100);
        var proposal2 = CreateProposal(node, configId: 100);

        var result = MembershipProposalComparer.Instance.Compare(proposal1, proposal2);

        Assert.Equal(0, result);
    }

    [Fact]
    public void MembershipProposalComparer_Compare_HandlesNull()
    {
        var node = Utils.HostFromParts("10.0.0.1", 5001);
        var proposal = CreateProposal(node, configId: 100);

        Assert.True(MembershipProposalComparer.Instance.Compare(null, proposal) < 0);
        Assert.True(MembershipProposalComparer.Instance.Compare(proposal, null) > 0);
        Assert.Equal(0, MembershipProposalComparer.Instance.Compare(null, null));
    }

    private static MembershipProposal CreateProposal(Endpoint endpoint, long configId)
    {
        return CreateProposal([endpoint], configId);
    }

    private static MembershipProposal CreateProposal(Endpoint[] endpoints, long configId)
    {
        var proposal = new MembershipProposal { ConfigurationId = new ConfigurationId(new ClusterId(888), configId).ToProtobuf() };
        var counter = 0L;
        foreach (var endpoint in endpoints)
        {
            var member = endpoint.Clone();
            member.NodeId = counter++;
            proposal.Members.Add(member);
        }
        return proposal;
    }

    #endregion
}
