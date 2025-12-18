using CsCheck;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Comprehensive tests for MemberInfo, including IComparable, IEquatable, and comparers.
/// </summary>
public class MemberInfoTests
{
    #region Constructor Tests

    [Fact]
    public void Constructor_WithEndpointAndMetadata_SetsProperties()
    {
        var endpoint = Utils.HostFromParts("localhost", 5000, 42);
        var metadata = new Metadata();
        metadata.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8("value");

        var memberInfo = new MemberInfo(endpoint, metadata);

        Assert.Same(endpoint, memberInfo.Endpoint);
        Assert.Same(metadata, memberInfo.Metadata);
        Assert.Equal(42, memberInfo.NodeId);
    }

    [Fact]
    public void Constructor_WithEndpointOnly_SetsEmptyMetadata()
    {
        var endpoint = Utils.HostFromParts("localhost", 5000, 42);

        var memberInfo = new MemberInfo(endpoint);

        Assert.Same(endpoint, memberInfo.Endpoint);
        Assert.NotNull(memberInfo.Metadata);
        Assert.Empty(memberInfo.Metadata.Metadata_);
    }

    [Fact]
    public void Constructor_WithNullEndpoint_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new MemberInfo(null!));
    }

    [Fact]
    public void Constructor_WithNullMetadata_ThrowsArgumentNullException()
    {
        var endpoint = Utils.HostFromParts("localhost", 5000);
        Assert.Throws<ArgumentNullException>(() => new MemberInfo(endpoint, null!));
    }

    #endregion

    #region NodeId Property Tests

    [Fact]
    public void NodeId_ReturnsEndpointNodeId()
    {
        var endpoint = Utils.HostFromParts("localhost", 5000, 12345);
        var memberInfo = new MemberInfo(endpoint);

        Assert.Equal(12345, memberInfo.NodeId);
    }

    #endregion

    #region IComparable Tests

    [Fact]
    public void CompareTo_DifferentHostname_ComparesCorrectly()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));

        Assert.True(member1.CompareTo(member2) < 0);
        Assert.True(member2.CompareTo(member1) > 0);
    }

    [Fact]
    public void CompareTo_DifferentPort_ComparesCorrectly()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5001));

        Assert.True(member1.CompareTo(member2) < 0);
        Assert.True(member2.CompareTo(member1) > 0);
    }

    [Fact]
    public void CompareTo_SameAddress_ReturnsZero()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        // CompareTo uses Endpoint comparison which compares by hostname and port only
        Assert.Equal(0, member1.CompareTo(member2));
    }

    [Fact]
    public void CompareTo_Null_ReturnsPositive()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member.CompareTo(null) > 0);
    }

    [Fact]
    public void CompareTo_DifferentMetadata_ComparesOnlyByEndpoint()
    {
        var metadata1 = new Metadata();
        metadata1.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8("value1");
        var metadata2 = new Metadata();
        metadata2.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8("value2");

        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000), metadata1);
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000), metadata2);

        // Metadata should not affect comparison
        Assert.Equal(0, member1.CompareTo(member2));
    }

    #endregion

    #region IEquatable Tests

    [Fact]
    public void Equals_SameHostAndPort_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        // Equality is based on address only, ignoring NodeId
        Assert.True(member1.Equals(member2));
    }

    [Fact]
    public void Equals_DifferentHostname_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("host1", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("host2", 5000));

        Assert.False(member1.Equals(member2));
    }

    [Fact]
    public void Equals_DifferentPort_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5001));

        Assert.False(member1.Equals(member2));
    }

    [Fact]
    public void Equals_DifferentMetadata_ReturnsTrue()
    {
        var metadata1 = new Metadata();
        metadata1.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8("value1");
        var metadata2 = new Metadata();
        metadata2.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8("value2");

        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000), metadata1);
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000), metadata2);

        // Metadata should not affect equality
        Assert.True(member1.Equals(member2));
    }

    [Fact]
    public void Equals_Null_ReturnsFalse()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        MemberInfo? nullMember = null;

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.False(member.Equals(nullMember));
#pragma warning restore CA1508
    }

    [Fact]
    public void Equals_Object_SameAddress_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        object member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member1.Equals(member2));
    }

    [Fact]
    public void Equals_Object_DifferentType_ReturnsFalse()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        object notMemberInfo = "not a MemberInfo";

        Assert.False(member.Equals(notMemberInfo));
    }

    [Fact]
    public void GetHashCode_SameAddress_ReturnsSameHash()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        Assert.Equal(member1.GetHashCode(), member2.GetHashCode());
    }

    [Fact]
    public void GetHashCode_DifferentAddress_DifferentHash()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5001));

        // Technically hash codes can collide, but for these simple cases they shouldn't
        Assert.NotEqual(member1.GetHashCode(), member2.GetHashCode());
    }

    #endregion

    #region Equality Operator Tests

    [Fact]
    public void EqualityOperator_EqualMembers_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member1 == member2);
    }

    [Fact]
    public void EqualityOperator_DifferentMembers_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5001));

        Assert.False(member1 == member2);
    }

    [Fact]
    public void EqualityOperator_BothNull_ReturnsTrue()
    {
        MemberInfo? member1 = null;
        MemberInfo? member2 = null;

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.True(member1 == member2);
#pragma warning restore CA1508
    }

    [Fact]
    public void EqualityOperator_LeftNull_ReturnsFalse()
    {
        MemberInfo? member1 = null;
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.False(member1 == member2);
#pragma warning restore CA1508
    }

    [Fact]
    public void EqualityOperator_RightNull_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        MemberInfo? member2 = null;

#pragma warning disable CA1508 // Test intentionally checks null behavior
        Assert.False(member1 == member2);
#pragma warning restore CA1508
    }

    [Fact]
    public void InequalityOperator_EqualMembers_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.False(member1 != member2);
    }

    [Fact]
    public void InequalityOperator_DifferentMembers_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5001));

        Assert.True(member1 != member2);
    }

    #endregion

    #region Comparison Operator Tests

    [Fact]
    public void LessThanOperator_SmallerMember_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));

        Assert.True(member1 < member2);
    }

    [Fact]
    public void LessThanOperator_LargerMember_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));

        Assert.False(member1 < member2);
    }

    [Fact]
    public void LessThanOperator_EqualMembers_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.False(member1 < member2);
    }

    [Fact]
    public void LessThanOperator_LeftNull_ReturnsTrue()
    {
        MemberInfo? member1 = null;
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member1 < member2);
    }

    [Fact]
    public void LessThanOperator_RightNull_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        MemberInfo? member2 = null;

        Assert.False(member1 < member2);
    }

    [Fact]
    public void LessThanOperator_BothNull_ReturnsFalse()
    {
        MemberInfo? member1 = null;
        MemberInfo? member2 = null;

        Assert.False(member1 < member2);
    }

    [Fact]
    public void GreaterThanOperator_LargerMember_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));

        Assert.True(member1 > member2);
    }

    [Fact]
    public void GreaterThanOperator_SmallerMember_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));

        Assert.False(member1 > member2);
    }

    [Fact]
    public void GreaterThanOperator_EqualMembers_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.False(member1 > member2);
    }

    [Fact]
    public void GreaterThanOperator_LeftNull_ReturnsFalse()
    {
        MemberInfo? member1 = null;
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.False(member1 > member2);
    }

    [Fact]
    public void GreaterThanOperator_RightNull_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        MemberInfo? member2 = null;

        // null is treated as "less than everything" so member1 > null is true
        Assert.True(member1 > member2);
    }

    [Fact]
    public void LessThanOrEqualOperator_SmallerMember_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));

        Assert.True(member1 <= member2);
    }

    [Fact]
    public void LessThanOrEqualOperator_EqualMembers_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member1 <= member2);
    }

    [Fact]
    public void LessThanOrEqualOperator_LeftNull_ReturnsTrue()
    {
        MemberInfo? member1 = null;
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member1 <= member2);
    }

    [Fact]
    public void LessThanOrEqualOperator_BothNull_ReturnsTrue()
    {
        MemberInfo? member1 = null;
        MemberInfo? member2 = null;

        Assert.True(member1 <= member2);
    }

    [Fact]
    public void GreaterThanOrEqualOperator_LargerMember_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));

        Assert.True(member1 >= member2);
    }

    [Fact]
    public void GreaterThanOrEqualOperator_EqualMembers_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(member1 >= member2);
    }

    [Fact]
    public void GreaterThanOrEqualOperator_LeftNull_RightNotNull_ReturnsFalse()
    {
        MemberInfo? member1 = null;
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.False(member1 >= member2);
    }

    [Fact]
    public void GreaterThanOrEqualOperator_BothNull_ReturnsTrue()
    {
        MemberInfo? member1 = null;
        MemberInfo? member2 = null;

        Assert.True(member1 >= member2);
    }

    #endregion

    #region ToString Tests

    [Fact]
    public void ToString_ReturnsExpectedFormat()
    {
        var endpoint = Utils.HostFromParts("localhost", 5000, 42);
        var member = new MemberInfo(endpoint);

        var result = member.ToString();

        Assert.Contains("localhost", result, StringComparison.Ordinal);
        Assert.Contains("5000", result, StringComparison.Ordinal);
        Assert.Contains("42", result, StringComparison.Ordinal);
    }

    #endregion

    #region Sorting Tests

    [Fact]
    public void MemberInfoList_SortsCorrectly()
    {
        var members = new List<MemberInfo>
        {
            new(Utils.HostFromParts("delta.local", 5000)),
            new(Utils.HostFromParts("alpha.local", 5000)),
            new(Utils.HostFromParts("charlie.local", 5000)),
            new(Utils.HostFromParts("beta.local", 5000)),
        };

        members.Sort();

        Assert.Equal("alpha.local", members[0].Endpoint.Hostname.ToStringUtf8());
        Assert.Equal("beta.local", members[1].Endpoint.Hostname.ToStringUtf8());
        Assert.Equal("charlie.local", members[2].Endpoint.Hostname.ToStringUtf8());
        Assert.Equal("delta.local", members[3].Endpoint.Hostname.ToStringUtf8());
    }

    [Fact]
    public void MemberInfoList_SortsByHostnameThenPort()
    {
        var members = new List<MemberInfo>
        {
            new(Utils.HostFromParts("localhost", 5002)),
            new(Utils.HostFromParts("localhost", 5000)),
            new(Utils.HostFromParts("localhost", 5001)),
        };

        members.Sort();

        Assert.Equal(5000, members[0].Endpoint.Port);
        Assert.Equal(5001, members[1].Endpoint.Port);
        Assert.Equal(5002, members[2].Endpoint.Port);
    }

    #endregion

    #region HashSet/Dictionary Tests

    [Fact]
    public void HashSet_TreatsEqualMembersAsOne()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        var set = new HashSet<MemberInfo> { member1, member2 };

        Assert.Single(set);
    }

    [Fact]
    public void Dictionary_AllowsLookupByEqualMemberInfo()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        var dict = new Dictionary<MemberInfo, string>
        {
            [member1] = "value1"
        };

        // Should be able to look up using member2 (same address)
        Assert.True(dict.ContainsKey(member2));
        Assert.Equal("value1", dict[member2]);
    }

    #endregion

    #region MemberInfoAddressComparer Tests

    [Fact]
    public void MemberInfoAddressComparer_Equals_SameAddress_ReturnsTrue()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        Assert.True(MemberInfoAddressComparer.Instance.Equals(member1, member2));
    }

    [Fact]
    public void MemberInfoAddressComparer_Equals_DifferentAddress_ReturnsFalse()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5001));

        Assert.False(MemberInfoAddressComparer.Instance.Equals(member1, member2));
    }

    [Fact]
    public void MemberInfoAddressComparer_Equals_SameReference_ReturnsTrue()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(MemberInfoAddressComparer.Instance.Equals(member, member));
    }

    [Fact]
    public void MemberInfoAddressComparer_Equals_BothNull_ReturnsTrue()
    {
        Assert.True(MemberInfoAddressComparer.Instance.Equals(null, null));
    }

    [Fact]
    public void MemberInfoAddressComparer_Equals_OneNull_ReturnsFalse()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.False(MemberInfoAddressComparer.Instance.Equals(member, null));
        Assert.False(MemberInfoAddressComparer.Instance.Equals(null, member));
    }

    [Fact]
    public void MemberInfoAddressComparer_GetHashCode_SameAddress_SameHash()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        Assert.Equal(
            MemberInfoAddressComparer.Instance.GetHashCode(member1),
            MemberInfoAddressComparer.Instance.GetHashCode(member2));
    }

    [Fact]
    public void HashSet_WithMemberInfoAddressComparer_TreatsEqualAddressesAsOne()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        var set = new HashSet<MemberInfo>(MemberInfoAddressComparer.Instance) { member1, member2 };

        Assert.Single(set);
    }

    #endregion

    #region MemberInfoComparer Tests

    [Fact]
    public void MemberInfoComparer_Compare_DifferentHostname_ComparesCorrectly()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));

        Assert.True(MemberInfoComparer.Instance.Compare(member1, member2) < 0);
        Assert.True(MemberInfoComparer.Instance.Compare(member2, member1) > 0);
    }

    [Fact]
    public void MemberInfoComparer_Compare_SameAddress_ReturnsZero()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 1));
        var member2 = new MemberInfo(Utils.HostFromParts("localhost", 5000, 2));

        Assert.Equal(0, MemberInfoComparer.Instance.Compare(member1, member2));
    }

    [Fact]
    public void MemberInfoComparer_Compare_SameReference_ReturnsZero()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.Equal(0, MemberInfoComparer.Instance.Compare(member, member));
    }

    [Fact]
    public void MemberInfoComparer_Compare_LeftNull_ReturnsNegative()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(MemberInfoComparer.Instance.Compare(null, member) < 0);
    }

    [Fact]
    public void MemberInfoComparer_Compare_RightNull_ReturnsPositive()
    {
        var member = new MemberInfo(Utils.HostFromParts("localhost", 5000));

        Assert.True(MemberInfoComparer.Instance.Compare(member, null) > 0);
    }

    [Fact]
    public void MemberInfoComparer_Compare_BothNull_ReturnsZero()
    {
        Assert.Equal(0, MemberInfoComparer.Instance.Compare(null, null));
    }

    [Fact]
    public void SortedSet_WithMemberInfoComparer_SortsCorrectly()
    {
        var member1 = new MemberInfo(Utils.HostFromParts("charlie.local", 5000));
        var member2 = new MemberInfo(Utils.HostFromParts("alpha.local", 5000));
        var member3 = new MemberInfo(Utils.HostFromParts("beta.local", 5000));

        var set = new SortedSet<MemberInfo>(MemberInfoComparer.Instance) { member1, member2, member3 };

        var list = set.ToList();
        Assert.Equal("alpha.local", list[0].Endpoint.Hostname.ToStringUtf8());
        Assert.Equal("beta.local", list[1].Endpoint.Hostname.ToStringUtf8());
        Assert.Equal("charlie.local", list[2].Endpoint.Hostname.ToStringUtf8());
    }

    #endregion

    #region Property-Based Tests

    [Fact]
    public void Property_CompareTo_Is_Reflexive()
    {
        Gen.Select(Gen.String[1, 20], Gen.Int[1000, 65535])
            .Sample((hostname, port) =>
            {
                var member = new MemberInfo(Utils.HostFromParts(hostname, port));
                return member.CompareTo(member) == 0;
            });
    }

    [Fact]
    public void Property_CompareTo_Is_Antisymmetric()
    {
        Gen.Select(Gen.String[1, 20], Gen.Int[1000, 65535], Gen.String[1, 20], Gen.Int[1000, 65535])
            .Sample((hostname1, port1, hostname2, port2) =>
            {
                var member1 = new MemberInfo(Utils.HostFromParts(hostname1, port1));
                var member2 = new MemberInfo(Utils.HostFromParts(hostname2, port2));

                // If member1 <= member2 and member2 <= member1, then member1 == member2
                if (member1.CompareTo(member2) <= 0 && member2.CompareTo(member1) <= 0)
                {
                    return member1.CompareTo(member2) == 0;
                }
                return true;
            });
    }

    [Fact]
    public void Property_CompareTo_Is_Transitive()
    {
        Gen.Select(Gen.String[1, 10], Gen.Int[1000, 65535],
                   Gen.String[1, 10], Gen.Int[1000, 65535],
                   Gen.String[1, 10], Gen.Int[1000, 65535])
            .Sample((h1, p1, h2, p2, h3, p3) =>
            {
                var member1 = new MemberInfo(Utils.HostFromParts(h1, p1));
                var member2 = new MemberInfo(Utils.HostFromParts(h2, p2));
                var member3 = new MemberInfo(Utils.HostFromParts(h3, p3));

                // If member1 <= member2 and member2 <= member3, then member1 <= member3
                if (member1.CompareTo(member2) <= 0 && member2.CompareTo(member3) <= 0)
                {
                    return member1.CompareTo(member3) <= 0;
                }
                return true;
            });
    }

    [Fact]
    public void Property_Equals_Is_Reflexive()
    {
        Gen.Select(Gen.String[1, 20], Gen.Int[1000, 65535])
            .Sample((hostname, port) =>
            {
                var member = new MemberInfo(Utils.HostFromParts(hostname, port));
                return member.Equals(member);
            });
    }

    [Fact]
    public void Property_Equals_Is_Symmetric()
    {
        Gen.Select(Gen.String[1, 20], Gen.Int[1000, 65535], Gen.Long, Gen.Long)
            .Sample((hostname, port, nodeId1, nodeId2) =>
            {
                var member1 = new MemberInfo(Utils.HostFromParts(hostname, port, nodeId1));
                var member2 = new MemberInfo(Utils.HostFromParts(hostname, port, nodeId2));

                // Same address should mean both are equal to each other
                return member1.Equals(member2) == member2.Equals(member1);
            });
    }

    [Fact]
    public void Property_Equals_Implies_Same_HashCode()
    {
        Gen.Select(Gen.String[1, 20], Gen.Int[1000, 65535], Gen.Long, Gen.Long)
            .Sample((hostname, port, nodeId1, nodeId2) =>
            {
                var member1 = new MemberInfo(Utils.HostFromParts(hostname, port, nodeId1));
                var member2 = new MemberInfo(Utils.HostFromParts(hostname, port, nodeId2));

                if (member1.Equals(member2))
                {
                    return member1.GetHashCode() == member2.GetHashCode();
                }
                return true;
            });
    }

    [Fact]
    public void Property_Metadata_Does_Not_Affect_Equality()
    {
        Gen.Select(Gen.String[1, 20], Gen.Int[1000, 65535], Gen.String[1, 20], Gen.String[1, 20])
            .Sample((hostname, port, value1, value2) =>
            {
                var metadata1 = new Metadata();
                metadata1.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8(value1);
                var metadata2 = new Metadata();
                metadata2.Metadata_["key"] = Google.Protobuf.ByteString.CopyFromUtf8(value2);

                var member1 = new MemberInfo(Utils.HostFromParts(hostname, port), metadata1);
                var member2 = new MemberInfo(Utils.HostFromParts(hostname, port), metadata2);

                // Same address means equal, regardless of metadata
                return member1.Equals(member2);
            });
    }

    #endregion
}
