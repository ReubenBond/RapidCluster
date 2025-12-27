using CsCheck;
using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for MembershipProposal equality behavior.
/// This is critical for Fast Paxos vote counting where proposals must be correctly
/// identified as equal or different to reach consensus.
/// </summary>
public class MembershipProposalEqualityTests
{
    #region Basic Equality Tests

    [Fact]
    public void Equals_SameReference_ReturnsTrue()
    {
        var proposal = CreateProposal([Utils.HostFromParts("10.0.0.1", 5001, 1)], configVersion: 1);

        Assert.True(proposal.Equals(proposal));
    }

    [Fact]
    public void Equals_Null_ReturnsFalse()
    {
        var proposal = CreateProposal([Utils.HostFromParts("10.0.0.1", 5001, 1)], configVersion: 1);

        Assert.False(proposal.Equals(null));
    }

    [Fact]
    public void Equals_IdenticalProposals_ReturnsTrue()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1);

        Assert.True(proposal1.Equals(proposal2));
        Assert.True(proposal2.Equals(proposal1));
    }

    [Fact]
    public void Equals_EmptyProposals_ReturnsTrue()
    {
        var proposal1 = CreateProposal([], configVersion: 1);
        var proposal2 = CreateProposal([], configVersion: 1);

        Assert.True(proposal1.Equals(proposal2));
    }

    #endregion

    #region ConfigurationId Tests

    [Fact]
    public void Equals_DifferentConfigVersion_ReturnsFalse()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 2);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentClusterId_ReturnsFalse()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1, clusterId: 100);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1, clusterId: 200);

        Assert.False(proposal1.Equals(proposal2));
    }

    #endregion

    #region Members List Tests

    [Fact]
    public void Equals_DifferentMemberCount_ReturnsFalse()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.2", 5002, 2);

        var proposal1 = CreateProposal([endpoint1.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint1.Clone(), endpoint2.Clone()], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentMemberHostname_ReturnsFalse()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.2", 5001, 1);

        var proposal1 = CreateProposal([endpoint1], configVersion: 1);
        var proposal2 = CreateProposal([endpoint2], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentMemberPort_ReturnsFalse()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.1", 5002, 1);

        var proposal1 = CreateProposal([endpoint1], configVersion: 1);
        var proposal2 = CreateProposal([endpoint2], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentMemberNodeId_ReturnsFalse()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.1", 5001, 2);

        var proposal1 = CreateProposal([endpoint1], configVersion: 1);
        var proposal2 = CreateProposal([endpoint2], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_SameMembersDifferentOrder_ReturnsFalse()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.2", 5002, 2);

        var proposal1 = CreateProposal([endpoint1.Clone(), endpoint2.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint2.Clone(), endpoint1.Clone()], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_MultipleMembers_AllMatch_ReturnsTrue()
    {
        var endpoints = new[]
        {
            Utils.HostFromParts("10.0.0.1", 5001, 1),
            Utils.HostFromParts("10.0.0.2", 5002, 2),
            Utils.HostFromParts("10.0.0.3", 5003, 3)
        };

        var proposal1 = CreateProposal(endpoints.Select(e => e.Clone()).ToArray(), configVersion: 1);
        var proposal2 = CreateProposal(endpoints.Select(e => e.Clone()).ToArray(), configVersion: 1);

        Assert.True(proposal1.Equals(proposal2));
    }

    #endregion

    #region MaxNodeId Tests

    [Fact]
    public void Equals_DifferentMaxNodeId_ReturnsFalse()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1, maxNodeId: 10);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1, maxNodeId: 20);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_SameMaxNodeId_ReturnsTrue()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1, maxNodeId: 10);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1, maxNodeId: 10);

        Assert.True(proposal1.Equals(proposal2));
    }

    #endregion

    #region Metadata Tests

    [Fact]
    public void Equals_EmptyMetadata_ReturnsTrue()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposalWithMetadata([endpoint.Clone()], [], configVersion: 1);
        var proposal2 = CreateProposalWithMetadata([endpoint.Clone()], [], configVersion: 1);

        Assert.True(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_SameMetadata_ReturnsTrue()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var metadata = CreateMetadata("key1", "value1");

        var proposal1 = CreateProposalWithMetadata([endpoint.Clone()], [metadata.Clone()], configVersion: 1);
        var proposal2 = CreateProposalWithMetadata([endpoint.Clone()], [metadata.Clone()], configVersion: 1);

        Assert.True(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentMetadataKey_ReturnsFalse()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var metadata1 = CreateMetadata("key1", "value1");
        var metadata2 = CreateMetadata("key2", "value1");

        var proposal1 = CreateProposalWithMetadata([endpoint.Clone()], [metadata1], configVersion: 1);
        var proposal2 = CreateProposalWithMetadata([endpoint.Clone()], [metadata2], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentMetadataValue_ReturnsFalse()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var metadata1 = CreateMetadata("key1", "value1");
        var metadata2 = CreateMetadata("key1", "value2");

        var proposal1 = CreateProposalWithMetadata([endpoint.Clone()], [metadata1], configVersion: 1);
        var proposal2 = CreateProposalWithMetadata([endpoint.Clone()], [metadata2], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    [Fact]
    public void Equals_DifferentMetadataCount_ReturnsFalse()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.2", 5002, 2);
        var metadata = CreateMetadata("key1", "value1");

        var proposal1 = CreateProposalWithMetadata([endpoint1.Clone(), endpoint2.Clone()], [metadata.Clone()], configVersion: 1);
        var proposal2 = CreateProposalWithMetadata([endpoint1.Clone(), endpoint2.Clone()], [metadata.Clone(), metadata.Clone()], configVersion: 1);

        Assert.False(proposal1.Equals(proposal2));
    }

    #endregion

    #region Dictionary Key Tests (Critical for Fast Paxos)

    [Fact]
    public void Dictionary_IdenticalProposals_AreSameKey()
    {
        var dict = new Dictionary<MembershipProposal, int>();
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);

        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1);

        dict[proposal1] = 100;

        Assert.True(dict.TryGetValue(proposal2, out var value));
        Assert.Equal(100, value);
    }

    [Fact]
    public void Dictionary_DifferentProposals_AreDifferentKeys()
    {
        var dict = new Dictionary<MembershipProposal, int>();
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.2", 5002, 2);

        var proposal1 = CreateProposal([endpoint1], configVersion: 1);
        var proposal2 = CreateProposal([endpoint2], configVersion: 1);

        dict[proposal1] = 100;
        dict[proposal2] = 200;

        Assert.Equal(2, dict.Count);
        Assert.Equal(100, dict[proposal1]);
        Assert.Equal(200, dict[proposal2]);
    }

    [Fact]
    public void Dictionary_IncrementVoteCount_WorksCorrectly()
    {
        // This simulates the Fast Paxos vote counting scenario
        var dict = new Dictionary<MembershipProposal, int>();
        var endpoints = new[]
        {
            Utils.HostFromParts("10.0.0.1", 5001, 1),
            Utils.HostFromParts("10.0.0.2", 5002, 2),
            Utils.HostFromParts("10.0.0.3", 5003, 3)
        };

        // Simulate receiving 3 identical votes from different nodes
        for (var i = 0; i < 3; i++)
        {
            var proposal = CreateProposal(endpoints.Select(e => e.Clone()).ToArray(), configVersion: 1);

            if (dict.TryGetValue(proposal, out var count))
            {
                dict[proposal] = count + 1;
            }
            else
            {
                dict[proposal] = 1;
            }
        }

        Assert.Single(dict);
        Assert.Equal(3, dict.Values.First());
    }

    #endregion

    #region MembershipProposalComparer vs Built-in Equals Tests

    [Fact]
    public void Comparer_AndBuiltInEquals_AreConsistent_ForIdenticalProposals()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1);

        var comparerResult = MembershipProposalComparer.Instance.Equals(proposal1, proposal2);
        var builtInResult = proposal1.Equals(proposal2);

        // Both should agree these are equal
        Assert.True(comparerResult);
        Assert.True(builtInResult);
    }

    [Fact]
    public void Comparer_AndBuiltInEquals_AreConsistent_ForDifferentConfigVersions()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 2);

        var comparerResult = MembershipProposalComparer.Instance.Equals(proposal1, proposal2);
        var builtInResult = proposal1.Equals(proposal2);

        // Both should agree these are not equal
        Assert.False(comparerResult);
        Assert.False(builtInResult);
    }

    [Fact]
    public void Comparer_AndBuiltInEquals_AreConsistent_ForDifferentMembers()
    {
        var endpoint1 = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var endpoint2 = Utils.HostFromParts("10.0.0.2", 5002, 2);
        var proposal1 = CreateProposal([endpoint1], configVersion: 1);
        var proposal2 = CreateProposal([endpoint2], configVersion: 1);

        var comparerResult = MembershipProposalComparer.Instance.Equals(proposal1, proposal2);
        var builtInResult = proposal1.Equals(proposal2);

        // Both should agree these are not equal
        Assert.False(comparerResult);
        Assert.False(builtInResult);
    }

    [Fact]
    public void Comparer_IgnoresMaxNodeIdAndMetadata_ButBuiltInDoesNot()
    {
        // This documents an important behavioral difference:
        // - MembershipProposalComparer only considers ConfigurationId and Members
        // - Built-in Equals considers ALL fields including MaxNodeId and Metadata

        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1, maxNodeId: 10);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1, maxNodeId: 20);

        var comparerResult = MembershipProposalComparer.Instance.Equals(proposal1, proposal2);
        var builtInResult = proposal1.Equals(proposal2);

        // Comparer ignores MaxNodeId - considers them equal
        Assert.True(comparerResult);
        // Built-in Equals considers MaxNodeId - considers them different
        Assert.False(builtInResult);
    }

    #endregion

    #region GetHashCode Tests

    [Fact]
    public void GetHashCode_IdenticalProposals_SameHashCode()
    {
        var endpoint = Utils.HostFromParts("10.0.0.1", 5001, 1);
        var proposal1 = CreateProposal([endpoint.Clone()], configVersion: 1);
        var proposal2 = CreateProposal([endpoint.Clone()], configVersion: 1);

        Assert.Equal(proposal1.GetHashCode(), proposal2.GetHashCode());
    }

    [Fact]
    public void GetHashCode_DifferentProposals_LikelyDifferentHashCodes()
    {
        // Create several different proposals
        var proposals = new List<MembershipProposal>();
        for (var i = 1; i <= 10; i++)
        {
            var endpoint = Utils.HostFromParts($"10.0.0.{i}", 5000 + i, i);
            proposals.Add(CreateProposal([endpoint], configVersion: i, clusterId: i * 100));
        }

        var hashCodes = proposals.Select(p => p.GetHashCode()).Distinct().ToList();

        // Hash codes should vary across different proposals
        // (not guaranteed by contract, but most should differ in practice)
        // We expect at least half to be unique
        Assert.True(hashCodes.Count >= 5, $"Expected at least 5 unique hash codes, but got {hashCodes.Count}");
    }

    #endregion

    #region Property-Based Tests

    private static Gen<Endpoint> GenEndpoint() =>
        Gen.Select(
            Gen.Int[1, 255],
            Gen.Int[1000, 65535],
            Gen.Long[1, 1000]
        ).Select((octet, port, nodeId) => new Endpoint
        {
            Hostname = ByteString.CopyFromUtf8($"10.0.0.{octet}"),
            Port = port,
            NodeId = nodeId
        });

    private static Gen<MembershipProposal> GenProposal(int minMembers, int maxMembers) =>
        Gen.Select(
            Gen.Int[minMembers, maxMembers],
            Gen.Long[1, 100],
            Gen.Long[1, 1000]
        ).SelectMany((count, configVersion, clusterId) =>
            GenEndpoint().Array[count].Select(endpoints =>
            {
                var proposal = new MembershipProposal
                {
                    ConfigurationId = new ConfigurationId(new ClusterId(clusterId), configVersion).ToProtobuf(),
                    MaxNodeId = endpoints.Length > 0 ? endpoints.Max(e => e.NodeId) : 0
                };
                proposal.Members.AddRange(endpoints);
                return proposal;
            }));

    [Fact]
    public void Property_Reflexive_ProposalEqualsItself()
    {
        GenProposal(1, 5).Sample(proposal =>
        {
            return proposal.Equals(proposal);
        });
    }

    [Fact]
    public void Property_Symmetric_EqualsIsSymmetric()
    {
        Gen.Select(GenProposal(1, 5), GenProposal(1, 5))
            .Sample((p1, p2) =>
            {
                var p1EqualsP2 = p1.Equals(p2);
                var p2EqualsP1 = p2.Equals(p1);
                return p1EqualsP2 == p2EqualsP1;
            });
    }

    [Fact]
    public void Property_HashCodeConsistentWithEquals()
    {
        GenProposal(1, 5).Sample(proposal =>
        {
            // Clone the proposal to create an equal instance
            var clone = proposal.Clone();
            if (proposal.Equals(clone))
            {
                return proposal.GetHashCode() == clone.GetHashCode();
            }
            return true; // If not equal, no constraint on hash code
        });
    }

    [Fact]
    public void Property_ClonedProposalIsEqual()
    {
        GenProposal(1, 5).Sample(proposal =>
        {
            var clone = proposal.Clone();
            return proposal.Equals(clone) && clone.Equals(proposal);
        });
    }

    #endregion

    #region Helper Methods

    private static MembershipProposal CreateProposal(
        Endpoint[] endpoints,
        long configVersion,
        long clusterId = 888,
        long? maxNodeId = null)
    {
        var proposal = new MembershipProposal
        {
            ConfigurationId = new ConfigurationId(new ClusterId(clusterId), configVersion).ToProtobuf(),
            MaxNodeId = maxNodeId ?? (endpoints.Length > 0 ? endpoints.Max(e => e.NodeId) : 0)
        };
        proposal.Members.AddRange(endpoints);
        return proposal;
    }

    private static MembershipProposal CreateProposalWithMetadata(
        Endpoint[] endpoints,
        Metadata[] metadata,
        long configVersion,
        long clusterId = 888,
        long? maxNodeId = null)
    {
        var proposal = new MembershipProposal
        {
            ConfigurationId = new ConfigurationId(new ClusterId(clusterId), configVersion).ToProtobuf(),
            MaxNodeId = maxNodeId ?? (endpoints.Length > 0 ? endpoints.Max(e => e.NodeId) : 0)
        };
        proposal.Members.AddRange(endpoints);
        proposal.MemberMetadata.AddRange(metadata);
        return proposal;
    }

    private static Metadata CreateMetadata(string key, string value)
    {
        var metadata = new Metadata();
        metadata.Metadata_.Add(key, ByteString.CopyFromUtf8(value));
        return metadata;
    }

    #endregion
}
