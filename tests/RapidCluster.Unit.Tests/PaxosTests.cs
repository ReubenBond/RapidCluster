using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for Paxos and FastPaxos protocols
/// </summary>
public class PaxosTests
{
    /// <summary>
    /// Test rank comparison - higher round wins
    /// </summary>
    [Fact]
    public void RankComparisonHigherRoundWins()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 2, NodeIndex = 0 };

        // rank2 should be greater
        Assert.True(CompareRanks(rank2, rank1) > 0);
        Assert.True(CompareRanks(rank1, rank2) < 0);
    }

    /// <summary>
    /// Test rank comparison - same round, higher node index wins
    /// </summary>
    [Fact]
    public void RankComparisonSameRoundHigherNodeIndexWins()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 0 };
        var rank2 = new Rank { Round = 1, NodeIndex = 1 };

        // rank2 should be greater
        Assert.True(CompareRanks(rank2, rank1) > 0);
        Assert.True(CompareRanks(rank1, rank2) < 0);
    }

    /// <summary>
    /// Test rank comparison - equal ranks
    /// </summary>
    [Fact]
    public void RankComparisonEqualRanks()
    {
        var rank1 = new Rank { Round = 1, NodeIndex = 5 };
        var rank2 = new Rank { Round = 1, NodeIndex = 5 };

        Assert.Equal(0, CompareRanks(rank1, rank2));
    }

    /// <summary>
    /// Test that Phase1bMessage can be created correctly
    /// </summary>
    [Fact]
    public void Phase1bMessageCreation()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);
        var rnd = new Rank { Round = 5, NodeIndex = 1 };
        var vrnd = new Rank { Round = 3, NodeIndex = 0 };

        var msg = new Phase1bMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = rnd,
            Vrnd = vrnd
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(100, msg.ConfigurationId.Version);
        Assert.Equal(5, msg.Rnd.Round);
        Assert.Equal(3, msg.Vrnd.Round);
    }

    /// <summary>
    /// Test that Phase2aMessage can be created correctly
    /// </summary>
    [Fact]
    public void Phase2aMessageCreation()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);
        var value = Utils.HostFromParts("127.0.0.1", 5678);
        var rnd = new Rank { Round = 5, NodeIndex = 1 };

        var msg = new Phase2aMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = rnd,
            Proposal = CreateProposal(value)
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(100, msg.ConfigurationId.Version);
        Assert.Equal(5, msg.Rnd.Round);
        Assert.Single(msg.Proposal.Members);
    }

    private static int CompareRanks(Rank r1, Rank r2)
    {
        if (r1.Round != r2.Round)
            return r1.Round.CompareTo(r2.Round);
        return r1.NodeIndex.CompareTo(r2.NodeIndex);
    }

    [Fact]
    public void Phase1aMessageCreationSetsAllFields()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);
        var rank = new Rank { Round = 5, NodeIndex = 10 };

        var msg = new Phase1aMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
            Rank = rank
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(1, msg.ConfigurationId.Version);
        Assert.Equal(5, msg.Rank.Round);
        Assert.Equal(10, msg.Rank.NodeIndex);
    }

    [Fact]
    public void Phase1aMessageDefaultRankIsNull()
    {
        var msg = new Phase1aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf()
        };

        Assert.Null(msg.Rank);
    }

    [Fact]
    public void Phase1aMessageCloneCreatesIndependentCopy()
    {
        var original = new Phase1aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rank = new Rank { Round = 5, NodeIndex = 10 }
        };

        var clone = original.Clone();

        Assert.Equal(original.ConfigurationId.Version, clone.ConfigurationId.Version);
        Assert.Equal(original.Rank.Round, clone.Rank.Round);

        clone.ConfigurationId = new ConfigurationId(new ClusterId(888), 200).ToProtobuf();
        Assert.Equal(100, original.ConfigurationId.Version);
    }

    [Fact]
    public void Phase1bMessageCreationSetsAllFields()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);
        var rnd = new Rank { Round = 5, NodeIndex = 1 };
        var vrnd = new Rank { Round = 3, NodeIndex = 0 };
        var vval = Utils.HostFromParts("127.0.0.1", 9999);

        var msg = new Phase1bMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = rnd,
            Vrnd = vrnd,
            Proposal = CreateProposal(vval)
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(100, msg.ConfigurationId.Version);
        Assert.Equal(5, msg.Rnd.Round);
        Assert.Equal(3, msg.Vrnd.Round);
        Assert.Single(msg.Proposal.Members);
        Assert.Equal(vval.Hostname, msg.Proposal.Members[0].Hostname);
        Assert.Equal(vval.Port, msg.Proposal.Members[0].Port);
    }

    [Fact]
    public void Phase1bMessageEmptyProposalIsValid()
    {
        var msg = new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 5, NodeIndex = 1 },
            Vrnd = new Rank { Round = 0, NodeIndex = 0 }
        };

        Assert.Null(msg.Proposal);
    }

    [Fact]
    public void Phase1bMessageMultipleMembersAllStored()
    {
        var proposal = new MembershipProposal { ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf() };
        proposal.Members.Add(Utils.HostFromParts("127.0.0.1", 1001, Utils.GetNextNodeId()));
        proposal.Members.Add(Utils.HostFromParts("127.0.0.1", 1002, Utils.GetNextNodeId()));
        proposal.Members.Add(Utils.HostFromParts("127.0.0.1", 1003, Utils.GetNextNodeId()));

        var msg = new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 5, NodeIndex = 1 },
            Vrnd = new Rank { Round = 1, NodeIndex = 0 },
            Proposal = proposal
        };

        Assert.Equal(3, msg.Proposal.Members.Count);
    }

    [Fact]
    public void Phase2aMessageCreationSetsAllFields()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);
        var rnd = new Rank { Round = 5, NodeIndex = 1 };

        var msg = new Phase2aMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = rnd,
            Proposal = CreateProposal(Utils.HostFromParts("127.0.0.1", 5678))
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(100, msg.ConfigurationId.Version);
        Assert.Equal(5, msg.Rnd.Round);
        Assert.Single(msg.Proposal.Members);
    }

    [Fact]
    public void Phase2aMessageMultipleMembersWorksCorrectly()
    {
        var msg = new Phase2aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 2, NodeIndex = 1 },
            Proposal = CreateProposal(
                Utils.HostFromParts("10.0.0.1", 5001),
                Utils.HostFromParts("10.0.0.2", 5002),
                Utils.HostFromParts("10.0.0.3", 5003))
        };

        Assert.Equal(3, msg.Proposal.Members.Count);
    }

    [Fact]
    public void Phase2bMessageCreationSetsAllFields()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);
        var rnd = new Rank { Round = 5, NodeIndex = 1 };

        var msg = new Phase2bMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = rnd,
            Proposal = CreateProposal(Utils.HostFromParts("127.0.0.1", 5678))
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(100, msg.ConfigurationId.Version);
        Assert.Equal(5, msg.Rnd.Round);
        Assert.Single(msg.Proposal.Members);
    }

    [Fact]
    public void Phase2bMessageMultipleMembersAllStored()
    {
        var proposal = new MembershipProposal { ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf() };
        for (var i = 0; i < 10; i++)
        {
            proposal.Members.Add(Utils.HostFromParts("192.168.1." + i, 5000 + i, Utils.GetNextNodeId()));
        }

        var msg = new Phase2bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 2, NodeIndex = 1 },
            Proposal = proposal
        };

        Assert.Equal(10, msg.Proposal.Members.Count);
    }

    [Fact]
    public void FastRoundPhase2bMessageCreationSetsAllFields()
    {
        var sender = Utils.HostFromParts("127.0.0.1", 1234);

        var msg = new FastRoundPhase2bMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Proposal = CreateProposal(Utils.HostFromParts("127.0.0.1", 5678))
        };

        Assert.Equal(sender, msg.Sender);
        Assert.Equal(100, msg.ConfigurationId.Version);
        Assert.Single(msg.Proposal.Members);
    }

    [Fact]
    public void FastRoundPhase2bMessageEmptyProposalIsValid()
    {
        var msg = new FastRoundPhase2bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf()
        };

        Assert.Null(msg.Proposal);
    }

    [Fact]
    public void FastRoundPhase2bMessageLargeProposalHandledCorrectly()
    {
        var proposal = new MembershipProposal { ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf() };
        for (var i = 0; i < 100; i++)
        {
            proposal.Members.Add(Utils.HostFromParts("10.0.0." + i, 5000, Utils.GetNextNodeId()));
        }

        var msg = new FastRoundPhase2bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Proposal = proposal
        };

        Assert.Equal(100, msg.Proposal.Members.Count);
    }

    [Fact]
    public void RankFastRoundIsRound1()
    {
        var fastRound = new Rank { Round = 1, NodeIndex = 0 };
        var classicRound = new Rank { Round = 2, NodeIndex = 100 };

        Assert.True(classicRound.CompareTo(fastRound) > 0);
    }

    [Fact]
    public void RankClassicRoundsStartAtRound2()
    {
        var fastRound = new Rank { Round = 1, NodeIndex = 0 };
        var classicRound2 = new Rank { Round = 2, NodeIndex = 0 };

        Assert.True(classicRound2 > fastRound);
    }

    [Fact]
    public void RankNodeIndexBreaksTies()
    {
        var rank1 = new Rank { Round = 5, NodeIndex = 100 };
        var rank2 = new Rank { Round = 5, NodeIndex = 200 };

        Assert.True(rank2 > rank1);
    }

    [Fact]
    public void RankFResiliencyCalculation()
    {
        // f = floor((n-1)/4) for Fast Paxos
        Assert.Equal(1, (int)Math.Floor((5 - 1) / 4.0));
        Assert.Equal(2, (int)Math.Floor((10 - 1) / 4.0));
        Assert.Equal(4, (int)Math.Floor((20 - 1) / 4.0));
    }

    [Fact]
    public void RankQuorumThresholdCalculation()
    {
        // Threshold = n - f for Fast Paxos
        int n = 5;
        int f = (int)Math.Floor((n - 1) / 4.0);
        Assert.Equal(4, n - f);

        n = 10;
        f = (int)Math.Floor((n - 1) / 4.0);
        Assert.Equal(8, n - f);

        n = 20;
        f = (int)Math.Floor((n - 1) / 4.0);
        Assert.Equal(16, n - f);
    }

    [Fact]
    public void ConsensusResponseDefaultFields()
    {
        var response = new ConsensusResponse();

        Assert.NotNull(response);
    }

    [Fact]
    public void Phase1aMessageSerializeDeserializeRoundtrips()
    {
        var original = new Phase1aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 999).ToProtobuf(),
            Rank = new Rank { Round = 10, NodeIndex = 20 }
        };

        var bytes = Google.Protobuf.MessageExtensions.ToByteArray(original);
        var deserialized = Phase1aMessage.Parser.ParseFrom(bytes);

        Assert.Equal(original.ConfigurationId.Version, deserialized.ConfigurationId.Version);
        Assert.Equal(original.Rank.Round, deserialized.Rank.Round);
        Assert.Equal(original.Rank.NodeIndex, deserialized.Rank.NodeIndex);
    }

    [Fact]
    public void Phase2aMessageSerializeDeserializePreservesProposal()
    {
        var original = new Phase2aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 5, NodeIndex = 10 },
            Proposal = CreateProposal(
                Utils.HostFromParts("10.0.0.1", 5001),
                Utils.HostFromParts("10.0.0.2", 5002))
        };

        var bytes = Google.Protobuf.MessageExtensions.ToByteArray(original);
        var deserialized = Phase2aMessage.Parser.ParseFrom(bytes);

        Assert.Equal(2, deserialized.Proposal.Members.Count);
        Assert.Equal("10.0.0.1", deserialized.Proposal.Members[0].Hostname.ToStringUtf8());
        Assert.Equal(5001, deserialized.Proposal.Members[0].Port);
    }

    [Fact]
    public void RankZeroValuesCompareCorrectly()
    {
        var zeroRank = new Rank { Round = 0, NodeIndex = 0 };
        var nonZeroRank = new Rank { Round = 0, NodeIndex = 1 };

        Assert.True(nonZeroRank > zeroRank);
    }

    [Fact]
    public void Phase1bMessageSameVrndAsRndIsValid()
    {
        var msg = new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 5, NodeIndex = 10 },
            Vrnd = new Rank { Round = 5, NodeIndex = 10 }
        };

        Assert.Equal(msg.Rnd.Round, msg.Vrnd.Round);
    }

    [Fact]
    public void ConfigurationIdCanBeNegative()
    {
        var msg = new Phase1aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), -1).ToProtobuf(),
            Rank = new Rank { Round = 1, NodeIndex = 1 }
        };

        Assert.Equal(-1, msg.ConfigurationId.ToConfigurationId().Version);
    }

    [Fact]
    public void ConfigurationIdCanBeLargeValue()
    {
        var msg = new Phase1aMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), long.MaxValue).ToProtobuf(),
            Rank = new Rank { Round = 1, NodeIndex = 1 }
        };

        Assert.Equal(long.MaxValue, msg.ConfigurationId.ToConfigurationId().Version);
    }

    /// <summary>
    /// Helper to create a MembershipProposal with specified endpoints
    /// </summary>
    private static MembershipProposal CreateProposal(params Endpoint[] endpoints)
    {
        var proposal = new MembershipProposal { ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf() };
        foreach (var endpoint in endpoints)
        {
            // If the endpoint doesn't have a node_id set, assign one
            if (endpoint.NodeId == 0)
            {
                endpoint.NodeId = Utils.GetNextNodeId();
            }
            proposal.Members.Add(endpoint);
        }
        return proposal;
    }

    /// <summary>
    /// Helper to create a Phase1bMessage with specific vrnd and proposal
    /// </summary>
    private static Phase1bMessage CreatePhase1bMessage(int vrndRound, int vrndNodeIndex, params Endpoint[] endpoints)
    {
        var msg = new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 1234),
            ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf(),
            Rnd = new Rank { Round = 2, NodeIndex = 1 },
            Vrnd = new Rank { Round = vrndRound, NodeIndex = vrndNodeIndex }
        };

        if (endpoints.Length > 0)
        {
            msg.Proposal = CreateProposal(endpoints);
        }

        return msg;
    }

    [Fact]
    public void ChooseValue_ReturnsNull_WhenAllMessagesHaveEmptyProposal()
    {
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(0, 0),  // empty proposal
            CreatePhase1bMessage(0, 0),  // empty proposal
            CreatePhase1bMessage(0, 0)   // empty proposal
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        Assert.Null(result);
    }

    [Fact]
    public void ChooseValue_ReturnsSingleValue_WhenAllProposalsIdentical()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(1, 1, node1),
            CreatePhase1bMessage(1, 1, node1),
            CreatePhase1bMessage(1, 1, node1)
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_UsesMaxVrnd_WhenMixedVrnds()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(0, 0, node2),  // lower vrnd - should be ignored
            CreatePhase1bMessage(1, 1, node1),  // higher vrnd - should be chosen
            CreatePhase1bMessage(1, 1, node1)   // higher vrnd - should be chosen
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_PicksValueWithMoreThanNOver4Votes_WhenConflicting()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        // N=5, so N/4 = 1. We need > 1 votes (i.e., 2 or more) for a value
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(1, 1, node1),
            CreatePhase1bMessage(1, 1, node1),  // 2 votes for node1 - exceeds N/4
            CreatePhase1bMessage(1, 1, node2)   // 1 vote for node2
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_FallsBackToAnyNonEmptyProposal_WhenNoMajority()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);
        var node3 = Utils.HostFromParts("10.0.0.3", 5003);

        // N=20, so N/4 = 5. We need > 5 votes for a value to win
        // Each value has only 1 vote, so no majority - should fall back to any non-empty
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(1, 1, node1),
            CreatePhase1bMessage(1, 1, node2),
            CreatePhase1bMessage(1, 1, node3)
        };

        var result = PaxosProposer.ChooseValue(messages, n: 20);

        // Should return one of the values (first non-empty proposal from any message)
        Assert.NotNull(result);
        Assert.Single(result.Members);
        var endpoint = result.Members[0];
        Assert.True(
            (endpoint.Hostname.Equals(node1.Hostname) && endpoint.Port == node1.Port) ||
            (endpoint.Hostname.Equals(node2.Hostname) && endpoint.Port == node2.Port) ||
            (endpoint.Hostname.Equals(node3.Hostname) && endpoint.Port == node3.Port));
    }

    [Fact]
    public void ChooseValue_FallsBackToNonEmptyProposal_WhenMaxVrndMessagesHaveEmptyProposals()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);

        // Higher vrnd messages have empty proposal, lower vrnd has value
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(0, 0, node1),  // lower vrnd with value
            CreatePhase1bMessage(1, 1),          // higher vrnd, empty proposal
            CreatePhase1bMessage(1, 1)           // higher vrnd, empty proposal
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        // Should fall back to the first non-empty proposal
        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_ThresholdN4_ForLargeCluster()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        // N=20, N/4 = 5. Need > 5 (i.e., 6) votes to exceed threshold
        var messages = new List<Phase1bMessage>();

        // Add 5 votes for node2 - NOT enough (need > N/4)
        for (var i = 0; i < 5; i++)
        {
            messages.Add(CreatePhase1bMessage(1, 1, node2));
        }

        // Add 1 vote for node1
        messages.Add(CreatePhase1bMessage(1, 1, node1));

        var result = PaxosProposer.ChooseValue(messages, n: 20);

        // No value exceeds N/4, should fall back to smallest proposal (deterministic)
        Assert.NotNull(result);
        Assert.Single(result.Members);
        // Falls back to smallest proposal by lexicographic order: node1 < node2
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_ThresholdN4_ExceedsWhenEnoughVotes()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        // N=20, N/4 = 5. Need > 5 (i.e., 6) votes to exceed threshold
        var messages = new List<Phase1bMessage>();

        // Add 6 votes for node2 - enough to exceed N/4
        for (var i = 0; i < 6; i++)
        {
            messages.Add(CreatePhase1bMessage(1, 1, node2));
        }

        // Add 4 votes for node1
        for (var i = 0; i < 4; i++)
        {
            messages.Add(CreatePhase1bMessage(1, 1, node1));
        }

        var result = PaxosProposer.ChooseValue(messages, n: 20);

        // node2 has 6 votes which exceeds N/4=5
        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(node2.Hostname, result.Members[0].Hostname);
        Assert.Equal(node2.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_MultipleEndpointsInProposal()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(1, 1, node1, node2),  // proposal with 2 endpoints
            CreatePhase1bMessage(1, 1, node1, node2),
            CreatePhase1bMessage(1, 1, node1, node2)
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        Assert.NotNull(result);
        Assert.Equal(2, result.Members.Count);
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
        Assert.Equal(node2.Hostname, result.Members[1].Hostname);
        Assert.Equal(node2.Port, result.Members[1].Port);
    }

    [Fact]
    public void ChooseValue_EmptyMessageList_ReturnsNull()
    {
        var messages = new List<Phase1bMessage>();

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        Assert.Null(result);
    }

    [Fact]
    public void ChooseValue_VrndNodeIndexBreaksTies()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);

        // Same round, different node index - higher node index wins
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(1, 1, node2),   // vrnd (1,1) - lower
            CreatePhase1bMessage(1, 2, node1),   // vrnd (1,2) - higher
            CreatePhase1bMessage(1, 2, node1)    // vrnd (1,2) - higher
        };

        var result = PaxosProposer.ChooseValue(messages, n: 5);

        // Should choose node1 because vrnd (1,2) > (1,1)
        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(node1.Hostname, result.Members[0].Hostname);
        Assert.Equal(node1.Port, result.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_IsDeterministic_WithMultipleDifferentProposals()
    {
        var node1 = Utils.HostFromParts("10.0.0.1", 5001);
        var node2 = Utils.HostFromParts("10.0.0.2", 5002);
        var node3 = Utils.HostFromParts("10.0.0.3", 5003);

        // Run multiple times with the same inputs to verify determinism
        var results = new List<MembershipProposal?>();
        for (var iteration = 0; iteration < 10; iteration++)
        {
            // Create messages in different orders to stress-test determinism
            var messages = new List<Phase1bMessage>
            {
                CreatePhase1bMessage(1, 1, node3), // node3 first
                CreatePhase1bMessage(1, 1, node1), // node1 second
                CreatePhase1bMessage(1, 1, node2)  // node2 third
            };

            // Shuffle messages on each iteration using a seeded approach for deterministic test behavior
#pragma warning disable CA5394 // Random is fine for deterministic test shuffling (not security)
            var random = new Random(iteration * 12345);
            messages = [.. messages.OrderBy(_ => random.Next())];
#pragma warning restore CA5394

            var result = PaxosProposer.ChooseValue(messages, n: 20);
            results.Add(result);
        }

        // All results should be identical (deterministic)
        var firstResult = results[0];
        Assert.NotNull(firstResult);
        Assert.All(results, r =>
        {
            Assert.NotNull(r);
            Assert.True(MembershipProposalComparer.Instance.Equals(firstResult, r));
        });

        // The result should be the "smallest" proposal by lexicographic order
        // node1 (10.0.0.1) < node2 (10.0.0.2) < node3 (10.0.0.3)
        Assert.Equal(node1.Hostname, firstResult.Members[0].Hostname);
        Assert.Equal(node1.Port, firstResult.Members[0].Port);
    }

    [Fact]
    public void ChooseValue_SelectsSmallestProposal_WhenNoMajority()
    {
        // Create proposals with predictable ordering
        var nodeA = Utils.HostFromParts("10.0.0.1", 5001); // Smallest
        var nodeB = Utils.HostFromParts("10.0.0.2", 5002);
        var nodeC = Utils.HostFromParts("10.0.0.3", 5003); // Largest

        // N=20, so N/4 = 5. Each value has only 1 vote, no majority
        var messages = new List<Phase1bMessage>
        {
            CreatePhase1bMessage(1, 1, nodeC), // Largest first (would be picked by non-deterministic FirstOrDefault)
            CreatePhase1bMessage(1, 1, nodeB),
            CreatePhase1bMessage(1, 1, nodeA)  // Smallest last
        };

        var result = PaxosProposer.ChooseValue(messages, n: 20);

        // Should select nodeA (smallest by lexicographic order), not nodeC (first in list)
        Assert.NotNull(result);
        Assert.Single(result.Members);
        Assert.Equal(nodeA.Hostname, result.Members[0].Hostname);
        Assert.Equal(nodeA.Port, result.Members[0].Port);
    }
}
