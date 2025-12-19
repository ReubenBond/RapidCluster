using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for FastPaxos consensus protocol.
/// </summary>
public class FastPaxosTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    #region Basic Decision Tests

    [Fact]
    public async Task DeclaresSuccess_BeforeAllVotesReceived()
    {
        // In a 5-node cluster, threshold is N - f = 5 - 1 = 4
        // Early success: if a single proposal gets 4 votes, decide immediately
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send 4 votes for the same proposal (threshold = 5 - 1 = 4)
        for (var i = 0; i < 4; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Should have decided after exactly 4 votes (not waiting for 5th)
        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    [Fact]
    public async Task DetectsVoteSplit_WhenThresholdReached()
    {
        // In a 5-node cluster, threshold is N - f = 5 - 1 = 4
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        // Send 2 votes for proposal A
        for (var i = 0; i < 2; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposalA
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Send 2 votes for proposal B (total 4 votes, but split)
        for (var i = 0; i < 2; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 2000 + i),
                Proposal = proposalB
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Should detect vote split when threshold reached but no single proposal has enough
        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.VoteSplit>(result);
    }

    [Fact]
    public void DoesNotDecide_BeforeThreshold()
    {
        // In a 5-node cluster, threshold is N - f = 5 - 1 = 4
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send only 3 votes (threshold is 4)
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Should NOT have decided yet
        Assert.False(fastPaxos.Result.IsCompleted);
    }

    #endregion

    #region Threshold Calculation Tests

    [Theory]
    [InlineData(1, 1)]   // 1-node cluster: threshold = 1 - 0 = 1
    [InlineData(2, 2)]   // 2-node cluster: threshold = 2 - 0 = 2
    [InlineData(3, 3)]   // 3-node cluster: threshold = 3 - 0 = 3
    [InlineData(4, 4)]   // 4-node cluster: threshold = 4 - 0 = 4
    [InlineData(5, 4)]   // 5-node cluster: threshold = 5 - 1 = 4, f = floor((5-1)/4) = 1
    [InlineData(9, 7)]   // 9-node cluster: threshold = 9 - 2 = 7, f = floor((9-1)/4) = 2
    [InlineData(10, 8)]  // 10-node cluster: threshold = 10 - 2 = 8
    [InlineData(13, 10)] // 13-node cluster: threshold = 13 - 3 = 10, f = floor((13-1)/4) = 3
    public async Task DeclaresSuccess_AtExactThreshold(int membershipSize, int expectedThreshold)
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send exactly threshold - 1 votes (should not decide yet)
        for (var i = 0; i < expectedThreshold - 1; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        Assert.False(fastPaxos.Result.IsCompleted, $"Should not decide with {expectedThreshold - 1} votes (threshold is {expectedThreshold})");

        // Send the threshold-reaching vote
        var finalMsg = new FastRoundPhase2bMessage
        {
            ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
            Sender = Utils.HostFromParts("127.0.0.1", 1000 + expectedThreshold - 1),
            Proposal = proposal
        };
        fastPaxos.HandleFastRoundProposalResponse(finalMsg);

        Assert.True(fastPaxos.Result.IsCompleted, $"Should decide with {expectedThreshold} votes");
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    #endregion

    #region Configuration ID Tests

    [Fact]
    public void IgnoresVotes_WithWrongConfigurationId()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send votes with wrong configuration ID
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 999).ToProtobuf(), // Wrong config ID
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Should NOT have decided because votes were ignored
        Assert.False(fastPaxos.Result.IsCompleted);
    }

    [Fact]
    public async Task AcceptsVotes_WithCorrectConfigurationId()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 42), membershipSize: 3, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send votes with correct configuration ID
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 42).ToProtobuf(), // Correct config ID
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    #endregion

    #region Duplicate Vote Tests

    [Fact]
    public void IgnoresDuplicateVotes_FromSameSender()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var sender = Utils.HostFromParts("127.0.0.1", 1000);

        // Send the same vote 10 times from the same sender
        for (var i = 0; i < 10; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = sender,
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Should NOT have decided because duplicates are ignored
        // (Only 1 unique vote, threshold is 4)
        Assert.False(fastPaxos.Result.IsCompleted);
    }

    [Fact]
    public async Task CountsUniqueVoters_NotTotalVotes()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send duplicate votes interspersed with unique votes
        var senders = new[]
        {
            Utils.HostFromParts("127.0.0.1", 1000),
            Utils.HostFromParts("127.0.0.1", 1000), // duplicate
            Utils.HostFromParts("127.0.0.2", 1001),
            Utils.HostFromParts("127.0.0.2", 1001), // duplicate
            Utils.HostFromParts("127.0.0.3", 1002),
            Utils.HostFromParts("127.0.0.3", 1002), // duplicate
            Utils.HostFromParts("127.0.0.4", 1003), // 4th unique vote - should trigger decision
        };

        foreach (var sender in senders)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = sender,
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    #endregion

    #region Null Proposal Tests

    [Fact]
    public void IgnoresVotes_WithNullProposal()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        // Send votes with null proposal
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = null // Null proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Should NOT have decided because null proposals are ignored
        Assert.False(fastPaxos.Result.IsCompleted);
    }

    #endregion

    #region Votes After Decision Tests

    [Fact]
    public async Task IgnoresVotes_AfterDecision()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        // Send enough votes to decide on proposal A
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposalA
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        var decided = Assert.IsType<ConsensusResult.Decided>(result);

        // Try to send more votes for a different proposal
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 2000 + i),
                Proposal = proposalB
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        // Decision should still be proposal A
        var finalResult = await fastPaxos.Result;
        var finalDecided = Assert.IsType<ConsensusResult.Decided>(finalResult);
        Assert.Equal(decided.Value, finalDecided.Value);
    }

    #endregion

    #region Cancellation Tests

    [Fact]
    public async Task Cancel_CompletesWithCancelledResult()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        // Not enough votes to decide
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var msg = new FastRoundPhase2bMessage
        {
            ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
            Sender = Utils.HostFromParts("127.0.0.1", 1000),
            Proposal = proposal
        };
        fastPaxos.HandleFastRoundProposalResponse(msg);

        Assert.False(fastPaxos.Result.IsCompleted);

        // Cancel
        fastPaxos.Cancel();

        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Cancelled>(result);
    }

    [Fact]
    public async Task Cancel_DoesNotOverrideExistingDecision()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Decide first
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        Assert.True(fastPaxos.Result.IsCompleted);

        // Try to cancel after decision
        fastPaxos.Cancel();

        // Should still be Decided, not Cancelled
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    [Fact]
    public async Task RegisterTimeoutToken_CancelledTokenCompleteWithCancelled()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        using var cts = new CancellationTokenSource();
        fastPaxos.RegisterTimeoutToken(cts.Token);

        Assert.False(fastPaxos.Result.IsCompleted);

        // Cancel the token - the callback fires synchronously
        cts.SafeCancel();

        // The result should be completed immediately after cancellation
        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Cancelled>(result);
    }

    #endregion

    #region Three-Way Vote Split Tests

    [Fact]
    public async Task DetectsVoteSplit_WithThreeProposals()
    {
        // In a 9-node cluster, threshold is N - f = 9 - 2 = 7
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 9, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));
        var proposalC = CreateProposal(Utils.HostFromParts("10.0.0.3", 5003));

        // Send 3 votes for each proposal (total 9 votes, but split 3-ways)
        // No proposal can reach threshold of 7
        var proposals = new[] { proposalA, proposalB, proposalC };
        for (var p = 0; p < 3; p++)
        {
            for (var i = 0; i < 3; i++)
            {
                var msg = new FastRoundPhase2bMessage
                {
                    ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                    Sender = Utils.HostFromParts($"127.0.0.{p + 1}", 1000 + i),
                    Proposal = proposals[p]
                };
                fastPaxos.HandleFastRoundProposalResponse(msg);
            }
        }

        // After threshold (7) total votes, should detect vote split
        // We sent 9 votes total > 7 threshold
        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.VoteSplit>(result);
    }

    #endregion

    #region Single Node Cluster Tests

    [Fact]
    public async Task SingleNodeCluster_DecidesImmediately()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 1, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Single vote should decide in a 1-node cluster
        var msg = new FastRoundPhase2bMessage
        {
            ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
            Sender = myAddr,
            Proposal = proposal
        };
        fastPaxos.HandleFastRoundProposalResponse(msg);

        Assert.True(fastPaxos.Result.IsCompleted);
        var result = await fastPaxos.Result;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    #endregion

    #region Decided Value Tests

    [Fact]
    public async Task DecidedValue_ContainsCorrectProposal()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var expectedEndpoint = Utils.HostFromParts("10.0.0.99", 9999);
        var proposal = CreateProposal(expectedEndpoint);

        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposalResponse(msg);
        }

        var result = await fastPaxos.Result;
        var decided = Assert.IsType<ConsensusResult.Decided>(result);

        Assert.Single(decided.Value.Members);
        Assert.Equal(expectedEndpoint.Hostname, decided.Value.Members[0].Hostname);
        Assert.Equal(expectedEndpoint.Port, decided.Value.Members[0].Port);
    }

    #endregion

    private static MembershipProposal CreateProposal(params Endpoint[] endpoints)
    {
        var proposal = new MembershipProposal { ConfigurationId = new ConfigurationId(new ClusterId(888), 100).ToProtobuf() };
        foreach (var endpoint in endpoints)
        {
            endpoint.NodeId = Utils.GetNextNodeId();
            proposal.Members.Add(endpoint);
        }
        return proposal;
    }

    /// <summary>
    /// Simple test broadcaster that does nothing (for unit testing FastPaxos).
    /// </summary>
    private sealed class TestBroadcaster : Messaging.IBroadcaster
    {
        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) { }
        public void Broadcast(RapidClusterRequest request, Messaging.BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }
    }

    /// <summary>
    /// Simple meter factory for test metrics collection.
    /// </summary>
    private sealed class TestMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = [];

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            _meters.Add(meter);
            return meter;
        }

        public void Dispose()
        {
            foreach (var meter in _meters)
            {
                meter.Dispose();
            }
            _meters.Clear();
        }
    }
}
