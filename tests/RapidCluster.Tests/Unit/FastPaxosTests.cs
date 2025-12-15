using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for FastPaxos consensus protocol.
/// </summary>
public class FastPaxosTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public async Task DeclaresSuccess_BeforeAllVotesReceived()
    {
        // In a 5-node cluster, threshold is N - f = 5 - 1 = 4
        // Early success: if a single proposal gets 4 votes, decide immediately
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send 4 votes for the same proposal (threshold = 5 - 1 = 4)
        for (var i = 0; i < 4; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = 1,
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposal(msg);
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
        var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        // Send 2 votes for proposal A
        for (var i = 0; i < 2; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = 1,
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposalA
            };
            fastPaxos.HandleFastRoundProposal(msg);
        }

        // Send 2 votes for proposal B (total 4 votes, but split)
        for (var i = 0; i < 2; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = 1,
                Sender = Utils.HostFromParts("127.0.0.2", 2000 + i),
                Proposal = proposalB
            };
            fastPaxos.HandleFastRoundProposal(msg);
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
        var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, CreateMetrics(), NullLogger<FastPaxos>.Instance);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Send only 3 votes (threshold is 4)
        for (var i = 0; i < 3; i++)
        {
            var msg = new FastRoundPhase2bMessage
            {
                ConfigurationId = 1,
                Sender = Utils.HostFromParts("127.0.0.1", 1000 + i),
                Proposal = proposal
            };
            fastPaxos.HandleFastRoundProposal(msg);
        }

        // Should NOT have decided yet
        Assert.False(fastPaxos.Result.IsCompleted);
    }

    private static MembershipProposal CreateProposal(params Endpoint[] endpoints)
    {
        var proposal = new MembershipProposal { ConfigurationId = 100 };
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
