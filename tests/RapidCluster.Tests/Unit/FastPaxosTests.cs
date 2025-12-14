using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for FastPaxos consensus protocol.
/// </summary>
public sealed class FastPaxosTests : IDisposable
{
    private readonly TestMeterFactory _meterFactory = new();
    private readonly RapidClusterMetrics _metrics;

    public FastPaxosTests()
    {
        _metrics = new RapidClusterMetrics(_meterFactory);
    }

    public void Dispose()
    {
        _meterFactory.Dispose();
    }

    [Fact]
    public async Task DeclaresSuccess_BeforeAllVotesReceived()
    {
        // In a 5-node cluster, threshold is N - f = 5 - 1 = 4
        // Early success: if a single proposal gets 4 votes, decide immediately
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var broadcaster = new TestBroadcaster();
        var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, _metrics, NullLogger<FastPaxos>.Instance);

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
        var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, _metrics, NullLogger<FastPaxos>.Instance);

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
        var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, _metrics, NullLogger<FastPaxos>.Instance);

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
        var counter = 0;
        foreach (var endpoint in endpoints)
        {
            proposal.Members.Add(new MemberInfo
            {
                Endpoint = endpoint,
                NodeId = CreateNodeId("node" + counter++)
            });
        }
        return proposal;
    }

    private static NodeId CreateNodeId(string id)
    {
        return new NodeId
        {
            High = (long)id.GetHashCode(StringComparison.Ordinal),
            Low = (long)id.GetHashCode(StringComparison.Ordinal) * 31
        };
    }

    /// <summary>
    /// Simple test broadcaster that does nothing (for unit testing FastPaxos).
    /// </summary>
    private sealed class TestBroadcaster : global::RapidCluster.Messaging.IBroadcaster
    {
        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) { }
        public void Broadcast(RapidClusterRequest request, global::RapidCluster.Messaging.BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }
    }

    /// <summary>
    /// Simple test meter factory that creates real meters for unit testing.
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
