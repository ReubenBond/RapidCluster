using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class PaxosProposerTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public void HandlePhase1b_IgnoresWrongConfigurationId()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000, nodeId: 123);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var membershipViewAccessor = new TestMembershipViewAccessor(CreateMembershipView(myAddr, size: 3));

        var decidedTcs = new TaskCompletionSource<ConsensusResult>();
        var proposer = new PaxosProposer(
            myAddr,
            configId,
            membershipSize: 3,
            broadcaster,
            membershipViewAccessor,
            CreateMetrics(),
            decidedTcs.Task,
            NullLogger<PaxosProposer>.Instance);

        proposer.StartPhase1a(round: 2, TestContext.Current.CancellationToken);
        broadcasted.Clear();

        proposer.HandlePhase1bMessage(new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: 1),
            ConfigurationId = new ConfigurationId(new ClusterId(888), version: 999).ToProtobuf(),
            Rnd = new Rank { Round = 2, NodeIndex = 123 },
            Vrnd = new Rank { Round = 1, NodeIndex = 1 },
            Proposal = CreateProposal(configId)
        }, TestContext.Current.CancellationToken);

        Assert.Empty(broadcasted);
    }

    [Fact]
    public void HandlePhase1b_IgnoresRoundMismatch()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000, nodeId: 123);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var membershipViewAccessor = new TestMembershipViewAccessor(CreateMembershipView(myAddr, size: 3));

        var decidedTcs = new TaskCompletionSource<ConsensusResult>();
        var proposer = new PaxosProposer(
            myAddr,
            configId,
            membershipSize: 3,
            broadcaster,
            membershipViewAccessor,
            CreateMetrics(),
            decidedTcs.Task,
            NullLogger<PaxosProposer>.Instance);

        proposer.StartPhase1a(round: 2, TestContext.Current.CancellationToken);
        broadcasted.Clear();

        proposer.HandlePhase1bMessage(new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: 1),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = new Rank { Round = 3, NodeIndex = 123 },
            Vrnd = new Rank { Round = 1, NodeIndex = 1 },
            Proposal = CreateProposal(configId)
        }, TestContext.Current.CancellationToken);

        Assert.Empty(broadcasted);
    }

    [Fact]
    public void HandlePhase1b_BroadcastsPhase2aOnce_WhenMajorityReached()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000, nodeId: 123);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var membershipViewAccessor = new TestMembershipViewAccessor(CreateMembershipView(myAddr, size: 5));

        var decidedTcs = new TaskCompletionSource<ConsensusResult>();
        var proposer = new PaxosProposer(
            myAddr,
            configId,
            membershipSize: 5,
            broadcaster,
            membershipViewAccessor,
            CreateMetrics(),
            decidedTcs.Task,
            NullLogger<PaxosProposer>.Instance);

        proposer.StartPhase1a(round: 2, TestContext.Current.CancellationToken);
        broadcasted.Clear();

        var proposal = CreateProposal(configId);
        var rnd = new Rank { Round = 2, NodeIndex = 123 };

        // Majority for 5 nodes is 3.
        proposer.HandlePhase1bMessage(CreatePhase1b(configId, senderPort: 2000, rnd, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal), TestContext.Current.CancellationToken);
        proposer.HandlePhase1bMessage(CreatePhase1b(configId, senderPort: 2001, rnd, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal), TestContext.Current.CancellationToken);
        proposer.HandlePhase1bMessage(CreatePhase1b(configId, senderPort: 2002, rnd, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal), TestContext.Current.CancellationToken);

        Assert.Single(broadcasted);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase2AMessage, broadcasted[0].ContentCase);
        Assert.Equal(2, broadcasted[0].Phase2AMessage.Rnd.Round);
        Assert.Equal(123, broadcasted[0].Phase2AMessage.Rnd.NodeIndex);
        Assert.NotNull(broadcasted[0].Phase2AMessage.Proposal);

        // Additional Phase1b messages should not cause rebroadcast.
        proposer.HandlePhase1bMessage(CreatePhase1b(configId, senderPort: 2003, rnd, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal), TestContext.Current.CancellationToken);
        Assert.Single(broadcasted);
    }

    [Fact]
    public void HandlePhase1b_DoesNotBroadcast_WhenNoValueChosen()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000, nodeId: 123);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var membershipViewAccessor = new TestMembershipViewAccessor(CreateMembershipView(myAddr, size: 5));

        var decidedTcs = new TaskCompletionSource<ConsensusResult>();
        var proposer = new PaxosProposer(
            myAddr,
            configId,
            membershipSize: 5,
            broadcaster,
            membershipViewAccessor,
            CreateMetrics(),
            decidedTcs.Task,
            NullLogger<PaxosProposer>.Instance);

        proposer.StartPhase1a(round: 2, TestContext.Current.CancellationToken);
        broadcasted.Clear();

        var rnd = new Rank { Round = 2, NodeIndex = 123 };

        proposer.HandlePhase1bMessage(new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: 1),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = rnd,
            Vrnd = null,
            Proposal = null
        }, TestContext.Current.CancellationToken);

        proposer.HandlePhase1bMessage(new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2001, nodeId: 2),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = rnd,
            Vrnd = null,
            Proposal = null
        }, TestContext.Current.CancellationToken);

        proposer.HandlePhase1bMessage(new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2002, nodeId: 3),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = rnd,
            Vrnd = null,
            Proposal = null
        }, TestContext.Current.CancellationToken);

        Assert.Empty(broadcasted);
    }

    [Fact]
    public void HandlePaxosNackMessage_StartsHigherRound_WhenCurrentRoundRejected()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000, nodeId: 123);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var membershipViewAccessor = new TestMembershipViewAccessor(CreateMembershipView(myAddr, size: 3));

        var decidedTcs = new TaskCompletionSource<ConsensusResult>();
        var proposer = new PaxosProposer(
            myAddr,
            configId,
            membershipSize: 3,
            broadcaster,
            membershipViewAccessor,
            CreateMetrics(),
            decidedTcs.Task,
            NullLogger<PaxosProposer>.Instance);

        proposer.StartPhase1a(round: 2, TestContext.Current.CancellationToken);
        broadcasted.Clear();

        proposer.HandlePaxosNackMessage(new PaxosNackMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: 1),
            ConfigurationId = configId.ToProtobuf(),
            Received = new Rank { Round = 2, NodeIndex = 123 },
            Promised = new Rank { Round = 5, NodeIndex = 7 }
        }, TestContext.Current.CancellationToken);

        Assert.Single(broadcasted);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase1AMessage, broadcasted[0].ContentCase);
        Assert.Equal(6, broadcasted[0].Phase1AMessage.Rank.Round);
    }

    [Fact]
    public void HandlePaxosNackMessage_IgnoresConfigMismatch()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000, nodeId: 123);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var membershipViewAccessor = new TestMembershipViewAccessor(CreateMembershipView(myAddr, size: 3));

        var decidedTcs = new TaskCompletionSource<ConsensusResult>();
        var proposer = new PaxosProposer(
            myAddr,
            configId,
            membershipSize: 3,
            broadcaster,
            membershipViewAccessor,
            CreateMetrics(),
            decidedTcs.Task,
            NullLogger<PaxosProposer>.Instance);

        proposer.StartPhase1a(round: 2, TestContext.Current.CancellationToken);
        broadcasted.Clear();

        proposer.HandlePaxosNackMessage(new PaxosNackMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: 1),
            ConfigurationId = new ConfigurationId(new ClusterId(888), version: 999).ToProtobuf(),
            Received = new Rank { Round = 2, NodeIndex = 123 },
            Promised = new Rank { Round = 5, NodeIndex = 7 }
        }, TestContext.Current.CancellationToken);

        Assert.Empty(broadcasted);
    }

    private static Phase1bMessage CreatePhase1b(ConfigurationId configId, int senderPort, Rank rnd, Rank vrnd, MembershipProposal proposal)
    {
        return new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", senderPort, nodeId: Utils.GetNextNodeId()),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = rnd,
            Vrnd = vrnd,
            Proposal = proposal
        };
    }

    private static MembershipProposal CreateProposal(ConfigurationId configId)
    {
        var proposal = new MembershipProposal { ConfigurationId = configId.ToProtobuf() };
        proposal.Members.Add(Utils.HostFromParts("10.0.0.1", 5001, Utils.GetNextNodeId()));
        return proposal;
    }

    private static MembershipView CreateMembershipView(Endpoint myAddr, int size)
    {
        var builder = new MembershipViewBuilder(maxRingCount: 10);
        builder.RingAdd(myAddr);

        for (var i = 1; i < size; i++)
        {
            builder.RingAdd(Utils.HostFromParts("127.0.0.1", 1000 + i, Utils.GetNextNodeId()));
        }

        return builder.Build();
    }

    private sealed class CapturingBroadcaster(List<RapidClusterRequest> broadcasted) : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken)
        {
            broadcasted.Add(request);
        }

        public void Broadcast(RapidClusterRequest request, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
        {
            broadcasted.Add(request);
        }
    }

    private sealed class TestMembershipViewAccessor(MembershipView view) : IMembershipViewAccessor
    {
        public MembershipView CurrentView => view;
        public BroadcastChannelReader<MembershipView> Updates => throw new NotImplementedException();
    }

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
