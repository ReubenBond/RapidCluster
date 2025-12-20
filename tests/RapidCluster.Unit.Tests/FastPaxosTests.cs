using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class FastRoundConsensusCoordinatorTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public async Task Decides_WhenThresholdVotesReceived()
    {
        var membershipSize = 5;
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        await using var client = new NoopMessagingClient();
        var broadcaster = new NoopBroadcaster();
        var viewAccessor = new TestMembershipViewAccessor(membershipSize);
        var options = Options.Create(new RapidClusterProtocolOptions());
        var sharedResources = new SharedResources(new FakeTimeProvider(), random: new Random(42));
        var metrics = CreateMetrics();

        await using var coordinator = new ConsensusCoordinator(
            myAddr,
            configId,
            membershipSize,
            client,
            broadcaster,
            viewAccessor,
            options,
            sharedResources,
            metrics,
            NullLogger<ConsensusCoordinator>.Instance);

        var proposal = CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001));
        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        var threshold = membershipSize - (int)Math.Floor((membershipSize - 1) / 4.0);
        for (var i = 0; i < threshold; i++)
        {
            coordinator.HandleMessages(new RapidClusterRequest
            {
                FastRoundPhase2BMessage = new FastRoundPhase2bMessage
                {
                    ConfigurationId = configId.ToProtobuf(),
                    Sender = Utils.HostFromParts("127.0.0.1", 2000 + i, nodeId: Utils.GetNextNodeId()),
                    Proposal = proposal
                }
            }, TestContext.Current.CancellationToken);
        }

        var decided = await coordinator.Decided.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
        Assert.True(MembershipProposalComparer.Instance.Equals(proposal, decided));
    }

    [Fact]
    public async Task DoesNotDecide_BeforeThreshold()
    {
        var membershipSize = 5;
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        await using var client = new NoopMessagingClient();
        var broadcaster = new NoopBroadcaster();
        var viewAccessor = new TestMembershipViewAccessor(membershipSize);
        var options = Options.Create(new RapidClusterProtocolOptions());
        var sharedResources = new SharedResources(new FakeTimeProvider(), random: new Random(42));
        var metrics = CreateMetrics();

        await using var coordinator = new ConsensusCoordinator(
            myAddr,
            configId,
            membershipSize,
            client,
            broadcaster,
            viewAccessor,
            options,
            sharedResources,
            metrics,
            NullLogger<ConsensusCoordinator>.Instance);

        var proposal = CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001));
        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        var threshold = membershipSize - (int)Math.Floor((membershipSize - 1) / 4.0);
        for (var i = 0; i < threshold - 1; i++)
        {
            coordinator.HandleMessages(new RapidClusterRequest
            {
                FastRoundPhase2BMessage = new FastRoundPhase2bMessage
                {
                    ConfigurationId = configId.ToProtobuf(),
                    Sender = Utils.HostFromParts("127.0.0.1", 2000 + i, nodeId: Utils.GetNextNodeId()),
                    Proposal = proposal
                }
            }, TestContext.Current.CancellationToken);
        }

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(coordinator.Decided.IsCompleted);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(5)]
    [InlineData(9)]
    [InlineData(10)]
    [InlineData(13)]
    public async Task Decides_AtExactThreshold(int membershipSize)
    {
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        await using var client = new NoopMessagingClient();
        var broadcaster = new NoopBroadcaster();
        var viewAccessor = new TestMembershipViewAccessor(membershipSize);
        var options = Options.Create(new RapidClusterProtocolOptions());
        var sharedResources = new SharedResources(new FakeTimeProvider(), random: new Random(42));
        var metrics = CreateMetrics();

        await using var coordinator = new ConsensusCoordinator(
            myAddr,
            configId,
            membershipSize,
            client,
            broadcaster,
            viewAccessor,
            options,
            sharedResources,
            metrics,
            NullLogger<ConsensusCoordinator>.Instance);

        var proposal = CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001));
        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        var threshold = membershipSize - (int)Math.Floor((membershipSize - 1) / 4.0);

        for (var i = 0; i < Math.Max(0, threshold - 1); i++)
        {
            coordinator.HandleMessages(new RapidClusterRequest
            {
                FastRoundPhase2BMessage = new FastRoundPhase2bMessage
                {
                    ConfigurationId = configId.ToProtobuf(),
                    Sender = Utils.HostFromParts("127.0.0.1", 3000 + i, nodeId: Utils.GetNextNodeId()),
                    Proposal = proposal
                }
            }, TestContext.Current.CancellationToken);
        }

        if (threshold > 1)
        {
            await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.False(coordinator.Decided.IsCompleted);
        }

        coordinator.HandleMessages(new RapidClusterRequest
        {
            FastRoundPhase2BMessage = new FastRoundPhase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.1", 4000, nodeId: Utils.GetNextNodeId()),
                Proposal = proposal
            }
        }, TestContext.Current.CancellationToken);

        _ = await coordinator.Decided.WaitAsync(TimeSpan.FromSeconds(2), TestContext.Current.CancellationToken);
    }

    private static MembershipProposal CreateProposal(ConfigurationId configId, params Endpoint[] endpoints)
    {
        var proposal = new MembershipProposal { ConfigurationId = configId.ToProtobuf() };
        foreach (var endpoint in endpoints)
        {
            endpoint.NodeId = Utils.GetNextNodeId();
            proposal.Members.Add(endpoint);
        }
        return proposal;
    }

    private sealed class NoopBroadcaster : IBroadcaster
    {
        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) { }

        public void Broadcast(
            RapidClusterRequest request,
            Rank? rank,
            BroadcastFailureCallback? onDeliveryFailure,
            CancellationToken cancellationToken) { }

        public void SetMembership(IReadOnlyList<Endpoint> membership) { }
    }

    private sealed class NoopMessagingClient : IMessagingClient
    {
        public void SendOneWayMessage(
            Endpoint remote,
            RapidClusterRequest request,
            Rank? rank,
            DeliveryFailureCallback? onDeliveryFailure,
            CancellationToken cancellationToken) { }

        public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new RapidClusterResponse());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TestMembershipViewAccessor : IMembershipViewAccessor
    {
        private readonly MembershipView _view;

        public TestMembershipViewAccessor(int membershipSize)
        {
            _view = Utils.CreateMembershipView(membershipSize);
        }

        public MembershipView CurrentView => _view;
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
