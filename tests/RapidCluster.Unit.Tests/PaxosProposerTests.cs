using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class ClassicProposerConsensusCoordinatorTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();

    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public async Task Phase1b_IgnoresWrongConfigurationId()
    {
        const int membershipSize = 3;
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        await using var client = new NoopMessagingClient();
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

        coordinator.Propose(CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001)));

        await WaitUntilAsync(() => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage), TimeSpan.FromSeconds(2));
        var phase1aRank = broadcasted.First(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage).Phase1AMessage.Rank;

        while (broadcasted.TryDequeue(out _)) { }

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = new Phase1bMessage
            {
                Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: Utils.GetNextNodeId()),
                ConfigurationId = new ConfigurationId(new ClusterId(888), version: 999).ToProtobuf(),
                Rnd = phase1aRank,
                Vrnd = new Rank { Round = 1, NodeIndex = 1 },
                Proposal = CreateProposal(configId, Utils.HostFromParts("10.0.0.2", 5002)),
            },
        }, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Empty(broadcasted);
    }

    [Fact]
    public async Task Phase1b_IgnoresRoundMismatch()
    {
        const int membershipSize = 3;
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        await using var client = new NoopMessagingClient();
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

        coordinator.Propose(CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001)));

        await WaitUntilAsync(() => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage), TimeSpan.FromSeconds(2));
        var phase1aRank = broadcasted.First(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage).Phase1AMessage.Rank;

        while (broadcasted.TryDequeue(out _)) { }

        // A Phase1b with a lower round should be ignored.
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = new Phase1bMessage
            {
                Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: Utils.GetNextNodeId()),
                ConfigurationId = configId.ToProtobuf(),
                Rnd = new Rank { Round = phase1aRank.Round - 1, NodeIndex = phase1aRank.NodeIndex },
                Vrnd = new Rank { Round = 1, NodeIndex = 1 },
                Proposal = CreateProposal(configId, Utils.HostFromParts("10.0.0.2", 5002)),
            },
        }, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.Empty(broadcasted);
    }

    [Fact]
    public async Task Phase1b_BroadcastsPhase2aOnce_WhenMajorityReached()
    {
        const int membershipSize = 3;
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        await using var client = new NoopMessagingClient();
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

        coordinator.Propose(CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001)));

        await WaitUntilAsync(() => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage), TimeSpan.FromSeconds(2));
        var phase1aRank = broadcasted.First(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage).Phase1AMessage.Rank;
        while (broadcasted.TryDequeue(out _)) { }

        var proposal = CreateProposal(configId, Utils.HostFromParts("10.0.0.2", 5002));

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = CreatePhase1b(configId, senderPort: 2000, phase1aRank, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal),
        }, TestContext.Current.CancellationToken);

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = CreatePhase1b(configId, senderPort: 2001, phase1aRank, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal),
        }, TestContext.Current.CancellationToken);

        await WaitUntilAsync(() => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase2AMessage), TimeSpan.FromSeconds(2));
        var phase2aMessages = broadcasted.Where(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase2AMessage).ToList();
        Assert.Single(phase2aMessages);

        // Additional Phase1b messages should not cause rebroadcast.
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = CreatePhase1b(configId, senderPort: 2002, phase1aRank, vrnd: new Rank { Round = 1, NodeIndex = 10 }, proposal),
        }, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        phase2aMessages = [.. broadcasted.Where(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase2AMessage)];
        Assert.Single(phase2aMessages);
    }

    [Fact]
    public async Task Phase1b_DoesNotBroadcast_WhenNoValueChosen()
    {
        const int membershipSize = 3;
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), version: 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        await using var client = new NoopMessagingClient();
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

        coordinator.Propose(CreateProposal(configId, Utils.HostFromParts("10.0.0.1", 5001)));

        await WaitUntilAsync(() => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage), TimeSpan.FromSeconds(2));
        var phase1aRank = broadcasted.First(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage).Phase1AMessage.Rank;
        while (broadcasted.TryDequeue(out _)) { }

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = new Phase1bMessage
            {
                Sender = Utils.HostFromParts("127.0.0.1", 2000, nodeId: Utils.GetNextNodeId()),
                ConfigurationId = configId.ToProtobuf(),
                Rnd = phase1aRank,
            },
        }, TestContext.Current.CancellationToken);

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase1BMessage = new Phase1bMessage
            {
                Sender = Utils.HostFromParts("127.0.0.1", 2001, nodeId: Utils.GetNextNodeId()),
                ConfigurationId = configId.ToProtobuf(),
                Rnd = phase1aRank,
            },
        }, TestContext.Current.CancellationToken);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.DoesNotContain(broadcasted, r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase2AMessage);
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

    private static Phase1bMessage CreatePhase1b(ConfigurationId configId, int senderPort, Rank rnd, Rank vrnd, MembershipProposal proposal)
    {
        return new Phase1bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", senderPort, nodeId: Utils.GetNextNodeId()),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = rnd,
            Vrnd = vrnd,
            Proposal = proposal,
        };
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (!predicate())
        {
            if (sw.Elapsed > timeout)
            {
                throw new TimeoutException("Condition not met in time.");
            }

            await Task.Delay(5);
        }
    }

    private sealed class CapturingBroadcaster(ConcurrentQueue<RapidClusterRequest> broadcasted) : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) => broadcasted.Enqueue(request);

        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
        {
            broadcasted.Enqueue(request);

            // For N=3, a single delivery failure is enough to make fast Paxos impossible.
            onDeliveryFailure?.Invoke(
                Utils.HostFromParts("127.0.0.8", 9000, nodeId: Utils.GetNextNodeId()),
                rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
    }

    private sealed class NoopMessagingClient : IMessagingClient
    {
        public void SendOneWayMessage(
            Endpoint remote,
            RapidClusterRequest request,
            Rank? rank,
            DeliveryFailureCallback? onDeliveryFailure,
            CancellationToken cancellationToken)
        { }

        public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new RapidClusterResponse());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class TestMembershipViewAccessor(int membershipSize) : IMembershipViewAccessor
    {
        public MembershipView CurrentView { get; } = Utils.CreateMembershipView(membershipSize);

        public BroadcastChannelReader<MembershipView> Updates => throw new NotSupportedException();
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
