using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class PaxosAcceptorTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public void HandlePhase1a_PromisesOnlyForHigherRank()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var sent = new List<(Endpoint Remote, RapidClusterRequest Request)>();
        using var client = new CapturingMessagingClient(sent);
        var broadcaster = new NoopBroadcaster();

        var acceptor = new PaxosAcceptor(
            myAddr,
            configId,
            client,
            broadcaster,
            CreateMetrics(),
            NullLogger.Instance);

        var sender = Utils.HostFromParts("127.0.0.1", 2000);

        // First prepare: should promise.
        acceptor.HandlePhase1aMessage(new Phase1aMessage
        {
            Sender = sender,
            ConfigurationId = configId.ToProtobuf(),
            Rank = new Rank { Round = 2, NodeIndex = 1 },
        }, TestContext.Current.CancellationToken);

        Assert.Single(sent);
        Assert.Equal(sender, sent[0].Remote);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase1BMessage, sent[0].Request.ContentCase);
        Assert.Equal(2, sent[0].Request.Phase1BMessage.Rnd.Round);

        // Lower/equal prepare: should send NACK.
        acceptor.HandlePhase1aMessage(new Phase1aMessage
        {
            Sender = sender,
            ConfigurationId = configId.ToProtobuf(),
            Rank = new Rank { Round = 2, NodeIndex = 1 },
        }, TestContext.Current.CancellationToken);

        acceptor.HandlePhase1aMessage(new Phase1aMessage
        {
            Sender = sender,
            ConfigurationId = configId.ToProtobuf(),
            Rank = new Rank { Round = 1, NodeIndex = 99 },
        }, TestContext.Current.CancellationToken);

        Assert.Equal(3, sent.Count);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase1BMessage, sent[1].Request.ContentCase);
        Assert.Equal(2, sent[1].Request.Phase1BMessage.Rnd.Round);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase1BMessage, sent[2].Request.ContentCase);
        Assert.Equal(2, sent[2].Request.Phase1BMessage.Rnd.Round);
    }

    [Fact]
    public void HandlePhase1a_DoesNotNackOnConfigMismatch()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var sent = new List<(Endpoint Remote, RapidClusterRequest Request)>();
        using var client = new CapturingMessagingClient(sent);
        var broadcaster = new NoopBroadcaster();

        var acceptor = new PaxosAcceptor(
            myAddr,
            configId,
            client,
            broadcaster,
            CreateMetrics(),
            NullLogger.Instance);

        var sender = Utils.HostFromParts("127.0.0.1", 2000);

        acceptor.HandlePhase1aMessage(new Phase1aMessage
        {
            Sender = sender,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 999).ToProtobuf(),
            Rank = new Rank { Round = 2, NodeIndex = 1 },
        }, TestContext.Current.CancellationToken);

        Assert.Empty(sent);
    }

    [Fact]
    public void HandlePhase2a_AcceptsOnlyIfAtOrAbovePromise()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);

        var sent = new List<(Endpoint Remote, RapidClusterRequest Request)>();
        using var client = new CapturingMessagingClient(sent);

        var acceptor = new PaxosAcceptor(
            myAddr,
            configId,
            client,
            broadcaster,
            CreateMetrics(),
            NullLogger.Instance);

        var coordinator = Utils.HostFromParts("127.0.0.1", 2000);
        var proposal = new MembershipProposal { ConfigurationId = configId.ToProtobuf() };
        proposal.Members.Add(Utils.HostFromParts("10.0.0.1", 5001, Utils.GetNextNodeId()));

        // Establish a promise at rank (2,1).
        acceptor.HandlePhase1aMessage(new Phase1aMessage
        {
            Sender = coordinator,
            ConfigurationId = configId.ToProtobuf(),
            Rank = new Rank { Round = 2, NodeIndex = 1 },
        }, TestContext.Current.CancellationToken);

        // Lower accept should be rejected (and send NACK).
        acceptor.HandlePhase2aMessage(new Phase2aMessage
        {
            Sender = coordinator,
            ConfigurationId = configId.ToProtobuf(),
            Rnd = new Rank { Round = 1, NodeIndex = 1 },
            Proposal = proposal,
        }, TestContext.Current.CancellationToken);

        Assert.Empty(broadcasted);

        // Phase1a sent a Phase1b, then Phase2a rejection replied with a Phase2b NACK.
        Assert.Equal(2, sent.Count);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase1BMessage, sent[0].Request.ContentCase);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase2BMessage, sent[1].Request.ContentCase);
        Assert.Equal(2, sent[1].Request.Phase2BMessage.Rnd.Round);
        Assert.Null(sent[1].Request.Phase2BMessage.Proposal);

        // Accept at promised rank should be accepted and broadcast.
        acceptor.HandlePhase2aMessage(new Phase2aMessage
        {
            Sender = coordinator,
            ConfigurationId = configId.ToProtobuf(),
            Rnd = new Rank { Round = 2, NodeIndex = 1 },
            Proposal = proposal,
        }, TestContext.Current.CancellationToken);

        Assert.Single(broadcasted);
        Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase2BMessage, broadcasted[0].ContentCase);
        Assert.Equal(2, broadcasted[0].Phase2BMessage.Rnd.Round);

        // Duplicate accept for same round should rebroadcast (idempotency).
        acceptor.HandlePhase2aMessage(new Phase2aMessage
        {
            Sender = coordinator,
            ConfigurationId = configId.ToProtobuf(),
            Rnd = new Rank { Round = 2, NodeIndex = 1 },
            Proposal = proposal,
        }, TestContext.Current.CancellationToken);

        Assert.Equal(2, broadcasted.Count);
        Assert.All(broadcasted, msg => Assert.Equal(RapidClusterRequest.ContentOneofCase.Phase2BMessage, msg.ContentCase));
    }

    [Fact]
    public void HandlePhase2a_DoesNotNackOnConfigMismatch()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new List<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);

        var sent = new List<(Endpoint Remote, RapidClusterRequest Request)>();
        using var client = new CapturingMessagingClient(sent);

        var acceptor = new PaxosAcceptor(
            myAddr,
            configId,
            client,
            broadcaster,
            CreateMetrics(),
            NullLogger.Instance);

        var coordinator = Utils.HostFromParts("127.0.0.1", 2000);
        var proposal = new MembershipProposal { ConfigurationId = configId.ToProtobuf() };
        proposal.Members.Add(Utils.HostFromParts("10.0.0.1", 5001, Utils.GetNextNodeId()));

        acceptor.HandlePhase2aMessage(new Phase2aMessage
        {
            Sender = coordinator,
            ConfigurationId = new ConfigurationId(new ClusterId(888), 999).ToProtobuf(),
            Rnd = new Rank { Round = 1, NodeIndex = 1 },
            Proposal = proposal,
        }, TestContext.Current.CancellationToken);

        Assert.Empty(sent);
        Assert.Empty(broadcasted);
    }

    private sealed class NoopBroadcaster : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }
        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) { }
        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
    }

    private sealed class CapturingBroadcaster(List<RapidClusterRequest> broadcasted) : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) => broadcasted.Add(request);

        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) => broadcasted.Add(request);
    }

    private sealed class CapturingMessagingClient(List<(Endpoint Remote, RapidClusterRequest Request)> sent) : IMessagingClient, IDisposable
    {
        public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) => sent.Add((remote, request));

        public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken) => Task.FromResult(new RapidClusterResponse());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public void Dispose() { }
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
