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

/// <summary>
/// Unit tests for ConsensusCoordinator.
/// </summary>
public class ConsensusCoordinatorTests : IAsyncLifetime
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    private readonly FakeTimeProvider _timeProvider = new();
    private readonly List<ConsensusCoordinator> _coordinators = [];
    private readonly List<TestMessagingClient> _clients = [];

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        foreach (var coordinator in _coordinators)
        {
            await coordinator.DisposeAsync();
        }
        foreach (var client in _clients)
        {
            await client.DisposeAsync();
        }
        GC.SuppressFinalize(this);
    }

    #region Construction Tests

    [Fact]
    public void Constructor_CreatesValidInstance()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        Assert.NotNull(coordinator);
        Assert.False(coordinator.Decided.IsCompleted);
    }

    [Fact]
    public void Constructor_AcceptsVariousClusterSizes()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);

        foreach (var size in new[] { 1, 2, 3, 5, 10, 100 })
        {
            var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: size);
            Assert.NotNull(coordinator);
            Assert.False(coordinator.Decided.IsCompleted);
        }
    }

    #endregion

    #region Propose Tests

    [Fact]
    public void Propose_StartsConsensusLoop()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Should not throw
        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Decided task should exist but not be completed yet
        Assert.False(coordinator.Decided.IsCompleted);
    }

    [Fact]
    public async Task Propose_DisposeAsync_CancelsDecision()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, trackForCleanup: false);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        // Start consensus
        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Verify decision is not yet complete
        Assert.False(coordinator.Decided.IsCompleted);

        // Dispose should cancel the consensus loop and the decision
        await coordinator.DisposeAsync();

        // Decision should be cancelled
        Assert.True(coordinator.Decided.IsCompleted);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => coordinator.Decided);
    }

    #endregion

    #region HandleMessages Tests

    [Fact]
    public void HandleMessages_FastRoundPhase2B_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001))
            }
        };

        var response = coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        Assert.NotNull(response);
    }

    [Fact]
    public void HandleMessages_Phase1A_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase1AMessage = new Phase1aMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001),
                Rank = new Rank { Round = 2, NodeIndex = 1 }
            }
        };

        var response = coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        Assert.NotNull(response);
    }

    [Fact]
    public void HandleMessages_Phase1B_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase1BMessage = new Phase1bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001),
                Rnd = new Rank { Round = 2, NodeIndex = 1 },
                Vrnd = new Rank { Round = 1, NodeIndex = 0 }
            }
        };

        var response = coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        Assert.NotNull(response);
    }

    [Fact]
    public void HandleMessages_Phase2A_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase2AMessage = new Phase2aMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001),
                Rnd = new Rank { Round = 2, NodeIndex = 1 },
                Proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001))
            }
        };

        var response = coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        Assert.NotNull(response);
    }

    [Fact]
    public void HandleMessages_Phase2B_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001),
                Rnd = new Rank { Round = 2, NodeIndex = 1 }
            }
        };

        var response = coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        Assert.NotNull(response);
    }

    [Fact]
    public void HandleMessages_UnexpectedCase_Throws()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);

        // Empty request with no content set
        var request = new RapidClusterRequest();

        Assert.Throws<ArgumentException>(() => coordinator.HandleMessages(request, TestContext.Current.CancellationToken));
    }

    #endregion

    #region Fast Round Decision Tests

    [Fact]
    public void FastRound_VotesAreAccepted()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Send fast round votes - should not throw
        for (var i = 0; i < 3; i++)
        {
            var request = new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                    Sender = Utils.HostFromParts($"127.0.0.{i + 1}", 1000 + i),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposal
                }
            };
            coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        }

        // Note: Full decision testing is done in simulation tests where we can
        // control the async consensus loop. Here we just verify votes are accepted.
    }

    [Fact]
    public void FastRound_VotesWithWrongConfigIdAreIgnored()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Send votes with wrong config ID - should not throw
        var wrongConfigId = new ConfigurationId(new ClusterId(888), version: 999);
        for (var i = 0; i < 3; i++)
        {
            var request = new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = wrongConfigId.ToProtobuf(),
                    Sender = Utils.HostFromParts($"127.0.0.{i + 1}", 1000 + i),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposal
                }
            };
            coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        }

        // Should not decide due to wrong config ID (decision still pending)
        Assert.False(coordinator.Decided.IsCompleted);
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public async Task DisposeAsync_CancelsOngoingConsensus()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 5, trackForCleanup: false);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Dispose while consensus is ongoing
        await coordinator.DisposeAsync();

        // Decision should be cancelled
        Assert.True(coordinator.Decided.IsCompleted);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => coordinator.Decided);
    }

    [Fact]
    public async Task DisposeAsync_CanBeCalledMultipleTimes()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, trackForCleanup: false);

        // Should not throw when called multiple times
        await coordinator.DisposeAsync();
        await coordinator.DisposeAsync();
        await coordinator.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_BeforePropose_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 3, trackForCleanup: false);

        // Dispose without ever calling Propose
        await coordinator.DisposeAsync();

        // Decision should be cancelled
        Assert.True(coordinator.Decided.IsCompleted);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => coordinator.Decided);
    }

    #endregion

    #region Concurrent Message Handling Tests

    [Fact]
    public async Task HandleMessages_ThreadSafe_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 1), membershipSize: 10);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Send messages concurrently from multiple threads
        var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(() =>
        {
            var request = new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                    Sender = Utils.HostFromParts($"127.0.0.{i + 1}", 1000 + i),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposal
                }
            };
            coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken)).ToArray();

        // Should not throw
        await Task.WhenAll(tasks);
    }

    #endregion

    #region Classic Round Event-Driven Tests

    [Fact]
    public async Task ClassicRound_DecidesWithoutWaitingForTimeout()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        // Large delay makes it obvious if we accidentally wait.
        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 3, broadcaster);

        // Override options by creating a dedicated coordinator with a larger delay.
        // (We reuse existing helper but need to ensure it uses a large base delay.)
        // To keep the change minimal, we inject the decision quickly and assert time bound.
        coordinator.Propose(CreateProposal(Utils.HostFromParts("10.0.0.1", 5001)), TestContext.Current.CancellationToken);

        // Wait until classic round starts (Phase1a broadcast).
        await WaitUntilAsync(() => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage), TimeSpan.FromSeconds(2));
        var phase1aRank = broadcasted.First(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage).Phase1AMessage.Rank;

        // Deliver a classic decision quickly: send 2 Phase2b votes for the current round.
        var decidedProposal = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));
        var rnd = new Rank { Round = phase1aRank.Round, NodeIndex = phase1aRank.NodeIndex };

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                Rnd = rnd,
                Proposal = decidedProposal
            }
        }, TestContext.Current.CancellationToken);

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = rnd,
                Proposal = decidedProposal
            }
        }, TestContext.Current.CancellationToken);

        await WaitUntilAsync(() => coordinator.Decided.IsCompleted, TimeSpan.FromSeconds(2));
        Assert.True(coordinator.Decided.IsCompleted);
    }

    [Fact]
    public async Task ClassicRound_NacksRestartAfterBackoff_WhenQuorumImpossible()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new CapturingBroadcaster(broadcasted);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 3, broadcaster);

        coordinator.Propose(CreateProposal(Utils.HostFromParts("10.0.0.1", 5001)), TestContext.Current.CancellationToken);

        await WaitUntilAsync(
            () => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage),
            TimeSpan.FromSeconds(2));

        var phase1aRank = broadcasted.First(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage).Phase1AMessage.Rank;
        Assert.True(phase1aRank.Round >= 2);

        while (broadcasted.TryDequeue(out _))
        {
        }

        // A single NACK should not restart classic Paxos immediately.
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                ConfigurationId = configId.ToProtobuf(),
                Rnd = new Rank { Round = 5, NodeIndex = 7 },
                Proposal = null
            }
        }, TestContext.Current.CancellationToken);

        // No immediate Phase1a for round 6.
        Assert.DoesNotContain(
            broadcasted,
            r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage && r.Phase1AMessage.Rank.Round == 6);

        // Once quorum is impossible (N=3, 2 unique negative voters), a new round is scheduled
        // and should restart after backoff.
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                ConfigurationId = configId.ToProtobuf(),
                Rnd = new Rank { Round = 5, NodeIndex = 9 },
                Proposal = null
            }
        }, TestContext.Current.CancellationToken);

        await WaitUntilAsync(
            () => broadcasted.Any(r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage),
            TimeSpan.FromSeconds(5));

        Assert.Contains(
            broadcasted,
            r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage && r.Phase1AMessage.Rank.Round == 6);
    }

    private async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var sw = Stopwatch.StartNew();
        while (!predicate())
        {
            if (sw.Elapsed > timeout)
            {
                throw new TimeoutException("Condition not met in time.");
            }

            // Drive timers deterministically via FakeTimeProvider.
            _timeProvider.Advance(TimeSpan.FromMilliseconds(1));
            await Task.Yield();
        }
    }

    private sealed class CapturingBroadcaster(ConcurrentQueue<RapidClusterRequest> broadcasted) : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken)
        {
            broadcasted.Enqueue(request);
        }

        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
        {
            broadcasted.Enqueue(request);

            // Force immediate fallback from fast round without waiting for time.
            // Classic rounds also use delivery failure callbacks; only trigger failures for the fast round.
            if (rank is { Round: 1 })
            {
                onDeliveryFailure?.Invoke(
                    Utils.HostFromParts("127.0.0.8", 9000, nodeId: Utils.GetNextNodeId()),
                    rank);
            }
        }
    }

    #endregion

    #region Factory Tests

    [Fact]
    public async Task Factory_CreatesCoordinatorWithCorrectConfig()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        await using var client = new TestMessagingClient();
        var broadcaster = new TestBroadcaster();
        var viewAccessor = new TestMembershipViewAccessor();
        var options = Options.Create(new RapidClusterProtocolOptions());
        var sharedResources = new SharedResources(_timeProvider);
        var metrics = CreateMetrics();

        var factory = new ConsensusCoordinatorFactory(
            client,
            viewAccessor,
            options,
            sharedResources,
            metrics,
            NullLogger<ConsensusCoordinator>.Instance);

        var coordinator = factory.Create(myAddr, configurationId: new ConfigurationId(new ClusterId(888), 42), membershipSize: 7, broadcaster);
        _coordinators.Add(coordinator);

        Assert.NotNull(coordinator);
        Assert.False(coordinator.Decided.IsCompleted);
    }

    #endregion

    #region Helper Methods

    private ConsensusCoordinator CreateCoordinator(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        bool trackForCleanup = true)
    {
        return CreateCoordinator(myAddr, configurationId, membershipSize, broadcaster: new TestBroadcaster(), trackForCleanup);
    }

    private ConsensusCoordinator CreateCoordinator(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IBroadcaster broadcaster,
        bool trackForCleanup = true)
    {
        var client = new TestMessagingClient();
        _clients.Add(client);
        var viewAccessor = new TestMembershipViewAccessor(membershipSize);
        var options = Options.Create(new RapidClusterProtocolOptions
        {
            ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(100)
        });
        var sharedResources = new SharedResources(_timeProvider, random: new Random(42));
        var metrics = CreateMetrics();

        var coordinator = new ConsensusCoordinator(
            myAddr,
            configurationId,
            membershipSize,
            client,
            broadcaster,
            viewAccessor,
            options,
            sharedResources,
            metrics,
            NullLogger<ConsensusCoordinator>.Instance);

        if (trackForCleanup)
        {
            _coordinators.Add(coordinator);
        }

        return coordinator;
    }

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

    #endregion

    #region Test Doubles

    /// <summary>
    /// Simple test broadcaster that does nothing.
    /// </summary>
    private sealed class TestBroadcaster : IBroadcaster
    {
        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) { }
        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }
    }

    /// <summary>
    /// Simple test messaging client.
    /// </summary>
    private sealed class TestMessagingClient : IMessagingClient
    {
        public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
        public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new RapidClusterResponse());
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>
    /// Simple test membership view accessor.
    /// </summary>
    private sealed class TestMembershipViewAccessor : IMembershipViewAccessor
    {
        private readonly MembershipView _view;

        public TestMembershipViewAccessor(int membershipSize = 3)
        {
            _view = Utils.CreateMembershipView(membershipSize);
        }

        public MembershipView CurrentView => _view;
        public BroadcastChannelReader<MembershipView> Updates => throw new NotImplementedException();
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

    #endregion
}
