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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

        Assert.NotNull(coordinator);
        Assert.False(coordinator.Decided.IsCompleted);
    }

    [Fact]
    public void Constructor_AcceptsVariousClusterSizes()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);

        foreach (var size in new[] { 1, 2, 3, 5, 10, 100 })
        {
            var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: size);
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 5, trackForCleanup: false);
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

        var request = new RapidClusterRequest
        {
            FastRoundPhase2BMessage = new FastRoundPhase2bMessage
            {
                ConfigurationId = 1,
                Sender = Utils.HostFromParts("127.0.0.2", 1001),
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase1AMessage = new Phase1aMessage
            {
                ConfigurationId = 1,
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase1BMessage = new Phase1bMessage
            {
                ConfigurationId = 1,
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase2AMessage = new Phase2aMessage
            {
                ConfigurationId = 1,
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

        var request = new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = 1,
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);

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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Send fast round votes - should not throw
        for (var i = 0; i < 3; i++)
        {
            var request = new RapidClusterRequest
            {
                FastRoundPhase2BMessage = new FastRoundPhase2bMessage
                {
                    ConfigurationId = 1,
                    Sender = Utils.HostFromParts($"127.0.0.{i + 1}", 1000 + i),
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Send votes with wrong config ID - should not throw
        for (var i = 0; i < 3; i++)
        {
            var request = new RapidClusterRequest
            {
                FastRoundPhase2BMessage = new FastRoundPhase2bMessage
                {
                    ConfigurationId = 999, // Wrong config ID
                    Sender = Utils.HostFromParts($"127.0.0.{i + 1}", 1000 + i),
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 5, trackForCleanup: false);
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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3, trackForCleanup: false);

        // Should not throw when called multiple times
        await coordinator.DisposeAsync();
        await coordinator.DisposeAsync();
        await coordinator.DisposeAsync();
    }

    [Fact]
    public async Task DisposeAsync_BeforePropose_DoesNotThrow()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 3, trackForCleanup: false);

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
        var coordinator = CreateCoordinator(myAddr, configurationId: 1, membershipSize: 10);
        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal, TestContext.Current.CancellationToken);

        // Send messages concurrently from multiple threads
        var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(() =>
        {
            var request = new RapidClusterRequest
            {
                FastRoundPhase2BMessage = new FastRoundPhase2bMessage
                {
                    ConfigurationId = 1,
                    Sender = Utils.HostFromParts($"127.0.0.{i + 1}", 1000 + i),
                    Proposal = proposal
                }
            };
            coordinator.HandleMessages(request, TestContext.Current.CancellationToken);
        }, TestContext.Current.CancellationToken)).ToArray();

        // Should not throw
        await Task.WhenAll(tasks);
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
            NullLogger<ConsensusCoordinator>.Instance,
            NullLogger<FastPaxos>.Instance,
            NullLogger<Paxos>.Instance);

        var coordinator = factory.Create(myAddr, configurationId: 42, membershipSize: 7, broadcaster);
        _coordinators.Add(coordinator);

        Assert.NotNull(coordinator);
        Assert.False(coordinator.Decided.IsCompleted);
    }

    #endregion

    #region Helper Methods

    private ConsensusCoordinator CreateCoordinator(
        Endpoint myAddr,
        long configurationId,
        int membershipSize,
        bool trackForCleanup = true)
    {
        var client = new TestMessagingClient();
        _clients.Add(client);
        var broadcaster = new TestBroadcaster();
        var viewAccessor = new TestMembershipViewAccessor(membershipSize);
        var options = Options.Create(new RapidClusterProtocolOptions
        {
            ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(100),
            MaxConsensusRounds = 10
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
            NullLogger<ConsensusCoordinator>.Instance,
            NullLogger<FastPaxos>.Instance,
            NullLogger<Paxos>.Instance);

        if (trackForCleanup)
        {
            _coordinators.Add(coordinator);
        }

        return coordinator;
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

    #endregion

    #region Test Doubles

    /// <summary>
    /// Simple test broadcaster that does nothing.
    /// </summary>
    private sealed class TestBroadcaster : IBroadcaster
    {
        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) { }
        public void Broadcast(RapidClusterRequest request, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }
    }

    /// <summary>
    /// Simple test messaging client.
    /// </summary>
    private sealed class TestMessagingClient : IMessagingClient
    {
        public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) { }
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
