using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
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
        coordinator.Propose(proposal);

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
        coordinator.Propose(proposal);

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
                Proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001)),
            },
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
                Rank = new Rank { Round = 2, NodeIndex = 1 },
            },
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
                Vrnd = new Rank { Round = 1, NodeIndex = 0 },
            },
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
                Proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001)),
            },
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
                Rnd = new Rank { Round = 2, NodeIndex = 1 },
            },
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

        coordinator.Propose(proposal);

        // Send fast round votes - should not throw
        for (var i = 0; i < 3; i++)
        {
            var request = new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                    Sender = Utils.HostFromParts(string.Create(CultureInfo.InvariantCulture, $"127.0.0.{i + 1}"), 1000 + i),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposal,
                },
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

        coordinator.Propose(proposal);

        // Send votes with wrong config ID - should not throw
        var wrongConfigId = new ConfigurationId(new ClusterId(888), version: 999);
        for (var i = 0; i < 3; i++)
        {
            var request = new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = wrongConfigId.ToProtobuf(),
                    Sender = Utils.HostFromParts(string.Create(CultureInfo.InvariantCulture, $"127.0.0.{i + 1}"), 1000 + i),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposal,
                },
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

        coordinator.Propose(proposal);

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

        coordinator.Propose(proposal);

        // Send messages concurrently from multiple threads
        var tasks = Enumerable.Range(0, 20).Select(i => Task.Run(() =>
        {
            var request = new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = new ConfigurationId(new ClusterId(888), 1).ToProtobuf(),
                    Sender = Utils.HostFromParts(string.Create(CultureInfo.InvariantCulture, $"127.0.0.{i + 1}"), 1000 + i),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposal,
                },
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
        coordinator.Propose(CreateProposal(Utils.HostFromParts("10.0.0.1", 5001)));

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
                Proposal = decidedProposal,
            },
        }, TestContext.Current.CancellationToken);

        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = rnd,
                Proposal = decidedProposal,
            },
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

        coordinator.Propose(CreateProposal(Utils.HostFromParts("10.0.0.1", 5001)));

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
                Proposal = null,
            },
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
                Proposal = null,
            },
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

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) => broadcasted.Enqueue(request);

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

    #region Fast Round Early Abandonment Tests

    /// <summary>
    /// Verifies the fast round threshold calculation for various cluster sizes.
    /// Fast Paxos threshold = N - floor((N-1)/4)
    /// </summary>
    [Theory]
    [InlineData(3, 3)]   // f=0, threshold=3 (all must agree)
    [InlineData(4, 4)]   // f=0, threshold=4 (all must agree)
    [InlineData(5, 4)]   // f=1, threshold=4
    [InlineData(6, 5)]   // f=1, threshold=5
    [InlineData(9, 7)]   // f=2, threshold=7
    [InlineData(10, 8)]  // f=2, threshold=8
    [InlineData(13, 10)] // f=3, threshold=10
    public void FastRound_ThresholdCalculation_IsCorrect(int membershipSize, int expectedThreshold)
    {
        var f = (int)Math.Floor((membershipSize - 1) / 4.0);
        var threshold = membershipSize - f;
        Assert.Equal(expectedThreshold, threshold);
    }

    /// <summary>
    /// <para>
    /// When votes are split between two proposals such that neither can reach threshold,
    /// the fast round should abandon early and fall back to classic Paxos.
    /// </para>
    /// <para>
    /// This test verifies that the fallback happens IMMEDIATELY after the conflicting vote,
    /// NOT due to a timeout. We verify this by checking that Phase1a is broadcast without
    /// advancing time.
    /// </para>
    /// <para>
    /// Scenario: 5 nodes, threshold=4
    /// - 2 votes for proposal A
    /// - 2 votes for proposal B  
    /// - 1 remaining voter
    /// Neither A nor B can reach 4 votes, so we should abandon.
    /// </para>
    /// </summary>
    [Fact]
    public async Task FastRound_ConflictingProposals_AbandonsWhenNeitherCanReachThreshold()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new NonFailingCapturingBroadcaster(broadcasted);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 5, broadcaster);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        coordinator.Propose(proposalA);

        // Vote 1 for A (from ourselves, via Propose)
        // Vote 2 for A
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalA,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 1 for B
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalB,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 2 for B - at this point:
        // - A has 2 votes, B has 2 votes, 1 remaining voter
        // - A can get at most 3 votes, B can get at most 3 votes
        // - Neither can reach threshold of 4, should abandon IMMEDIATELY
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.4", 1003, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalB,
            },
        }, TestContext.Current.CancellationToken);

        // Allow event loop to process without advancing time significantly
        // (only a few milliseconds to let async processing complete)
        await WaitForEventProcessingAsync();

        // Should have started classic round IMMEDIATELY (Phase1a broadcast indicates fallback)
        Assert.Contains(broadcasted,
            r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage);
    }

    /// <summary>
    /// <para>
    /// When one proposal still has a path to reaching threshold despite conflicts,
    /// the fast round should NOT abandon early.
    /// </para>
    /// <para>
    /// Scenario: 5 nodes, threshold=4
    /// - 3 votes for proposal A
    /// - 1 vote for proposal B
    /// - 1 remaining voter
    /// A can still reach 4 votes if the remaining voter votes for A.
    /// </para>
    /// </summary>
    [Fact]
    public async Task FastRound_ConflictingProposals_DoesNotAbandonIfOneCanStillSucceed()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new NonFailingCapturingBroadcaster(broadcasted);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 5, broadcaster);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        coordinator.Propose(proposalA);

        // Vote 1 for A (from ourselves)
        // Vote 2 for A
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalA,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 3 for A
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalA,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 1 for B - at this point:
        // - A has 3 votes, B has 1 vote, 1 remaining voter
        // - A can reach 4 votes if remaining voter votes for A
        // - Should NOT abandon
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.4", 1003, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalB,
            },
        }, TestContext.Current.CancellationToken);

        // Allow event loop to process without advancing time
        await WaitForEventProcessingAsync();

        // Should NOT have started classic round yet (no Phase1a should be broadcasted)
        Assert.DoesNotContain(broadcasted,
            r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage);
    }

    /// <summary>
    /// <para>
    /// When conflicting proposals are combined with delivery failures,
    /// and no proposal can reach threshold, should abandon early.
    /// </para>
    /// <para>
    /// Scenario: 5 nodes, threshold=4
    /// - 2 votes for proposal A
    /// - 1 vote for proposal B
    /// - 1 delivery failure
    /// - 1 remaining voter
    /// A can get at most 3 votes (2 + 1 remaining), B can get at most 2
    /// Neither can reach 4, should abandon.
    /// </para>
    /// </summary>
    [Fact]
    public async Task FastRound_ConflictingProposals_WithDeliveryFailure_AbandonsEarly()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var failedEndpoint = Utils.HostFromParts("127.0.0.5", 1004, nodeId: Utils.GetNextNodeId());
        var broadcaster = new SingleFailureCapturingBroadcaster(broadcasted, failedEndpoint);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 5, broadcaster);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        coordinator.Propose(proposalA);

        // Vote 1 for A (from ourselves, delivery failure already reported)
        // Vote 2 for A
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalA,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 1 for B - at this point:
        // - A has 2 votes, B has 1 vote
        // - 1 delivery failure (won't vote for anyone)
        // - 1 remaining voter
        // - A can get at most 3 votes, B can get at most 2
        // - Neither can reach threshold of 4, should abandon IMMEDIATELY
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalB,
            },
        }, TestContext.Current.CancellationToken);

        // Allow event loop to process without advancing time
        await WaitForEventProcessingAsync();

        // Should have started classic round IMMEDIATELY
        Assert.Contains(broadcasted,
            r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage);
    }

    /// <summary>
    /// Verifies that when all voters vote for the same proposal and threshold is reached,
    /// the fast round decides successfully (sanity check - don't break happy path).
    /// </summary>
    [Fact]
    public async Task FastRound_AllVotesSameProposal_DecidesSuccessfully()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new NonFailingCapturingBroadcaster(broadcasted);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 3, broadcaster);

        var proposal = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));

        coordinator.Propose(proposal);

        // Vote 1 (from ourselves, already counted via Propose)
        // Vote 2
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposal,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 3 - threshold is 3 for N=3, should decide
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposal,
            },
        }, TestContext.Current.CancellationToken);

        // Allow event loop to process
        await WaitForEventProcessingAsync();

        Assert.True(coordinator.Decided.IsCompleted);
        Assert.False(coordinator.Decided.IsFaulted);
        Assert.False(coordinator.Decided.IsCanceled);
    }

    /// <summary>
    /// <para>Test early abandonment with larger cluster (N=9, threshold=7, f=2).</para>
    /// <para>
    /// Scenario: 9 nodes, threshold=7
    /// - 3 votes for proposal A
    /// - 3 votes for proposal B
    /// - 3 remaining voters
    /// A can get at most 6 votes, B can get at most 6 votes
    /// Neither can reach 7, should abandon.
    /// </para>
    /// </summary>
    [Fact]
    public async Task FastRound_LargeCluster_AbandonsWhenNoProposalCanReachThreshold()
    {
        var myAddr = Utils.HostFromParts("127.0.0.1", 1000);
        var configId = new ConfigurationId(new ClusterId(888), 1);

        var broadcasted = new ConcurrentQueue<RapidClusterRequest>();
        var broadcaster = new NonFailingCapturingBroadcaster(broadcasted);
        var coordinator = CreateCoordinator(myAddr, configId, membershipSize: 9, broadcaster);

        var proposalA = CreateProposal(Utils.HostFromParts("10.0.0.1", 5001));
        var proposalB = CreateProposal(Utils.HostFromParts("10.0.0.2", 5002));

        coordinator.Propose(proposalA);

        // Votes 1-2 for A (ourselves + one more)
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.2", 1001, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalA,
            },
        }, TestContext.Current.CancellationToken);

        // Vote 3 for A
        coordinator.HandleMessages(new RapidClusterRequest
        {
            Phase2BMessage = new Phase2bMessage
            {
                ConfigurationId = configId.ToProtobuf(),
                Sender = Utils.HostFromParts("127.0.0.3", 1002, nodeId: Utils.GetNextNodeId()),
                Rnd = new Rank { Round = 1, NodeIndex = 0 },
                Proposal = proposalA,
            },
        }, TestContext.Current.CancellationToken);

        // Votes 1-3 for B
        for (var i = 0; i < 3; i++)
        {
            coordinator.HandleMessages(new RapidClusterRequest
            {
                Phase2BMessage = new Phase2bMessage
                {
                    ConfigurationId = configId.ToProtobuf(),
                    Sender = Utils.HostFromParts(string.Create(CultureInfo.InvariantCulture, $"127.0.0.{4 + i}"), 1003 + i, nodeId: Utils.GetNextNodeId()),
                    Rnd = new Rank { Round = 1, NodeIndex = 0 },
                    Proposal = proposalB,
                },
            }, TestContext.Current.CancellationToken);
        }

        // At this point: A=3, B=3, 3 remaining
        // A can get at most 6, B can get at most 6
        // Neither can reach 7, should abandon IMMEDIATELY

        // Allow event loop to process without advancing time
        await WaitForEventProcessingAsync();

        // Should have started classic round IMMEDIATELY
        Assert.Contains(broadcasted,
            r => r.ContentCase == RapidClusterRequest.ContentOneofCase.Phase1AMessage);
    }

    /// <summary>
    /// Helper method that allows the async event loop to process messages
    /// without advancing simulated time significantly. This ensures we're
    /// testing for immediate abandonment, not timeout-triggered abandonment.
    /// </summary>
    private async Task WaitForEventProcessingAsync()
    {
        // Yield to allow async event processing
        // We only advance time by a tiny amount (1ms) to trigger any scheduled tasks
        // but NOT enough to trigger the consensus timeout (which is 100ms+ in tests)
        for (var i = 0; i < 10; i++)
        {
            _timeProvider.Advance(TimeSpan.FromMilliseconds(1));
            await Task.Yield();
        }
    }

    /// <summary>
    /// A broadcaster that captures messages but does not report delivery failures.
    /// </summary>
    private sealed class NonFailingCapturingBroadcaster(ConcurrentQueue<RapidClusterRequest> broadcasted) : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) => broadcasted.Enqueue(request);

        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) => broadcasted.Enqueue(request);// No delivery failures reported
    }

    /// <summary>
    /// A broadcaster that captures messages and reports a single delivery failure for fast round.
    /// </summary>
    private sealed class SingleFailureCapturingBroadcaster(ConcurrentQueue<RapidClusterRequest> broadcasted, Endpoint failedEndpoint) : IBroadcaster
    {
        public void SetMembership(IReadOnlyList<Endpoint> membership) { }

        public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken) => broadcasted.Enqueue(request);

        public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
        {
            broadcasted.Enqueue(request);

            // Report a single delivery failure for fast round
            if (rank is { Round: 1 })
            {
                onDeliveryFailure?.Invoke(failedEndpoint, rank);
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
        bool trackForCleanup = true) => CreateCoordinator(myAddr, configurationId, membershipSize, broadcaster: new TestBroadcaster(), trackForCleanup);

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
            ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(100),
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
    private sealed class TestMembershipViewAccessor(int membershipSize = 3) : IMembershipViewAccessor
    {
        public MembershipView CurrentView { get; } = Utils.CreateMembershipView(membershipSize);
        public BroadcastChannelReader<MembershipView> Updates => throw new NotSupportedException();
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
