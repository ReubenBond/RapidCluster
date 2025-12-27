using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Coordinates consensus for a single configuration change decision.
/// Manages the progression from fast round (round 1) through multiple
/// classic Paxos rounds (rounds 2, 3, ...) until a decision is reached.
///
/// This coordinator is event-driven: inbound messages and timeouts are serialized
/// through an internal event loop.
/// </summary>
internal sealed class ConsensusCoordinator : IAsyncDisposable
{
    private readonly ConsensusCoordinatorLogger _log;
    private readonly FastPaxosLogger _fastLog;
    private readonly PaxosLogger _paxosLog;

    private readonly RapidClusterMetrics _metrics;
    private readonly ProtocolMetrics _metricsScope;
    private readonly double _jitterRate;

    private readonly Endpoint _myAddr;
    private readonly ConfigurationId _configurationId;
    private readonly int _membershipSize;
    private readonly IMembershipViewAccessor _membershipViewAccessor;
    private readonly IBroadcaster _broadcaster;
    private readonly RapidClusterProtocolOptions _options;
    private readonly SharedResources _sharedResources;

    // Classic Paxos roles.
    private readonly PaxosAcceptor _paxosAcceptor;
    private readonly PaxosLearner _paxosLearner;

    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private Task? _consensusLoopTask;
    private int _disposed;

    private readonly TaskCompletionSource<MembershipProposal> _onDecidedTcs = new();

    private readonly Channel<ConsensusEvent> _events = Channel.CreateUnbounded<ConsensusEvent>(new UnboundedChannelOptions
    {
        SingleReader = true,
        SingleWriter = false,
        AllowSynchronousContinuations = true
    });

    private readonly OneShotTimer _timer = new();
    private readonly OneShotTimer _classicRestartTimer = new();

    private Rank _currentRank = new() { Round = 1, NodeIndex = 0 };
    private int _highestRoundSeen;

    private int? _scheduledClassicRoundStart;
    private TimeSpan? _scheduledClassicRoundDelay;

    private readonly Dictionary<MembershipProposal, int> _votesPerProposal = new(MembershipProposalComparer.Instance);
    private readonly HashSet<Endpoint> _votesReceived = new(EndpointAddressComparer.Instance);
    private readonly HashSet<Endpoint> _deliveryFailureEndpoints = new(EndpointAddressComparer.Instance);
    private int _fallbackSignaled;

    private readonly List<Phase1bMessage> _phase1bResponses = [];
    private MembershipProposal? _candidateValue;

    private readonly HashSet<Endpoint> _negativeVoters = new(EndpointAddressComparer.Instance);

    public Task<MembershipProposal> Decided => _onDecidedTcs.Task;

    private readonly record struct ProtocolMetrics(string ProtocolName)
    {
        public void RecordProposal(RapidClusterMetrics metrics) => metrics.RecordConsensusProposal(ProtocolName);
        public void RecordRoundStarted(RapidClusterMetrics metrics) => metrics.RecordConsensusRoundStarted(ProtocolName);
        public void RecordRoundCompleted(RapidClusterMetrics metrics, string result) => metrics.RecordConsensusRoundCompleted(ProtocolName, result);
        public void RecordLatency(RapidClusterMetrics metrics, string result, Stopwatch stopwatch) => metrics.RecordConsensusLatency(ProtocolName, result, stopwatch);
    }

    private abstract record ConsensusEvent
    {
        public sealed record Inbound(RapidClusterRequest Request, CancellationToken CancellationToken) : ConsensusEvent;
        public sealed record Timeout(int Round, TimeSpan Delay) : ConsensusEvent;
        public sealed record ClassicRestart(int ExpectedRound, TimeSpan Delay) : ConsensusEvent;
        public sealed record DeliveryFailure(Endpoint FailedEndpoint, Rank Rank) : ConsensusEvent;
    }

    public ConsensusCoordinator(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IMessagingClient client,
        IBroadcaster broadcaster,
        IMembershipViewAccessor membershipViewAccessor,
        IOptions<RapidClusterProtocolOptions> options,
        SharedResources sharedResources,
        RapidClusterMetrics metrics,
        ILogger<ConsensusCoordinator> logger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _broadcaster = broadcaster;
        _membershipViewAccessor = membershipViewAccessor;
        _options = options.Value;
        _sharedResources = sharedResources;
        _metrics = metrics;
        _metricsScope = new ProtocolMetrics(MetricNames.Protocols.Consensus);

        _log = new ConsensusCoordinatorLogger(logger);
        _fastLog = new FastPaxosLogger(logger);
        _paxosLog = new PaxosLogger(logger);

        _jitterRate = 1 / (double)membershipSize;

        _paxosAcceptor = new PaxosAcceptor(
            myAddr,
            configurationId,
            client,
            broadcaster,
            metrics,
            logger);

        _paxosLearner = new PaxosLearner(
            configurationId,
            membershipSize,
            metrics,
            logger);

        _log.Initialized(myAddr, configurationId, membershipSize);
    }

    public void Propose(MembershipProposal proposal, CancellationToken cancellationToken = default)
    {
        _log.Propose(proposal);

        // Ensure our acceptor state includes our local fast-round vote.
        _paxosAcceptor.RegisterFastRoundVote(proposal);

        _consensusLoopTask = RunConsensusLoopAsync(proposal, _disposeCts.Token);
    }

    private async Task RunConsensusLoopAsync(MembershipProposal proposal, CancellationToken cancellationToken)
    {
        var consensusStopwatch = Stopwatch.StartNew();

        try
        {
            _currentRank = new Rank { Round = 1, NodeIndex = 0 };
            _highestRoundSeen = 1;
            _fallbackSignaled = 0;
            _deliveryFailureEndpoints.Clear();
            _votesPerProposal.Clear();
            _votesReceived.Clear();
            _scheduledClassicRoundStart = null;
            _scheduledClassicRoundDelay = null;

            _log.StartingFastRound();
            _metricsScope.RecordProposal(_metrics);
            _metricsScope.RecordRoundStarted(_metrics);

            var timeout = GetRandomDelay();
            _timer.Schedule(
                _sharedResources.TimeProvider,
                timeout,
                () =>
                {
                    if (_onDecidedTcs.Task.IsCompleted)
                    {
                        return;
                    }

                    _events.Writer.TryWrite(new ConsensusEvent.Timeout(1, timeout));
                });

            StartFastRound(proposal, cancellationToken);

            while (await _events.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(true))
            {
                while (_events.Reader.TryRead(out var @event))
                {
                    HandleEvent(@event, consensusStopwatch, cancellationToken);

                    if (_onDecidedTcs.Task.IsCompleted)
                    {
                        return;
                    }

                    if (TryCompleteFromLearner(consensusStopwatch, cancellationToken))
                    {
                        return;
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            CompleteCancelled(
                _metricsScope,
                consensusStopwatch,
                recordRoundCompleted: false,
                recordLatency: true,
                cancellationToken);
        }
        finally
        {
            _timer.Dispose();
            _classicRestartTimer.Dispose();
        }
    }

    private void StartFastRound(MembershipProposal proposal, CancellationToken cancellationToken)
    {
        _fastLog.Propose(proposal);
        _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.FastVote);

        var consensusMessage = new Phase2bMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Proposal = proposal,
            Rnd = _currentRank
        };

        var proposalMessage = consensusMessage.ToRapidClusterRequest();

        _broadcaster.Broadcast(
            proposalMessage,
            _currentRank,
            (failedEndpoint, rank) => _events.Writer.TryWrite(new ConsensusEvent.DeliveryFailure(failedEndpoint, rank)),
            cancellationToken);

        // Ensure we process our own vote immediately, even if the broadcaster
        // does not loop back to the local node.
        _events.Writer.TryWrite(new ConsensusEvent.Inbound(proposalMessage, cancellationToken));
    }

    private void HandleDeliveryFailure(
        Endpoint failedEndpoint,
        Rank failedRank,
        CancellationToken cancellationToken)
    {
        if (failedRank.Round == 1)
        {
            if (_currentRank.Round != 1)
            {
                return;
            }

            if (!UpdateFastRoundOutcome(deliveryFailure: true, failedEndpoint: failedEndpoint))
            {
                return;
            }

            _ = failedEndpoint;
            _log.FastRoundFailedEarly();
            _metricsScope.RecordRoundCompleted(_metrics, MetricNames.Results.Conflict);
            _metrics.RecordConsensusConflict();

            StartClassicRound(cancellationToken);
            return;
        }


        if (_currentRank.Round < 2 || failedRank.Round != _currentRank.Round)
        {
            return;
        }

        RegisterClassicNegativeVote(
            failedEndpoint,
            nackRound: failedRank.Round,
            highestRoundSeen: null,
            cancellationToken);
    }

    private void RegisterClassicNegativeVote(
        Endpoint endpoint,
        int nackRound,
        int? highestRoundSeen,
        CancellationToken cancellationToken)
    {
        if (nackRound != _currentRank.Round)
        {
            return;
        }

        if (highestRoundSeen is { } roundSeen && roundSeen > _highestRoundSeen)
        {
            _highestRoundSeen = roundSeen;
        }

        _negativeVoters.Add(endpoint);

        var majorityThreshold = _membershipSize / 2 + 1;
        var maxPossiblePositiveVotes = _membershipSize - _negativeVoters.Count;
        if (maxPossiblePositiveVotes >= majorityThreshold)
        {
            return;
        }

        ScheduleClassicRoundRestart(cancellationToken);
    }

    private void ScheduleClassicRoundRestart(CancellationToken cancellationToken)
    {
        if (_currentRank.Round < 2 && _currentRank.Round != 1)
        {
            throw new UnreachableException($"Unexpected round: {_currentRank.Round}");
        }

        var nextRound = GetNextClassicRoundNumber();
        var delay = _scheduledClassicRoundDelay ?? GetRetryDelay(nextRound);

        if (_scheduledClassicRoundStart is { } scheduledRound)
        {
            if (scheduledRound >= nextRound)
            {
                return;
            }

            // A later round was requested; keep the earlier timer so we don't starve progress,
            // but bump the target round.
            _scheduledClassicRoundStart = nextRound;
            _scheduledClassicRoundDelay = delay;
            return;
        }

        _scheduledClassicRoundStart = nextRound;
        _scheduledClassicRoundDelay = delay;

        var expectedRound = _currentRank.Round;
        _classicRestartTimer.Schedule(
            _sharedResources.TimeProvider,
            delay,
            () => _events.Writer.TryWrite(new ConsensusEvent.ClassicRestart(expectedRound, delay)));

        int GetNextClassicRoundNumber()
        {
            if (_currentRank.Round == 1)
            {
                return 2;
            }

            _highestRoundSeen = Math.Max(_highestRoundSeen, _currentRank.Round);
            return _highestRoundSeen + 1;
        }
    }

    private void HandleEvent(
        ConsensusEvent @event,
        Stopwatch consensusStopwatch,
        CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case ConsensusEvent.Inbound(var request, _):
                HandleInbound(request, consensusStopwatch, cancellationToken);
                return;

            case ConsensusEvent.Timeout(var timedOutRound, var delay):
                if (timedOutRound != _currentRank.Round)
                {
                    return;
                }

                if (_currentRank.Round == 1)
                {
                    _log.FastRoundTimeout(_configurationId, delay);
                    _metricsScope.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
                    _metrics.RecordConsensusConflict();
                    StartClassicRound(cancellationToken);
                    return;
                }

                if (_currentRank.Round >= 2)
                {
                    _log.ClassicRoundTimeout(timedOutRound, delay);
                    _metricsScope.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
                    ScheduleClassicRoundRestart(cancellationToken);
                    return;
                }

                throw new UnreachableException($"Unexpected round: {_currentRank.Round}");

            case ConsensusEvent.ClassicRestart(var expectedRound, _):
                if (_currentRank.Round != expectedRound)
                {
                    return;
                }

                if (_currentRank.Round == 1)
                {
                    // We allow direct transition into round 2 when fast round has already been abandoned.
                    StartClassicRound(cancellationToken);
                    return;
                }

                if (_scheduledClassicRoundStart is not { } scheduledRound)
                {
                    return;
                }

                if (_currentRank.Round >= 2)
                {
                    StartClassicRound(scheduledRound, cancellationToken);
                    return;
                }

                throw new UnreachableException($"Unexpected round: {_currentRank.Round}");

            case ConsensusEvent.DeliveryFailure(var failedEndpoint, var failedRank):
                HandleDeliveryFailure(failedEndpoint, failedRank, cancellationToken);
                return;

            default:
                throw new UnreachableException($"Unexpected event type: {@event.GetType().Name}");
        }
    }


    private void HandleInbound(
        RapidClusterRequest request,
        Stopwatch consensusStopwatch,
        CancellationToken cancellationToken)
    {
        _log.HandleMessages(request.ContentCase);

        switch (request.ContentCase)
        {
            case RapidClusterRequest.ContentOneofCase.Phase2BMessage:
                HandlePhase2bMessage(request.Phase2BMessage, consensusStopwatch, cancellationToken);
                return;

            case RapidClusterRequest.ContentOneofCase.Phase1AMessage:
                _paxosAcceptor.HandlePhase1aMessage(request.Phase1AMessage, cancellationToken);
                return;

            case RapidClusterRequest.ContentOneofCase.Phase1BMessage:
                HandlePhase1bMessage(request.Phase1BMessage, cancellationToken);
                return;

            case RapidClusterRequest.ContentOneofCase.Phase2AMessage:
                _paxosAcceptor.HandlePhase2aMessage(request.Phase2AMessage, cancellationToken);
                return;

            default:
                throw new ArgumentException($"Unexpected message case: {request.ContentCase}");
        }
    }

    private bool UpdateFastRoundOutcome(bool deliveryFailure, Endpoint? failedEndpoint)
    {
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        var threshold = _membershipSize - f;

        if (deliveryFailure)
        {
            if (failedEndpoint == null)
            {
                throw new InvalidOperationException("failedEndpoint must be provided when deliveryFailure is true.");
            }

            if (!_deliveryFailureEndpoints.Add(failedEndpoint))
            {
                return false;
            }
        }

        var failures = _deliveryFailureEndpoints.Count;

        // Early fallback is only warranted when it's impossible for any proposal
        // to still reach the Fast Paxos threshold with the remaining responsive acceptors.
        var possibleRemainingVotes = _membershipSize - failures;
        if (possibleRemainingVotes >= threshold)
        {
            return false;
        }

        if (Interlocked.Exchange(ref _fallbackSignaled, 1) != 0)
        {
            return false;
        }

        _fastLog.EarlyFallbackNeeded(failures, f, threshold);
        return true;
    }




    private void StartClassicRound(CancellationToken cancellationToken)
    {
        StartClassicRound(targetRound: null, cancellationToken);
    }

    private void StartClassicRound(int? targetRound, CancellationToken cancellationToken)
    {
        if (_currentRank.Round == 1)
        {
            if (targetRound is not null and not 2)
            {
                throw new ArgumentOutOfRangeException(nameof(targetRound), targetRound, "Fast-round fallback must start classic round 2.");
            }

            _timer.Dispose();
            var nodeId = _membershipViewAccessor.CurrentView.GetNodeId(_myAddr);
            var nodeIndex = unchecked((int)nodeId);
            _currentRank = new Rank { Round = 2, NodeIndex = nodeIndex };
        }
        else if (_currentRank.Round >= 2)
        {
            if (targetRound is { } specifiedRound)
            {
                if (specifiedRound <= _currentRank.Round)
                {
                    return;
                }

                _highestRoundSeen = Math.Max(_highestRoundSeen, specifiedRound - 1);
                _currentRank = new Rank { Round = specifiedRound, NodeIndex = _currentRank.NodeIndex };
            }
            else
            {
                // This round is being abandoned; track it so we advance monotonically.
                _highestRoundSeen = Math.Max(_highestRoundSeen, _currentRank.Round);
                _currentRank = new Rank { Round = _highestRoundSeen + 1, NodeIndex = _currentRank.NodeIndex };
            }
        }
        else
        {
            throw new UnreachableException($"Unexpected round: {_currentRank.Round}");
        }

        _scheduledClassicRoundStart = null;
        _scheduledClassicRoundDelay = null;
        _classicRestartTimer.Dispose();

        _negativeVoters.Clear();
        _candidateValue = null;
        _phase1bResponses.Clear();

        _deliveryFailureEndpoints.Clear();
        _votesPerProposal.Clear();
        _votesReceived.Clear();
        _fallbackSignaled = 0;

        var delay = GetRetryDelay(_currentRank.Round);
        _log.StartingClassicRound(_currentRank.Round, delay);
        _metricsScope.RecordRoundStarted(_metrics);

        _paxosLog.PrepareCalled(_myAddr, _currentRank);

        var prepare = new Phase1aMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Rank = _currentRank
        };

        var request = prepare.ToRapidClusterRequest();
        _paxosLog.BroadcastingPhase1a();

        _broadcaster.Broadcast(
            request,
            _currentRank,
            (failedEndpoint, rank) => _events.Writer.TryWrite(new ConsensusEvent.DeliveryFailure(failedEndpoint, rank)),
            cancellationToken);

        // Ensure our acceptor responds to our prepare without relying on network loopback.
        var localPhase1b = _paxosAcceptor.HandlePhase1aMessageLocal(prepare);
        if (localPhase1b != null)
        {
            HandlePhase1bMessage(localPhase1b, cancellationToken);
        }

        var timedOutRound = _currentRank.Round;
        _timer.Schedule(
            _sharedResources.TimeProvider,
            delay,
            () =>
            {
                if (_onDecidedTcs.Task.IsCompleted)
                {
                    return;
                }

                _events.Writer.TryWrite(new ConsensusEvent.Timeout(timedOutRound, delay));
            });

    }

    private void HandlePhase2bMessage(
        Phase2bMessage phase2bMessage,
        Stopwatch consensusStopwatch,
        CancellationToken cancellationToken)
    {
        var messageConfigId = phase2bMessage.ConfigurationId.ToConfigurationId();
        if (messageConfigId != _configurationId)
        {
            if (phase2bMessage.Rnd.Round == 1)
            {
                _fastLog.ConfigurationMismatch(_configurationId, messageConfigId);
            }

            return;
        }

        if (phase2bMessage.Rnd.Round == 1)
        {
            if (_currentRank.Round != 1)
            {
                return;
            }

            if (!phase2bMessage.Rnd.Equals(_currentRank))
            {
                return;
            }

            _fastLog.HandleFastRoundProposalReceived(phase2bMessage.Sender, phase2bMessage.Proposal, messageConfigId);

            if (!_votesReceived.Add(phase2bMessage.Sender))
            {
                _fastLog.DuplicateFastRoundVote(phase2bMessage.Sender);
                return;
            }

            _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.FastVote);

            var proposal = phase2bMessage.Proposal;
            if (proposal == null)
            {
                return;
            }

            ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_votesPerProposal, proposal, out _);
            ++entry;

            var count = entry;
            var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
            var threshold = _membershipSize - f;
            _fastLog.FastRoundVoteCount(count, _votesReceived.Count, threshold, f);

            if (count >= threshold)
            {
                _fastLog.DecidedViewChange(proposal);
                _metricsScope.RecordRoundCompleted(_metrics, MetricNames.Results.Success);
                CompleteDecided(_metricsScope, consensusStopwatch, proposal);
                return;
            }

            if (UpdateFastRoundOutcome(deliveryFailure: false, failedEndpoint: null))
            {
                _fastLog.FastRoundMayNotSucceed();
                _log.FastRoundFailedEarly();
                _metricsScope.RecordRoundCompleted(_metrics, MetricNames.Results.Conflict);
                _metrics.RecordConsensusConflict();

                StartClassicRound(cancellationToken);
            }

            return;
        }

        if (_currentRank.Round >= 2)
        {
            if (RankComparer.Instance.Compare(phase2bMessage.Rnd, _currentRank) > 0)
            {
                RegisterClassicNegativeVote(
                    phase2bMessage.Sender,
                    nackRound: _currentRank.Round,
                    highestRoundSeen: phase2bMessage.Rnd.Round,
                    cancellationToken);
                return;
            }

            if (!phase2bMessage.Rnd.Equals(_currentRank))
            {
                return;
            }

            var highestSeen = phase2bMessage.Rnd.Round;
            if (highestSeen > _highestRoundSeen)
            {
                _highestRoundSeen = highestSeen;
            }
        }

        _paxosLearner.HandlePhase2bMessage(phase2bMessage);
    }


    private void HandlePhase1bMessage(Phase1bMessage phase1bMessage, CancellationToken cancellationToken)
    {
        if (_currentRank.Round < 2)
        {
            return;
        }

        var messageConfigId = phase1bMessage.ConfigurationId.ToConfigurationId();
        _paxosLog.HandlePhase1bReceived(phase1bMessage.Sender, phase1bMessage.Rnd, phase1bMessage.Vrnd, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _paxosLog.Phase1bConfigMismatch(_configurationId, messageConfigId);
            return;
        }

        var highestSeen = Math.Max(phase1bMessage.Rnd.Round, phase1bMessage.Vrnd.Round);
        if (highestSeen > _highestRoundSeen)
        {
            _highestRoundSeen = highestSeen;
        }

        // Treat a Phase1b from a higher promised round as a negative vote (NACK).
        // This indicates another coordinator is ahead, so our current round may be unsatisfiable.
        if (RankComparer.Instance.Compare(phase1bMessage.Rnd, _currentRank) > 0)
        {
            RegisterClassicNegativeVote(
                phase1bMessage.Sender,
                nackRound: _currentRank.Round,
                highestRoundSeen: phase1bMessage.Rnd.Round,
                cancellationToken);
            return;
        }

        if (!phase1bMessage.Rnd.Equals(_currentRank))
        {
            _paxosLog.Phase1bRoundMismatch(_currentRank, phase1bMessage.Rnd);
            return;
        }

        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.Phase1b);
        _phase1bResponses.Add(phase1bMessage);

        var majorityThreshold = _membershipSize / 2 + 1;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        _paxosLog.Phase1bCollected(_phase1bResponses.Count, majorityThreshold, f);

        if (_phase1bResponses.Count < majorityThreshold)
        {
            return;
        }

        var chosenValue = ChooseValue(_phase1bResponses, _membershipSize);

        if (_candidateValue != null || chosenValue == null)
        {
            return;
        }

        _candidateValue = chosenValue;
        _paxosLog.Phase1bChosenValue(_candidateValue);

        var phase2a = new Phase2aMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Rnd = _currentRank,
            Proposal = _candidateValue
        };

        var request = phase2a.ToRapidClusterRequest();
        _broadcaster.Broadcast(
            request,
            _currentRank,
            (failedEndpoint, rank) => _events.Writer.TryWrite(new ConsensusEvent.DeliveryFailure(failedEndpoint, rank)),
            cancellationToken);

        // Ensure we do not depend on the broadcaster sending Phase2a to ourselves.
        // Route the local acceptor response through the same event loop.
        var localPhase2b = _paxosAcceptor.HandlePhase2aMessageLocal(phase2a);
        if (localPhase2b != null)
        {
            _events.Writer.TryWrite(new ConsensusEvent.Inbound(localPhase2b.ToRapidClusterRequest(), cancellationToken));
        }
    }

    internal static MembershipProposal? ChooseValue(List<Phase1bMessage> phase1bMessages, int n)
    {
        var maxVrnd = phase1bMessages
            .Select(m => m.Vrnd)
            .Max(RankComparer.Instance);

        if (maxVrnd == null)
        {
            return null;
        }

        var collectedProposals = phase1bMessages
            .Where(m => RankComparer.Instance.Compare(m.Vrnd, maxVrnd) == 0)
            .Where(m => m.Proposal != null && m.Proposal.Members.Count > 0)
            .Select(m => m.Proposal!)
            .ToList();

        if (collectedProposals.Count > 0)
        {
            var firstValue = collectedProposals[0];
            var allIdentical = collectedProposals.All(p => MembershipProposalComparer.Instance.Equals(p, firstValue));
            if (allIdentical)
            {
                return firstValue;
            }
        }

        if (collectedProposals.Count > 1)
        {
            var valueCounts = new Dictionary<MembershipProposal, int>(MembershipProposalComparer.Instance);
            foreach (var proposal in collectedProposals)
            {
                ref var count = ref CollectionsMarshal.GetValueRefOrAddDefault(valueCounts, proposal, out _);
                ++count;
                if (count > n / 4)
                {
                    return proposal;
                }
            }
        }

        return phase1bMessages
            .Where(m => m.Proposal != null && m.Proposal.Members.Count > 0)
            .Select(m => m.Proposal!)
            .OrderBy(p => p, MembershipProposalComparer.Instance)
            .FirstOrDefault();
    }

    private bool TryCompleteFromLearner(Stopwatch consensusStopwatch, CancellationToken cancellationToken)
    {
        if (_paxosLearner.Decided is ConsensusResult.Decided classicDecided)
        {
            CompleteDecided(_metricsScope, consensusStopwatch, classicDecided.Value);
            return true;
        }

        if (_paxosLearner.Decided is ConsensusResult.Cancelled && cancellationToken.IsCancellationRequested)
        {
            CompleteCancelled(_metricsScope, recordRoundCompleted: true, cancellationToken);
            return true;
        }

        return false;
    }

    private void CompleteDecided(ProtocolMetrics protocolMetrics, Stopwatch consensusStopwatch, MembershipProposal decidedValue)
    {
        _log.ConsensusDecided(_configurationId, decidedValue);
        protocolMetrics.RecordLatency(_metrics, MetricNames.Results.Success, consensusStopwatch);
        _onDecidedTcs.TrySetResult(decidedValue);

        _events.Writer.TryComplete();
    }

    private void CompleteCancelled(
        ProtocolMetrics protocolMetrics,
        bool recordRoundCompleted,
        CancellationToken cancellationToken)
    {
        _log.ConsensusCancelled();

        if (recordRoundCompleted)
        {
            protocolMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Aborted);
        }

        _onDecidedTcs.TrySetCanceled(cancellationToken);
        _events.Writer.TryComplete();
    }

    private void CompleteCancelled(
        ProtocolMetrics protocolMetrics,
        Stopwatch consensusStopwatch,
        bool recordRoundCompleted,
        bool recordLatency,
        CancellationToken cancellationToken)
    {
        _log.ConsensusCancelled();

        if (recordRoundCompleted)
        {
            protocolMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Aborted);
        }

        if (recordLatency)
        {
            protocolMetrics.RecordLatency(_metrics, MetricNames.Results.Aborted, consensusStopwatch);
        }

        _onDecidedTcs.TrySetCanceled(cancellationToken);
        _events.Writer.TryComplete();
    }

    public RapidClusterResponse HandleMessages(RapidClusterRequest request, CancellationToken cancellationToken = default)
    {
        if (request.ContentCase == RapidClusterRequest.ContentOneofCase.None)
        {
            throw new ArgumentException($"Unexpected message case: {request.ContentCase}");
        }

        lock (_lock)
        {
            if (_disposed == 0 && _consensusLoopTask != null && !_onDecidedTcs.Task.IsCompleted)
            {
                _events.Writer.TryWrite(new ConsensusEvent.Inbound(request, cancellationToken));
            }
        }

        return new ConsensusResponse().ToRapidClusterResponse();
    }

    private TimeSpan GetRandomDelay()
    {
        var jitter = (long)(-1000 * Math.Log(1 - _sharedResources.NextRandomDouble()) / _jitterRate);
        return TimeSpan.FromMilliseconds(jitter + (long)_options.ConsensusFallbackTimeoutBaseDelay.TotalMilliseconds);
    }

    private TimeSpan GetRetryDelay(int roundNumber)
    {
        var baseMs = _options.ConsensusFallbackTimeoutBaseDelay.TotalMilliseconds;
        var multiplier = Math.Min(Math.Pow(1.5, roundNumber - 2), 8);
        var jitter = (long)(-1000 * Math.Log(1 - _sharedResources.NextRandomDouble()) / _jitterRate);

        var ringPositionDelay = GetRingPositionDelay();

        return TimeSpan.FromMilliseconds(baseMs * multiplier + jitter + ringPositionDelay);
    }

    private double GetRingPositionDelay()
    {
        var view = _membershipViewAccessor.CurrentView;
        if (view.Size <= 1)
        {
            return 0;
        }

        var position = view.GetRingPosition(_myAddr);
        if (position < 0)
        {
            return 0;
        }

        var baseDelayMs = _options.ConsensusFallbackTimeoutBaseDelay.TotalMilliseconds;
        return position / (double)view.Size * baseDelayMs * 0.25;
    }

    /// <summary>
    /// Cancels the consensus operation without waiting for it to complete.
    /// Use this for fast shutdown. Call <see cref="DisposeAsync"/> after to clean up resources.
    /// </summary>
    public void Cancel()
    {
        _log.Cancelling();

        _disposeCts.SafeCancel(_log.Logger);
        _events.Writer.TryComplete();
        _paxosLearner.Cancel();

        // Dispose timers immediately to stop any scheduled callbacks
        _timer.Dispose();
        _classicRestartTimer.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _log.Dispose();

        // Cancel if not already cancelled
        _disposeCts.SafeCancel(_log.Logger);
        _events.Writer.TryComplete();

        _paxosLearner.Cancel();

        if (_consensusLoopTask is { } task)
        {
            await task.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        }

        _timer.Dispose();
        _classicRestartTimer.Dispose();

        _onDecidedTcs.TrySetCanceled();

        _disposeCts.Dispose();
    }
}
