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
    private readonly ProtocolMetrics _fastMetrics;
    private readonly ProtocolMetrics _classicMetrics;
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

    private readonly OneShotTimer _timeout = new();

    // --- Fast round state (merged from FastPaxosProposer) ---
    private readonly Dictionary<MembershipProposal, int> _fastVotesPerProposal = new(MembershipProposalComparer.Instance);
    private readonly HashSet<Endpoint> _fastVotesReceived = new(EndpointAddressComparer.Instance);
    private int _fastDeliveryFailureCount;
    private int _fastDeliveryFailureSignaled;
    private bool _isInClassic;

    // --- Classic proposer state (merged from PaxosProposer) ---
    private readonly List<Phase1bMessage> _phase1bMessages = [];
    private Rank _currentClassicRank = new();
    private MembershipProposal? _candidateValue;
    private int _currentClassicRound;

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
        public sealed record FastTimeout(TimeSpan Timeout) : ConsensusEvent;
        public sealed record FastDeliveryFailure(Rank Rank) : ConsensusEvent;
        public sealed record ClassicTimeout(int Round, TimeSpan Delay) : ConsensusEvent;
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
        _fastMetrics = new ProtocolMetrics(MetricNames.Protocols.FastPaxos);
        _classicMetrics = new ProtocolMetrics(MetricNames.Protocols.ClassicPaxos);

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
        var activeProtocolMetrics = _fastMetrics;

        try
        {
            _log.StartingFastRound();
            _fastMetrics.RecordProposal(_metrics);
            _fastMetrics.RecordRoundStarted(_metrics);

            var fastRoundTimeout = GetRandomDelay();
            _timeout.Schedule(
                _sharedResources.TimeProvider,
                fastRoundTimeout,
                () =>
                {
                    if (_onDecidedTcs.Task.IsCompleted)
                    {
                        return;
                    }

                    _events.Writer.TryWrite(new ConsensusEvent.FastTimeout(fastRoundTimeout));
                });

            StartFastRound(proposal, cancellationToken);

            while (await _events.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(true))
            {
                while (_events.Reader.TryRead(out var @event))
                {
                    HandleEvent(@event, consensusStopwatch, ref activeProtocolMetrics, cancellationToken);

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
                activeProtocolMetrics,
                consensusStopwatch,
                recordRoundCompleted: false,
                recordLatency: true,
                cancellationToken);
        }
        finally
        {
            _timeout.Dispose();
        }
    }

    private void StartFastRound(MembershipProposal proposal, CancellationToken cancellationToken)
    {
        _fastLog.Propose(proposal);
        _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.FastVote);

        var consensusMessage = new FastRoundPhase2bMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Proposal = proposal
        };

        var proposalMessage = consensusMessage.ToRapidClusterRequest();

        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        var fastPaxosThreshold = _membershipSize - f;

        var fastRoundRank = new Rank { Round = 1, NodeIndex = 1 };

        _broadcaster.Broadcast(proposalMessage, fastRoundRank, (failedEndpoint, rank) =>
        {
            _ = failedEndpoint;
            var newFailureCount = Interlocked.Increment(ref _fastDeliveryFailureCount);
            var maxPossibleVotes = _membershipSize - newFailureCount;

            if (maxPossibleVotes >= fastPaxosThreshold)
            {
                return;
            }

            if (Interlocked.Exchange(ref _fastDeliveryFailureSignaled, 1) != 0)
            {
                return;
            }

            _fastLog.EarlyFallbackNeeded(newFailureCount, f, fastPaxosThreshold);
            _events.Writer.TryWrite(new ConsensusEvent.FastDeliveryFailure(rank));
        }, cancellationToken);
    }

    private void HandleEvent(
        ConsensusEvent @event,
        Stopwatch consensusStopwatch,
        ref ProtocolMetrics activeProtocolMetrics,
        CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case ConsensusEvent.Inbound(var request, var requestCancellationToken):
                HandleInbound(request, consensusStopwatch, ref activeProtocolMetrics, requestCancellationToken);
                return;

            case ConsensusEvent.FastTimeout(var timeout):
                if (_isInClassic)
                {
                    return;
                }

                _log.FastRoundTimeout(_configurationId, timeout);
                _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
                _metrics.RecordConsensusConflict();

                StartClassicRound(roundNumber: 2, cancellationToken);
                activeProtocolMetrics = _classicMetrics;
                return;

            case ConsensusEvent.FastDeliveryFailure(var failedRank):
                if (_isInClassic || failedRank.Round != 1)
                {
                    return;
                }

                _log.FastRoundFailedEarly();
                _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Conflict);
                _metrics.RecordConsensusConflict();

                StartClassicRound(roundNumber: 2, cancellationToken);
                activeProtocolMetrics = _classicMetrics;
                return;

            case ConsensusEvent.ClassicTimeout(var timedOutRound, var delay):
                if (!_isInClassic || timedOutRound != _currentClassicRound)
                {
                    return;
                }

                _log.ClassicRoundTimeout(timedOutRound, delay);
                _classicMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);

                StartClassicRound(timedOutRound + 1, cancellationToken);
                return;

            default:
                throw new UnreachableException($"Unexpected event type: {@event.GetType().Name}");
        }
    }

    private void HandleInbound(
        RapidClusterRequest request,
        Stopwatch consensusStopwatch,
        ref ProtocolMetrics activeProtocolMetrics,
        CancellationToken cancellationToken)
    {
        _log.HandleMessages(request.ContentCase);

        switch (request.ContentCase)
        {
            case RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage:
                HandleFastRoundPhase2b(request.FastRoundPhase2BMessage, consensusStopwatch, cancellationToken);
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

            case RapidClusterRequest.ContentOneofCase.Phase2BMessage:
                _paxosLearner.HandlePhase2bMessage(request.Phase2BMessage);
                return;

            case RapidClusterRequest.ContentOneofCase.PaxosNackMessage:
                if (!_isInClassic)
                {
                    return;
                }

                var requestedRound = HandlePaxosNackMessage(request.PaxosNackMessage);
                if (requestedRound is { } nackRound && nackRound > _currentClassicRound)
                {
                    StartClassicRound(nackRound, cancellationToken);
                }

                return;

            default:
                throw new ArgumentException($"Unexpected message case: {request.ContentCase}");
        }
    }

    private void HandleFastRoundPhase2b(FastRoundPhase2bMessage proposalMessage, Stopwatch consensusStopwatch, CancellationToken cancellationToken)
    {
        if (_isInClassic)
        {
            return;
        }

        var messageConfigId = proposalMessage.ConfigurationId.ToConfigurationId();
        _fastLog.HandleFastRoundProposalReceived(proposalMessage.Sender, proposalMessage.Proposal, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _fastLog.ConfigurationMismatch(_configurationId, messageConfigId);
            return;
        }

        if (!_fastVotesReceived.Add(proposalMessage.Sender))
        {
            _fastLog.DuplicateFastRoundVote(proposalMessage.Sender);
            return;
        }

        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.FastVote);

        var proposal = proposalMessage.Proposal;
        if (proposal == null)
        {
            return;
        }

        ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_fastVotesPerProposal, proposal, out _);
        ++entry;

        var count = entry;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        var threshold = _membershipSize - f;

        _fastLog.FastRoundVoteCount(count, _fastVotesReceived.Count, threshold, f);

        if (count >= threshold)
        {
            _fastLog.DecidedViewChange(proposal);
            _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Success);
            CompleteDecided(_fastMetrics, consensusStopwatch, proposal);
            return;
        }

        // Once we receive threshold total votes without any single proposal reaching threshold,
        // fast round cannot succeed and we should fall back.
        if (_fastVotesReceived.Count >= threshold)
        {
            _fastLog.FastRoundMayNotSucceed();
            _log.FastRoundFailedEarly();
            _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Conflict);
            _metrics.RecordConsensusConflict();

            StartClassicRound(roundNumber: 2, cancellationToken);
        }
    }

    private void StartClassicRound(int roundNumber, CancellationToken cancellationToken)
    {
        _timeout.Dispose();

        _isInClassic = true;

        if (roundNumber <= _currentClassicRound)
        {
            return;
        }

        _currentClassicRound = roundNumber;

        var delay = GetRetryDelay(roundNumber);
        _log.StartingClassicRound(roundNumber, delay);
        _classicMetrics.RecordRoundStarted(_metrics);

        var nodeId = _membershipViewAccessor.CurrentView.GetNodeId(_myAddr);
        var nodeIndex = unchecked((int)nodeId);

        StartPhase1a(new Rank { Round = roundNumber, NodeIndex = nodeIndex }, cancellationToken);

        _timeout.Schedule(
            _sharedResources.TimeProvider,
            delay,
            () =>
            {
                if (_onDecidedTcs.Task.IsCompleted)
                {
                    return;
                }

                _events.Writer.TryWrite(new ConsensusEvent.ClassicTimeout(roundNumber, delay));
            });
    }

    private void StartPhase1a(Rank round, CancellationToken cancellationToken)
    {
        if (_currentClassicRank.Round > round.Round)
        {
            _paxosLog.StartPhase1aSkipped(_currentClassicRank.Round, round.Round);
            return;
        }

        if (_paxosLearner.IsDecided)
        {
            return;
        }

        _currentClassicRank = round;
        _candidateValue = null;
        _phase1bMessages.Clear();

        _paxosLog.PrepareCalled(_myAddr, _currentClassicRank);

        var prepare = new Phase1aMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Rank = _currentClassicRank
        };

        var request = prepare.ToRapidClusterRequest();
        _paxosLog.BroadcastingPhase1a();

        _broadcaster.Broadcast(request, cancellationToken);
    }

    private int? HandlePaxosNackMessage(PaxosNackMessage nack)
    {
        var messageConfigId = nack.ConfigurationId.ToConfigurationId();
        if (messageConfigId != _configurationId)
        {
            return null;
        }

        // We only react to NACKs for our current round. Older rounds are effectively stale.
        if (!nack.Received.Equals(_currentClassicRank))
        {
            return null;
        }

        if (RankComparer.Instance.Compare(nack.Promised, _currentClassicRank) <= 0)
        {
            return null;
        }

        return nack.Promised.Round + 1;
    }

    private void HandlePhase1bMessage(Phase1bMessage phase1bMessage, CancellationToken cancellationToken)
    {
        if (!_isInClassic)
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

        if (!phase1bMessage.Rnd.Equals(_currentClassicRank))
        {
            _paxosLog.Phase1bRoundMismatch(_currentClassicRank, phase1bMessage.Rnd);
            return;
        }

        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.Phase1b);
        _phase1bMessages.Add(phase1bMessage);

        var majorityThreshold = _membershipSize / 2 + 1;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        _paxosLog.Phase1bCollected(_phase1bMessages.Count, majorityThreshold, f);

        if (_phase1bMessages.Count < majorityThreshold)
        {
            return;
        }

        var chosenValue = ChooseValue(_phase1bMessages, _membershipSize);

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
            Rnd = _currentClassicRank,
            Proposal = _candidateValue
        };

        var request = phase2a.ToRapidClusterRequest();
        _broadcaster.Broadcast(request, cancellationToken);
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
            CompleteDecided(_classicMetrics, consensusStopwatch, classicDecided.Value);
            return true;
        }

        if (_paxosLearner.Decided is ConsensusResult.Cancelled && cancellationToken.IsCancellationRequested)
        {
            CompleteCancelled(_classicMetrics, recordRoundCompleted: true, cancellationToken);
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

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _log.Dispose();

        _disposeCts.SafeCancel(_log.Logger);
        _events.Writer.TryComplete();

        _paxosLearner.Cancel();

        if (_consensusLoopTask is { } task)
        {
            await task.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        }

        _timeout.Dispose();

        _onDecidedTcs.TrySetCanceled();
        _disposeCts.Dispose();
    }
}
