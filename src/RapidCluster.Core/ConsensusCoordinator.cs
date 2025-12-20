using System.Diagnostics;
using System.Threading;
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
/// This coordinator is event-driven: inbound messages, timeouts, and role callbacks
/// are serialized through an internal event loop.
/// </summary>
internal sealed class ConsensusCoordinator : IAsyncDisposable
{
    private readonly ConsensusCoordinatorLogger _log;
    private readonly RapidClusterMetrics _metrics;
    private readonly ProtocolMetrics _fastMetrics;
    private readonly ProtocolMetrics _classicMetrics;
    private readonly double _jitterRate;

    private readonly record struct ProtocolMetrics(string ProtocolName)
    {
        public void RecordProposal(RapidClusterMetrics metrics) => metrics.RecordConsensusProposal(ProtocolName);
        public void RecordRoundStarted(RapidClusterMetrics metrics) => metrics.RecordConsensusRoundStarted(ProtocolName);
        public void RecordRoundCompleted(RapidClusterMetrics metrics, string result) => metrics.RecordConsensusRoundCompleted(ProtocolName, result);
        public void RecordLatency(RapidClusterMetrics metrics, string result, Stopwatch stopwatch) => metrics.RecordConsensusLatency(ProtocolName, result, stopwatch);
    }

    private readonly Endpoint _myAddr;
    private readonly ConfigurationId _configurationId;
    private readonly IMembershipViewAccessor _membershipViewAccessor;
    private readonly RapidClusterProtocolOptions _options;
    private readonly SharedResources _sharedResources;

    private readonly FastPaxosProposer _fastPaxosProposer;

    // Classic Paxos roles.
    private readonly PaxosAcceptor _paxosAcceptor;
    private readonly PaxosProposer _paxosProposer;
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

    private int _currentClassicRound;
    private bool _isInClassic;

    public Task<MembershipProposal> Decided => _onDecidedTcs.Task;

    private abstract record ConsensusEvent
    {
        public sealed record Inbound(RapidClusterRequest Request, CancellationToken CancellationToken) : ConsensusEvent;
        public sealed record FastTimeout : ConsensusEvent;
        public sealed record ClassicTimeout(int Round, TimeSpan Delay) : ConsensusEvent;
        public sealed record Pulse : ConsensusEvent;
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
        ILogger<ConsensusCoordinator> logger,
        ILogger<FastPaxosProposer> fastPaxosLogger,
        ILogger<PaxosProposer> paxosLogger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipViewAccessor = membershipViewAccessor;
        _options = options.Value;
        _sharedResources = sharedResources;
        _metrics = metrics;
        _fastMetrics = new ProtocolMetrics(MetricNames.Protocols.FastPaxos);
        _classicMetrics = new ProtocolMetrics(MetricNames.Protocols.ClassicPaxos);
        _log = new ConsensusCoordinatorLogger(logger);

        _jitterRate = 1 / (double)membershipSize;

        _fastPaxosProposer = new FastPaxosProposer(
            myAddr,
            configurationId,
            membershipSize,
            broadcaster,
            metrics,
            fastPaxosLogger);

        _paxosAcceptor = new PaxosAcceptor(
            myAddr,
            configurationId,
            client,
            broadcaster,
            metrics,
            paxosLogger);

        _paxosLearner = new PaxosLearner(
            configurationId,
            membershipSize,
            metrics,
            paxosLogger);

        _paxosProposer = new PaxosProposer(
            myAddr,
            configurationId,
            membershipSize,
            broadcaster,
            metrics,
            isDecided: () => _paxosLearner.IsDecided,
            paxosLogger);

        _log.Initialized(myAddr, configurationId, membershipSize);

        _fastPaxosProposer.RegisterResultCallback(_ => TryPulse());
        _paxosLearner.RegisterDecidedCallback(_ => TryPulse());
    }

    public void Propose(MembershipProposal proposal, CancellationToken cancellationToken = default)
    {
        _log.Propose(proposal);

        // Ensure our acceptor state includes our local fast-round vote.
        _paxosAcceptor.RegisterFastRoundVote(proposal);

        _consensusLoopTask = RunConsensusLoopAsync(proposal, _disposeCts.Token);
    }

    private void TryPulse() =>
        // Avoid allocating/failing in common paths.
        _events.Writer.TryWrite(new ConsensusEvent.Pulse());

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

                    _events.Writer.TryWrite(new ConsensusEvent.FastTimeout());
                });

            _fastPaxosProposer.Propose(proposal, cancellationToken);

            while (await _events.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(true))
            {
                while (_events.Reader.TryRead(out var @event))
                {
                    HandleEvent(@event, cancellationToken);

                    if (TryCompleteFromRoleState(consensusStopwatch, fastRoundTimeout, cancellationToken, ref activeProtocolMetrics))
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

    private void HandleEvent(ConsensusEvent @event, CancellationToken cancellationToken)
    {
        switch (@event)
        {
            case ConsensusEvent.Pulse:
                return;

            case ConsensusEvent.FastTimeout:
                _fastPaxosProposer.Cancel();
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

            case ConsensusEvent.Inbound(var request, var requestCancellationToken):
                _log.HandleMessages(request.ContentCase);
                switch (request.ContentCase)
                {
                    case RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage:
                        _fastPaxosProposer.HandleFastRoundProposalResponse(request.FastRoundPhase2BMessage);
                        break;

                    case RapidClusterRequest.ContentOneofCase.Phase1AMessage:
                        _paxosAcceptor.HandlePhase1aMessage(request.Phase1AMessage, requestCancellationToken);
                        break;

                    case RapidClusterRequest.ContentOneofCase.Phase1BMessage:
                        _paxosProposer.HandlePhase1bMessage(request.Phase1BMessage, requestCancellationToken);
                        break;

                    case RapidClusterRequest.ContentOneofCase.Phase2AMessage:
                        _paxosAcceptor.HandlePhase2aMessage(request.Phase2AMessage, requestCancellationToken);
                        break;

                    case RapidClusterRequest.ContentOneofCase.Phase2BMessage:
                        _paxosLearner.HandlePhase2bMessage(request.Phase2BMessage);
                        break;

                    case RapidClusterRequest.ContentOneofCase.PaxosNackMessage:
                        var requestedRound = _paxosProposer.HandlePaxosNackMessage(request.PaxosNackMessage);
                        if (requestedRound is { } nackRound && _isInClassic && nackRound > _currentClassicRound)
                        {
                            StartClassicRound(nackRound, cancellationToken);
                        }
                        break;

                    default:
                        throw new ArgumentException($"Unexpected message case: {request.ContentCase}");
                }

                return;

            default:
                throw new UnreachableException($"Unexpected event type: {@event.GetType().Name}");
        }
    }

    private bool TryCompleteFromRoleState(
        Stopwatch consensusStopwatch,
        TimeSpan fastRoundTimeout,
        CancellationToken cancellationToken,
        ref ProtocolMetrics activeProtocolMetrics)
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

        if (!_isInClassic && _fastPaxosProposer.Result is { } fastRoundResult)
        {
            switch (fastRoundResult)
            {
                case ConsensusResult.Decided decided:
                    CompleteDecided(_fastMetrics, consensusStopwatch, decided.Value);
                    return true;

                case ConsensusResult.VoteSplit or ConsensusResult.DeliveryFailure:
                    _log.FastRoundFailedEarly();
                    _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Conflict);
                    _metrics.RecordConsensusConflict();
                    StartClassicRound(roundNumber: 2, cancellationToken);
                    activeProtocolMetrics = _classicMetrics;
                    return false;

                case ConsensusResult.Cancelled:
                    if (cancellationToken.IsCancellationRequested)
                    {
                        CompleteCancelled(_fastMetrics, recordRoundCompleted: true, cancellationToken);
                        return true;
                    }

                    _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                    _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
                    _metrics.RecordConsensusConflict();
                    StartClassicRound(roundNumber: 2, cancellationToken);
                    activeProtocolMetrics = _classicMetrics;
                    return false;

                default:
                    throw new UnreachableException($"Unexpected consensus result: {fastRoundResult}");
            }
        }

        return false;
    }

    private void StartClassicRound(int roundNumber, CancellationToken cancellationToken)
    {
        _timeout.Dispose();

        _isInClassic = true;
        _currentClassicRound = roundNumber;

        var delay = GetRetryDelay(roundNumber);
        _log.StartingClassicRound(roundNumber, delay);
        _classicMetrics.RecordRoundStarted(_metrics);

        var nodeId = _membershipViewAccessor.CurrentView.GetNodeId(_myAddr);
        var nodeIndex = unchecked((int)nodeId);
        _paxosProposer.StartPhase1a(new Rank { Round = roundNumber, NodeIndex = nodeIndex }, cancellationToken);
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

    private void CompleteDecided(ProtocolMetrics protocolMetrics, Stopwatch consensusStopwatch, MembershipProposal decidedValue)
    {
        _log.ConsensusDecided(_configurationId, decidedValue);
        protocolMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Success);
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
            if (_disposed != 0 || _consensusLoopTask == null || _onDecidedTcs.Task.IsCompleted)
            {
                return new ConsensusResponse().ToRapidClusterResponse();
            }

            _events.Writer.TryWrite(new ConsensusEvent.Inbound(request, cancellationToken));
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

        _fastPaxosProposer.Cancel();
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
