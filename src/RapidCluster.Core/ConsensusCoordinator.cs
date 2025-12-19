using System.Diagnostics;
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
        public void RecordProposal(RapidClusterMetrics metrics)
        {
            metrics.RecordConsensusProposal(ProtocolName);
        }

        public void RecordRoundStarted(RapidClusterMetrics metrics)
        {
            metrics.RecordConsensusRoundStarted(ProtocolName);
        }

        public void RecordRoundCompleted(RapidClusterMetrics metrics, string result)
        {
            metrics.RecordConsensusRoundCompleted(ProtocolName, result);
        }

        public void RecordLatency(RapidClusterMetrics metrics, string result, Stopwatch stopwatch)
        {
            metrics.RecordConsensusLatency(ProtocolName, result, stopwatch);
        }
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

    public Task<MembershipProposal> Decided => _onDecidedTcs.Task;

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
            membershipViewAccessor,
            metrics,
            isDecided: () => _paxosLearner.IsDecided,
            paxosLogger);

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
            _log.StartingFastRound();
            _fastMetrics.RecordProposal(_metrics);
            _fastMetrics.RecordRoundStarted(_metrics);

            var fastRoundTimeout = GetRandomDelay();
            var fastRoundResult = await RunFastRoundAsync(proposal, fastRoundTimeout, cancellationToken).ConfigureAwait(true);

            if (TryHandleFastRoundTerminalResult(fastRoundResult, fastRoundTimeout, consensusStopwatch, cancellationToken))
            {
                return;
            }

            await RunClassicRoundsAsync(consensusStopwatch, cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            CompleteCancelled(
                _fastMetrics,
                consensusStopwatch,
                recordRoundCompleted: false,
                recordLatency: true,
                cancellationToken);
        }
    }

    private void CompleteDecided(ProtocolMetrics protocolMetrics, Stopwatch consensusStopwatch, MembershipProposal decidedValue)
    {
        _log.ConsensusDecided(_configurationId, decidedValue);
        protocolMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Success);
        protocolMetrics.RecordLatency(_metrics, MetricNames.Results.Success, consensusStopwatch);
        _onDecidedTcs.TrySetResult(decidedValue);
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
    }

    private async Task<ConsensusResult> RunFastRoundAsync(
        MembershipProposal proposal,
        TimeSpan fastRoundTimeout,
        CancellationToken cancellationToken)
    {
        using var fastRoundTimeoutCts = new CancellationTokenSource(fastRoundTimeout, _sharedResources.TimeProvider);
        using var fastRoundCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, fastRoundTimeoutCts.Token);

        var resultTcs = new TaskCompletionSource<ConsensusResult>();
        _fastPaxosProposer.RegisterResultCallback(result => resultTcs.TrySetResult(result));

        _fastPaxosProposer.RegisterTimeoutToken(fastRoundCts.Token);
        _fastPaxosProposer.Propose(proposal, cancellationToken);

        return await resultTcs.Task.WaitAsync(cancellationToken).ConfigureAwait(true);
    }

    private bool TryHandleFastRoundTerminalResult(
        ConsensusResult fastRoundResult,
        TimeSpan fastRoundTimeout,
        Stopwatch consensusStopwatch,
        CancellationToken cancellationToken)
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
                return false;

            case ConsensusResult.Cancelled:
                if (cancellationToken.IsCancellationRequested)
                {
                    CompleteCancelled(
                        _fastMetrics,
                        recordRoundCompleted: true,
                        cancellationToken);
                    return true;
                }

                _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
                _metrics.RecordConsensusConflict();
                return false;

            case ConsensusResult.Timeout:
                _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                _fastMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
                _metrics.RecordConsensusConflict();
                return false;

            default:
                throw new UnreachableException($"Unexpected consensus result: {fastRoundResult}");
        }
    }

    private async Task RunClassicRoundsAsync(Stopwatch consensusStopwatch, CancellationToken cancellationToken)
    {
        for (var roundNumber = 2; !cancellationToken.IsCancellationRequested; roundNumber++)
        {
            if (TryCompleteClassicDecision(roundNumber - 1, consensusStopwatch))
            {
                return;
            }

            var delay = GetRetryDelay(roundNumber);
            _log.StartingClassicRound(roundNumber, delay);
            _classicMetrics.RecordRoundStarted(_metrics);

            _paxosProposer.StartPhase1a(roundNumber, cancellationToken);

            await Task.Delay(delay, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);

            if (TryCompleteClassicDecision(roundNumber, consensusStopwatch))
            {
                return;
            }

            if (TryCompleteClassicCancellationIfRequested(cancellationToken))
            {
                return;
            }

            _log.ClassicRoundTimeout(roundNumber, delay);
            _classicMetrics.RecordRoundCompleted(_metrics, MetricNames.Results.Timeout);
        }

        CompleteCancelled(
            _classicMetrics,
            consensusStopwatch,
            recordRoundCompleted: false,
            recordLatency: true,
            cancellationToken);
    }

    private bool TryCompleteClassicDecision(int roundNumber, Stopwatch consensusStopwatch)
    {
        if (_paxosLearner.Decided is not ConsensusResult.Decided decided)
        {
            return false;
        }

        CompleteDecided(_classicMetrics, consensusStopwatch, decided.Value);
        return true;
    }

    private bool TryCompleteClassicCancellationIfRequested(CancellationToken cancellationToken)
    {
        if (_paxosLearner.Decided is not ConsensusResult.Cancelled || !cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        CompleteCancelled(
            _classicMetrics,
            recordRoundCompleted: true,
            cancellationToken);
        return true;
    }

    public RapidClusterResponse HandleMessages(RapidClusterRequest request, CancellationToken cancellationToken = default)
    {
        _log.HandleMessages(request.ContentCase);

        lock (_lock)
        {
            switch (request.ContentCase)
            {
                case RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage:
                    _fastPaxosProposer.HandleFastRoundProposalResponse(request.FastRoundPhase2BMessage);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase1AMessage:
                    _paxosAcceptor.HandlePhase1aMessage(request.Phase1AMessage, cancellationToken);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase1BMessage:
                    _paxosProposer.HandlePhase1bMessage(request.Phase1BMessage, cancellationToken);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase2AMessage:
                    _paxosAcceptor.HandlePhase2aMessage(request.Phase2AMessage, cancellationToken);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase2BMessage:
                    _paxosLearner.HandlePhase2bMessage(request.Phase2BMessage);
                    break;
                case RapidClusterRequest.ContentOneofCase.PaxosNackMessage:
                    _paxosProposer.HandlePaxosNackMessage(request.PaxosNackMessage, cancellationToken);
                    break;
                default:
                    throw new ArgumentException($"Unexpected message case: {request.ContentCase}");
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
        return (position / (double)view.Size) * baseDelayMs * 0.25;
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _log.Dispose();

        _disposeCts.SafeCancel(_log.Logger);

        _fastPaxosProposer.Cancel();
        _paxosLearner.Cancel();

        if (_consensusLoopTask is { } task)
        {
            await task.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        }

        _onDecidedTcs.TrySetCanceled();
        _disposeCts.Dispose();
    }
}
