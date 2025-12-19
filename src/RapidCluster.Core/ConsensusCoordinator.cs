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
    private readonly double _jitterRate;
    private readonly Endpoint _myAddr;
    private readonly ConfigurationId _configurationId;
    private readonly int _membershipSize;
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
        _membershipSize = membershipSize;
        _membershipViewAccessor = membershipViewAccessor;
        _options = options.Value;
        _sharedResources = sharedResources;
        _metrics = metrics;
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
            _metrics.RecordConsensusProposal(MetricNames.Protocols.FastPaxos);
            _metrics.RecordConsensusRoundStarted(MetricNames.Protocols.FastPaxos);

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
            _log.ConsensusCancelled();
            _metrics.RecordConsensusLatency(MetricNames.Protocols.FastPaxos, MetricNames.Results.Aborted, consensusStopwatch);
            _onDecidedTcs.TrySetCanceled(cancellationToken);
        }
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
                _log.FastRoundDecided(_configurationId, decided.Value);
                _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Success);
                _metrics.RecordConsensusLatency(MetricNames.Protocols.FastPaxos, MetricNames.Results.Success, consensusStopwatch);
                _onDecidedTcs.TrySetResult(decided.Value);
                return true;

            case ConsensusResult.VoteSplit or ConsensusResult.DeliveryFailure:
                _log.FastRoundFailedEarly();
                _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Conflict);
                _metrics.RecordConsensusConflict();
                return false;

            case ConsensusResult.Cancelled:
                if (cancellationToken.IsCancellationRequested)
                {
                    _log.ConsensusCancelled();
                    _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Aborted);
                    _onDecidedTcs.TrySetCanceled(cancellationToken);
                    return true;
                }

                _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Timeout);
                _metrics.RecordConsensusConflict();
                return false;

            case ConsensusResult.Timeout:
                _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Timeout);
                _metrics.RecordConsensusConflict();
                return false;

            default:
                throw new UnreachableException($"Unexpected consensus result: {fastRoundResult}");
        }
    }

    private async Task RunClassicRoundsAsync(Stopwatch consensusStopwatch, CancellationToken cancellationToken)
    {
        var maxRounds = _options.MaxConsensusRounds;

        for (var roundNumber = 2; roundNumber <= maxRounds && !cancellationToken.IsCancellationRequested; roundNumber++)
        {
            if (TryCompleteClassicDecision(roundNumber - 1, consensusStopwatch))
            {
                return;
            }

            var delay = GetRetryDelay(roundNumber);
            _log.StartingClassicRound(roundNumber, delay);
            _metrics.RecordConsensusRoundStarted(MetricNames.Protocols.ClassicPaxos);

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
            _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Timeout);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            _log.ConsensusCancelled();
            _metrics.RecordConsensusLatency(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Aborted, consensusStopwatch);
            _onDecidedTcs.TrySetCanceled(cancellationToken);
        }
        else
        {
            _log.ConsensusExhausted(maxRounds);
            _metrics.RecordConsensusLatency(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Failed, consensusStopwatch);
            _onDecidedTcs.TrySetException(new InvalidOperationException(
                $"Consensus failed: exhausted all {maxRounds} rounds without reaching decision for configId={_configurationId}"));
        }
    }

    private bool TryCompleteClassicDecision(int roundNumber, Stopwatch consensusStopwatch)
    {
        if (_paxosLearner.Decided is not ConsensusResult.Decided decided)
        {
            return false;
        }

        _log.ClassicRoundDecided(roundNumber, _configurationId, decided.Value);
        _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Success);
        _metrics.RecordConsensusLatency(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Success, consensusStopwatch);
        _onDecidedTcs.TrySetResult(decided.Value);
        return true;
    }

    private bool TryCompleteClassicCancellationIfRequested(CancellationToken cancellationToken)
    {
        if (_paxosLearner.Decided is not ConsensusResult.Cancelled || !cancellationToken.IsCancellationRequested)
        {
            return false;
        }

        _log.ConsensusCancelled();
        _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Aborted);
        _onDecidedTcs.TrySetCanceled(cancellationToken);
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
