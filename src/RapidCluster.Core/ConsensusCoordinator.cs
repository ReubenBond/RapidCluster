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

    private readonly FastPaxos _fastPaxos;

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
        ILogger<FastPaxos> fastPaxosLogger,
        ILogger<Paxos> paxosLogger)
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

        _fastPaxos = new FastPaxos(
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
            decided: _paxosLearner.Decided,
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

            using var fastRoundTimeoutCts = new CancellationTokenSource(fastRoundTimeout, _sharedResources.TimeProvider);
            using var fastRoundCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, fastRoundTimeoutCts.Token);

            _fastPaxos.RegisterTimeoutToken(fastRoundCts.Token);
            _fastPaxos.Propose(proposal, cancellationToken);

            var fastRoundResult = await _fastPaxos.Result.WaitAsync(cancellationToken).ConfigureAwait(true);

            switch (fastRoundResult)
            {
                case ConsensusResult.Decided decided:
                    _log.FastRoundDecided(_configurationId, decided.Value);
                    _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Success);
                    _metrics.RecordConsensusLatency(MetricNames.Protocols.FastPaxos, MetricNames.Results.Success, consensusStopwatch);
                    _onDecidedTcs.TrySetResult(decided.Value);
                    return;

                case ConsensusResult.VoteSplit or ConsensusResult.DeliveryFailure:
                    _log.FastRoundFailedEarly();
                    _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Conflict);
                    _metrics.RecordConsensusConflict();
                    break;

                case ConsensusResult.Cancelled:
                    if (cancellationToken.IsCancellationRequested)
                    {
                        _log.ConsensusCancelled();
                        _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Aborted);
                        _onDecidedTcs.TrySetCanceled(cancellationToken);
                        return;
                    }
                    _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                    _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Timeout);
                    _metrics.RecordConsensusConflict();
                    break;

                case ConsensusResult.Timeout:
                    _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                    _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.FastPaxos, MetricNames.Results.Timeout);
                    _metrics.RecordConsensusConflict();
                    break;
            }

            var roundNumber = 2;
            var maxRounds = _options.MaxConsensusRounds;

            while (!cancellationToken.IsCancellationRequested && roundNumber <= maxRounds)
            {
                if (_paxosLearner.Decided.IsCompletedSuccessfully)
                {
                    var paxosResult = await _paxosLearner.Decided.WaitAsync(cancellationToken).ConfigureAwait(true);
                    if (paxosResult is ConsensusResult.Decided decided)
                    {
                        _log.ClassicRoundDecided(roundNumber - 1, _configurationId, decided.Value);
                        _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Success);
                        _metrics.RecordConsensusLatency(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Success, consensusStopwatch);
                        _onDecidedTcs.TrySetResult(decided.Value);
                        return;
                    }
                }

                var delay = GetRetryDelay(roundNumber);
                _log.StartingClassicRound(roundNumber, delay);
                _metrics.RecordConsensusRoundStarted(MetricNames.Protocols.ClassicPaxos);

                _paxosProposer.StartPhase1a(roundNumber, cancellationToken);

                await Task.Delay(delay, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);

                if (_paxosLearner.Decided.IsCompletedSuccessfully)
                {
                    var paxosResult = await _paxosLearner.Decided.ConfigureAwait(true);
                    if (paxosResult is ConsensusResult.Decided decided)
                    {
                        _log.ClassicRoundDecided(roundNumber, _configurationId, decided.Value);
                        _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Success);
                        _metrics.RecordConsensusLatency(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Success, consensusStopwatch);
                        _onDecidedTcs.TrySetResult(decided.Value);
                        return;
                    }
                    else if (paxosResult is ConsensusResult.Cancelled && cancellationToken.IsCancellationRequested)
                    {
                        _log.ConsensusCancelled();
                        _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Aborted);
                        _onDecidedTcs.TrySetCanceled(cancellationToken);
                        return;
                    }
                }

                _log.ClassicRoundTimeout(roundNumber, delay);
                _metrics.RecordConsensusRoundCompleted(MetricNames.Protocols.ClassicPaxos, MetricNames.Results.Timeout);
                roundNumber++;
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
        catch (OperationCanceledException)
        {
            _log.ConsensusCancelled();
            _metrics.RecordConsensusLatency(MetricNames.Protocols.FastPaxos, MetricNames.Results.Aborted, consensusStopwatch);
            _onDecidedTcs.TrySetCanceled(cancellationToken);
        }
    }

    public RapidClusterResponse HandleMessages(RapidClusterRequest request, CancellationToken cancellationToken = default)
    {
        _log.HandleMessages(request.ContentCase);

        lock (_lock)
        {
            switch (request.ContentCase)
            {
                case RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage:
                    _fastPaxos.HandleFastRoundProposalResponse(request.FastRoundPhase2BMessage);
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

        _fastPaxos.Cancel();
        _paxosLearner.Cancel();

        if (_consensusLoopTask is { } task)
        {
            await task.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        }

        _onDecidedTcs.TrySetCanceled();
        _disposeCts.Dispose();
    }
}
