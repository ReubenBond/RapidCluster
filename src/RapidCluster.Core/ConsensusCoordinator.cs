using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Coordinates consensus for a single configuration change decision.
/// Manages the progression from fast round (round 1) through multiple
/// classic Paxos rounds (rounds 2, 3, ...) until a decision is reached.
/// 
/// This class creates and coordinates FastPaxos (for round 1) and Paxos
/// (for classic rounds 2, 3, ...) instances. It runs an async loop that
/// continues until either:
/// - A decision is reached (from FastPaxos or Paxos)
/// - The operation is cancelled (e.g., system shutdown)
/// </summary>
internal sealed class ConsensusCoordinator : IAsyncDisposable
{
    private readonly ConsensusCoordinatorLogger _log;
    private readonly ILogger<FastPaxos> _fastPaxosLogger;
    private readonly ILogger<Paxos> _paxosLogger;
    private readonly double _jitterRate;
    private readonly Endpoint _myAddr;
    private readonly long _configurationId;
    private readonly int _membershipSize;
    private readonly IMessagingClient _client;
    private readonly IBroadcaster _broadcaster;
    private readonly IMembershipViewAccessor _membershipViewAccessor;
    private readonly RapidClusterProtocolOptions _options;
    private readonly SharedResources _sharedResources;

    // The FastPaxos instance for round 1
    // Created in constructor so it can receive votes before Propose() is called
    private readonly FastPaxos _fastPaxos;

    // The Paxos instance for classic rounds (2, 3, ...)
    // Also holds acceptor state shared across all rounds
    // Created in constructor so it can receive messages before Propose() is called
    private readonly Paxos _paxos;

    // Synchronization
    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _disposeCts = new();
    private Task? _consensusLoopTask;
    private int _disposed;

    // Decision
    private readonly TaskCompletionSource<MembershipProposal> _onDecidedTcs = new();

    /// <summary>
    /// Task that completes when consensus is reached.
    /// </summary>
    public Task<MembershipProposal> Decided => _onDecidedTcs.Task;

    public ConsensusCoordinator(
        Endpoint myAddr,
        long configurationId,
        int membershipSize,
        IMessagingClient client,
        IBroadcaster broadcaster,
        IMembershipViewAccessor membershipViewAccessor,
        IOptions<RapidClusterProtocolOptions> options,
        SharedResources sharedResources,
        ILogger<ConsensusCoordinator> logger,
        ILogger<FastPaxos> fastPaxosLogger,
        ILogger<Paxos> paxosLogger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _client = client;
        _broadcaster = broadcaster;
        _membershipViewAccessor = membershipViewAccessor;
        _options = options.Value;
        _sharedResources = sharedResources;
        _log = new ConsensusCoordinatorLogger(logger);
        _fastPaxosLogger = fastPaxosLogger;
        _paxosLogger = paxosLogger;

        // The rate of a random expovariate variable, used to determine jitter
        _jitterRate = 1 / (double)membershipSize;

        // Create FastPaxos and Paxos instances up front so they can receive votes
        // before this node has locally decided to propose
        _fastPaxos = new FastPaxos(
            myAddr,
            configurationId,
            membershipSize,
            broadcaster,
            fastPaxosLogger);

        _paxos = new Paxos(
            myAddr,
            configurationId,
            membershipSize,
            client,
            broadcaster,
            membershipViewAccessor,
            paxosLogger);

        _log.Initialized(myAddr, configurationId, membershipSize);
    }

    /// <summary>
    /// Propose a value for consensus, starting the consensus loop.
    /// </summary>
    public void Propose(MembershipProposal proposal, CancellationToken cancellationToken = default)
    {
        _log.Propose(proposal);

        // Register our fast round vote in the acceptor state
        _paxos.RegisterFastRoundVote(proposal);

        // Start the consensus loop with the dispose token so it can be cancelled during disposal
        _consensusLoopTask = RunConsensusLoopAsync(proposal, _disposeCts.Token);
    }

    /// <summary>
    /// The main consensus loop. Runs until a decision is reached or cancelled.
    /// </summary>
    private async Task RunConsensusLoopAsync(MembershipProposal proposal, CancellationToken cancellationToken)
    {
        try
        {
            // Phase 1: Fast round
            _log.StartingFastRound();

            var fastRoundTimeout = GetRandomDelay();

            // Create a CancellationTokenSource that times out after the fast round delay.
            // When cancelled, FastPaxos.Result will complete with ConsensusResult.Cancelled.
            using var fastRoundTimeoutCts = new CancellationTokenSource(fastRoundTimeout, _sharedResources.TimeProvider);
            using var fastRoundCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, fastRoundTimeoutCts.Token);

            // Register the timeout token with FastPaxos
            _fastPaxos.RegisterTimeoutToken(fastRoundCts.Token);

            // Broadcast fast round proposal
            _fastPaxos.Propose(proposal, cancellationToken);

            // Wait for fast round result - will complete when decided, failed, or timeout (via cancellation)
            var fastRoundResult = await _fastPaxos.Result.WaitAsync(cancellationToken).ConfigureAwait(true);

            switch (fastRoundResult)
            {
                case ConsensusResult.Decided decided:
                    _log.FastRoundDecided(_configurationId, decided.Value);
                    _onDecidedTcs.TrySetResult(decided.Value);
                    return;

                case ConsensusResult.VoteSplit or ConsensusResult.DeliveryFailure:
                    _log.FastRoundFailedEarly();
                    // Fall through to classic rounds
                    break;

                case ConsensusResult.Cancelled:
                    if (cancellationToken.IsCancellationRequested)
                    {
                        _log.ConsensusCancelled();
                        _onDecidedTcs.TrySetCanceled(cancellationToken);
                        return;
                    }
                    // Otherwise it was a timeout - fall through to classic rounds
                    _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                    break;

                case ConsensusResult.Timeout:
                    _log.FastRoundTimeout(_configurationId, fastRoundTimeout);
                    break;
            }

            // Phase 2+: Classic Paxos rounds
            var roundNumber = 2;
            var maxRounds = _options.MaxConsensusRounds;

            while (!cancellationToken.IsCancellationRequested && roundNumber <= maxRounds)
            {
                // Check if Paxos already decided (from a previous round's messages arriving late)
                if (_paxos!.Decided.IsCompletedSuccessfully)
                {
                    var paxosResult = await _paxos.Decided.WaitAsync(cancellationToken).ConfigureAwait(true);
                    if (paxosResult is ConsensusResult.Decided decided)
                    {
                        _log.ClassicRoundDecided(roundNumber - 1, _configurationId, decided.Value);
                        _onDecidedTcs.TrySetResult(decided.Value);
                        return;
                    }
                }

                var delay = GetRetryDelay(roundNumber);
                _log.StartingClassicRound(roundNumber, delay);

                // Start the classic round
                _paxos.StartPhase1a(roundNumber, cancellationToken);

                // Wait for decision or timeout using delay
                await Task.Delay(delay, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);

                // Check if we decided during the delay
                if (_paxos.Decided.IsCompletedSuccessfully)
                {
                    var paxosResult = await _paxos.Decided.ConfigureAwait(true);
                    if (paxosResult is ConsensusResult.Decided decided)
                    {
                        _log.ClassicRoundDecided(roundNumber, _configurationId, decided.Value);
                        _onDecidedTcs.TrySetResult(decided.Value);
                        return;
                    }
                    else if (paxosResult is ConsensusResult.Cancelled && cancellationToken.IsCancellationRequested)
                    {
                        _log.ConsensusCancelled();
                        _onDecidedTcs.TrySetCanceled(cancellationToken);
                        return;
                    }
                }

                // Round timed out, try next round
                _log.ClassicRoundTimeout(roundNumber, delay);
                roundNumber++;
            }

            // Exhausted all rounds without decision
            if (cancellationToken.IsCancellationRequested)
            {
                _log.ConsensusCancelled();
                _onDecidedTcs.TrySetCanceled(cancellationToken);
            }
            else
            {
                // All rounds exhausted without reaching consensus - this can happen during
                // network partitions where this node can't communicate with enough peers.
                // Signal failure so MembershipService can handle appropriately.
                _log.ConsensusExhausted(maxRounds);
                _onDecidedTcs.TrySetException(new InvalidOperationException(
                    $"Consensus failed: exhausted all {maxRounds} rounds without reaching decision for configId={_configurationId}"));
            }
        }
        catch (OperationCanceledException)
        {
            _log.ConsensusCancelled();
            _onDecidedTcs.TrySetCanceled(cancellationToken);
        }
    }

    /// <summary>
    /// Handle an incoming consensus message, routing to the appropriate handler.
    /// </summary>
    public RapidClusterResponse HandleMessages(RapidClusterRequest request, CancellationToken cancellationToken = default)
    {
        _log.HandleMessages(request.ContentCase);

        lock (_lock)
        {
            switch (request.ContentCase)
            {
                case RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage:
                    _fastPaxos.HandleFastRoundProposal(request.FastRoundPhase2BMessage);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase1AMessage:
                    _paxos.HandlePhase1aMessage(request.Phase1AMessage, cancellationToken);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase1BMessage:
                    _paxos.HandlePhase1bMessage(request.Phase1BMessage, cancellationToken);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase2AMessage:
                    _paxos.HandlePhase2aMessage(request.Phase2AMessage, cancellationToken);
                    break;
                case RapidClusterRequest.ContentOneofCase.Phase2BMessage:
                    _paxos.HandlePhase2bMessage(request.Phase2BMessage);
                    break;
                default:
                    throw new ArgumentException($"Unexpected message case: {request.ContentCase}");
            }
        }

        return new ConsensusResponse().ToRapidClusterResponse();
    }

    /// <summary>
    /// Random expovariate variable plus a base delay for fast round timeout.
    /// </summary>
    private TimeSpan GetRandomDelay()
    {
        var jitter = (long)(-1000 * Math.Log(1 - _sharedResources.NextRandomDouble()) / _jitterRate);
        return TimeSpan.FromMilliseconds(jitter + (long)_options.ConsensusFallbackTimeoutBaseDelay.TotalMilliseconds);
    }

    /// <summary>
    /// Calculate delay for a retry round with exponential backoff, jitter, and ring-position-based priority.
    /// Nodes earlier in the ring (lower position) get shorter delays, reducing coordinator dueling.
    /// </summary>
    private TimeSpan GetRetryDelay(int roundNumber)
    {
        var baseMs = _options.ConsensusFallbackTimeoutBaseDelay.TotalMilliseconds;
        var multiplier = Math.Min(Math.Pow(1.5, roundNumber - 2), 8); // Cap at 8x
        var jitter = (long)(-1000 * Math.Log(1 - _sharedResources.NextRandomDouble()) / _jitterRate);

        // Add ring-position-based delay to reduce coordinator dueling.
        // Nodes earlier in the ring get shorter delays, giving them priority.
        var ringPositionDelay = GetRingPositionDelay();

        return TimeSpan.FromMilliseconds(baseMs * multiplier + jitter + ringPositionDelay);
    }

    /// <summary>
    /// Calculate additional delay based on ring position to reduce coordinator dueling.
    /// Returns 0 for the first node in the ring, increasing delay for later positions.
    /// </summary>
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
            // Node not in ring (shouldn't happen during normal operation)
            return 0;
        }

        // Scale delay by position: first node gets 0 delay, last node gets up to 25% of baseDelay additional delay
        // This spreads out coordinator attempts and gives priority to nodes earlier in the ring
        // We use a small fraction (0.25) to avoid adding too much total delay while still reducing dueling
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

        // Cancel the consensus loop first to unblock any Task.Delay calls
        _disposeCts.SafeCancel(_log.Logger);

        // Cancel both FastPaxos and Paxos to unblock any waiters
        _fastPaxos.Cancel();
        _paxos.Cancel();

        if (_consensusLoopTask is { } task)
        {
            await task.ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        }

        _onDecidedTcs.TrySetCanceled();
        _disposeCts.Dispose();
    }
}
