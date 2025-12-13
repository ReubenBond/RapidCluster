using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class ConsensusCoordinatorLogger(ILogger<ConsensusCoordinator> logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger => _logger;

    // Logging helpers
    internal readonly struct LoggableEndpoints(IEnumerable<Endpoint> endpoints)
    {
        private readonly IEnumerable<Endpoint> _endpoints = endpoints;
        public override readonly string ToString() => string.Join(", ", _endpoints.Select(RapidClusterUtils.Loggable));
    }

    internal readonly struct LoggableEndpoint(Endpoint endpoint)
    {
        private readonly Endpoint _endpoint = endpoint;
        public override readonly string ToString() => RapidClusterUtils.Loggable(_endpoint);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "ConsensusCoordinator initialized: myAddr={MyAddr}, configId={ConfigId}, membershipSize={MembershipSize}")]
    public partial void Initialized(LoggableEndpoint myAddr, long configId, int membershipSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Propose: starting consensus loop with proposal={Proposal}")]
    public partial void Propose(LoggableEndpoints proposal);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting fast round (round 1)")]
    public partial void StartingFastRound();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round decided (configId={ConfigId}): {Decision}")]
    public partial void FastRoundDecided(long configId, LoggableEndpoints decision);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round timed out or cancelled (configId={ConfigId}) after {Timeout}, falling back to classic Paxos")]
    public partial void FastRoundTimeout(long configId, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round failed early, falling back to classic Paxos")]
    public partial void FastRoundFailedEarly();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Classic round {Round} timed out after {Timeout}")]
    public partial void ClassicRoundTimeout(int round, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting classic round {Round} with delay {Delay}")]
    public partial void StartingClassicRound(int round, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Classic round {Round} decided (configId={ConfigId}): {Decision}")]
    public partial void ClassicRoundDecided(int round, long configId, LoggableEndpoints decision);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Consensus loop cancelled")]
    public partial void ConsensusCancelled();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Consensus failed: exhausted all {MaxRounds} rounds without reaching decision")]
    public partial void ConsensusExhausted(int maxRounds);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleMessages: received {MessageType}")]
    public partial void HandleMessages(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dispose: cleaning up ConsensusCoordinator resources")]
    public partial void Dispose();
}
