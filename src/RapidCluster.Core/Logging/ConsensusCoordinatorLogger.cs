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

    [LoggerMessage(EventName = nameof(Initialized), Level = LogLevel.Debug, Message = "ConsensusCoordinator initialized: myAddr={MyAddr}, configId={ConfigId}, membershipSize={MembershipSize}")]
    private partial void InitializedCore(LoggableEndpoint myAddr, ConfigurationId configId, int membershipSize);
    public void Initialized(Endpoint myAddr, ConfigurationId configId, int membershipSize) => InitializedCore(new(myAddr), configId, membershipSize);

    [LoggerMessage(EventName = nameof(Propose), Level = LogLevel.Debug, Message = "Propose: starting consensus loop with proposal={Proposal}")]
    private partial void ProposeCore(LoggableMembershipProposal proposal);
    public void Propose(MembershipProposal? proposal) => ProposeCore(new(proposal));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting fast round (round 1)")]
    public partial void StartingFastRound();

    [LoggerMessage(EventName = nameof(FastRoundDecided), Level = LogLevel.Debug, Message = "Fast round decided (configId={ConfigId}): {Decision}")]
    private partial void FastRoundDecidedCore(ConfigurationId configId, LoggableMembershipProposal decision);
    public void FastRoundDecided(ConfigurationId configId, MembershipProposal? decision) => FastRoundDecidedCore(configId, new(decision));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round timed out or cancelled (configId={ConfigId}) after {Timeout}, falling back to classic Paxos")]
    public partial void FastRoundTimeout(ConfigurationId configId, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round failed early, falling back to classic Paxos")]
    public partial void FastRoundFailedEarly();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Classic round {Round} timed out after {Timeout}")]
    public partial void ClassicRoundTimeout(int round, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting classic round {Round} with delay {Delay}")]
    public partial void StartingClassicRound(int round, TimeSpan delay);

    [LoggerMessage(EventName = nameof(ClassicRoundDecided), Level = LogLevel.Debug, Message = "Classic round {Round} decided (configId={ConfigId}): {Decision}")]
    private partial void ClassicRoundDecidedCore(int round, ConfigurationId configId, LoggableMembershipProposal decision);
    public void ClassicRoundDecided(int round, ConfigurationId configId, MembershipProposal? decision) => ClassicRoundDecidedCore(round, configId, new(decision));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Consensus loop cancelled")]
    public partial void ConsensusCancelled();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Consensus failed: exhausted all {MaxRounds} rounds without reaching decision")]
    public partial void ConsensusExhausted(int maxRounds);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleMessages: received {MessageType}")]
    public partial void HandleMessages(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dispose: cleaning up ConsensusCoordinator resources")]
    public partial void Dispose();
}
