using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class ConsensusCoordinatorLogger(ILogger<ConsensusCoordinator> logger)
{
    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger { get; } = logger;

    [LoggerMessage(EventName = nameof(Initialized), Level = LogLevel.Debug, Message = "ConsensusCoordinator initialized: myAddr={MyAddr}, configId={ConfigId}, membershipSize={MembershipSize}")]
    private partial void InitializedCore(LoggableEndpoint myAddr, ConfigurationId configId, int membershipSize);
    public void Initialized(Endpoint myAddr, ConfigurationId configId, int membershipSize) => InitializedCore(new(myAddr), configId, membershipSize);

    [LoggerMessage(EventName = nameof(Propose), Level = LogLevel.Debug, Message = "Propose: starting consensus loop with proposal={Proposal}")]
    private partial void ProposeCore(LoggableMembershipProposal proposal);
    public void Propose(MembershipProposal? proposal) => ProposeCore(new(proposal));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting fast round (round 1)")]
    public partial void StartingFastRound();

    [LoggerMessage(EventName = nameof(ConsensusDecided), Level = LogLevel.Debug, Message = "Consensus decided (configId={ConfigId}): {Decision}")]
    private partial void ConsensusDecidedCore(ConfigurationId configId, LoggableMembershipProposal decision);
    public void ConsensusDecided(ConfigurationId configId, MembershipProposal? decision) => ConsensusDecidedCore(configId, new(decision));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round timed out or cancelled (configId={ConfigId}) after {Timeout}, falling back to classic Paxos")]
    public partial void FastRoundTimeout(ConfigurationId configId, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Fast round failed early, falling back to classic Paxos")]
    public partial void FastRoundFailedEarly();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Classic round {Round} timed out after {Timeout}")]
    public partial void ClassicRoundTimeout(int round, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting classic round {Round} with delay {Delay}")]
    public partial void StartingClassicRound(int round, TimeSpan delay);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Consensus loop cancelled")]
    public partial void ConsensusCancelled();

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleMessages: received {MessageType}")]
    public partial void HandleMessages(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dispose: cleaning up ConsensusCoordinator resources")]
    public partial void Dispose();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Cancelling consensus coordinator")]
    public partial void Cancelling();
}
