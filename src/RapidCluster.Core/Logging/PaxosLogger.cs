using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class PaxosLogger(ILogger logger)
{
    private readonly ILogger _logger = logger;

    [LoggerMessage(EventName = nameof(PrepareCalled), Level = LogLevel.Debug, Message = "Prepare called by {MyAddr} for round {Crnd}")]
    private partial void PrepareCalledCore(LoggableEndpoint myAddr, Rank crnd);
    public void PrepareCalled(Endpoint myAddr, Rank crnd) => PrepareCalledCore(new(myAddr), crnd);

    [LoggerMessage(EventName = nameof(DecidedValue), Level = LogLevel.Debug, Message = "Decided on value: {Value}")]
    private partial void DecidedValueCore(LoggableMembershipProposal value);
    public void DecidedValue(MembershipProposal? value) => DecidedValueCore(new(value));

    [LoggerMessage(Level = LogLevel.Trace, Message = "Broadcasting startPhase1a message")]
    public partial void BroadcastingPhase1a();

    [LoggerMessage(EventName = nameof(PaxosInitialized), Level = LogLevel.Debug, Message = "Paxos initialized: myAddr={MyAddr}, configId={ConfigId}, n={N}")]
    private partial void PaxosInitializedCore(LoggableEndpoint myAddr, ConfigurationId configId, int n);
    public void PaxosInitialized(Endpoint myAddr, ConfigurationId configId, int n) => PaxosInitializedCore(new(myAddr), configId, n);

    [LoggerMessage(EventName = nameof(RegisterFastRoundVote), Level = LogLevel.Debug, Message = "RegisterFastRoundVote: proposal={Proposal}, voteCount={VoteCount}")]
    private partial void RegisterFastRoundVoteCore(LoggableMembershipProposal proposal, int voteCount);
    public void RegisterFastRoundVote(MembershipProposal? proposal, int voteCount) => RegisterFastRoundVoteCore(new(proposal), voteCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "StartPhase1a: skipping, current round {CurrentRound} > requested {RequestedRound}")]
    public partial void StartPhase1aSkipped(int currentRound, int requestedRound);

    [LoggerMessage(EventName = nameof(HandlePhase1aReceived), Level = LogLevel.Debug, Message = "HandlePhase1aMessage: received from {Sender}, rank={Rank}, configId={ConfigId}")]
    private partial void HandlePhase1aReceivedCore(LoggableEndpoint sender, Rank rank, ConfigurationId configId);
    public void HandlePhase1aReceived(Endpoint sender, Rank rank, ConfigurationId configId) => HandlePhase1aReceivedCore(new(sender), rank, configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1aMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase1aConfigMismatch(ConfigurationId expected, ConfigurationId got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1aMessage: rank too low, received={Received}, current={Current}")]
    public partial void Phase1aRankTooLow(Rank received, Rank current);

    [LoggerMessage(EventName = nameof(SendingPhase1b), Level = LogLevel.Debug, Message = "HandlePhase1aMessage: sending phase1b to {Destination}, rnd={Rnd}, vrnd={Vrnd}, vval={Vval}")]
    private partial void SendingPhase1bCore(LoggableEndpoint destination, Rank rnd, Rank vrnd, LoggableMembershipProposal vval);
    public void SendingPhase1b(Endpoint destination, Rank rnd, Rank vrnd, MembershipProposal? vval) => SendingPhase1bCore(new(destination), rnd, vrnd, new(vval));

    [LoggerMessage(EventName = nameof(HandlePhase1bReceived), Level = LogLevel.Debug, Message = "HandlePhase1bMessage: received from {Sender}, rnd={Rnd}, vrnd={Vrnd}, configId={ConfigId}")]
    private partial void HandlePhase1bReceivedCore(LoggableEndpoint sender, Rank rnd, Rank vrnd, ConfigurationId configId);
    public void HandlePhase1bReceived(Endpoint sender, Rank rnd, Rank vrnd, ConfigurationId configId) => HandlePhase1bReceivedCore(new(sender), rnd, vrnd, configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase1bConfigMismatch(ConfigurationId expected, ConfigurationId got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: round mismatch, expected={Expected}, got={Got}")]
    public partial void Phase1bRoundMismatch(Rank expected, Rank got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: collected {Count} responses, threshold={Threshold}, f={F}")]
    public partial void Phase1bCollected(int count, int threshold, int f);

    [LoggerMessage(EventName = nameof(Phase1bChosenValue), Level = LogLevel.Debug, Message = "HandlePhase1bMessage: chosen value={ChosenValue}, broadcasting phase2a")]
    private partial void Phase1bChosenValueCore(LoggableMembershipProposal chosenValue);
    public void Phase1bChosenValue(MembershipProposal? chosenValue) => Phase1bChosenValueCore(new(chosenValue));

    [LoggerMessage(EventName = nameof(HandlePhase2aReceived), Level = LogLevel.Debug, Message = "HandlePhase2aMessage: received from {Sender}, rnd={Rnd}, vval={Vval}, configId={ConfigId}")]
    private partial void HandlePhase2aReceivedCore(LoggableEndpoint sender, Rank rnd, LoggableMembershipProposal vval, ConfigurationId configId);
    public void HandlePhase2aReceived(Endpoint sender, Rank rnd, MembershipProposal? vval, ConfigurationId configId) => HandlePhase2aReceivedCore(new(sender), rnd, new(vval), configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2aMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase2aConfigMismatch(ConfigurationId expected, ConfigurationId got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2aMessage: rank too low, received={Received}, current={Current}")]
    public partial void Phase2aRankTooLow(Rank received, Rank current);

    [LoggerMessage(EventName = nameof(SendingPhase2b), Level = LogLevel.Debug, Message = "HandlePhase2aMessage: accepting value, sending phase2b to {Destination}, rnd={Rnd}, vval={Vval}")]
    private partial void SendingPhase2bCore(LoggableEndpoint destination, Rank rnd, LoggableMembershipProposal vval);
    public void SendingPhase2b(Endpoint destination, Rank rnd, MembershipProposal? vval) => SendingPhase2bCore(new(destination), rnd, new(vval));

    [LoggerMessage(EventName = nameof(HandlePhase2bReceived), Level = LogLevel.Debug, Message = "HandlePhase2bMessage: received from {Sender}, rnd={Rnd}, endpoints={Endpoints}, configId={ConfigId}")]
    private partial void HandlePhase2bReceivedCore(LoggableEndpoint sender, Rank rnd, LoggableMembershipProposal endpoints, ConfigurationId configId);
    public void HandlePhase2bReceived(Endpoint sender, Rank rnd, MembershipProposal? endpoints, ConfigurationId configId) => HandlePhase2bReceivedCore(new(sender), rnd, new(endpoints), configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2bMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase2bConfigMismatch(ConfigurationId expected, ConfigurationId got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2bMessage: collected {Count} accept responses for round {Rnd}, threshold={Threshold}, f={F}")]
    public partial void Phase2bCollected(int count, Rank rnd, int threshold, int f);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: processing {Count} phase1b messages, n={N}")]
    public partial void ChooseValueStart(int count, int n);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: no values with non-empty vval, returning empty")]
    public partial void ChooseValueEmpty();

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: maxVrnd={MaxVrnd}, valuesWithMaxVrnd count={Count}")]
    public partial void ChooseValueMaxVrnd(Rank maxVrnd, int count);

    [LoggerMessage(EventName = nameof(ChooseValueIdentical), Level = LogLevel.Debug, Message = "ChooseValue: all values identical, returning {Value}")]
    private partial void ChooseValueIdenticalCore(LoggableMembershipProposal value);
    public void ChooseValueIdentical(MembershipProposal? value) => ChooseValueIdenticalCore(new(value));

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: maxCount={MaxCount}, threshold={Threshold}, choosing by count")]
    public partial void ChooseValueByCount(int maxCount, int threshold);
}
