using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class FastPaxosLogger(ILogger<FastPaxos> logger)
{
    private readonly ILogger _logger = logger;

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleFastRoundProposal: config mismatch, expected={CurrentConfig}, got={ReceivedConfig}")]
    public partial void ConfigurationMismatch(ConfigurationId currentConfig, ConfigurationId receivedConfig);

    [LoggerMessage(EventName = nameof(DecidedViewChange), Level = LogLevel.Trace, Message = "Decided on a view change: {Proposal}")]
    private partial void DecidedViewChangeCore(LoggableMembershipProposal proposal);
    public void DecidedViewChange(MembershipProposal? proposal) => DecidedViewChangeCore(new(proposal));

    [LoggerMessage(Level = LogLevel.Trace, Message = "Fast round may not succeed for proposal")]
    public partial void FastRoundMayNotSucceed();

    [LoggerMessage(EventName = nameof(FastPaxosInitialized), Level = LogLevel.Debug, Message = "FastPaxos initialized: myAddr={MyAddr}, configId={ConfigId}, membershipSize={MembershipSize}")]
    private partial void FastPaxosInitializedCore(LoggableEndpoint myAddr, ConfigurationId configId, long membershipSize);
    public void FastPaxosInitialized(Endpoint myAddr, ConfigurationId configId, long membershipSize) => FastPaxosInitializedCore(new(myAddr), configId, membershipSize);

    [LoggerMessage(EventName = nameof(Propose), Level = LogLevel.Debug, Message = "Propose: broadcasting fast round proposal={Proposal}")]
    private partial void ProposeCore(LoggableMembershipProposal proposal);
    public void Propose(MembershipProposal? proposal) => ProposeCore(new(proposal));

    [LoggerMessage(EventName = nameof(HandleFastRoundProposalReceived), Level = LogLevel.Debug, Message = "HandleFastRoundProposal: received from {Sender}, endpoints={Endpoints}, configId={ConfigId}")]
    private partial void HandleFastRoundProposalReceivedCore(LoggableEndpoint sender, LoggableMembershipProposal endpoints, ConfigurationId configId);
    public void HandleFastRoundProposalReceived(Endpoint sender, MembershipProposal? endpoints, ConfigurationId configId) => HandleFastRoundProposalReceivedCore(new(sender), new(endpoints), configId);

    [LoggerMessage(EventName = nameof(DuplicateFastRoundVote), Level = LogLevel.Debug, Message = "HandleFastRoundProposal: duplicate vote from {Sender}, ignoring")]
    private partial void DuplicateFastRoundVoteCore(LoggableEndpoint sender);
    public void DuplicateFastRoundVote(Endpoint sender) => DuplicateFastRoundVoteCore(new(sender));

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleFastRoundProposal: already decided (configId={ConfigId}), ignoring")]
    public partial void FastRoundAlreadyDecided(ConfigurationId configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleFastRoundProposal: vote count for proposal={Count}, total votes received={TotalVotes}, threshold={Threshold}, f={F}")]
    public partial void FastRoundVoteCount(int count, int totalVotes, long threshold, int f);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleFastRoundProposal: fast round succeeded (configId={ConfigId})")]
    public partial void FastRoundSucceeded(ConfigurationId configId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Early fallback needed: {FailureCount} delivery failures (f={F}, need at least {Threshold} for Fast Paxos)")]
    public partial void EarlyFallbackNeeded(int failureCount, int f, long threshold);
}
