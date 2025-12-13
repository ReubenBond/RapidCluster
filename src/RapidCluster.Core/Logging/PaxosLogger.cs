using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class PaxosLogger(ILogger<Paxos> logger)
{
    private readonly ILogger _logger = logger;

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

    [LoggerMessage(Level = LogLevel.Debug, Message = "Prepare called by {MyAddr} for round {Crnd}")]
    public partial void PrepareCalled(Endpoint myAddr, Rank crnd);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Decided on value: {Value}")]
    public partial void DecidedValue(LoggableEndpoints value);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Broadcasting startPhase1a message")]
    public partial void BroadcastingPhase1a();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Paxos initialized: myAddr={MyAddr}, configId={ConfigId}, n={N}")]
    public partial void PaxosInitialized(LoggableEndpoint myAddr, long configId, int n);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RegisterFastRoundVote: proposal={Proposal}, voteCount={VoteCount}")]
    public partial void RegisterFastRoundVote(LoggableEndpoints proposal, int voteCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "StartPhase1a: skipping, current round {CurrentRound} > requested {RequestedRound}")]
    public partial void StartPhase1aSkipped(int currentRound, int requestedRound);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1aMessage: received from {Sender}, rank={Rank}, configId={ConfigId}")]
    public partial void HandlePhase1aReceived(LoggableEndpoint sender, Rank rank, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1aMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase1aConfigMismatch(long expected, long got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1aMessage: rank too low, received={Received}, current={Current}")]
    public partial void Phase1aRankTooLow(Rank received, Rank current);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1aMessage: sending phase1b to {Destination}, rnd={Rnd}, vrnd={Vrnd}, vval={Vval}")]
    public partial void SendingPhase1b(LoggableEndpoint destination, Rank rnd, Rank vrnd, LoggableEndpoints vval);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: received from {Sender}, rnd={Rnd}, vrnd={Vrnd}, configId={ConfigId}")]
    public partial void HandlePhase1bReceived(LoggableEndpoint sender, Rank rnd, Rank vrnd, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase1bConfigMismatch(long expected, long got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: round mismatch, expected={Expected}, got={Got}")]
    public partial void Phase1bRoundMismatch(Rank expected, Rank got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: collected {Count} responses, threshold={Threshold}, f={F}")]
    public partial void Phase1bCollected(int count, int threshold, int f);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase1bMessage: chosen value={ChosenValue}, broadcasting phase2a")]
    public partial void Phase1bChosenValue(LoggableEndpoints chosenValue);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2aMessage: received from {Sender}, rnd={Rnd}, vval={Vval}, configId={ConfigId}")]
    public partial void HandlePhase2aReceived(LoggableEndpoint sender, Rank rnd, LoggableEndpoints vval, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2aMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase2aConfigMismatch(long expected, long got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2aMessage: rank too low, received={Received}, current={Current}")]
    public partial void Phase2aRankTooLow(Rank received, Rank current);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2aMessage: accepting value, sending phase2b to {Destination}, rnd={Rnd}, vval={Vval}")]
    public partial void SendingPhase2b(LoggableEndpoint destination, Rank rnd, LoggableEndpoints vval);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2bMessage: received from {Sender}, rnd={Rnd}, endpoints={Endpoints}, configId={ConfigId}")]
    public partial void HandlePhase2bReceived(LoggableEndpoint sender, Rank rnd, LoggableEndpoints endpoints, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2bMessage: config mismatch, expected={Expected}, got={Got}")]
    public partial void Phase2bConfigMismatch(long expected, long got);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePhase2bMessage: collected {Count} accept responses for round {Rnd}, threshold={Threshold}, f={F}")]
    public partial void Phase2bCollected(int count, Rank rnd, int threshold, int f);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: processing {Count} phase1b messages, n={N}")]
    public partial void ChooseValueStart(int count, int n);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: no values with non-empty vval, returning empty")]
    public partial void ChooseValueEmpty();

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: maxVrnd={MaxVrnd}, valuesWithMaxVrnd count={Count}")]
    public partial void ChooseValueMaxVrnd(Rank maxVrnd, int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: all values identical, returning {Value}")]
    public partial void ChooseValueIdentical(LoggableEndpoints value);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ChooseValue: maxCount={MaxCount}, threshold={Threshold}, choosing by count")]
    public partial void ChooseValueByCount(int maxCount, int threshold);
}
