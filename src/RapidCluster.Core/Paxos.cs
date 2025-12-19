using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Single-decree consensus container.
///
/// Classic Paxos is implemented by explicit roles:
/// - <see cref="PaxosProposer"/>
/// - <see cref="PaxosAcceptor"/>
/// - <see cref="PaxosLearner"/>
///
/// This type is a thin facade kept for integration convenience.
/// </summary>
internal sealed class Paxos
{
    private readonly PaxosLogger _log;

    private readonly PaxosAcceptor _acceptor;
    private readonly PaxosProposer _proposer;
    private readonly PaxosLearner _learner;

    /// <summary>
    /// Task that completes when classic Paxos consensus is learned.
    /// </summary>
    public Task<ConsensusResult> Decided => _learner.Decided;

    public Paxos(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IMessagingClient client,
        IBroadcaster broadcaster,
        IMembershipViewAccessor membershipViewAccessor,
        RapidClusterMetrics metrics,
        ILogger<Paxos> logger)
    {
        _log = new PaxosLogger(logger);

        _acceptor = new PaxosAcceptor(
            myAddr,
            configurationId,
            client,
            broadcaster,
            metrics,
            logger);

        _learner = new PaxosLearner(
            configurationId,
            membershipSize,
            metrics,
            logger);

        _proposer = new PaxosProposer(
            myAddr,
            configurationId,
            membershipSize,
            broadcaster,
            membershipViewAccessor,
            metrics,
            decided: _learner.Decided,
            logger);

        _log.PaxosInitialized(myAddr, configurationId, membershipSize);
    }

    /// <summary>
    /// Records this node's local fast-round vote in the shared acceptor state.
    /// </summary>
    public void RegisterFastRoundVote(MembershipProposal proposal) => _acceptor.RegisterFastRoundVote(proposal);

    /// <summary>
    /// Starts a classic Paxos round by broadcasting Phase1a.
    /// </summary>
    public void StartPhase1a(int round, CancellationToken cancellationToken = default) => _proposer.StartPhase1a(round, cancellationToken);

    /// <summary>
    /// Acceptor-side handler for Phase1a.
    /// </summary>
    public void HandlePhase1aMessage(Phase1aMessage phase1aMessage, CancellationToken cancellationToken = default) => _acceptor.HandlePhase1aMessage(phase1aMessage, cancellationToken);

    /// <summary>
    /// Proposer-side handler for Phase1b.
    /// </summary>
    public void HandlePhase1bMessage(Phase1bMessage phase1bMessage, CancellationToken cancellationToken = default) => _proposer.HandlePhase1bMessage(phase1bMessage, cancellationToken);

    /// <summary>
    /// Acceptor-side handler for Phase2a.
    /// </summary>
    public void HandlePhase2aMessage(Phase2aMessage phase2aMessage, CancellationToken cancellationToken = default) => _acceptor.HandlePhase2aMessage(phase2aMessage, cancellationToken);

    /// <summary>
    /// Learner-side handler for Phase2b.
    /// </summary>
    public void HandlePhase2bMessage(Phase2bMessage phase2bMessage) => _learner.HandlePhase2bMessage(phase2bMessage);

    internal static MembershipProposal? ChooseValue(List<Phase1bMessage> phase1bMessages, int n) => PaxosProposer.ChooseValue(phase1bMessages, n);

    public void Cancel()
    {
        _learner.Cancel();
    }
}
