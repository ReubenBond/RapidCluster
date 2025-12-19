using Microsoft.Extensions.Logging;

using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Facade for the Fast Paxos proposer (fast round, round 1).
///
/// Kept to minimize churn in callers while making the role explicit.
/// </summary>
internal sealed class FastPaxos
{
    private readonly FastPaxosProposer _proposer;

    public FastPaxos(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IBroadcaster broadcaster,
        RapidClusterMetrics metrics,
        ILogger<FastPaxos> logger)
    {
        _proposer = new FastPaxosProposer(myAddr, configurationId, membershipSize, broadcaster, metrics, logger);
    }

    public Task<ConsensusResult> Result => _proposer.Result;

    public void RegisterTimeoutToken(CancellationToken timeoutToken) => _proposer.RegisterTimeoutToken(timeoutToken);

    public void Propose(MembershipProposal proposal, CancellationToken cancellationToken = default) => _proposer.Propose(proposal, cancellationToken);

    public void HandleFastRoundProposalResponse(FastRoundPhase2bMessage proposalMessage) => _proposer.HandleFastRoundProposalResponse(proposalMessage);

    public void Cancel() => _proposer.Cancel();
}
