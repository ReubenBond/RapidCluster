using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// <para>Paxos Learner role.</para>
/// <para>Collects accept votes (Phase2b) and determines when a value is chosen.</para>
/// </summary>
internal sealed class PaxosLearner(
    ConfigurationId configurationId,
    int membershipSize,
    RapidClusterMetrics metrics,
    ILogger logger)
{
    private readonly PaxosLogger _log = new(logger);
    private readonly RapidClusterMetrics _metrics = metrics;
    private readonly ConfigurationId _configurationId = configurationId;
    private readonly int _membershipSize = membershipSize;

    private readonly Dictionary<Rank, Dictionary<Endpoint, Phase2bMessage>> _acceptResponses = [];

    public bool IsDecided => Decided != null;

    public ConsensusResult? Decided { get; private set; }

    private bool TryDecide(ConsensusResult result)
    {
        if (Decided != null)
        {
            return false;
        }

        Decided = result;
        return true;
    }

    /// <summary>
    /// Handles an acceptor vote (Phase2b).
    /// </summary>
    public void HandlePhase2bMessage(Phase2bMessage phase2bMessage)
    {
        // Fast round votes are transported as Phase2b messages, but are handled by the coordinator
        // with a different threshold (N - f). The Paxos learner uses a classic majority threshold,
        // so it must ignore fast-round traffic.
        if (phase2bMessage.Rnd.Round == 1)
        {
            return;
        }

        var messageConfigId = phase2bMessage.ConfigurationId.ToConfigurationId();
        _log.HandlePhase2bReceived(phase2bMessage.Sender, phase2bMessage.Rnd, phase2bMessage.Proposal, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.Phase2bConfigMismatch(_configurationId, messageConfigId);
            return;
        }

        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.Phase2b);

        var messageRnd = phase2bMessage.Rnd;
        if (!_acceptResponses.TryGetValue(messageRnd, out var acceptResponses))
        {
            _acceptResponses[messageRnd] = acceptResponses = [];
        }

        acceptResponses[phase2bMessage.Sender] = phase2bMessage;

        var majorityThreshold = _membershipSize / 2 + 1;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        _log.Phase2bCollected(acceptResponses.Count, messageRnd, majorityThreshold, f);

        if (acceptResponses.Count >= majorityThreshold)
        {
            var proposal = phase2bMessage.Proposal;
            if (proposal != null && TryDecide(new ConsensusResult.Decided(proposal)))
            {
                _log.DecidedValue(proposal);
            }
        }
    }

    public void Cancel() => TryDecide(ConsensusResult.Cancelled.Instance);
}
