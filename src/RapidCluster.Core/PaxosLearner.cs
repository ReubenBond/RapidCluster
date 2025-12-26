using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Paxos Learner role.
///
/// Collects accept votes (Phase2b) and determines when a value is chosen.
/// </summary>
internal sealed class PaxosLearner
{
    private readonly PaxosLogger _log;
    private readonly RapidClusterMetrics _metrics;
    private readonly ConfigurationId _configurationId;
    private readonly int _membershipSize;

    private readonly Dictionary<Rank, Dictionary<Endpoint, Phase2bMessage>> _acceptResponses = [];

    private ConsensusResult? _decided;

    public PaxosLearner(
        ConfigurationId configurationId,
        int membershipSize,
        RapidClusterMetrics metrics,
        ILogger logger)
    {
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _metrics = metrics;
        _log = new PaxosLogger(logger);
    }

    public bool IsDecided => _decided != null;

    public ConsensusResult? Decided => _decided;

    private bool TryDecide(ConsensusResult result)
    {
        if (_decided != null)
        {
            return false;
        }

        _decided = result;
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

    public void Cancel()
    {
        TryDecide(ConsensusResult.Cancelled.Instance);
    }
}
