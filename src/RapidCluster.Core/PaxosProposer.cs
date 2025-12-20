using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Paxos Proposer role.
///
/// Drives classic Paxos rounds (Phase1a/1b/2a).
/// </summary>
internal sealed class PaxosProposer
{
    private readonly PaxosLogger _log;
    private readonly RapidClusterMetrics _metrics;
    private readonly IBroadcaster _broadcaster;
    private readonly IMembershipViewAccessor _membershipViewAccessor;
    private readonly ConfigurationId _configurationId;
    private readonly Endpoint _myAddr;
    private readonly int _membershipSize;
    private readonly Func<bool> _isDecided;

    private readonly List<Phase1bMessage> _phase1bMessages = [];

    private Rank _currentRound;
    private MembershipProposal? _candidateValue;

    public PaxosProposer(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IBroadcaster broadcaster,
        IMembershipViewAccessor membershipViewAccessor,
        RapidClusterMetrics metrics,
        Func<bool> isDecided,
        ILogger<PaxosProposer> logger)
    {
        ArgumentNullException.ThrowIfNull(isDecided);

        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _broadcaster = broadcaster;
        _membershipViewAccessor = membershipViewAccessor;
        _metrics = metrics;
        _isDecided = isDecided;
        _log = new PaxosLogger(logger);

        _currentRound = new Rank { Round = 0, NodeIndex = 0 };
    }

    /// <summary>
    /// Starts a classic Paxos round by broadcasting a prepare request (Phase1a).
    /// </summary>
    public void StartPhase1a(int round, CancellationToken cancellationToken = default)
    {
        if (_currentRound.Round > round)
        {
            _log.StartPhase1aSkipped(_currentRound.Round, round);
            return;
        }

        if (_isDecided())
        {
            return;
        }

        _currentRound = new Rank { Round = round, NodeIndex = ComputeNodeIndex() };
        _candidateValue = null;
        _phase1bMessages.Clear();

        _log.PrepareCalled(_myAddr, _currentRound);

        var prepare = new Phase1aMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Rank = _currentRound
        };

        var request = prepare.ToRapidClusterRequest();
        _log.BroadcastingPhase1a();

        _broadcaster.Broadcast(request, cancellationToken);
    }

    public int? HandlePaxosNackMessage(PaxosNackMessage nack)
    {
        var messageConfigId = nack.ConfigurationId.ToConfigurationId();
        if (messageConfigId != _configurationId)
        {
            return null;
        }

        // We only react to NACKs for our current round. Older rounds are effectively stale.
        if (!nack.Received.Equals(_currentRound))
        {
            return null;
        }

        if (RankComparer.Instance.Compare(nack.Promised, _currentRound) <= 0)
        {
            return null;
        }

        return nack.Promised.Round + 1;
    }

    private int ComputeNodeIndex()
    {
        var nodeId = _membershipViewAccessor.CurrentView.GetNodeId(_myAddr);
        return unchecked((int)nodeId);
    }

    /// <summary>
    /// Handles a promise response (Phase1b). Once a majority is collected, chooses a value and broadcasts accept (Phase2a).
    /// </summary>
    public void HandlePhase1bMessage(Phase1bMessage phase1bMessage, CancellationToken cancellationToken = default)
    {
        var messageConfigId = phase1bMessage.ConfigurationId.ToConfigurationId();
        _log.HandlePhase1bReceived(phase1bMessage.Sender, phase1bMessage.Rnd, phase1bMessage.Vrnd, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.Phase1bConfigMismatch(_configurationId, messageConfigId);
            return;
        }

        if (!phase1bMessage.Rnd.Equals(_currentRound))
        {
            _log.Phase1bRoundMismatch(_currentRound, phase1bMessage.Rnd);
            return;
        }

        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.Phase1b);
        _phase1bMessages.Add(phase1bMessage);

        var majorityThreshold = (_membershipSize / 2) + 1;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        _log.Phase1bCollected(_phase1bMessages.Count, majorityThreshold, f);

        if (_phase1bMessages.Count >= majorityThreshold)
        {
            var chosenValue = ChooseValue(_phase1bMessages, _membershipSize);

            if (_candidateValue == null && chosenValue != null)
            {
                _candidateValue = chosenValue;
                _log.Phase1bChosenValue(_candidateValue);

                var phase2a = new Phase2aMessage
                {
                    ConfigurationId = _configurationId.ToProtobuf(),
                    Sender = _myAddr,
                    Rnd = _currentRound,
                    Proposal = _candidateValue
                };

                var request = phase2a.ToRapidClusterRequest();
                _broadcaster.Broadcast(request, cancellationToken);
            }
        }
    }

    /// <summary>
    /// Coordinator value selection rule based on received Phase1b messages.
    ///
    /// Corresponds to Figure 2 in the Fast Paxos paper.
    /// </summary>
    internal static MembershipProposal? ChooseValue(List<Phase1bMessage> phase1bMessages, int n)
    {
        var maxVrnd = phase1bMessages
            .Select(m => m.Vrnd)
            .Max(RankComparer.Instance);

        if (maxVrnd == null)
        {
            return null;
        }

        var collectedProposals = phase1bMessages
            .Where(m => RankComparer.Instance.Compare(m.Vrnd, maxVrnd) == 0)
            .Where(m => m.Proposal != null && m.Proposal.Members.Count > 0)
            .Select(m => m.Proposal!)
            .ToList();

        if (collectedProposals.Count > 0)
        {
            var firstValue = collectedProposals[0];
            var allIdentical = collectedProposals.All(p => MembershipProposalComparer.Instance.Equals(p, firstValue));
            if (allIdentical)
            {
                return firstValue;
            }
        }

        if (collectedProposals.Count > 1)
        {
            var valueCounts = new Dictionary<MembershipProposal, int>(MembershipProposalComparer.Instance);
            foreach (var proposal in collectedProposals)
            {
                ref var count = ref CollectionsMarshal.GetValueRefOrAddDefault(valueCounts, proposal, out _);
                if (count + 1 > n / 4)
                {
                    return proposal;
                }
                count = count + 1;
            }
        }

        return phase1bMessages
            .Where(m => m.Proposal != null && m.Proposal.Members.Count > 0)
            .Select(m => m.Proposal!)
            .OrderBy(p => p, MembershipProposalComparer.Instance)
            .FirstOrDefault();
    }
}
