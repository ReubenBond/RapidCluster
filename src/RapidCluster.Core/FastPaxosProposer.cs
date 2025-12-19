using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Fast Paxos proposer for the single fast round (round 1).
///
/// This proposer broadcasts fast-round accept votes and locally aggregates votes to determine
/// whether the fast round is successful, split, or cannot succeed due to delivery failures.
/// </summary>
internal sealed class FastPaxosProposer
{
    private readonly FastPaxosLogger _log;
    private readonly RapidClusterMetrics _metrics;
    private readonly Endpoint _myAddr;
    private readonly ConfigurationId _configurationId;
    private readonly long _membershipSize;
    private readonly IBroadcaster _broadcaster;
    private readonly Dictionary<MembershipProposal, int> _votesPerProposal = new(MembershipProposalComparer.Instance);
    private readonly HashSet<Endpoint> _votesReceived = [];

    private ConsensusResult? _result;
    private Action<ConsensusResult>? _resultCallback;
    private CancellationTokenRegistration _cancellationRegistration;

    public FastPaxosProposer(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IBroadcaster broadcaster,
        RapidClusterMetrics metrics,
        ILogger<FastPaxosProposer> logger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _broadcaster = broadcaster;
        _metrics = metrics;
        _log = new FastPaxosLogger(logger);

        _log.FastPaxosInitialized(myAddr, configurationId, membershipSize);
    }

    public bool IsCompleted => _result != null;

    public ConsensusResult? Result => _result;

    public void RegisterResultCallback(Action<ConsensusResult> callback)
    {
        ArgumentNullException.ThrowIfNull(callback);

        _resultCallback += callback;
        if (_result != null)
        {
            callback(_result);
        }
    }

    private bool TryComplete(ConsensusResult result)
    {
        if (_result != null)
        {
            return false;
        }

        _result = result;
        _resultCallback?.Invoke(result);
        return true;
    }

    public void RegisterTimeoutToken(CancellationToken timeoutToken)
    {
        if (timeoutToken.CanBeCanceled)
        {
            _cancellationRegistration = timeoutToken.Register(() =>
            {
                TryComplete(ConsensusResult.Cancelled.Instance);
            });
        }
    }

    public void Propose(MembershipProposal proposal, CancellationToken cancellationToken = default)
    {
        _log.Propose(proposal);
        _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.FastVote);

        var consensusMessage = new FastRoundPhase2bMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Proposal = proposal
        };

        var proposalMessage = consensusMessage.ToRapidClusterRequest();

        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        var fastPaxosThreshold = _membershipSize - f;

        int failureCount = 0;
        _broadcaster.Broadcast(proposalMessage, failedEndpoint =>
        {
            var newFailureCount = Interlocked.Increment(ref failureCount);
            var maxPossibleVotes = _membershipSize - newFailureCount;

            if (maxPossibleVotes < fastPaxosThreshold && _result == null)
            {
                _log.EarlyFallbackNeeded(newFailureCount, f, fastPaxosThreshold);
                TryComplete(ConsensusResult.DeliveryFailure.Instance);
            }
        }, cancellationToken);
    }

    public void HandleFastRoundProposalResponse(FastRoundPhase2bMessage proposalMessage)
    {
        var messageConfigId = proposalMessage.ConfigurationId.ToConfigurationId();
        _log.HandleFastRoundProposalReceived(proposalMessage.Sender, proposalMessage.Proposal, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.ConfigurationMismatch(_configurationId, messageConfigId);
            return;
        }

        if (_votesReceived.Contains(proposalMessage.Sender))
        {
            _log.DuplicateFastRoundVote(proposalMessage.Sender);
            return;
        }

        if (_result != null)
        {
            _log.FastRoundAlreadyDecided(_configurationId);
            return;
        }

        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.FastVote);
        _votesReceived.Add(proposalMessage.Sender);

        var proposal = proposalMessage.Proposal;
        if (proposal == null)
        {
            _log.FastRoundAlreadyDecided(_configurationId);
            return;
        }

        ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_votesPerProposal, proposal, out _);
        ++entry;

        var count = entry;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        var threshold = _membershipSize - f;

        _log.FastRoundVoteCount(count, _votesReceived.Count, threshold, f);

        if (count >= threshold)
        {
            _log.DecidedViewChange(proposal);

            if (TryComplete(new ConsensusResult.Decided(proposal)))
            {
                _log.FastRoundSucceeded(_configurationId);
            }
            return;
        }

        if (_votesReceived.Count >= threshold)
        {
            _log.FastRoundMayNotSucceed();
            TryComplete(ConsensusResult.VoteSplit.Instance);
        }
    }

    public void Cancel()
    {
        _cancellationRegistration.Dispose();
        TryComplete(ConsensusResult.Cancelled.Instance);
    }
}
