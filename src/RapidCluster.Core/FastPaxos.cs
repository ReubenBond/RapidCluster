using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Handles the fast round (round 1) of Fast Paxos consensus.
/// 
/// This class is responsible for:
/// - Broadcasting fast round proposals (Phase 2b messages)
/// - Collecting fast round votes and deciding if threshold is met
/// - Returning a result indicating success, vote split, or delivery failure
/// 
/// This class does NOT handle classic Paxos rounds. The ConsensusCoordinator
/// is responsible for creating Paxos instances for classic rounds (2, 3, ...).
/// </summary>
internal sealed class FastPaxos
{
    private readonly FastPaxosLogger _log;
    private readonly RapidClusterMetrics _metrics;
    private readonly Endpoint _myAddr;
    private readonly long _configurationId;
    private readonly long _membershipSize;
    private readonly IBroadcaster _broadcaster;
    private readonly Dictionary<MembershipProposal, int> _votesPerProposal = new(MembershipProposalComparer.Instance);
    private readonly HashSet<Endpoint> _votesReceived = [];

    private readonly TaskCompletionSource<ConsensusResult> _resultTcs = new();
    private CancellationTokenRegistration _cancellationRegistration;

    /// <summary>
    /// Task that completes when fast round finishes (either success or failure).
    /// </summary>
    public Task<ConsensusResult> Result => _resultTcs.Task;

    public FastPaxos(
            Endpoint myAddr,
            long configurationId,
            int membershipSize,
            IBroadcaster broadcaster,
            RapidClusterMetrics metrics,
            ILogger<FastPaxos> logger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _broadcaster = broadcaster;
        _metrics = metrics;
        _log = new FastPaxosLogger(logger);

        _log.FastPaxosInitialized(myAddr, configurationId, membershipSize);
    }

    /// <summary>
    /// Register a timeout cancellation token that will complete the result task with Cancelled.
    /// This should be called when starting the fast round to set up the timeout.
    /// </summary>
    public void RegisterTimeoutToken(CancellationToken timeoutToken)
    {
        if (timeoutToken.CanBeCanceled)
        {
            _cancellationRegistration = timeoutToken.Register(() =>
            {
                _resultTcs.TrySetResult(ConsensusResult.Cancelled.Instance);
            });
        }
    }

    /// <summary>
    /// Propose a value for a fast round. Only broadcasts the fast round message.
    /// The coordinator is responsible for scheduling classic round fallback.
    /// </summary>
    /// <param name="proposal">the membership change proposal towards a configuration change.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public void Propose(MembershipProposal proposal, CancellationToken cancellationToken = default)
    {
        _log.Propose(proposal);
        _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.FastVote);

        var consensusMessage = new FastRoundPhase2bMessage
        {
            ConfigurationId = _configurationId,
            Sender = _myAddr,
            Proposal = proposal
        };

        var proposalMessage = consensusMessage.ToRapidClusterRequest();

        // Calculate threshold for early fallback detection
        // Fast Paxos requires N - f votes, where f = floor((N-1)/4)
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        var fastPaxosThreshold = _membershipSize - f;

        // Track delivery failures to notify coordinator
        int failureCount = 0;

        // Broadcast with failure callback to detect when fast round cannot succeed
        _broadcaster.Broadcast(proposalMessage, failedEndpoint =>
        {
            var newFailureCount = Interlocked.Increment(ref failureCount);

            // Calculate max possible votes: membership - failures
            var maxPossibleVotes = _membershipSize - newFailureCount;

            // If we can't reach the threshold due to delivery failures, complete with failure
            if (maxPossibleVotes < fastPaxosThreshold && !_resultTcs.Task.IsCompleted)
            {
                _log.EarlyFallbackNeeded(newFailureCount, f, fastPaxosThreshold);
                _resultTcs.TrySetResult(ConsensusResult.DeliveryFailure.Instance);
            }
        }, cancellationToken);
    }

    /// <summary>
    /// Handle a fast round proposal (Phase 2b of Fast Paxos).
    /// </summary>
    /// <param name="proposalMessage">the membership change proposal towards a configuration change.</param>
    public void HandleFastRoundProposal(FastRoundPhase2bMessage proposalMessage)
    {
        _log.HandleFastRoundProposalReceived(proposalMessage.Sender, proposalMessage.Proposal, proposalMessage.ConfigurationId);

        if (proposalMessage.ConfigurationId != _configurationId)
        {
            _log.ConfigurationMismatch(_configurationId, proposalMessage.ConfigurationId);
            return;
        }

        if (_votesReceived.Contains(proposalMessage.Sender))
        {
            _log.DuplicateFastRoundVote(proposalMessage.Sender);
            return;
        }

        if (_resultTcs.Task.IsCompleted)
        {
            _log.FastRoundAlreadyDecided(_configurationId);
            return;
        }

        // Record the vote received
        _metrics.RecordConsensusVoteReceived(MetricNames.VoteTypes.FastVote);
        _votesReceived.Add(proposalMessage.Sender);

        var proposal = proposalMessage.Proposal;
        if (proposal == null)
        {
            _log.FastRoundAlreadyDecided(_configurationId); // Log as no-op
            return;
        }

        ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_votesPerProposal, proposal, out var exists);
        ++entry;

        var count = entry;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0); // Fast Paxos resiliency.
        var threshold = _membershipSize - f;

        _log.FastRoundVoteCount(count, _votesReceived.Count, threshold, f);

        // Early success detection: if any proposal reaches the threshold, we can decide immediately
        // without waiting for all votes to arrive. This is safe because:
        // 1. Fast Paxos requires N - f votes for success
        // 2. Once a proposal has N - f votes, no other proposal can also reach N - f votes
        //    (since that would require 2*(N-f) > N votes for N > 0)
        if (count >= threshold)
        {
            _log.DecidedViewChange(proposal);

            // We have a successful proposal. Consume it.
            if (_resultTcs.TrySetResult(new ConsensusResult.Decided(proposal)))
            {
                _log.FastRoundSucceeded(_configurationId);
            }
            return;
        }

        // Check if we've received enough total votes to determine that no proposal can succeed
        if (_votesReceived.Count >= threshold)
        {
            // We have enough votes but no single proposal reached the threshold - vote split
            _log.FastRoundMayNotSucceed();
            _resultTcs.TrySetResult(ConsensusResult.VoteSplit.Instance);
        }
    }

    /// <summary>
    /// Cancel the fast round, completing the result task with Cancelled.
    /// Also cleans up any cancellation token registration.
    /// </summary>
    public void Cancel()
    {
        _cancellationRegistration.Dispose();
        _resultTcs.TrySetResult(ConsensusResult.Cancelled.Instance);
    }
}
