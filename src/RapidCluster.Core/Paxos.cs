using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Single-decree consensus. Implements classic Paxos with the modified rule for the coordinator to pick values as per
/// the Fast Paxos paper: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
///
/// The code below assumes that the first round in a consensus instance (done per configuration change) is the
/// only round that is a fast round. A round is identified by a tuple (rnd-number, nodeId), where nodeId is a unique
/// identifier per node that initiates phase1.
/// 
/// NodeIndex is computed from the monotonic_node_id assigned during join. This ensures uniqueness across
/// node restarts - a restarted node gets a new, higher monotonic ID, preventing Paxos safety violations
/// from conflicting promises/votes made by the old incarnation.
/// </summary>
internal sealed class Paxos
{
    private readonly PaxosLogger _log;
    private readonly IBroadcaster _broadcaster;
    private readonly IMessagingClient _client;
    private readonly IMembershipViewAccessor _membershipViewAccessor;
    private readonly long _configurationId;
    private readonly Endpoint _myAddr;
    private readonly int _membershipSize;

    private Rank _rnd;
    private Rank _vrnd;
    private MembershipProposal? _vval;
    private readonly List<Phase1bMessage> _phase1bMessages = [];
    private readonly Dictionary<Rank, Dictionary<Endpoint, Phase2bMessage>> _acceptResponses = [];

    private Rank _crnd;
    private MembershipProposal? _cval;

    private readonly TaskCompletionSource<ConsensusResult> _decidedTcs = new();

    /// <summary>
    /// Task that completes when classic Paxos consensus is reached.
    /// </summary>
    public Task<ConsensusResult> Decided => _decidedTcs.Task;

    // Fast round votes tracking
    private readonly Dictionary<MembershipProposal, int> _fastRoundVotes = new(MembershipProposalComparer.Instance);

    public Paxos(
        Endpoint myAddr,
        long configurationId,
        int membershipSize,
        IMessagingClient client,
        IBroadcaster broadcaster,
        IMembershipViewAccessor membershipViewAccessor,
        ILogger<Paxos> logger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _membershipSize = membershipSize;
        _broadcaster = broadcaster;
        _client = client;
        _membershipViewAccessor = membershipViewAccessor;
        _log = new PaxosLogger(logger);

        _crnd = new Rank { Round = 0, NodeIndex = 0 };
        _rnd = new Rank { Round = 0, NodeIndex = 0 };
        _vrnd = new Rank { Round = 0, NodeIndex = 0 };

        _log.PaxosInitialized(new PaxosLogger.LoggableEndpoint(myAddr), configurationId, membershipSize);
    }

    /// <summary>
    /// This is how we're notified that a fast round is initiated. Invoked by a FastPaxos instance. This
    /// represents the logic at an acceptor receiving a phase2a message directly.
    /// </summary>
    /// <param name="proposal">the vote for the fast round</param>
    public void RegisterFastRoundVote(MembershipProposal proposal)
    {
        // Do not participate in our only fast round if we are already participating in a classic round.
        if (_rnd.Round > 1)
        {
            return;
        }

        // This is the 1st round in the consensus instance, is always a fast round, and is always the *only* fast round.
        // If this round does not succeed and we fallback to a classic round, we start with round number 2
        // and each node sets its node-index as the hash of its hostname. Doing so ensures that all classic
        // rounds initiated by any host is higher than the fast round, and there is an ordering between rounds
        // initiated by different endpoints.
        _rnd = new Rank { Round = 1, NodeIndex = 1 };
        _vrnd = _rnd;
        _vval = proposal;

        ref var voteCount = ref CollectionsMarshal.GetValueRefOrAddDefault(_fastRoundVotes, proposal, out var _);
        ++voteCount;
        _log.RegisterFastRoundVote(new PaxosLogger.LoggableEndpoints(proposal.Members.Select(m => m.Endpoint)), voteCount);
    }

    /// <summary>
    /// At coordinator, start a classic round. We ensure that even if round numbers are not unique, the
    /// "rank" = (round, nodeId) is unique by using unique node IDs.
    /// 
    /// NodeIndex is computed using a hash of the endpoint address for determinism.
    /// </summary>
    /// <param name="round">The round number to initiate.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public void StartPhase1a(int round, CancellationToken cancellationToken = default)
    {
        if (_crnd.Round > round)
        {
            _log.StartPhase1aSkipped(_crnd.Round, round);
            return;
        }

        if (_decidedTcs.Task.IsCompleted)
        {
            return; // Already decided
        }

        _crnd = new Rank { Round = round, NodeIndex = ComputeNodeIndex() };
        _log.PrepareCalled(_myAddr, _crnd);

        var prepare = new Phase1aMessage
        {
            ConfigurationId = _configurationId,
            Sender = _myAddr,
            Rank = _crnd
        };

        var request = prepare.ToRapidClusterRequest();
        _log.BroadcastingPhase1a();

        _broadcaster.Broadcast(request, cancellationToken);
    }

    /// <summary>
    /// Computes the NodeIndex for Paxos rank.
    /// Uses the monotonic_node_id from the current membership view. This ensures uniqueness across
    /// node restarts - a restarted node gets a new, higher monotonic ID, preventing Paxos safety
    /// violations from conflicting promises/votes made by the old incarnation.
    /// </summary>
    private int ComputeNodeIndex()
    {
        var monotonicId = _membershipViewAccessor.CurrentView.GetMonotonicNodeId(_myAddr);
        return unchecked((int)monotonicId);
    }

    /// <summary>
    /// At acceptor, handle a phase1a message from a coordinator.
    /// </summary>
    /// <param name="phase1aMessage">phase1a message from a coordinator.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public void HandlePhase1aMessage(Phase1aMessage phase1aMessage, CancellationToken cancellationToken = default)
    {
        _log.HandlePhase1aReceived(new PaxosLogger.LoggableEndpoint(phase1aMessage.Sender), phase1aMessage.Rank, phase1aMessage.ConfigurationId);

        if (phase1aMessage.ConfigurationId != _configurationId)
        {
            _log.Phase1aConfigMismatch(_configurationId, phase1aMessage.ConfigurationId);
            return;
        }

        if (phase1aMessage.Rank.CompareTo(_rnd) > 0)
        {
            _rnd = phase1aMessage.Rank;

            var phase1b = new Phase1bMessage
            {
                ConfigurationId = _configurationId,
                Sender = _myAddr,
                Rnd = _rnd,
                Vrnd = _vrnd,
                Proposal = _vval
            };

            _log.SendingPhase1b(new PaxosLogger.LoggableEndpoint(phase1aMessage.Sender), _rnd, _vrnd, new PaxosLogger.LoggableEndpoints(_vval?.Members.Select(m => m.Endpoint) ?? []));

            var request = phase1b.ToRapidClusterRequest();
            _client.SendOneWayMessage(phase1aMessage.Sender, request, cancellationToken);
        }
        else
        {
            _log.Phase1aRankTooLow(phase1aMessage.Rank, _rnd);
        }
    }

    /// <summary>
    /// At coordinator, collect phase1b messages from acceptors to learn whether they have already voted for
    /// any values, and if a value might have been chosen.
    /// </summary>
    /// <param name="phase1bMessage">startPhase1a response messages from acceptors.</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public void HandlePhase1bMessage(Phase1bMessage phase1bMessage, CancellationToken cancellationToken = default)
    {
        _log.HandlePhase1bReceived(new PaxosLogger.LoggableEndpoint(phase1bMessage.Sender), phase1bMessage.Rnd, phase1bMessage.Vrnd, phase1bMessage.ConfigurationId);

        if (phase1bMessage.ConfigurationId != _configurationId)
        {
            _log.Phase1bConfigMismatch(_configurationId, phase1bMessage.ConfigurationId);
            return;
        }

        if (!phase1bMessage.Rnd.Equals(_crnd))
        {
            _log.Phase1bRoundMismatch(_crnd, phase1bMessage.Rnd);
            return;
        }

        _phase1bMessages.Add(phase1bMessage);

        // Classic Paxos uses majority quorum (N/2 + 1), not Fast Paxos threshold
        var majorityThreshold = (_membershipSize / 2) + 1;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        _log.Phase1bCollected(_phase1bMessages.Count, majorityThreshold, f);

        if (_phase1bMessages.Count >= majorityThreshold)
        {
            // selectProposalUsingCoordinator rule may execute multiple times with each additional phase1bMessage
            // being received, but we can enter the following if statement only once when a valid cval is identified.
            var chosenValue = ChooseValue(_phase1bMessages, _membershipSize);

            // Only proceed if we haven't already chosen a value AND the chosen value is non-null
            // This matches the Java implementation guard: cval.isEmpty() && !chosenProposal.isEmpty()
            if (_cval == null && chosenValue != null)
            {
                _cval = chosenValue;

                _log.Phase1bChosenValue(new PaxosLogger.LoggableEndpoints(_cval.Members.Select(m => m.Endpoint)));

                var phase2a = new Phase2aMessage
                {
                    ConfigurationId = _configurationId,
                    Sender = _myAddr,
                    Rnd = _crnd,
                    Proposal = _cval
                };

                var request = phase2a.ToRapidClusterRequest();
                _broadcaster.Broadcast(request, cancellationToken);
            }
        }
    }

    /// <summary>
    /// At acceptor, handle an accept message from a coordinator.
    /// When accepting, broadcast the phase2b vote to all nodes so they can independently learn the decision.
    /// </summary>
    /// <param name="phase2aMessage">accept message from coordinator</param>
    /// <param name="cancellationToken">Cancellation token</param>
    public void HandlePhase2aMessage(Phase2aMessage phase2aMessage, CancellationToken cancellationToken = default)
    {
        _log.HandlePhase2aReceived(new PaxosLogger.LoggableEndpoint(phase2aMessage.Sender), phase2aMessage.Rnd, new PaxosLogger.LoggableEndpoints(phase2aMessage.Proposal?.Members.Select(m => m.Endpoint) ?? []), phase2aMessage.ConfigurationId);

        if (phase2aMessage.ConfigurationId != _configurationId)
        {
            _log.Phase2aConfigMismatch(_configurationId, phase2aMessage.ConfigurationId);
            return;
        }

        // Accept if this round is >= our current round and we haven't already accepted in this round
        if (phase2aMessage.Rnd.CompareTo(_rnd) >= 0 && !_vrnd.Equals(phase2aMessage.Rnd))
        {
            _rnd = phase2aMessage.Rnd;
            _vrnd = phase2aMessage.Rnd;
            _vval = phase2aMessage.Proposal;

            var phase2b = new Phase2bMessage
            {
                ConfigurationId = _configurationId,
                Sender = _myAddr,
                Rnd = _rnd,
                Proposal = _vval
            };

            _log.SendingPhase2b(new PaxosLogger.LoggableEndpoint(phase2aMessage.Sender), _rnd, new PaxosLogger.LoggableEndpoints(_vval?.Members.Select(m => m.Endpoint) ?? []));

            // Broadcast to all nodes so they can independently learn the decision
            // This matches the Java implementation
            var request = phase2b.ToRapidClusterRequest();
            _broadcaster.Broadcast(request, cancellationToken);
        }
        else
        {
            _log.Phase2aRankTooLow(phase2aMessage.Rnd, _rnd);
        }
    }

    /// <summary>
    /// At acceptor, learn about another acceptor's vote (phase2b messages).
    /// All acceptors collect phase2b messages and can independently learn the decision
    /// once they receive a majority of votes for any round.
    /// </summary>
    /// <param name="phase2bMessage">acceptor's vote</param>
    public void HandlePhase2bMessage(Phase2bMessage phase2bMessage)
    {
        _log.HandlePhase2bReceived(new PaxosLogger.LoggableEndpoint(phase2bMessage.Sender), phase2bMessage.Rnd, new PaxosLogger.LoggableEndpoints(phase2bMessage.Proposal?.Members.Select(m => m.Endpoint) ?? []), phase2bMessage.ConfigurationId);

        if (phase2bMessage.ConfigurationId != _configurationId)
        {
            _log.Phase2bConfigMismatch(_configurationId, phase2bMessage.ConfigurationId);
            return;
        }

        // Store by the message's round (not our crnd) so all acceptors can learn the decision
        // This matches the Java implementation where all nodes collect phase2b messages
        var messageRnd = phase2bMessage.Rnd;
        if (!_acceptResponses.TryGetValue(messageRnd, out var acceptResponses))
        {
            _acceptResponses[messageRnd] = acceptResponses = [];
        }

        acceptResponses[phase2bMessage.Sender] = phase2bMessage;

        // Classic Paxos uses majority quorum (N/2 + 1), not Fast Paxos threshold
        var majorityThreshold = (_membershipSize / 2) + 1;
        var f = (int)Math.Floor((_membershipSize - 1) / 4.0);
        _log.Phase2bCollected(acceptResponses.Count, messageRnd, majorityThreshold, f);

        if (acceptResponses.Count >= majorityThreshold)
        {
            var proposal = phase2bMessage.Proposal;
            if (proposal != null && _decidedTcs.TrySetResult(new ConsensusResult.Decided(proposal)))
            {
                _log.DecidedValue(new PaxosLogger.LoggableEndpoints(proposal.Members.Select(m => m.Endpoint)));
            }
        }
    }

    /// <summary>
    /// The rule with which a coordinator picks a value to propose based on the received phase1b messages.
    /// This corresponds to the logic in Figure 2 of the Fast Paxos paper:
    /// https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
    /// </summary>
    /// <param name="phase1bMessages">A list of phase1b messages from acceptors.</param>
    /// <param name="n">The membership size</param>
    /// <returns>a proposal to apply, or null if none</returns>
    internal static MembershipProposal? ChooseValue(List<Phase1bMessage> phase1bMessages, int n)
    {
        // Find the maximum vrnd among all messages
        var maxVrnd = phase1bMessages
            .Select(m => m.Vrnd)
            .Max(RankComparer.Instance);

        if (maxVrnd == null)
        {
            return null;
        }

        // Let k be the largest value of vr(a) for all a in Q.
        // V (collectedVvals) be the set of all vv(a) for all a in Q s.t vr(a) == k
        var collectedProposals = phase1bMessages
            .Where(m => RankComparer.Instance.Compare(m.Vrnd, maxVrnd) == 0)
            .Where(m => m.Proposal != null && m.Proposal.Members.Count > 0)
            .Select(m => m.Proposal!)
            .ToList();

        // If V has a single unique element (all values identical), then choose v.
        if (collectedProposals.Count > 0)
        {
            var firstValue = collectedProposals[0];
            var allIdentical = collectedProposals.All(p => MembershipProposalComparer.Instance.Equals(p, firstValue));
            if (allIdentical)
            {
                return firstValue;
            }
        }

        // if i-quorum Q of acceptors respond, and there is a k-quorum R such that vrnd = k and vval = v,
        // for all a in intersection(R, Q) -> then choose "v". When choosing E = N/4 and F = N/2, then
        // R intersection Q is N/4 -- meaning if there are more than N/4 identical votes.
        if (collectedProposals.Count > 1)
        {
            // Multiple values were proposed, check if any has more than N/4 votes
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

        // At this point, no value has been selected yet and it is safe for the coordinator to pick any proposed value.
        // To ensure deterministic selection across all coordinators, we sort proposals and pick the smallest.
        // This can happen because a quorum of acceptors that did not vote in prior rounds may have responded
        // to the coordinator first. This is safe to do here for two reasons:
        //      1) The coordinator will only proceed with phase 2 if it has a valid vote.
        //      2) It is likely that the coordinator (itself being an acceptor) is the only one with a valid proposal,
        //         and has not heard a Phase1bMessage from itself yet. Once that arrives, phase1b will be triggered
        //         again.
        return phase1bMessages
            .Where(m => m.Proposal != null && m.Proposal.Members.Count > 0)
            .Select(m => m.Proposal!)
            .OrderBy(p => p, MembershipProposalComparer.Instance)
            .FirstOrDefault();
    }

    /// <summary>
    /// Cancel classic Paxos (e.g., when coordinator is disposed).
    /// </summary>
    public void Cancel()
    {
        _decidedTcs.TrySetResult(ConsensusResult.Cancelled.Instance);
    }
}
