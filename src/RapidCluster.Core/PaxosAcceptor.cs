using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// <para>Paxos Acceptor role.</para>
/// <para>
/// Owns promise/accepted state and responds to classic Paxos messages.
/// Also stores the local acceptor state created by participating in the (single) fast round.
/// </para>
/// </summary>
internal sealed class PaxosAcceptor
{
    private readonly PaxosLogger _log;
    private readonly RapidClusterMetrics _metrics;
    private readonly IBroadcaster _broadcaster;
    private readonly IMessagingClient _client;
    private readonly ConfigurationId _configurationId;
    private readonly Endpoint _myAddr;

    private Rank _promisedRank;
    private Rank _acceptedRank;
    private MembershipProposal? _acceptedValue;

    // Local fast-round vote tracking (primarily for observability)
    private readonly Dictionary<MembershipProposal, int> _fastRoundVotes = new(MembershipProposalComparer.Instance);

    public PaxosAcceptor(
        Endpoint myAddr,
        ConfigurationId configurationId,
        IMessagingClient client,
        IBroadcaster broadcaster,
        RapidClusterMetrics metrics,
        ILogger logger)
    {
        _myAddr = myAddr;
        _configurationId = configurationId;
        _client = client;
        _broadcaster = broadcaster;
        _metrics = metrics;
        _log = new PaxosLogger(logger);

        _promisedRank = new Rank { Round = 0, NodeIndex = 0 };
        _acceptedRank = new Rank { Round = 0, NodeIndex = 0 };
    }

    /// <summary>
    /// <para>Records that this acceptor participated in the (single) fast round by locally accepting the proposal.</para>
    /// <para>Fast round is modeled as if the acceptor received an <c>AcceptRequest</c> directly.</para>
    /// </summary>
    public void RegisterFastRoundVote(MembershipProposal proposal)
    {
        // Do not participate in our only fast round if we are already participating in a classic round.
        if (_promisedRank.Round > 1)
        {
            return;
        }

        // The first round in the consensus instance is always the fast round.
        // Use NodeIndex=0 as a reserved fast-round proposer id.
        var fastRoundRank = new Rank { Round = 1, NodeIndex = 0 };
        _promisedRank = fastRoundRank;
        _acceptedRank = fastRoundRank;
        _acceptedValue = proposal;

        ref var voteCount = ref CollectionsMarshal.GetValueRefOrAddDefault(_fastRoundVotes, proposal, out _);
        ++voteCount;
        _log.RegisterFastRoundVote(proposal, voteCount);
    }

    /// <summary>
    /// Handles a classic Paxos prepare request (Phase1a) and replies with a promise (Phase1b).
    /// </summary>
    public void HandlePhase1aMessage(Phase1aMessage phase1aMessage, CancellationToken cancellationToken = default)
    {
        var phase1b = HandlePhase1aMessageLocal(phase1aMessage);
        if (phase1b == null)
        {
            return;
        }

        _client.SendOneWayMessage(phase1aMessage.Sender, phase1b.ToRapidClusterRequest(), cancellationToken);
    }

    public Phase1bMessage? HandlePhase1aMessageLocal(Phase1aMessage phase1aMessage)
    {
        var messageConfigId = phase1aMessage.ConfigurationId.ToConfigurationId();
        _log.HandlePhase1aReceived(phase1aMessage.Sender, phase1aMessage.Rank, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.Phase1aConfigMismatch(_configurationId, messageConfigId);
            return null;
        }

        // Phase1a: promise if request rank is higher than any previous promise.
        if (phase1aMessage.Rank.CompareTo(_promisedRank) > 0)
        {
            _promisedRank = phase1aMessage.Rank;
        }
        else
        {
            _log.Phase1aRankTooLow(phase1aMessage.Rank, _promisedRank);
        }

        // Always reply with our current promise/accepted state so the proposer
        // can observe the highest promised rank and advance rounds.
        var phase1b = new Phase1bMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Rnd = _promisedRank,
            Vrnd = _acceptedRank,
            Proposal = _acceptedValue,
        };

        _log.SendingPhase1b(phase1aMessage.Sender, _promisedRank, _acceptedRank, _acceptedValue);
        _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.Phase1b);

        return phase1b;
    }

    /// <summary>
    /// Handles a classic Paxos accept request (Phase2a). If accepted, broadcasts the accept vote (Phase2b).
    /// </summary>
    public void HandlePhase2aMessage(Phase2aMessage phase2aMessage, CancellationToken cancellationToken = default)
    {
        var phase2b = HandlePhase2aMessageLocal(phase2aMessage);
        if (phase2b == null)
        {
            return;
        }

        if (phase2b.Proposal == null)
        {
            _client.SendOneWayMessage(phase2aMessage.Sender, phase2b.ToRapidClusterRequest(), cancellationToken);
            return;
        }

        _broadcaster.Broadcast(phase2b.ToRapidClusterRequest(), cancellationToken);
    }

    public Phase2bMessage? HandlePhase2aMessageLocal(Phase2aMessage phase2aMessage)
    {
        var messageConfigId = phase2aMessage.ConfigurationId.ToConfigurationId();
        _log.HandlePhase2aReceived(phase2aMessage.Sender, phase2aMessage.Rnd, phase2aMessage.Proposal, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.Phase2aConfigMismatch(_configurationId, messageConfigId);
            return null;
        }

        // Phase2a: accept if request rank is >= promise.
        // Accepting is idempotent: re-sending Phase2a for the same rank should result in another Phase2b vote,
        // not a NACK.
        if (phase2aMessage.Rnd.CompareTo(_promisedRank) < 0)
        {
            _log.Phase2aRankTooLow(phase2aMessage.Rnd, _promisedRank);

            // Reply with a Phase2b carrying our promised rank to signal a NACK.
            // Do not include a proposal: this response is not an accepted vote.
            var nack = new Phase2bMessage
            {
                ConfigurationId = _configurationId.ToProtobuf(),
                Sender = _myAddr,
                Rnd = _promisedRank,
                Proposal = null,
            };

            _log.SendingPhase2b(phase2aMessage.Sender, _promisedRank, vval: null);
            _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.Phase2b);

            return nack;
        }

        if (phase2aMessage.Rnd.CompareTo(_promisedRank) > 0)
        {
            _promisedRank = phase2aMessage.Rnd;
        }

        if (!_acceptedRank.Equals(phase2aMessage.Rnd))
        {
            _acceptedRank = phase2aMessage.Rnd;
            _acceptedValue = phase2aMessage.Proposal;
        }
        else if (_acceptedValue == null && phase2aMessage.Proposal != null)
        {
            _acceptedValue = phase2aMessage.Proposal;
        }

        var phase2b = new Phase2bMessage
        {
            ConfigurationId = _configurationId.ToProtobuf(),
            Sender = _myAddr,
            Rnd = _acceptedRank,
            Proposal = _acceptedValue,
        };

        _log.SendingPhase2b(phase2aMessage.Sender, _acceptedRank, _acceptedValue);
        _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.Phase2b);

        return phase2b;
    }
}
