using System.Runtime.InteropServices;
using Microsoft.Extensions.Logging;

using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Paxos Acceptor role.
///
/// Owns promise/accepted state and responds to classic Paxos messages.
/// Also stores the local acceptor state created by participating in the (single) fast round.
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
        ILogger<PaxosProposer> logger)
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
    /// Records that this acceptor participated in the (single) fast round by locally accepting the proposal.
    ///
    /// Fast round is modeled as if the acceptor received an <c>AcceptRequest</c> directly.
    /// </summary>
    public void RegisterFastRoundVote(MembershipProposal proposal)
    {
        // Do not participate in our only fast round if we are already participating in a classic round.
        if (_promisedRank.Round > 1)
        {
            return;
        }

        // The first round in the consensus instance is always the fast round.
        var fastRoundRank = new Rank { Round = 1, NodeIndex = 1 };
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
        var messageConfigId = phase1aMessage.ConfigurationId.ToConfigurationId();
        _log.HandlePhase1aReceived(phase1aMessage.Sender, phase1aMessage.Rank, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.Phase1aConfigMismatch(_configurationId, messageConfigId);
            return;
        }

        // Phase1a: promise if request rank is higher than any previous promise.
        if (phase1aMessage.Rank.CompareTo(_promisedRank) > 0)
        {
            _promisedRank = phase1aMessage.Rank;

            var phase1b = new Phase1bMessage
            {
                ConfigurationId = _configurationId.ToProtobuf(),
                Sender = _myAddr,
                Rnd = _promisedRank,
                Vrnd = _acceptedRank,
                Proposal = _acceptedValue
            };

            _log.SendingPhase1b(phase1aMessage.Sender, _promisedRank, _acceptedRank, _acceptedValue);
            _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.Phase1b);

            var request = phase1b.ToRapidClusterRequest();
            _client.SendOneWayMessage(phase1aMessage.Sender, request, cancellationToken);
        }
        else
        {
            _log.Phase1aRankTooLow(phase1aMessage.Rank, _promisedRank);

            var nack = new PaxosNackMessage
            {
                Sender = _myAddr,
                ConfigurationId = _configurationId.ToProtobuf(),
                Received = phase1aMessage.Rank,
                Promised = _promisedRank
            };

            _client.SendOneWayMessage(phase1aMessage.Sender, nack.ToRapidClusterRequest(), cancellationToken);
        }
    }

    /// <summary>
    /// Handles a classic Paxos accept request (Phase2a). If accepted, broadcasts the accept vote (Phase2b).
    /// </summary>
    public void HandlePhase2aMessage(Phase2aMessage phase2aMessage, CancellationToken cancellationToken = default)
    {
        var messageConfigId = phase2aMessage.ConfigurationId.ToConfigurationId();
        _log.HandlePhase2aReceived(phase2aMessage.Sender, phase2aMessage.Rnd, phase2aMessage.Proposal, messageConfigId);

        if (messageConfigId != _configurationId)
        {
            _log.Phase2aConfigMismatch(_configurationId, messageConfigId);
            return;
        }

        // Phase2a: accept if request rank is >= promise and we haven't already accepted in this rank.
        if (phase2aMessage.Rnd.CompareTo(_promisedRank) >= 0 && !_acceptedRank.Equals(phase2aMessage.Rnd))
        {
            _promisedRank = phase2aMessage.Rnd;
            _acceptedRank = phase2aMessage.Rnd;
            _acceptedValue = phase2aMessage.Proposal;

            var phase2b = new Phase2bMessage
            {
                ConfigurationId = _configurationId.ToProtobuf(),
                Sender = _myAddr,
                Rnd = _promisedRank,
                Proposal = _acceptedValue
            };

            _log.SendingPhase2b(phase2aMessage.Sender, _promisedRank, _acceptedValue);
            _metrics.RecordConsensusVoteSent(MetricNames.VoteTypes.Phase2b);

            var request = phase2b.ToRapidClusterRequest();
            _broadcaster.Broadcast(request, cancellationToken);
        }
        else
        {
            _log.Phase2aRankTooLow(phase2aMessage.Rnd, _promisedRank);

            var nack = new PaxosNackMessage
            {
                Sender = _myAddr,
                ConfigurationId = _configurationId.ToProtobuf(),
                Received = phase2aMessage.Rnd,
                Promised = _promisedRank
            };

            _client.SendOneWayMessage(phase2aMessage.Sender, nack.ToRapidClusterRequest(), cancellationToken);
        }
    }
}
