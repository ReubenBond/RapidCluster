using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

public sealed class PaxosLearnerTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    [Fact]
    public void Decides_WhenMajorityReached()
    {
        var configId = new ConfigurationId(new ClusterId(888), version: 1);
        var learner = new PaxosLearner(configId, membershipSize: 5, CreateMetrics(), NullLogger<PaxosProposer>.Instance);

        var proposal = CreateProposal(configId);
        var rnd = new Rank { Round = 2, NodeIndex = 10 };

        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1000, rnd, proposal));
        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1001, rnd, proposal));

        Assert.False(learner.IsDecided);

        // Majority for 5 nodes is 3.
        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1002, rnd, proposal));

        var result = learner.Decided!;
        var decided = Assert.IsType<ConsensusResult.Decided>(result);
        Assert.Equal(proposal, decided.Value);
    }

    [Fact]
    public void IgnoresDuplicateVotes_FromSameSender()
    {
        var configId = new ConfigurationId(new ClusterId(888), version: 1);
        var learner = new PaxosLearner(configId, membershipSize: 3, CreateMetrics(), NullLogger<PaxosProposer>.Instance);

        var proposal = CreateProposal(configId);
        var rnd = new Rank { Round = 2, NodeIndex = 10 };

        var msg = CreatePhase2b(configId, senderPort: 1000, rnd, proposal);
        learner.HandlePhase2bMessage(msg);
        learner.HandlePhase2bMessage(msg);
        learner.HandlePhase2bMessage(msg);

        // Majority for 3 nodes is 2; duplicates from the same endpoint shouldn't decide.
        Assert.False(learner.IsDecided);

        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1001, rnd, proposal));

        var result = learner.Decided!;
        Assert.IsType<ConsensusResult.Decided>(result);
    }

    [Fact]
    public void IgnoresVotes_WithWrongConfigurationId()
    {
        var configId = new ConfigurationId(new ClusterId(888), version: 1);
        var learner = new PaxosLearner(configId, membershipSize: 3, CreateMetrics(), NullLogger<PaxosProposer>.Instance);

        var proposal = CreateProposal(configId);
        var rnd = new Rank { Round = 2, NodeIndex = 10 };

        learner.HandlePhase2bMessage(CreatePhase2b(new ConfigurationId(new ClusterId(888), version: 999), senderPort: 1000, rnd, proposal));
        learner.HandlePhase2bMessage(CreatePhase2b(new ConfigurationId(new ClusterId(888), version: 999), senderPort: 1001, rnd, proposal));
        learner.HandlePhase2bMessage(CreatePhase2b(new ConfigurationId(new ClusterId(888), version: 999), senderPort: 1002, rnd, proposal));

        Assert.False(learner.IsDecided);
    }

    [Fact]
    public void Cancel_CompletesAsCancelled()
    {
        var configId = new ConfigurationId(new ClusterId(888), version: 1);
        var learner = new PaxosLearner(configId, membershipSize: 3, CreateMetrics(), NullLogger<PaxosProposer>.Instance);

        learner.Cancel();

        var result = learner.Decided!;
        Assert.IsType<ConsensusResult.Cancelled>(result);
    }

    [Fact]
    public void DecidesOnlyOnce_EvenIfAnotherRoundReachesMajority()
    {
        var configId = new ConfigurationId(new ClusterId(888), version: 1);
        var learner = new PaxosLearner(configId, membershipSize: 3, CreateMetrics(), NullLogger<PaxosProposer>.Instance);

        var proposalA = CreateProposal(configId);
        var proposalB = CreateProposal(configId);

        var rndA = new Rank { Round = 2, NodeIndex = 10 };
        var rndB = new Rank { Round = 3, NodeIndex = 10 };

        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1000, rndA, proposalA));

        Assert.False(learner.IsDecided);

        // Majority for 3 nodes is 2.
        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1001, rndA, proposalA));

        Assert.True(learner.IsDecided);

        var decidedA = learner.Decided!;
        Assert.IsType<ConsensusResult.Decided>(decidedA);

        // Even if a later round gets a majority, the learner should not change its decision.
        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1002, rndB, proposalB));
        learner.HandlePhase2bMessage(CreatePhase2b(configId, senderPort: 1003, rndB, proposalB));

        var decidedAgain = learner.Decided!;
        Assert.Equal(decidedA, decidedAgain);
    }

    private static Phase2bMessage CreatePhase2b(ConfigurationId configId, int senderPort, Rank rnd, MembershipProposal proposal)
    {
        return new Phase2bMessage
        {
            Sender = Utils.HostFromParts("127.0.0.1", senderPort, nodeId: Utils.GetNextNodeId()),
            ConfigurationId = configId.ToProtobuf(),
            Rnd = rnd,
            Proposal = proposal
        };
    }

    private static MembershipProposal CreateProposal(ConfigurationId configId)
    {
        var proposal = new MembershipProposal { ConfigurationId = configId.ToProtobuf() };
        proposal.Members.Add(Utils.HostFromParts("10.0.0.1", 5001, Utils.GetNextNodeId()));
        return proposal;
    }

    private sealed class TestMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = [];

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            _meters.Add(meter);
            return meter;
        }

        public void Dispose()
        {
            foreach (var meter in _meters)
            {
                meter.Dispose();
            }
            _meters.Clear();
        }
    }
}
