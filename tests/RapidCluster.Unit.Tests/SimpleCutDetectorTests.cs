using CsCheck;
using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for simple cut detection used in clusters where MultiNodeCutDetector constraints cannot be satisfied
/// </summary>
public class SimpleCutDetectorTests
{
    private static readonly ConfigurationId DefaultConfigId = new(new ClusterId(888), -1); // Should not affect the following tests

    private static AlertMessage CreateAlertMessage(Endpoint src, Endpoint dst, EdgeStatus status,
        ConfigurationId configurationId, int ringNumber)
    {
        var msg = new AlertMessage
        {
            EdgeSrc = src,
            EdgeDst = dst,
            EdgeStatus = status,
            ConfigurationId = configurationId.ToProtobuf()
        };
        msg.RingNumber.Add(ringNumber);
        return msg;
    }

    /// <summary>
    /// Creates a MembershipView for testing SimpleCutDetector.
    /// </summary>
    private static MembershipView CreateTestView(int numNodes = 3, int k = 2)
    {
        var builder = new MembershipViewBuilder(k);
        for (var i = 0; i < numNodes; i++)
        {
            var node = Utils.HostFromParts("127.0.0." + (i + 1), 1000 + i);
            node.NodeId = Utils.GetNextNodeId();
            builder.RingAdd(node);
        }
        return builder.Build();
    }

    [Fact]
    public void Constructor_K1_Succeeds()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void Constructor_K2_Succeeds()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void Constructor_K0_Throws()
    {
        // MembershipViewBuilder throws on creation with k <= 0
        // This test verifies that the validation happens at the MembershipViewBuilder level,
        // which prevents SimpleCutDetector from ever receiving a view with RingCount=0
        Assert.Throws<ArgumentOutOfRangeException>(() => new MembershipViewBuilder(0));
    }

    [Fact]
    public void Constructor_K3_Succeeds()
    {
        var view = CreateTestView(10, 3);
        var detector = new SimpleCutDetector(view);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void Constructor_NullView_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new SimpleCutDetector(null!));
    }

    [Fact]
    public void K1_SingleVoteTriggersProposal()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Single(result);
        Assert.Equal(dst, result[0]);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void K1_DuplicateVoteIgnored()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result1 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Single(result1);
        Assert.Empty(result2);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void K1_MultipleDestinationsEachGetProposal()
    {
        var view = CreateTestView(3, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        var result1 = detector.AggregateForProposal(CreateAlertMessage(src, dst1, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src, dst2, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Single(result1);
        Assert.Single(result2);
        Assert.Equal(2, detector.GetNumProposals());
    }

    [Fact]
    public void K2_SingleVoteDoesNotTriggerProposal()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Empty(result);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void K2_TwoVotesTriggersProposal()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);
        var src1 = Utils.HostFromParts("127.0.0.1", 1);
        var src2 = Utils.HostFromParts("127.0.0.1", 2);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result1 = detector.AggregateForProposal(CreateAlertMessage(src1, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src2, dst, EdgeStatus.Up, DefaultConfigId, 1));

        Assert.Empty(result1);
        Assert.Single(result2);
        Assert.Equal(dst, result2[0]);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void K2_DuplicateRingNumberIgnored()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);
        var src1 = Utils.HostFromParts("127.0.0.1", 1);
        var src2 = Utils.HostFromParts("127.0.0.1", 2);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Both votes for ring 0
        var result1 = detector.AggregateForProposal(CreateAlertMessage(src1, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src2, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Empty(result1);
        Assert.Empty(result2); // Duplicate ring number ignored
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void K2_MultipleDestinationsIndependent()
    {
        var view = CreateTestView(4, 2);
        var detector = new SimpleCutDetector(view);
        var src1 = Utils.HostFromParts("127.0.0.1", 1);
        var src2 = Utils.HostFromParts("127.0.0.1", 2);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        // One vote for dst1
        detector.AggregateForProposal(CreateAlertMessage(src1, dst1, EdgeStatus.Up, DefaultConfigId, 0));
        Assert.Equal(0, detector.GetNumProposals());

        // Two votes for dst2
        detector.AggregateForProposal(CreateAlertMessage(src1, dst2, EdgeStatus.Up, DefaultConfigId, 0));
        var result = detector.AggregateForProposal(CreateAlertMessage(src2, dst2, EdgeStatus.Up, DefaultConfigId, 1));

        Assert.Single(result);
        Assert.Equal(dst2, result[0]);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void NewDetector_StartsWithZeroProposals()
    {
        // Old Clear tests are replaced with detector replacement pattern
        // Since detectors are now immutable and replaced on view change,
        // we test that a new detector starts fresh
        var view = CreateTestView(2, 1);
        var detector1 = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        detector1.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        Assert.Equal(1, detector1.GetNumProposals());

        // Create new detector (simulating view change)
        var detector2 = new SimpleCutDetector(view);
        Assert.Equal(0, detector2.GetNumProposals());
    }

    [Fact]
    public void NewDetector_AcceptsNewProposals()
    {
        var view = CreateTestView(2, 1);
        var detector1 = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        detector1.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        // Create new detector (simulating view change)
        var detector2 = new SimpleCutDetector(view);

        // Same message should trigger proposal again on new detector
        var result = detector2.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        Assert.Single(result);
        Assert.Equal(1, detector2.GetNumProposals());
    }

    [Fact]
    public void EdgeStatus_UpAndDownBothWork()
    {
        var view = CreateTestView(3, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        var resultUp = detector.AggregateForProposal(CreateAlertMessage(src, dst1, EdgeStatus.Up, DefaultConfigId, 0));
        var resultDown = detector.AggregateForProposal(CreateAlertMessage(src, dst2, EdgeStatus.Down, DefaultConfigId, 0));

        Assert.Single(resultUp);
        Assert.Single(resultDown);
        Assert.Equal(2, detector.GetNumProposals());
    }

    [Fact]
    public void MultipleRingNumbers_K1_TriggersOnFirst()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var msg = new AlertMessage
        {
            EdgeSrc = src,
            EdgeDst = dst,
            EdgeStatus = EdgeStatus.Up,
            ConfigurationId = DefaultConfigId.ToProtobuf()
        };
        // For K=1, only ring 0 is valid, but the message might contain it
        msg.RingNumber.Add(0);

        var result = detector.AggregateForProposal(msg);

        Assert.Single(result);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void RingNumber_LargerThanK_Throws()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Ring number 5 is larger than K=1, should throw
        Assert.Throws<ArgumentException>(() =>
            detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 5)));
    }

    [Fact]
    public void RingNumber_Negative_Throws()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        Assert.Throws<ArgumentException>(() =>
            detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, -1)));
    }

    [Fact]
    public void RingNumber_Zero_Valid()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Empty(result); // Need 2 votes for K=2
    }

    [Fact]
    public void RingNumber_MultipleRingsCountAsMultipleVotes()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Two different ring numbers from the same source count as two votes
        var result1 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 1));

        Assert.Empty(result1);
        Assert.Single(result2); // Second vote triggers proposal
    }

    [Fact]
    public void AggregateForProposal_NullMessage_Throws()
    {
        var view = CreateTestView(2, 1);
        var detector = new SimpleCutDetector(view);

        Assert.Throws<ArgumentNullException>(() => detector.AggregateForProposal(null!));
    }

    [Fact]
    public void InvalidateFailingEdges_NoDownEvents_ReturnsEmpty()
    {
        var view = CreateTestView(3, 2);
        var detector = new SimpleCutDetector(view);

        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Only UP events, no DOWN
        detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        var result = detector.InvalidateFailingEdges();

        Assert.Empty(result);
    }

    #region Property-Based Tests

    /// <summary>
    /// Generator for a list of unique endpoints with their NodeIds.
    /// </summary>
    private static Gen<List<Endpoint>> GenUniqueNodes(int minCount, int maxCount)
    {
        return Gen.Int[minCount, maxCount].SelectMany(count =>
            Gen.Select(
                Gen.Int[1, 255].Array[count].Where(a => a.Distinct().Count() == count),
                Gen.Int[1000, 65535].Array[count],
                Gen.Long.Array[count].Where(a => a.Distinct().Count() == count)
            ).Select((octets, ports, nodeIds) =>
            {
                var result = new List<Endpoint>(count);
                for (var i = 0; i < count; i++)
                {
                    var endpoint = new Endpoint
                    {
                        Hostname = ByteString.CopyFromUtf8($"127.0.0.{octets[i]}"),
                        Port = ports[i],
                        NodeId = nodeIds[i]
                    };
                    result.Add(endpoint);
                }
                return result;
            }));
    }

    [Fact]
    public void Property_Single_Report_With_Required_Votes_Triggers_Cut()
    {
        // Use enough nodes (at least 3) to ensure we can have observersPerSubject rings
        Gen.Select(Gen.Int[1, 2], GenUniqueNodes(3, 10))
            .Sample((observersPerSubject, nodes) =>
            {
                var builder = new MembershipViewBuilder(observersPerSubject);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Use actual ring count for iterations
                var actualRingCount = view.RingCount;

                var detector = new SimpleCutDetector(view);

                var subject = nodes[0];

                // Report actualRingCount times (simulating different observers on different rings)
                var proposals = new List<List<Endpoint>>();
                for (var i = 0; i < actualRingCount; i++)
                {
                    var result = detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[(i + 1) % nodes.Count],
                            EdgeDst = subject,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                    proposals.Add(result);
                }

                // After required reports, there should be a proposal
                var lastProposalHasSubject = proposals.Last().Contains(subject);
                return lastProposalHasSubject;
            });
    }

    [Fact]
    public void Property_Empty_For_Unknown_Subject()
    {
        // Use enough nodes (at least 3) to ensure we can have observersPerSubject rings
        Gen.Select(Gen.Int[1, 2], GenUniqueNodes(3, 10))
            .Sample((observersPerSubject, nodes) =>
            {
                var builder = new MembershipViewBuilder(observersPerSubject);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Use actual ring count
                var actualRingCount = view.RingCount;

                var detector = new SimpleCutDetector(view);

                // Create an unknown endpoint
                var unknownEndpoint = new Endpoint
                {
                    Hostname = ByteString.CopyFromUtf8("192.168.1.1"),
                    Port = 9999
                };

                // Report once for unknown subject - should not trigger until threshold
                var result = detector.AggregateForProposal(
                    new AlertMessage
                    {
                        EdgeSrc = nodes[0],
                        EdgeDst = unknownEndpoint,
                        EdgeStatus = EdgeStatus.Up,
                        RingNumber = { 0 }
                    });

                // With actualRingCount > 1, first report returns empty
                // With actualRingCount == 1, it triggers immediately
                return actualRingCount == 1 ? result.Contains(unknownEndpoint) : result.Count == 0;
            });
    }

    #endregion

    #region Unstable Mode Detection

    [Fact]
    public void HasNodesInUnstableMode_ReturnsTrue_WhenPendingProposals()
    {
        var view = CreateTestView(5, 2); // Small cluster uses SimpleCutDetector
        var detector = new SimpleCutDetector(view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Add 1 report (pending, needs 2)
        detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", 1), dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.True(detector.HasNodesInUnstableMode());
    }

    [Fact]
    public void ForcePromoteUnstableNodes_PromotesPendingNodes()
    {
        var view = CreateTestView(5, 2); // Small cluster uses SimpleCutDetector
        var detector = new SimpleCutDetector(view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Add 1 report (pending, needs 2)
        detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", 1), dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.True(detector.HasNodesInUnstableMode());
        Assert.Equal(0, detector.GetNumProposals());

        // Force promote
        var result = detector.ForcePromoteUnstableNodes();

        Assert.Single(result);
        Assert.Equal(dst, result[0]);
        Assert.False(detector.HasNodesInUnstableMode());
        Assert.Equal(1, detector.GetNumProposals());
    }

    #endregion
}
