using CsCheck;
using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for multi node cut detection
/// </summary>
public class MultiNodeCutDetectorTests
{
    private const int K = 10;
    private const int H = 8;
    private const int L = 2;
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
    /// Creates a MembershipView for testing MultiNodeCutDetector.
    /// </summary>
    private static MembershipView CreateTestView(int numNodes = 30, int k = K)
    {
        var builder = new MembershipViewBuilder(k);
        for (var i = 0; i < numNodes; i++)
        {
            var node = Utils.HostFromParts("127.0.0." + (i + 1), 1000 + i, Utils.GetNextNodeId());
            builder.RingAdd(node);
        }
        return builder.Build();
    }

    /// <summary>
    /// A series of updates with the right ring indexes
    /// </summary>
    [Fact]
    public void CutDetectionTest()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);
        List<Endpoint> ret;

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Single(ret);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void CutDetectionTestBlockingOneBlocker()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 2);
        List<Endpoint> ret;

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(ret);
        Assert.Equal(0, detector.GetNumProposals());

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst2, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Equal(2, ret.Count);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void CutDetectionTestBlockingThreeBlockers()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 2);
        var dst3 = Utils.HostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst3, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(ret);
        Assert.Equal(0, detector.GetNumProposals());

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst3, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(ret);
        Assert.Equal(0, detector.GetNumProposals());

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst2, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Equal(3, ret.Count);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void CutDetectionTestBlockingMultipleBlockersPastH()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 2);
        var dst3 = Utils.HostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst3, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        // Unlike the previous test, add more reports for dst1 and dst3 past the H boundary.
        detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H + 1), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(ret);
        Assert.Equal(0, detector.GetNumProposals());

        detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst3, EdgeStatus.Up, DefaultConfigId, H - 1));
        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H + 1), dst3, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(ret);
        Assert.Equal(0, detector.GetNumProposals());

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst2, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Equal(3, ret.Count);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void CutDetectionTestBelowL()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 2);
        var dst3 = Utils.HostFromParts("127.0.0.4", 2);
        List<Endpoint> ret;

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        // Unlike the previous test, dst2 has < L updates
        for (var i = 0; i < L - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst3, EdgeStatus.Up, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(ret);
        Assert.Equal(0, detector.GetNumProposals());

        ret = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst3, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Equal(2, ret.Count);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void CutDetectionTestBatch()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        const int numNodes = 3;
        var endpoints = new List<Endpoint>();

        for (var i = 0; i < numNodes; i++)
        {
            endpoints.Add(Utils.HostFromParts("127.0.0.2", 2 + i));
        }

        var proposal = new List<Endpoint>();
        foreach (var endpoint in endpoints)
        {
            for (var ringNumber = 0; ringNumber < K; ringNumber++)
            {
                proposal.AddRange(detector.AggregateForProposal(CreateAlertMessage(
                    Utils.HostFromParts("127.0.0.1", 1), endpoint, EdgeStatus.Up,
                    DefaultConfigId, ringNumber)));
            }
        }

        Assert.Equal(numNodes, proposal.Count);
    }

    [Fact]
    public void CutDetectionTestLinkInvalidation()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 30;
        var endpoints = new List<Endpoint>();

        for (var i = 0; i < numNodes; i++)
        {
            var node = Utils.HostFromParts("127.0.0.2", 2 + i, Utils.GetNextNodeId());
            endpoints.Add(node);
            builder.RingAdd(node);
        }

        var mView = builder.Build();
        var detector = new MultiNodeCutDetector(H, L, mView);

        var dst = endpoints[0];
        var observers = mView.GetObserversOf(dst);
        Assert.Equal(K, observers.Length);

        List<Endpoint> ret;

        // This adds alerts from the observers[0, H - 1) of node dst.
        for (var i = 0; i < H - 1; i++)
        {
            ret = detector.AggregateForProposal(CreateAlertMessage(observers[i], dst,
                EdgeStatus.Down, DefaultConfigId, i));
            Assert.Empty(ret);
            Assert.Equal(0, detector.GetNumProposals());
        }

        // Next, we add alerts *about* observers[H, K) of node dst.
        var failedObservers = new HashSet<Endpoint>();
        for (var i = H - 1; i < K; i++)
        {
            var observersOfObserver = mView.GetObserversOf(observers[i]);
            failedObservers.Add(observers[i]);
            for (var j = 0; j < K; j++)
            {
                ret = detector.AggregateForProposal(CreateAlertMessage(observersOfObserver[j], observers[i],
                    EdgeStatus.Down, DefaultConfigId, j));
                Assert.Empty(ret);
                Assert.Equal(0, detector.GetNumProposals());
            }
        }

        // At this point, (K - H - 1) observers of dst will be past H, and dst will be in H - 1. 
        // Link invalidation should bring the failed observers and dst to the stable region.
        ret = detector.InvalidateFailingEdges();
        Assert.Equal(4, ret.Count);
        Assert.Equal(1, detector.GetNumProposals());

        foreach (var node in ret)
        {
            Assert.True(failedObservers.Contains(node) || node.Equals(dst));
        }
    }

    [Fact]
    public void ConstructorValidParametersSucceeds()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void ConstructorMinimumKSucceeds()
    {
        var view = CreateTestView(10, 3);
        var detector = new MultiNodeCutDetector(2, 1, view);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void ConstructorNullViewThrows()
    {
        Assert.Throws<ArgumentNullException>(() => new MultiNodeCutDetector(8, 2, null!));
    }

    [Fact]
    public void ConstructorKBelowMinimumThrows()
    {
        var view = CreateTestView(10, 2); // K=2, which is below minimum of 3
        Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(1, 1, view));
    }

    [Fact]
    public void ConstructorHGreaterThanKThrows()
    {
        var view = CreateTestView(10, 5); // K=5
        Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(6, 2, view)); // H=6 > K=5
    }

    [Fact]
    public void ConstructorLGreaterThanHThrows()
    {
        var view = CreateTestView();
        Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(5, 6, view)); // L=6 > H=5
    }

    [Fact]
    public void ConstructorLZeroThrows()
    {
        var view = CreateTestView();
        Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(8, 0, view));
    }

    [Fact]
    public void ConstructorHZeroThrows()
    {
        var view = CreateTestView();
        Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(0, 0, view));
    }

    [Fact]
    public void ConstructorHEqualsLSucceeds()
    {
        var view = CreateTestView(10, 5);
        var detector = new MultiNodeCutDetector(3, 3, view);
        Assert.Equal(0, detector.GetNumProposals());
    }

    [Fact]
    public void ConstructorHEqualsKThrows()
    {
        var view = CreateTestView(10, 5); // K=5
        Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(5, 2, view)); // H=5 = K
    }

    [Fact]
    public void NewDetector_ResetsProposalCount()
    {
        var view = CreateTestView();
        var detector1 = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        for (var i = 0; i < K; i++)
        {
            detector1.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.Equal(1, detector1.GetNumProposals());

        // Create new detector (simulating view change)
        var detector2 = new MultiNodeCutDetector(H, L, view);

        Assert.Equal(0, detector2.GetNumProposals());
    }

    [Fact]
    public void NewDetector_AllowsNewProposals()
    {
        var view = CreateTestView();
        var detector1 = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        for (var i = 0; i < K; i++)
        {
            detector1.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.Equal(1, detector1.GetNumProposals());

        // Create new detector (simulating view change)
        var detector2 = new MultiNodeCutDetector(H, L, view);

        for (var i = 0; i < K; i++)
        {
            detector2.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.Equal(1, detector2.GetNumProposals());
    }

    [Fact]
    public void MultipleNewDetectors_Safe()
    {
        var view = CreateTestView();
        // Simulating multiple view changes - each creates a fresh detector
        var detector1 = new MultiNodeCutDetector(H, L, view);
        var detector2 = new MultiNodeCutDetector(H, L, view);
        var detector3 = new MultiNodeCutDetector(H, L, view);

        Assert.Equal(0, detector1.GetNumProposals());
        Assert.Equal(0, detector2.GetNumProposals());
        Assert.Equal(0, detector3.GetNumProposals());
    }

    [Fact]
    public void AggregateForProposalDuplicateAlertIgnored()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result1 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result3 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Empty(result1);
        Assert.Empty(result2);
        Assert.Empty(result3);
    }

    [Fact]
    public void AggregateForProposalDifferentRingNumbersNotDuplicate()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result1 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));
        var result2 = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 1));

        Assert.Empty(result1);
        Assert.Empty(result2);
    }

    [Fact]
    public void AggregateForProposalMultipleRingNumbersAllProcessed()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var msg = new AlertMessage
        {
            EdgeSrc = src,
            EdgeDst = dst,
            EdgeStatus = EdgeStatus.Up,
            ConfigurationId = DefaultConfigId.ToProtobuf()
        };
        for (var i = 0; i < K; i++)
        {
            msg.RingNumber.Add(i);
        }

        var result = detector.AggregateForProposal(msg);

        Assert.Single(result);
    }

    [Fact]
    public void AggregateForProposalEdgeStatusUpWorksCorrectly()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        for (var i = 0; i < K; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void AggregateForProposalEdgeStatusDownWorksCorrectly()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        for (var i = 0; i < K; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Down, DefaultConfigId, i));
        }

        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void AggregateForProposalMixedEdgeStatusProcessedSeparately()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        for (var i = 0; i < H; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst,
                i % 2 == 0 ? EdgeStatus.Up : EdgeStatus.Down,
                DefaultConfigId, i));
        }

        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void AggregateForProposalRingNumberExceedsKThrows()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        Assert.Throws<ArgumentException>(() =>
            detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, K)));
    }

    [Fact]
    public void AggregateForProposalRingNumberNegativeThrows()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        Assert.Throws<ArgumentException>(() =>
            detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, -1)));
    }

    [Fact]
    public void AggregateForProposalRingNumberZeroValid()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, 0));

        Assert.Empty(result);
    }

    [Fact]
    public void AggregateForProposalRingNumberKMinusOneValid()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var src = Utils.HostFromParts("127.0.0.1", 1);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        var result = detector.AggregateForProposal(CreateAlertMessage(src, dst, EdgeStatus.Up, DefaultConfigId, K - 1));

        Assert.Empty(result);
    }

    [Fact]
    public void AggregateForProposalTwoNodesReachHSimultaneously()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        for (var i = 0; i < H - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 100), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.Equal(0, detector.GetNumProposals());

        var result1 = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(result1);
        Assert.Equal(0, detector.GetNumProposals());

        var result2 = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H + 100), dst2, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Equal(2, result2.Count);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void AggregateForProposalManyDestinationsAllProposed()
    {
        var view = CreateTestView(100);
        var detector = new MultiNodeCutDetector(H, L, view);
        const int numDestinations = 50;
        var proposalCount = 0;

        for (var d = 0; d < numDestinations; d++)
        {
            var dst = Utils.HostFromParts("10.0.0." + d, 5000);

            for (var i = 0; i < K; i++)
            {
                var result = detector.AggregateForProposal(CreateAlertMessage(
                    Utils.HostFromParts("192.168.0." + (i + 1), 1000 + d), dst, EdgeStatus.Up, DefaultConfigId, i));

                if (result.Count > 0)
                {
                    proposalCount++;
                }
            }
        }

        Assert.Equal(numDestinations, proposalCount);
    }

    [Fact]
    public void InvalidateFailingEdgesNoDownEventsReturnsEmpty()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var dst = Utils.HostFromParts("127.0.0.2", 2);
        for (var i = 0; i < H - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
        }

        var result = detector.InvalidateFailingEdges();

        Assert.Empty(result);
    }

    [Fact]
    public void Constructor_EmptyMembershipView_ThrowsArgumentException()
    {
        // Empty membership view has ring count clamped to 1, which is insufficient
        // for MultiNodeCutDetector (requires at least 3 observers per subject)
        var view = new MembershipViewBuilder(K).Build();

        var ex = Assert.Throws<ArgumentException>(() => new MultiNodeCutDetector(H, L, view));
        Assert.Contains("at least 3 observers", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AggregateForProposalNullMessageThrows()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        Assert.Throws<ArgumentNullException>(() => detector.AggregateForProposal(null!));
    }

    [Fact]
    public void AggregateForProposalSingleRing_ProcessesSingleRing()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Create message with all K rings
        var msg = new AlertMessage
        {
            EdgeSrc = Utils.HostFromParts("127.0.0.1", 1),
            EdgeDst = dst,
            EdgeStatus = EdgeStatus.Up,
            ConfigurationId = DefaultConfigId.ToProtobuf()
        };
        for (var i = 0; i < K; i++)
        {
            msg.RingNumber.Add(i);
        }

        // Process only ring 0
        var result = detector.AggregateForProposalSingleRing(msg, 0);
        Assert.Empty(result);
        Assert.Equal(0, detector.GetNumProposals());

        // Process remaining rings to reach H
        for (var i = 1; i < H; i++)
        {
            result = detector.AggregateForProposalSingleRing(msg, i);
        }

        // Should now have a proposal
        Assert.Single(result);
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void AggregateForProposalSingleRing_IgnoresRingNotInMessage()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Create message with only ring 0
        var msg = CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", 1), dst, EdgeStatus.Up, DefaultConfigId, 0);

        // Process ring 0 - should work
        var result0 = detector.AggregateForProposalSingleRing(msg, 0);
        Assert.Empty(result0);

        // Process ring 1 - ring not in message, but the method still processes it
        // because the ring number is passed as parameter, not read from message
        var result1 = detector.AggregateForProposalSingleRing(msg, 1);
        Assert.Empty(result1);
    }

    [Fact]
    public void AggregateForProposalSingleRing_NullMessageThrows()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        Assert.Throws<ArgumentNullException>(() => detector.AggregateForProposalSingleRing(null!, 0));
    }

    [Fact]
    public void AggregateForProposalSingleRing_InvalidRingNumberThrows()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var msg = CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", 1),
            Utils.HostFromParts("127.0.0.2", 2),
            EdgeStatus.Up, DefaultConfigId, 0);

        Assert.Throws<ArgumentException>(() => detector.AggregateForProposalSingleRing(msg, K));
        Assert.Throws<ArgumentException>(() => detector.AggregateForProposalSingleRing(msg, -1));
    }

    [Fact]
    public void AggregateForProposalSingleRing_EquivalentToAggregateForProposal()
    {
        var view = CreateTestView();
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Create message with H rings
        var msg = new AlertMessage
        {
            EdgeSrc = Utils.HostFromParts("127.0.0.1", 1),
            EdgeDst = dst,
            EdgeStatus = EdgeStatus.Up,
            ConfigurationId = DefaultConfigId.ToProtobuf()
        };
        for (var i = 0; i < H; i++)
        {
            msg.RingNumber.Add(i);
        }

        // Method 1: AggregateForProposal
        var detector1 = new MultiNodeCutDetector(H, L, view);
        var result1 = detector1.AggregateForProposal(msg);

        // Method 2: AggregateForProposalSingleRing for each ring
        var detector2 = new MultiNodeCutDetector(H, L, view);
        var result2 = new List<Endpoint>();
        foreach (var ringNumber in msg.RingNumber)
        {
            result2.AddRange(detector2.AggregateForProposalSingleRing(msg, ringNumber));
        }

        Assert.Equal(result1.Count, result2.Count);
        Assert.Equal(detector1.GetNumProposals(), detector2.GetNumProposals());
    }

    /// <summary>
    /// Tests that processing messages sequentially (all rings per node) produces individual proposals.
    /// This is the NON-batched behavior.
    /// </summary>
    [Fact]
    public void SequentialProcessing_ProducesIndividualProposals()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);
        var dst3 = Utils.HostFromParts("127.0.0.4", 4);

        var proposalCount = 0;

        // Process all rings for dst1 - should produce proposal
        foreach (var dst in new[] { dst1, dst2, dst3 })
        {
            for (var ring = 0; ring < K; ring++)
            {
                var result = detector.AggregateForProposal(CreateAlertMessage(
                    Utils.HostFromParts("127.0.0.1", ring + 1), dst, EdgeStatus.Up, DefaultConfigId, ring));
                if (result.Count > 0)
                {
                    proposalCount++;
                }
            }
        }

        // Each node reaches H independently, so we get 3 separate proposals
        Assert.Equal(3, proposalCount);
        Assert.Equal(3, detector.GetNumProposals());
    }

    /// <summary>
    /// Tests that processing messages by ring number (interleaved) enables batching.
    /// Multiple nodes can be batched into fewer proposals.
    /// </summary>
    [Fact]
    public void InterleavedProcessing_EnablesBatching()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);
        var dst3 = Utils.HostFromParts("127.0.0.4", 4);
        var destinations = new[] { dst1, dst2, dst3 };

        // Create messages for each destination with all K rings
        var messages = destinations.Select(dst =>
        {
            var msg = new AlertMessage
            {
                EdgeSrc = Utils.HostFromParts("127.0.0.1", 1),
                EdgeDst = dst,
                EdgeStatus = EdgeStatus.Up,
                ConfigurationId = DefaultConfigId.ToProtobuf()
            };
            for (var r = 0; r < K; r++)
            {
                msg.RingNumber.Add(r);
            }
            return msg;
        }).ToList();

        var proposalCount = 0;
        var totalNodesProposed = 0;

        // Process by ring number (interleaved)
        for (var ring = 0; ring < K; ring++)
        {
            foreach (var msg in messages)
            {
                var result = detector.AggregateForProposalSingleRing(msg, ring);
                if (result.Count > 0)
                {
                    proposalCount++;
                    totalNodesProposed += result.Count;
                }
            }
        }

        // All 3 nodes should be proposed
        Assert.Equal(3, totalNodesProposed);
        // With interleaved processing, all nodes should be batched into 1 proposal
        Assert.Equal(1, proposalCount);
        Assert.Equal(1, detector.GetNumProposals());
    }

    /// <summary>
    /// Tests the specific batching scenario from the Rapid paper:
    /// Multiple nodes joining simultaneously should be batched into fewer view changes.
    /// </summary>
    [Fact]
    public void BatchingScenario_MultipleJoinsAreBatched()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        // Simulate 10 nodes joining simultaneously
        var joiningNodes = Enumerable.Range(0, 10)
            .Select(i => Utils.HostFromParts("10.0.0." + i, 5000 + i))
            .ToList();

        // Create alert messages from a single observer for all joining nodes
        var observer = Utils.HostFromParts("127.0.0.1", 1);
        var messages = joiningNodes.Select(dst =>
        {
            var msg = new AlertMessage
            {
                EdgeSrc = observer,
                EdgeDst = dst,
                EdgeStatus = EdgeStatus.Up,
                ConfigurationId = DefaultConfigId.ToProtobuf()
            };
            for (var r = 0; r < K; r++)
            {
                msg.RingNumber.Add(r);
            }
            return msg;
        }).ToList();

        var proposalEvents = 0;
        var nodesProposed = new HashSet<Endpoint>();

        // Process by ring number (simulating HandleBatchedAlertMessage behavior)
        for (var ring = 0; ring < K; ring++)
        {
            foreach (var msg in messages)
            {
                var result = detector.AggregateForProposalSingleRing(msg, ring);
                if (result.Count > 0)
                {
                    proposalEvents++;
                    foreach (var node in result)
                    {
                        nodesProposed.Add(node);
                    }
                }
            }
        }

        // All 10 nodes should be proposed
        Assert.Equal(10, nodesProposed.Count);
        // All nodes should be batched into a single proposal event
        Assert.Equal(1, proposalEvents);
        Assert.Equal(1, detector.GetNumProposals());
    }

    /// <summary>
    /// Tests that nodes reaching L threshold block proposals until they reach H.
    /// </summary>
    [Fact]
    public void PreProposal_BlocksUntilAllReachH()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        // Bring dst1 to H-1 reports (in preProposal)
        for (var i = 0; i < H - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
        }

        // Bring dst2 to L reports (in preProposal)
        for (var i = 0; i < L; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        // Push dst1 to H - should NOT trigger because dst2 is blocking
        var result = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));

        Assert.Empty(result);
        Assert.Equal(0, detector.GetNumProposals());

        // Push dst2 to H - should trigger proposal for BOTH nodes
        for (var i = L; i < H; i++)
        {
            result = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.Equal(2, result.Count);
        Assert.Contains(dst1, result);
        Assert.Contains(dst2, result);
        Assert.Equal(1, detector.GetNumProposals());
    }

    /// <summary>
    /// Tests that nodes below L threshold do NOT block proposals.
    /// </summary>
    [Fact]
    public void BelowL_DoesNotBlock()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        // Bring dst2 to L-1 reports (below L, should NOT block)
        for (var i = 0; i < L - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        // Bring dst1 to H reports
        List<Endpoint> result = [];
        for (var i = 0; i < H; i++)
        {
            result = detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 100), dst1, EdgeStatus.Up, DefaultConfigId, i));
        }

        // dst1 should be proposed (dst2 below L doesn't block)
        Assert.Single(result);
        Assert.Equal(dst1, result[0]);
        Assert.Equal(1, detector.GetNumProposals());
    }

    /// <summary>
    /// Tests multiple batches can occur sequentially.
    /// </summary>
    [Fact]
    public void MultipleBatches_CanOccurSequentially()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        // First batch: 3 nodes
        var batch1 = new[]
        {
            Utils.HostFromParts("10.0.0.1", 1),
            Utils.HostFromParts("10.0.0.2", 2),
            Utils.HostFromParts("10.0.0.3", 3)
        };

        var messages1 = batch1.Select(dst =>
        {
            var msg = new AlertMessage
            {
                EdgeSrc = Utils.HostFromParts("127.0.0.1", 1),
                EdgeDst = dst,
                EdgeStatus = EdgeStatus.Up,
                ConfigurationId = DefaultConfigId.ToProtobuf()
            };
            for (var r = 0; r < K; r++)
            {
                msg.RingNumber.Add(r);
            }
            return msg;
        }).ToList();

        // Process first batch by ring
        var batch1Proposals = 0;
        for (var ring = 0; ring < K; ring++)
        {
            foreach (var msg in messages1)
            {
                var result = detector.AggregateForProposalSingleRing(msg, ring);
                if (result.Count > 0) batch1Proposals++;
            }
        }

        Assert.Equal(1, batch1Proposals);
        Assert.Equal(1, detector.GetNumProposals());

        // Second batch: 2 more nodes
        var batch2 = new[]
        {
            Utils.HostFromParts("10.0.0.4", 4),
            Utils.HostFromParts("10.0.0.5", 5)
        };

        var messages2 = batch2.Select(dst =>
        {
            var msg = new AlertMessage
            {
                EdgeSrc = Utils.HostFromParts("127.0.0.1", 1),
                EdgeDst = dst,
                EdgeStatus = EdgeStatus.Up,
                ConfigurationId = DefaultConfigId.ToProtobuf()
            };
            for (var r = 0; r < K; r++)
            {
                msg.RingNumber.Add(r);
            }
            return msg;
        }).ToList();

        // Process second batch by ring
        var batch2Proposals = 0;
        for (var ring = 0; ring < K; ring++)
        {
            foreach (var msg in messages2)
            {
                var result = detector.AggregateForProposalSingleRing(msg, ring);
                if (result.Count > 0) batch2Proposals++;
            }
        }

        Assert.Equal(1, batch2Proposals);
        Assert.Equal(2, detector.GetNumProposals()); // Total proposals
    }

    /// <summary>
    /// Tests that InvalidateFailingEdges properly handles edges between failing nodes.
    /// This matches the Java test cutDetectionTestLinkInvalidation.
    /// </summary>
    [Fact]
    public void InvalidateFailingEdges_HandlesFailingObservers()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 30;
        var endpoints = new List<Endpoint>();

        for (var i = 0; i < numNodes; i++)
        {
            var node = Utils.HostFromParts("127.0.0.2", 2 + i, Utils.GetNextNodeId());
            endpoints.Add(node);
            builder.RingAdd(node);
        }

        var mView = builder.Build();
        var detector = new MultiNodeCutDetector(H, L, mView);

        var dst = endpoints[0];
        var observers = mView.GetObserversOf(dst);
        Assert.Equal(K, observers.Length);

        // Add alerts from observers[0, H-1) for node dst (failing node)
        for (var i = 0; i < H - 1; i++)
        {
            var result = detector.AggregateForProposal(CreateAlertMessage(observers[i], dst,
                EdgeStatus.Down, DefaultConfigId, i));
            Assert.Empty(result);
            Assert.Equal(0, detector.GetNumProposals());
        }

        // Add alerts ABOUT observers[H-1, K) (these observers are also failing)
        var failedObservers = new HashSet<Endpoint>();
        for (var i = H - 1; i < K; i++)
        {
            var observersOfObserver = mView.GetObserversOf(observers[i]);
            failedObservers.Add(observers[i]);
            for (var j = 0; j < K; j++)
            {
                var result = detector.AggregateForProposal(CreateAlertMessage(observersOfObserver[j], observers[i],
                    EdgeStatus.Down, DefaultConfigId, j));
                Assert.Empty(result);
                Assert.Equal(0, detector.GetNumProposals());
            }
        }

        // Link invalidation should bring the failed observers and dst to the stable region
        var invalidationResult = detector.InvalidateFailingEdges();

        // Should have 4 nodes: dst + 3 failed observers (K-H+1 = 10-8+1 = 3... wait, K-H-1+1 = 10-8 = 2)
        // Actually: observers from index H-1 to K-1 = indices 7,8,9 = 3 observers, plus dst = 4
        Assert.Equal(4, invalidationResult.Count);
        Assert.Equal(1, detector.GetNumProposals());

        foreach (var node in invalidationResult)
        {
            Assert.True(failedObservers.Contains(node) || node.Equals(dst));
        }
    }

    [Fact]
    public void InvalidateFailingEdges_NoEffect_WithOnlyUpEvents()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Add some UP events (joining nodes)
        for (var i = 0; i < H - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
        }

        // InvalidateFailingEdges should return empty (no DOWN events)
        var result = detector.InvalidateFailingEdges();
        Assert.Empty(result);
    }

    [Fact]
    public void InvalidateFailingEdges_HandlesMixedUpAndDown()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 20;
        var endpoints = new List<Endpoint>();

        for (var i = 0; i < numNodes; i++)
        {
            var node = Utils.HostFromParts("127.0.0." + (i + 1), 1000 + i, Utils.GetNextNodeId());
            endpoints.Add(node);
            builder.RingAdd(node);
        }

        var mView = builder.Build();
        var detector = new MultiNodeCutDetector(H, L, mView);

        // Add a DOWN event (failure detection)
        var failingNode = endpoints[0];
        for (var i = 0; i < L; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                endpoints[i + 1], failingNode, EdgeStatus.Down, DefaultConfigId, i));
        }

        // InvalidateFailingEdges should be callable (may or may not produce results)
        var result = detector.InvalidateFailingEdges();
        Assert.NotNull(result);
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

    /// <summary>
    /// Generator for K values (number of rings).
    /// </summary>
    private static readonly Gen<int> GenK = Gen.Int[1, 10];

    [Fact]
    public void Property_Respects_H_Threshold()
    {
        // Generate enough nodes so that k rings can actually be created (need k+1 nodes minimum)
        Gen.Select(GenK.Where(k => k >= 4), GenUniqueNodes(10, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Derive H and L from actual ring count to ensure K > H >= L >= 1
                var actualK = view.RingCount;
                if (actualK < 4) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = Math.Max(1, h / 2);

                var detector = new MultiNodeCutDetector(h, l, view);

                var subject = nodes[0];

                // Report h-1 times - should not trigger
                var proposals = new List<List<Endpoint>>();
                for (var i = 0; i < h - 1 && i < nodes.Count - 1; i++)
                {
                    var result = detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[i + 1],
                            EdgeDst = subject,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                    proposals.Add(result);
                }

                // Should not have proposals yet (H threshold not reached)
                return proposals.All(p => !p.Contains(subject));
            });
    }

    [Fact]
    public void Property_Does_Not_Duplicate_Reports_From_Same_Ring()
    {
        Gen.Select(GenK.Where(k => k >= 4), GenUniqueNodes(10, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 4) return true; // Skip if not enough rings

                // Use actualK > H >= L (e.g., H=actualK-1, L=1)
                var detector = new MultiNodeCutDetector(actualK - 1, 1, view);

                var observer = nodes[1];
                var subject = nodes[0];

                // Report multiple times from the same observer on the same ring
                var results = new List<List<Endpoint>>();
                for (var i = 0; i < 5; i++)
                {
                    var result = detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = observer,
                            EdgeDst = subject,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { 0 }  // Same ring each time
                        });
                    results.Add(result);
                }

                // Multiple reports from same observer/ring should not accumulate
                // (only the first one counts)
                // All results should be empty since we only have 1 unique ring report
                return results.All(r => r.Count == 0);
            });
    }

    /// <summary>
    /// Property: Reaching exactly H reports for a single node (with no other nodes in preProposal)
    /// should trigger a proposal containing that node.
    /// </summary>
    [Fact]
    public void Property_H_Reports_Triggers_Single_Proposal()
    {
        Gen.Select(GenK.Where(k => k >= 4), GenUniqueNodes(10, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 4) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = 1;

                var detector = new MultiNodeCutDetector(h, l, view);
                var subject = nodes[0];

                // Send exactly H reports on different rings
                List<Endpoint> lastResult = [];
                for (var i = 0; i < h; i++)
                {
                    lastResult = detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[(i % (nodes.Count - 1)) + 1],
                            EdgeDst = subject,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                }

                // The H-th report should trigger a proposal
                return lastResult.Count == 1 && lastResult[0].Equals(subject);
            });
    }

    /// <summary>
    /// Property: Nodes between L and H reports block other nodes from being proposed.
    /// When multiple nodes are at L &lt;= reports &lt; H, no proposals should be generated
    /// until all reach H or fall below L.
    /// </summary>
    [Fact]
    public void Property_PreProposal_Blocks_Proposal()
    {
        Gen.Select(GenK.Where(k => k >= 5), GenUniqueNodes(10, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 5) return true; // Skip if not enough rings

                // Use H=actualK-1 and L=2 so we have a meaningful range
                var h = actualK - 1;
                var l = 2;

                var detector = new MultiNodeCutDetector(h, l, view);
                var subject1 = nodes[0];
                var subject2 = nodes[1];

                // Bring subject1 to H-1 reports (just below H, in preProposal)
                for (var i = 0; i < h - 1; i++)
                {
                    detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[(i % (nodes.Count - 2)) + 2],
                            EdgeDst = subject1,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                }

                // Bring subject2 to L reports (in preProposal)
                for (var i = 0; i < l; i++)
                {
                    detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[(i % (nodes.Count - 2)) + 2],
                            EdgeDst = subject2,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                }

                // Now push subject1 to H - should NOT trigger proposal because subject2 is in preProposal
                var result = detector.AggregateForProposal(
                    new AlertMessage
                    {
                        EdgeSrc = nodes[2],
                        EdgeDst = subject1,
                        EdgeStatus = EdgeStatus.Up,
                        RingNumber = { h - 1 }
                    });

                // subject2 is still in preProposal (L <= reports < H), so no proposal yet
                return result.Count == 0;
            });
    }

    /// <summary>
    /// Property: When all nodes in preProposal reach H, they are all proposed together.
    /// This is the batching behavior.
    /// </summary>
    [Fact]
    public void Property_Batches_Multiple_Nodes()
    {
        Gen.Select(GenK.Where(k => k >= 5), GenUniqueNodes(15, 25))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 5) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = 2;

                var detector = new MultiNodeCutDetector(h, l, view);
                var subject1 = nodes[0];
                var subject2 = nodes[1];
                var subject3 = nodes[2];

                // Bring all three subjects to H-1 reports
                foreach (var subject in new[] { subject1, subject2, subject3 })
                {
                    for (var i = 0; i < h - 1; i++)
                    {
                        detector.AggregateForProposal(
                            new AlertMessage
                            {
                                EdgeSrc = nodes[(i % (nodes.Count - 3)) + 3],
                                EdgeDst = subject,
                                EdgeStatus = EdgeStatus.Up,
                                RingNumber = { i }
                            });
                    }
                }

                // Push first two to H - should not trigger (subject3 still blocking)
                detector.AggregateForProposal(
                    new AlertMessage
                    {
                        EdgeSrc = nodes[3],
                        EdgeDst = subject1,
                        EdgeStatus = EdgeStatus.Up,
                        RingNumber = { h - 1 }
                    });

                var result2 = detector.AggregateForProposal(
                    new AlertMessage
                    {
                        EdgeSrc = nodes[3],
                        EdgeDst = subject2,
                        EdgeStatus = EdgeStatus.Up,
                        RingNumber = { h - 1 }
                    });

                // Still blocked by subject3
                if (result2.Count != 0)
                    return false;

                // Push subject3 to H - now all three should be proposed together
                var finalResult = detector.AggregateForProposal(
                    new AlertMessage
                    {
                        EdgeSrc = nodes[3],
                        EdgeDst = subject3,
                        EdgeStatus = EdgeStatus.Up,
                        RingNumber = { h - 1 }
                    });

                // All three should be in the proposal
                return finalResult.Count == 3 &&
                       finalResult.Contains(subject1) &&
                       finalResult.Contains(subject2) &&
                       finalResult.Contains(subject3);
            });
    }

    /// <summary>
    /// Property: Processing by ring number (interleaved) produces same or fewer proposals than
    /// processing all rings for each node sequentially.
    /// This verifies the batching optimization works correctly.
    /// </summary>
    [Fact]
    public void Property_RingInterleaving_Enables_Batching()
    {
        Gen.Select(GenK.Where(k => k >= 5), GenUniqueNodes(15, 25))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 5) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = 2;

                // Create messages for 3 subjects, each with all actualK ring numbers
                var subjects = nodes.Take(3).ToList();
                var messages = new List<AlertMessage>();
                foreach (var subject in subjects)
                {
                    var msg = new AlertMessage
                    {
                        EdgeSrc = nodes[3],
                        EdgeDst = subject,
                        EdgeStatus = EdgeStatus.Up
                    };
                    for (var r = 0; r < actualK; r++)
                    {
                        msg.RingNumber.Add(r);
                    }
                    messages.Add(msg);
                }

                // Method 1: Process all rings for each message (sequential, non-batching)
                var detector1 = new MultiNodeCutDetector(h, l, view);
                var proposals1 = 0;
                foreach (var msg in messages)
                {
                    var result = detector1.AggregateForProposal(msg);
                    if (result.Count > 0)
                        proposals1++;
                }

                // Method 2: Process by ring number across all messages (interleaved, batching)
                var detector2 = new MultiNodeCutDetector(h, l, view);
                var proposals2 = 0;
                for (var ringNumber = 0; ringNumber < actualK; ringNumber++)
                {
                    foreach (var msg in messages)
                    {
                        var result = detector2.AggregateForProposalSingleRing(msg, ringNumber);
                        if (result.Count > 0)
                            proposals2++;
                    }
                }

                // Interleaved processing should produce same or fewer proposal events
                // (potentially batching multiple nodes into single proposal)
                return proposals2 <= proposals1;
            });
    }

    /// <summary>
    /// Property: AggregateForProposal and AggregateForProposalSingleRing produce the same
    /// final state when processing the same messages.
    /// </summary>
    [Fact]
    public void Property_SingleRing_Equivalent_To_Full_When_Sequential()
    {
        Gen.Select(GenK.Where(k => k >= 4), GenUniqueNodes(10, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 4) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = 1;

                var subject = nodes[0];

                // Create a message with multiple ring numbers (up to h)
                var msg = new AlertMessage
                {
                    EdgeSrc = nodes[1],
                    EdgeDst = subject,
                    EdgeStatus = EdgeStatus.Up
                };
                for (var r = 0; r < h; r++)
                {
                    msg.RingNumber.Add(r);
                }

                // Method 1: Use AggregateForProposal
                var detector1 = new MultiNodeCutDetector(h, l, view);
                var result1 = detector1.AggregateForProposal(msg);

                // Method 2: Use AggregateForProposalSingleRing for each ring
                var detector2 = new MultiNodeCutDetector(h, l, view);
                var result2 = new List<Endpoint>();
                foreach (var ringNumber in msg.RingNumber)
                {
                    result2.AddRange(detector2.AggregateForProposalSingleRing(msg, ringNumber));
                }

                // Both methods should produce the same final result
                return result1.Count == result2.Count &&
                       result1.All(e => result2.Contains(e)) &&
                       detector1.GetNumProposals() == detector2.GetNumProposals();
            });
    }

    /// <summary>
    /// Property: Nodes below L threshold do not block proposals.
    /// </summary>
    [Fact]
    public void Property_Below_L_Does_Not_Block()
    {
        Gen.Select(GenK.Where(k => k >= 5), GenUniqueNodes(15, 25))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 5) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = 3; // Use L=3 so we have room below L
                if (l > h - 1) l = h - 1; // Ensure L < H

                var detector = new MultiNodeCutDetector(h, l, view);
                var subject1 = nodes[0];
                var subject2 = nodes[1];

                // subject2 gets L-1 reports (below L, should not block)
                for (var i = 0; i < l - 1; i++)
                {
                    detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[(i % (nodes.Count - 2)) + 2],
                            EdgeDst = subject2,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                }

                // subject1 reaches H reports
                List<Endpoint> lastResult = [];
                for (var i = 0; i < h; i++)
                {
                    lastResult = detector.AggregateForProposal(
                        new AlertMessage
                        {
                            EdgeSrc = nodes[(i % (nodes.Count - 2)) + 2],
                            EdgeDst = subject1,
                            EdgeStatus = EdgeStatus.Up,
                            RingNumber = { i }
                        });
                }

                // subject1 should be proposed (subject2 with L-1 reports doesn't block)
                return lastResult.Count == 1 && lastResult[0].Equals(subject1);
            });
    }

    /// <summary>
    /// Property: Proposal count increments correctly.
    /// </summary>
    [Fact]
    public void Property_ProposalCount_Increments()
    {
        Gen.Select(GenK.Where(k => k >= 4), GenUniqueNodes(15, 25))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var actualK = view.RingCount;
                if (actualK < 4) return true; // Skip if not enough rings

                var h = actualK - 1;
                var l = 1;

                var detector = new MultiNodeCutDetector(h, l, view);

                // Generate proposals for 3 different subjects
                var expectedProposals = 0;
                for (var s = 0; s < 3; s++)
                {
                    var subject = nodes[s];
                    for (var i = 0; i < h; i++)
                    {
                        var result = detector.AggregateForProposal(
                            new AlertMessage
                            {
                                EdgeSrc = nodes[(i % (nodes.Count - 3)) + 3],
                                EdgeDst = subject,
                                EdgeStatus = EdgeStatus.Up,
                                RingNumber = { i }
                            });
                        if (result.Count > 0)
                            expectedProposals++;
                    }
                }

                return detector.GetNumProposals() == expectedProposals;
            });
    }

    #endregion

    #region Unstable Mode Detection

    [Fact]
    public void HasNodesInUnstableMode_ReturnsFalse_Initially()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        Assert.False(detector.HasNodesInUnstableMode());
    }

    [Fact]
    public void HasNodesInUnstableMode_ReturnsTrue_WhenNodesBetweenLAndH()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Add L reports (node enters unstable mode)
        for (var i = 0; i < L; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.True(detector.HasNodesInUnstableMode());
    }

    [Fact]
    public void HasNodesInUnstableMode_ReturnsFalse_AfterReachingH()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst = Utils.HostFromParts("127.0.0.2", 2);

        // Add H reports (node leaves unstable mode)
        for (var i = 0; i < H; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.False(detector.HasNodesInUnstableMode());
    }

    [Fact]
    public void ForcePromoteUnstableNodes_ReturnsEmpty_WhenNoUnstableNodes()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);

        var result = detector.ForcePromoteUnstableNodes();

        Assert.Empty(result);
    }

    [Fact]
    public void ForcePromoteUnstableNodes_PromotesUnstableNodes()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        // Bring dst1 to H-1 reports (almost stable but still unstable)
        for (var i = 0; i < H - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
        }

        // Bring dst2 to L reports (unstable)
        for (var i = 0; i < L; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 100), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        Assert.True(detector.HasNodesInUnstableMode());
        Assert.Equal(0, detector.GetNumProposals());

        // Force promote
        var result = detector.ForcePromoteUnstableNodes();

        // Both nodes should be proposed
        Assert.Equal(2, result.Count);
        Assert.Contains(dst1, result);
        Assert.Contains(dst2, result);
        Assert.False(detector.HasNodesInUnstableMode());
        Assert.Equal(1, detector.GetNumProposals());
    }

    [Fact]
    public void ForcePromoteUnstableNodes_IncludesStableNodesWaiting()
    {
        var view = CreateTestView();
        var detector = new MultiNodeCutDetector(H, L, view);
        var dst1 = Utils.HostFromParts("127.0.0.2", 2);
        var dst2 = Utils.HostFromParts("127.0.0.3", 3);

        // Bring dst1 to H reports (stable, but blocked by dst2)
        for (var i = 0; i < H - 1; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 1), dst1, EdgeStatus.Up, DefaultConfigId, i));
        }

        // Bring dst2 to L reports (unstable - blocking dst1)
        for (var i = 0; i < L; i++)
        {
            detector.AggregateForProposal(CreateAlertMessage(
                Utils.HostFromParts("127.0.0.1", i + 100), dst2, EdgeStatus.Up, DefaultConfigId, i));
        }

        // Now bring dst1 to H (should NOT trigger proposal due to dst2 blocking)
        var intermediateResult = detector.AggregateForProposal(CreateAlertMessage(
            Utils.HostFromParts("127.0.0.1", H), dst1, EdgeStatus.Up, DefaultConfigId, H - 1));
        Assert.Empty(intermediateResult); // Blocked

        // Force promote - both should be included
        var result = detector.ForcePromoteUnstableNodes();

        Assert.Equal(2, result.Count);
        Assert.Contains(dst1, result);
        Assert.Contains(dst2, result);
    }

    #endregion

}
