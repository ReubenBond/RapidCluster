using RapidCluster.Pb;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for RapidClusterUtils utility methods.
/// </summary>
public class RapidClusterUtilsTests
{

    [Fact]
    public void NodeIdFromUuidConvertsGuidCorrectly()
    {
        var guid = Guid.NewGuid();
        var nodeId = RapidClusterUtils.NodeIdFromUuid(guid);

        Assert.NotEqual(0, nodeId.High);
        Assert.NotEqual(0, nodeId.Low);
    }

    [Fact]
    public void NodeIdFromUuidProducesDeterministicResults()
    {
        var guid = new Guid("12345678-1234-1234-1234-123456789abc");
        var nodeId1 = RapidClusterUtils.NodeIdFromUuid(guid);
        var nodeId2 = RapidClusterUtils.NodeIdFromUuid(guid);

        Assert.Equal(nodeId1.High, nodeId2.High);
        Assert.Equal(nodeId1.Low, nodeId2.Low);
    }

    [Fact]
    public void NodeIdFromUuidDifferentGuidsProduceDifferentNodeIds()
    {
        var guid1 = Guid.NewGuid();
        var guid2 = Guid.NewGuid();

        var nodeId1 = RapidClusterUtils.NodeIdFromUuid(guid1);
        var nodeId2 = RapidClusterUtils.NodeIdFromUuid(guid2);

        Assert.False(nodeId1.High == nodeId2.High && nodeId1.Low == nodeId2.Low);
    }

    [Fact]
    public void NodeIdFromUuidEmptyGuidWorks()
    {
        var nodeId = RapidClusterUtils.NodeIdFromUuid(Guid.Empty);

        Assert.Equal(0, nodeId.High);
        Assert.Equal(0, nodeId.Low);
    }



    [Fact]
    public void HostFromStringValidInputParsesCorrectly()
    {
        var endpoint = RapidClusterUtils.HostFromString("127.0.0.1:8080");

        Assert.Equal("127.0.0.1", endpoint.Hostname.ToStringUtf8());
        Assert.Equal(8080, endpoint.Port);
    }

    [Fact]
    public void HostFromStringLocalhostWithPortParsesCorrectly()
    {
        var endpoint = RapidClusterUtils.HostFromString("localhost:9000");

        Assert.Equal("localhost", endpoint.Hostname.ToStringUtf8());
        Assert.Equal(9000, endpoint.Port);
    }

    [Fact]
    public void HostFromStringHighPortParsesCorrectly()
    {
        var endpoint = RapidClusterUtils.HostFromString("10.0.0.1:65535");

        Assert.Equal("10.0.0.1", endpoint.Hostname.ToStringUtf8());
        Assert.Equal(65535, endpoint.Port);
    }

    [Fact]
    public void HostFromStringNullInputThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(() => RapidClusterUtils.HostFromString(null!));

    [Fact]
    public void HostFromStringInvalidFormatNoColonThrows() => Assert.Throws<ArgumentException>(() => RapidClusterUtils.HostFromString("127.0.0.1"));

    [Fact]
    public void HostFromStringInvalidFormatNonNumericPortThrows() => Assert.Throws<ArgumentException>(() => RapidClusterUtils.HostFromString("127.0.0.1:abc"));

    [Fact]
    public void HostFromStringInvalidFormatEmptyPortThrows() => Assert.Throws<ArgumentException>(() => RapidClusterUtils.HostFromString("127.0.0.1:"));

    [Fact]
    public void HostFromStringInvalidFormatMultipleColonsThrows() => Assert.Throws<ArgumentException>(() => RapidClusterUtils.HostFromString("127.0.0.1:8080:extra"));



    [Fact]
    public void HostFromPartsValidInputCreatesEndpoint()
    {
        var endpoint = RapidClusterUtils.HostFromParts("192.168.1.1", 5000);

        Assert.Equal("192.168.1.1", endpoint.Hostname.ToStringUtf8());
        Assert.Equal(5000, endpoint.Port);
    }

    [Fact]
    public void HostFromPartsZeroPortWorks()
    {
        var endpoint = RapidClusterUtils.HostFromParts("localhost", 0);

        Assert.Equal("localhost", endpoint.Hostname.ToStringUtf8());
        Assert.Equal(0, endpoint.Port);
    }

    [Fact]
    public void HostFromPartsNegativePortWorks()
    {
        // Note: The method doesn't validate port range
        var endpoint = RapidClusterUtils.HostFromParts("localhost", -1);

        Assert.Equal(-1, endpoint.Port);
    }



    [Fact]
    public void LoggableSingleEndpointFormatsCorrectly()
    {
        var endpoint = RapidClusterUtils.HostFromParts("10.0.0.1", 9000);

        var result = RapidClusterUtils.Loggable(endpoint);

        Assert.Equal("10.0.0.1:9000", result);
    }

    [Fact]
    public void LoggableNullEndpointThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(() => RapidClusterUtils.Loggable((Endpoint)null!));

    [Fact]
    public void LoggableMultipleEndpointsFormatsCorrectly()
    {
        var endpoints = new List<Endpoint>
        {
            RapidClusterUtils.HostFromParts("10.0.0.1", 9000),
            RapidClusterUtils.HostFromParts("10.0.0.2", 9001),
            RapidClusterUtils.HostFromParts("10.0.0.3", 9002)
        };

        var result = RapidClusterUtils.Loggable(endpoints);

        Assert.Equal("[10.0.0.1:9000, 10.0.0.2:9001, 10.0.0.3:9002]", result);
    }

    [Fact]
    public void LoggableEmptyEndpointListReturnsEmptyBrackets()
    {
        var endpoints = new List<Endpoint>();

        var result = RapidClusterUtils.Loggable(endpoints);

        Assert.Equal("[]", result);
    }

    [Fact]
    public void LoggableSingleElementListFormatsCorrectly()
    {
        var endpoints = new List<Endpoint>
        {
            RapidClusterUtils.HostFromParts("127.0.0.1", 1234)
        };

        var result = RapidClusterUtils.Loggable(endpoints);

        Assert.Equal("[127.0.0.1:1234]", result);
    }



    [Fact]
    public void ToRapidClusterRequestPreJoinMessageWrapsCorrectly()
    {
        var msg = new PreJoinMessage
        {
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            NodeId = RapidClusterUtils.NodeIdFromUuid(Guid.NewGuid())
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.PreJoinMessage);
        Assert.Equal(msg.Sender, request.PreJoinMessage.Sender);
    }

    [Fact]
    public void ToRapidClusterRequestJoinMessageWrapsCorrectly()
    {
        var msg = new JoinMessage
        {
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            NodeId = RapidClusterUtils.NodeIdFromUuid(Guid.NewGuid()),
            ConfigurationId = 100
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.JoinMessage);
        Assert.Equal(msg.Sender, request.JoinMessage.Sender);
        Assert.Equal(100, request.JoinMessage.ConfigurationId);
    }

    [Fact]
    public void ToRapidClusterRequestBatchedAlertMessageWrapsCorrectly()
    {
        var msg = new BatchedAlertMessage();
        msg.Messages.Add(new AlertMessage
        {
            EdgeSrc = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            EdgeDst = RapidClusterUtils.HostFromParts("127.0.0.1", 1235),
            EdgeStatus = EdgeStatus.Down,
            ConfigurationId = 100
        });

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.BatchedAlertMessage);
        Assert.Single(request.BatchedAlertMessage.Messages);
    }

    [Fact]
    public void ToRapidClusterRequestProbeMessageWrapsCorrectly()
    {
        var msg = new ProbeMessage
        {
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234)
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.ProbeMessage);
        Assert.Equal(msg.Sender, request.ProbeMessage.Sender);
    }

    [Fact]
    public void ToRapidClusterRequestFastRoundPhase2bMessageWrapsCorrectly()
    {
        var proposal = new MembershipProposal { ConfigurationId = 100 };
        proposal.Members.Add(new MemberInfo
        {
            Endpoint = RapidClusterUtils.HostFromParts("127.0.0.1", 1235),
            NodeId = new NodeId { High = 1, Low = 2 }
        });

        var msg = new FastRoundPhase2bMessage
        {
            ConfigurationId = 100,
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            Proposal = proposal
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.FastRoundPhase2BMessage);
        Assert.Equal(100, request.FastRoundPhase2BMessage.ConfigurationId);
    }

    [Fact]
    public void ToRapidClusterRequestPhase1aMessageWrapsCorrectly()
    {
        var msg = new Phase1aMessage
        {
            ConfigurationId = 100,
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            Rank = new Rank { Round = 2, NodeIndex = 5 }
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.Phase1AMessage);
        Assert.Equal(2, request.Phase1AMessage.Rank.Round);
    }

    [Fact]
    public void ToRapidClusterRequestPhase1bMessageWrapsCorrectly()
    {
        var msg = new Phase1bMessage
        {
            ConfigurationId = 100,
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            Rnd = new Rank { Round = 2, NodeIndex = 5 },
            Vrnd = new Rank { Round = 1, NodeIndex = 3 }
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.Phase1BMessage);
        Assert.Equal(2, request.Phase1BMessage.Rnd.Round);
    }

    [Fact]
    public void ToRapidClusterRequestPhase2aMessageWrapsCorrectly()
    {
        var proposal = new MembershipProposal { ConfigurationId = 100 };
        proposal.Members.Add(new MemberInfo
        {
            Endpoint = RapidClusterUtils.HostFromParts("127.0.0.1", 1235),
            NodeId = new NodeId { High = 1, Low = 2 }
        });

        var msg = new Phase2aMessage
        {
            ConfigurationId = 100,
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            Rnd = new Rank { Round = 2, NodeIndex = 5 },
            Proposal = proposal
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.Phase2AMessage);
        Assert.Single(request.Phase2AMessage.Proposal.Members);
    }

    [Fact]
    public void ToRapidClusterRequestPhase2bMessageWrapsCorrectly()
    {
        var proposal = new MembershipProposal { ConfigurationId = 100 };
        proposal.Members.Add(new MemberInfo
        {
            Endpoint = RapidClusterUtils.HostFromParts("127.0.0.1", 1235),
            NodeId = new NodeId { High = 1, Low = 2 }
        });

        var msg = new Phase2bMessage
        {
            ConfigurationId = 100,
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234),
            Rnd = new Rank { Round = 2, NodeIndex = 5 },
            Proposal = proposal
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.Phase2BMessage);
        Assert.Single(request.Phase2BMessage.Proposal.Members);
    }

    [Fact]
    public void ToRapidClusterRequestLeaveMessageWrapsCorrectly()
    {
        var msg = new LeaveMessage
        {
            Sender = RapidClusterUtils.HostFromParts("127.0.0.1", 1234)
        };

        var request = msg.ToRapidClusterRequest();

        Assert.NotNull(request.LeaveMessage);
        Assert.Equal(msg.Sender, request.LeaveMessage.Sender);
    }



    [Fact]
    public void ToRapidClusterResponseJoinResponseWrapsCorrectly()
    {
        var msg = new JoinResponse
        {
            StatusCode = JoinStatusCode.SafeToJoin,
            ConfigurationId = 100
        };
        msg.Endpoints.Add(RapidClusterUtils.HostFromParts("127.0.0.1", 1234));

        var response = msg.ToRapidClusterResponse();

        Assert.NotNull(response.JoinResponse);
        Assert.Equal(JoinStatusCode.SafeToJoin, response.JoinResponse.StatusCode);
    }

    [Fact]
    public void ToRapidClusterResponseConsensusResponseWrapsCorrectly()
    {
        var msg = new ConsensusResponse();

        var response = msg.ToRapidClusterResponse();

        Assert.NotNull(response.ConsensusResponse);
    }

    [Fact]
    public void ToRapidClusterResponseProbeResponseWrapsCorrectly()
    {
        var msg = new ProbeResponse { Status = NodeStatus.Ok };

        var response = msg.ToRapidClusterResponse();

        Assert.NotNull(response.ProbeResponse);
        Assert.Equal(NodeStatus.Ok, response.ProbeResponse.Status);
    }

}
