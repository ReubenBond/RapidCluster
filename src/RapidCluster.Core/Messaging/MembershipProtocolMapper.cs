using Google.Protobuf.Collections;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Messaging;

/// <summary>
/// Helper class for converting between Domain objects and Protobuf messages.
/// </summary>
internal static class MembershipProtocolMapper
{
    /// <summary>
    /// Builds a list of MemberInfo from a protobuf response containing endpoints and metadata.
    /// Used to parse JoinResponse and MembershipViewResponse messages.
    /// </summary>
    public static List<MemberInfo> BuildMemberInfosFromResponse(
        RepeatedField<Endpoint> endpoints,
        RepeatedField<Endpoint> metadataKeys,
        RepeatedField<Metadata> metadataValues)
    {
        // Build metadata lookup from parallel key/value lists
        var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
        var count = Math.Min(metadataKeys.Count, metadataValues.Count);
        for (var i = 0; i < count; i++)
        {
            metadataMap[metadataKeys[i]] = metadataValues[i];
        }

        // Create MemberInfo for each endpoint, using empty metadata if not found
        return endpoints
            .Select(e => new MemberInfo(e, metadataMap.GetValueOrDefault(e, new Metadata())))
            .ToList();
    }

    /// <summary>
    /// Creates an endpoint with the specified node ID, preserving hostname and port from the source.
    /// </summary>
    public static Endpoint CreateEndpointWithNodeId(Endpoint source, long nodeId)
    {
        return new Endpoint
        {
            Hostname = source.Hostname,
            Port = source.Port,
            NodeId = nodeId
        };
    }

    /// <summary>
    /// Builds a MembershipView from a JoinResponse.
    /// Combines BuildMemberInfosFromResponse and MembershipViewBuilder.
    /// </summary>
    public static MembershipView BuildMembershipViewFromJoinResponse(
        JoinResponse response,
        int observersPerSubject)
    {
        var memberInfos = BuildMemberInfosFromResponse(
            response.Endpoints,
            response.MetadataKeys,
            response.MetadataValues);
            
        var builder = new MembershipViewBuilder(observersPerSubject, memberInfos, response.MaxNodeId);
        return builder.BuildWithConfigurationId(ConfigurationId.FromProtobuf(response.ConfigurationId));
    }

    /// <summary>
    /// Creates a JoinResponse for a successful join (SafeToJoin).
    /// </summary>
    public static JoinResponse CreateSafeToJoinResponse(
        Endpoint sender,
        ConfigurationId configurationId,
        long maxNodeId,
        IEnumerable<Endpoint> endpoints,
        IEnumerable<MemberInfo> memberInfos)
    {
        var response = new JoinResponse
        {
            Sender = sender,
            StatusCode = JoinStatusCode.SafeToJoin,
            ConfigurationId = configurationId.ToProtobuf(),
            MaxNodeId = maxNodeId
        };
        response.Endpoints.AddRange(endpoints);

        foreach (var member in memberInfos)
        {
            response.MetadataKeys.Add(member.Endpoint);
            response.MetadataValues.Add(member.Metadata);
        }

        return response;
    }

    /// <summary>
    /// Creates a JoinResponse for a pre-join request.
    /// </summary>
    public static JoinResponse CreatePreJoinResponse(
        Endpoint sender,
        ConfigurationId configurationId,
        JoinStatusCode statusCode,
        Endpoint[]? observers = null)
    {
        var response = new JoinResponse
        {
            Sender = sender,
            ConfigurationId = configurationId.ToProtobuf(),
            StatusCode = statusCode
        };

        if (observers != null)
        {
            response.Endpoints.AddRange(observers);
        }

        return response;
    }

    /// <summary>
    /// Creates a JoinResponse indicating the configuration has changed.
    /// </summary>
    public static JoinResponse CreateConfigChangedResponse(Endpoint sender, ConfigurationId configurationId)
    {
        return new JoinResponse
        {
            Sender = sender,
            StatusCode = JoinStatusCode.ConfigChanged,
            ConfigurationId = configurationId.ToProtobuf()
        };
    }

    /// <summary>
    /// Creates a MembershipViewResponse with the current view state.
    /// </summary>
    public static MembershipViewResponse CreateMembershipViewResponse(
        Endpoint sender,
        MembershipView membershipView)
    {
        var response = new MembershipViewResponse
        {
            Sender = sender,
            ConfigurationId = membershipView.ConfigurationId.ToProtobuf(),
            MaxNodeId = membershipView.MaxNodeId
        };

        response.Endpoints.AddRange(membershipView.Members);

        foreach (var member in membershipView.MemberInfos)
        {
            response.MetadataKeys.Add(member.Endpoint);
            response.MetadataValues.Add(member.Metadata);
        }

        return response;
    }

    /// <summary>
    /// Creates an AlertMessage for a joiner that is safe to join.
    /// </summary>
    public static AlertMessage CreateJoinerAlertMessage(
        Endpoint sender,
        Endpoint joiner,
        ConfigurationId configurationId,
        Metadata metadata,
        IEnumerable<int> ringNumbers)
    {
        var alertMsg = new AlertMessage
        {
            EdgeSrc = sender,
            EdgeDst = joiner,
            EdgeStatus = EdgeStatus.Up,
            ConfigurationId = configurationId.ToProtobuf(),
            Metadata = metadata
        };
        alertMsg.RingNumber.AddRange(ringNumbers);
        return alertMsg;
    }

    /// <summary>
    /// Creates a ProbeResponse.
    /// </summary>
    public static ProbeResponse CreateProbeResponse(ConfigurationId configurationId, bool senderInMembership)
    {
        return new ProbeResponse
        {
            ConfigurationId = configurationId.ToProtobuf(),
            SenderInMembership = senderInMembership
        };
    }
}
