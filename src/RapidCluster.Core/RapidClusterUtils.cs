using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Utility methods for RapidCluster.
/// </summary>
public static class RapidClusterUtils
{
    /// <summary>
    /// Converts a UUID to a NodeId.
    /// </summary>
    public static NodeId NodeIdFromUuid(Guid uuid)
    {
        var bytes = uuid.ToByteArray();
        var high = BitConverter.ToInt64(bytes, 0);
        var low = BitConverter.ToInt64(bytes, 8);
        return new NodeId { High = high, Low = low };
    }

    /// <summary>
    /// Creates an Endpoint from a host:port string.
    /// </summary>
    public static Endpoint HostFromString(string hostString)
    {
        ArgumentNullException.ThrowIfNull(hostString);
        var parts = hostString.Split(':');
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        {
            throw new ArgumentException($"Invalid host:port string: {hostString}");
        }
        return HostFromParts(parts[0], port);
    }

    /// <summary>
    /// Creates an Endpoint from hostname and port.
    /// </summary>
    public static Endpoint HostFromParts(string hostname, int port)
    {
        return new Endpoint
        {
            Hostname = ByteString.CopyFromUtf8(hostname),
            Port = port
        };
    }

    /// <summary>
    /// Creates a loggable string representation of an endpoint.
    /// </summary>
    public static string Loggable(Endpoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return $"{endpoint.Hostname.ToStringUtf8()}:{endpoint.Port}";
    }

    /// <summary>
    /// Creates a loggable string representation of endpoints.
    /// </summary>
    public static string Loggable(IEnumerable<Endpoint> endpoints) => $"[{string.Join(", ", endpoints.Select(Loggable))}]";
}

/// <summary>
/// Extension methods to convert protocol messages to RapidClusterRequest/RapidClusterResponse wrappers.
/// </summary>
public static class RapidClusterMessageExtensions
{
    // RapidClusterRequest extensions
    public static RapidClusterRequest ToRapidClusterRequest(this PreJoinMessage msg) =>
        new() { PreJoinMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this JoinMessage msg) =>
        new() { JoinMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this BatchedAlertMessage msg) =>
        new() { BatchedAlertMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this ProbeMessage msg) =>
        new() { ProbeMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this FastRoundPhase2bMessage msg) =>
        new() { FastRoundPhase2BMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this Phase1aMessage msg) =>
        new() { Phase1AMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this Phase1bMessage msg) =>
        new() { Phase1BMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this Phase2aMessage msg) =>
        new() { Phase2AMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this Phase2bMessage msg) =>
        new() { Phase2BMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this LeaveMessage msg) =>
        new() { LeaveMessage = msg };

    public static RapidClusterRequest ToRapidClusterRequest(this MembershipViewRequest msg) =>
        new() { MembershipViewRequest = msg };

    // RapidClusterResponse extensions
    public static RapidClusterResponse ToRapidClusterResponse(this JoinResponse msg) =>
        new() { JoinResponse = msg };

    public static RapidClusterResponse ToRapidClusterResponse(this ConsensusResponse msg) =>
        new() { ConsensusResponse = msg };

    public static RapidClusterResponse ToRapidClusterResponse(this ProbeResponse msg) =>
        new() { ProbeResponse = msg };

    public static RapidClusterResponse ToRapidClusterResponse(this MembershipViewResponse msg) =>
        new() { MembershipViewResponse = msg };
}
