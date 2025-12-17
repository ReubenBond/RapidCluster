using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Test utility methods.
/// </summary>
internal static class Utils
{
    private static long s_nodeIdCounter;

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
    /// Creates an Endpoint from hostname, port, and node_id.
    /// </summary>
    public static Endpoint HostFromParts(string hostname, int port, long nodeId)
    {
        return new Endpoint
        {
            Hostname = ByteString.CopyFromUtf8(hostname),
            Port = port,
            NodeId = nodeId
        };
    }

    /// <summary>
    /// Gets a unique node ID for testing purposes.
    /// Thread-safe via Interlocked.
    /// </summary>
    public static long GetNextNodeId() => Interlocked.Increment(ref s_nodeIdCounter);

    /// <summary>
    /// Creates a MembershipView with the specified number of nodes and K value.
    /// Useful for cut detector tests that need a view for InvalidateFailingEdges.
    /// </summary>
    public static MembershipView CreateMembershipView(int numNodes, int k = 10)
    {
        var builder = new MembershipViewBuilder(k);
        for (var i = 0; i < numNodes; i++)
        {
            var node = HostFromParts("127.0.0." + (i + 1), 1000 + i, GetNextNodeId());
            builder.RingAdd(node);
        }
        return builder.Build();
    }

    /// <summary>
    /// Creates an empty MembershipView with the specified K value.
    /// </summary>
    public static MembershipView CreateEmptyMembershipView(int k = 10)
    {
        return new MembershipViewBuilder(k).Build();
    }
}
