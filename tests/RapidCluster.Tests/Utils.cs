using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Tests;

/// <summary>
/// Test utility methods.
/// </summary>
internal static class Utils
{
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
    /// Creates a MembershipView with the specified number of nodes and K value.
    /// Useful for cut detector tests that need a view for InvalidateFailingEdges.
    /// </summary>
    public static MembershipView CreateMembershipView(int numNodes, int k = 10)
    {
        var builder = new MembershipViewBuilder(k);
        for (var i = 0; i < numNodes; i++)
        {
            var node = HostFromParts("127.0.0." + (i + 1), 1000 + i);
            builder.RingAdd(node, NodeIdFromUuid(Guid.NewGuid()));
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
