using System.Collections.Immutable;
using System.Net;
using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Internal conversion helpers between System.Net.EndPoint and protobuf Endpoint types.
/// </summary>
internal static class EndPointConversions
{
    /// <summary>
    /// Converts a System.Net.EndPoint to a protobuf Endpoint.
    /// </summary>
    /// <param name="endPoint">The endpoint to convert.</param>
    /// <returns>A protobuf Endpoint.</returns>
    /// <exception cref="ArgumentException">Thrown when the EndPoint type is not supported.</exception>
    internal static Endpoint ToProtobuf(this EndPoint endPoint)
    {
        return endPoint switch
        {
            DnsEndPoint dns => new Endpoint
            {
                Hostname = ByteString.CopyFromUtf8(dns.Host),
                Port = dns.Port
            },
            IPEndPoint ip => new Endpoint
            {
                Hostname = ByteString.CopyFromUtf8(ip.Address.ToString()),
                Port = ip.Port
            },
            _ => throw new ArgumentException($"Unsupported EndPoint type: {endPoint.GetType()}", nameof(endPoint))
        };
    }

    /// <summary>
    /// Converts a protobuf Endpoint to a System.Net.EndPoint.
    /// The result is always a DnsEndPoint since hostnames may be IPs or DNS names.
    /// </summary>
    /// <param name="endpoint">The protobuf endpoint to convert.</param>
    /// <returns>A DnsEndPoint representing the endpoint.</returns>
    internal static EndPoint ToEndPoint(this Endpoint endpoint)
    {
        var host = endpoint.Hostname.ToStringUtf8();
        return new DnsEndPoint(host, endpoint.Port);
    }

    /// <summary>
    /// Tries to parse a protobuf Endpoint as an IPEndPoint if the hostname is a valid IP address.
    /// Falls back to DnsEndPoint if not a valid IP.
    /// </summary>
    /// <param name="endpoint">The protobuf endpoint to convert.</param>
    /// <returns>An IPEndPoint if the hostname is a valid IP address, otherwise a DnsEndPoint.</returns>
    internal static EndPoint ToEndPointPreferIP(this Endpoint endpoint)
    {
        var host = endpoint.Hostname.ToStringUtf8();
        return IPAddress.TryParse(host, out var ip) ? new IPEndPoint(ip, endpoint.Port) : new DnsEndPoint(host, endpoint.Port);
    }
}

/// <summary>
/// Internal conversion helpers between metadata types.
/// </summary>
internal static class MetadataConversions
{
    /// <summary>
    /// Converts a protobuf Metadata to a ClusterMetadata instance.
    /// </summary>
    /// <param name="metadata">The protobuf metadata to convert.</param>
    /// <returns>A ClusterMetadata instance.</returns>
    internal static ClusterMetadata ToClusterMetadata(this Metadata metadata)
    {
        if (metadata.Metadata_.Count == 0)
        {
            return ClusterMetadata.Empty;
        }

        var dict = metadata.Metadata_
            .ToDictionary(
                kvp => kvp.Key,
                kvp => (ReadOnlyMemory<byte>)kvp.Value.ToByteArray());
        return new ClusterMetadata(dict);
    }

    /// <summary>
    /// Converts a dictionary of metadata to a protobuf Metadata message.
    /// </summary>
    /// <param name="metadata">The metadata dictionary to convert.</param>
    /// <returns>A protobuf Metadata message.</returns>
    internal static Metadata ToProtobuf(this Dictionary<string, byte[]> metadata)
    {
        var pb = new Metadata();
        foreach (var kvp in metadata)
        {
            pb.Metadata_.Add(kvp.Key, ByteString.CopyFrom(kvp.Value));
        }
        return pb;
    }

    /// <summary>
    /// Converts an IReadOnlyDictionary of metadata to a protobuf Metadata message.
    /// </summary>
    /// <param name="metadata">The metadata dictionary to convert.</param>
    /// <returns>A protobuf Metadata message.</returns>
    internal static Metadata ToProtobuf(this IReadOnlyDictionary<string, byte[]>? metadata)
    {
        var pb = new Metadata();
        if (metadata is null)
        {
            return pb;
        }

        foreach (var kvp in metadata)
        {
            pb.Metadata_.Add(kvp.Key, ByteString.CopyFrom(kvp.Value));
        }
        return pb;
    }
}

/// <summary>
/// Internal conversion helpers for ClusterMembershipView.
/// </summary>
internal static class MembershipViewConversions
{
    /// <summary>
    /// Creates a ClusterMembershipView from an internal MembershipView and metadata.
    /// </summary>
    /// <param name="view">The internal membership view.</param>
    /// <param name="protobufMetadata">The metadata dictionary (protobuf Endpoint -> Metadata).</param>
    /// <returns>A public ClusterMembershipView.</returns>
    internal static ClusterMembershipView ToClusterMembershipView(
        this MembershipView view,
        IReadOnlyDictionary<Endpoint, Metadata>? protobufMetadata = null)
    {
        var members = view.Members
            .Select(e => e.ToEndPointPreferIP())
            .ToImmutableHashSet(EndPointComparer.Instance);

        var metadata = protobufMetadata?.ToDictionary(kvp => kvp.Key.ToEndPointPreferIP(), kvp => kvp.Value.ToClusterMetadata(), EndPointComparer.Instance) ?? [];

        return new ClusterMembershipView(
            view.ConfigurationId,
            members,
            metadata);
    }
}
