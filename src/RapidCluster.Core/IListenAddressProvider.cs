using System.Net;

namespace RapidCluster;

/// <summary>
/// Provides the endpoint this node listens on.
/// </summary>
/// <remarks>
/// <para>
/// This interface allows the listen address to be determined at runtime rather than
/// at configuration time. This is useful in environments like ASP.NET Core where
/// the actual listening address may not be known until the server has started
/// (e.g., when using dynamic port assignment).
/// </para>
/// <para>
/// Implementations must ensure the <see cref="ListenAddress"/> property is available
/// by the time RapidCluster services are initialized. For server-derived addresses,
/// register the provider only after the server has started.
/// </para>
/// </remarks>
public interface IListenAddressProvider
{
    /// <summary>
    /// Gets the endpoint this node listens on.
    /// </summary>
    /// <remarks>
    /// Use <see cref="IPEndPoint"/> for IP addresses or <see cref="DnsEndPoint"/> for hostnames.
    /// </remarks>
    EndPoint ListenAddress { get; }
}
