using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using RapidCluster.Discovery;

namespace RapidCluster.Grpc;

/// <summary>
/// Utility class for resolving the server's advertised address from ASP.NET Core server features.
/// This is useful when using dynamic port assignment (e.g., Aspire) where the actual bound address
/// is only known after the server starts.
/// </summary>
public static class ServerAddressResolver
{
    /// <summary>
    /// Derives the advertised endpoint from the ASP.NET Core server's bound addresses.
    /// </summary>
    /// <param name="server">The ASP.NET Core server instance.</param>
    /// <param name="preferredHost">
    /// Optional preferred host to use. If null, the method will attempt to resolve a suitable
    /// non-localhost address from network interfaces.
    /// </param>
    /// <param name="preferHttps">
    /// Whether to prefer HTTPS addresses over HTTP. Defaults to false for gRPC H2C scenarios.
    /// </param>
    /// <returns>The resolved endpoint.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown when no server addresses are available or when unable to determine an address.
    /// </exception>
    public static EndPoint GetAdvertisedEndpoint(IServer server, string? preferredHost = null, bool preferHttps = false)
    {
        ArgumentNullException.ThrowIfNull(server);

        var address = GetPreferredAddress(server, preferHttps);
        if (!Uri.TryCreate(address, UriKind.Absolute, out var uri))
        {
            throw new InvalidOperationException($"Unable to parse server address '{address}' as a valid URI.");
        }

        var host = ResolveAdvertisedHost(uri.Host, preferredHost);
        if (IPAddress.TryParse(host, out var ipAddress))
        {
            return new IPEndPoint(ipAddress, uri.Port);
        }

        return new DnsEndPoint(host, uri.Port);
    }

    /// <summary>
    /// Derives the advertised URI from the ASP.NET Core server's bound addresses.
    /// </summary>
    /// <param name="server">The ASP.NET Core server instance.</param>
    /// <param name="preferredHost">
    /// Optional preferred host to use. If null, the method will attempt to resolve a suitable
    /// non-localhost address from network interfaces.
    /// </param>
    /// <param name="preferHttps">
    /// Whether to prefer HTTPS addresses over HTTP. Defaults to false for gRPC H2C scenarios.
    /// </param>
    /// <returns>The resolved URI.</returns>
    public static Uri GetAdvertisedUri(IServer server, string? preferredHost = null, bool preferHttps = false)
    {
        ArgumentNullException.ThrowIfNull(server);

        var address = GetPreferredAddress(server, preferHttps);
        if (!Uri.TryCreate(address, UriKind.Absolute, out var uri))
        {
            throw new InvalidOperationException($"Unable to parse server address '{address}' as a valid URI.");
        }

        var host = ResolveAdvertisedHost(uri.Host, preferredHost);
        var builder = new UriBuilder(uri)
        {
            Host = host,
        };

        return builder.Uri;
    }

    /// <summary>
    /// Gets the first available server address, useful for simple scenarios.
    /// Returns null if no addresses are available.
    /// </summary>
    /// <param name="server">The ASP.NET Core server instance.</param>
    /// <returns>The first server address as a URI, or null if none available.</returns>
    public static Uri? TryGetFirstAddress(IServer server)
    {
        ArgumentNullException.ThrowIfNull(server);

        var addressesFeature = server.Features.Get<IServerAddressesFeature>();
        var address = addressesFeature?.Addresses.FirstOrDefault();

        if (address != null && Uri.TryCreate(address, UriKind.Absolute, out var uri))
        {
            return uri;
        }

        return null;
    }

    /// <summary>
    /// Checks whether the server has any addresses available.
    /// </summary>
    /// <param name="server">The ASP.NET Core server instance.</param>
    /// <returns>True if addresses are available, false otherwise.</returns>
    public static bool HasAddresses(IServer server)
    {
        ArgumentNullException.ThrowIfNull(server);

        var addressesFeature = server.Features.Get<IServerAddressesFeature>();
        return addressesFeature?.Addresses.Count > 0;
    }

    private static string GetPreferredAddress(IServer server, bool preferHttps)
    {
        var addressesFeature = server.Features.Get<IServerAddressesFeature>();
        var addresses = addressesFeature?.Addresses;

        if (addresses == null || addresses.Count == 0)
        {
            throw new InvalidOperationException(
                "Server addresses are not available. Ensure the server has started before calling this method. " +
                "Hook into IHostApplicationLifetime.ApplicationStarted to ensure the server is ready.");
        }

        if (preferHttps)
        {
            return addresses.FirstOrDefault(a => a.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                ?? addresses.First();
        }

        // Prefer HTTP for gRPC H2C (HTTP/2 Cleartext) scenarios
        return addresses.FirstOrDefault(a => a.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            ?? addresses.First();
    }

    private static string ResolveAdvertisedHost(string boundHost, string? preferredHost)
    {
        if (!string.IsNullOrWhiteSpace(preferredHost))
        {
            return preferredHost;
        }

        if (IsWildcardOrLocalhost(boundHost))
        {
            var resolvedIp = HostAddressResolver.ResolveIPAddressOrDefault();
            if (resolvedIp != null)
            {
                return resolvedIp.ToString();
            }

            return Dns.GetHostName();
        }

        return boundHost;
    }

    private static bool IsWildcardOrLocalhost(string host)
    {
        if (string.IsNullOrEmpty(host))
        {
            return true;
        }

        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Wildcard indicator in server addresses (e.g., "+" or "*")
        if (host is "+" or "*")
        {
            return true;
        }

        if (IPAddress.TryParse(host, out var ip))
        {
            if (IPAddress.Any.Equals(ip) || IPAddress.IPv6Any.Equals(ip))
            {
                return true;
            }

            if (IPAddress.IsLoopback(ip))
            {
                return true;
            }
        }

        return false;
    }
}
