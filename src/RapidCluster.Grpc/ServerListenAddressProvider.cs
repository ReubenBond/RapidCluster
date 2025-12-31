using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;

namespace RapidCluster.Grpc;

/// <summary>
/// A listen address provider that reads the address from the ASP.NET Core server.
/// </summary>
/// <remarks>
/// <para>
/// This provider extracts the listen address from <see cref="IServer.Features"/> after the server has started.
/// It is designed for dynamic port scenarios where the actual listening address is not known until runtime
/// (e.g., when using Aspire's dynamic port assignment or when binding to port 0).
/// </para>
/// <para>
/// The address is resolved lazily on first access and cached. The provider must only be accessed
/// after the server has started (e.g., after <see cref="Microsoft.Extensions.Hosting.IHostApplicationLifetime.ApplicationStarted"/>).
/// </para>
/// </remarks>
public sealed class ServerListenAddressProvider : IListenAddressProvider
{
    private readonly IServer _server;
    private readonly bool _preferHttps;
    private EndPoint? _cachedAddress;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerListenAddressProvider"/> class.
    /// </summary>
    /// <param name="server">The ASP.NET Core server.</param>
    /// <param name="preferHttps">Whether to prefer HTTPS addresses over HTTP. Default is true.</param>
    public ServerListenAddressProvider(IServer server, bool preferHttps = true)
    {
        ArgumentNullException.ThrowIfNull(server);
        _server = server;
        _preferHttps = preferHttps;
    }

    /// <inheritdoc />
    public EndPoint ListenAddress => _cachedAddress ??= ResolveAddress();

    private EndPoint ResolveAddress()
    {
        var addressesFeature = _server.Features.Get<IServerAddressesFeature>();
        var addresses = addressesFeature?.Addresses ?? [];

        // Select address based on preference
        string? selectedAddress;
        if (_preferHttps)
        {
            // Prefer HTTPS addresses for gRPC (required for Http1AndHttp2 with ALPN negotiation)
            selectedAddress = addresses.FirstOrDefault(a => a.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
                ?? addresses.FirstOrDefault();
        }
        else
        {
            // Prefer HTTP addresses (e.g., for plain HTTP/2 without TLS)
            selectedAddress = addresses.FirstOrDefault(a => a.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
                ?? addresses.FirstOrDefault();
        }

        if (string.IsNullOrEmpty(selectedAddress))
        {
            throw new InvalidOperationException(
                "No server addresses are available. Ensure the server has started before accessing the listen address. " +
                "This provider should only be resolved after ApplicationStarted has fired.");
        }

        if (!Uri.TryCreate(selectedAddress, UriKind.Absolute, out var uri))
        {
            throw new InvalidOperationException(
                $"Failed to parse server address '{selectedAddress}' as a URI.");
        }

        // Convert to EndPoint
        if (IPAddress.TryParse(uri.Host, out var ipAddress))
        {
            return new IPEndPoint(ipAddress, uri.Port);
        }

        return new DnsEndPoint(uri.Host, uri.Port);
    }
}
