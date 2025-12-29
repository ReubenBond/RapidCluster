using System.Diagnostics.CodeAnalysis;
using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Options;

namespace RapidCluster.EndToEnd.Node;

/// <summary>
/// Configures RapidClusterOptions.ListenAddress after the server has started.
/// This is necessary because Aspire assigns dynamic ports via ASPNETCORE_URLS,
/// and the actual bound address is only available after the server starts.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed class DeferredRapidClusterOptionsConfigurator : IConfigureOptions<RapidClusterOptions>, IDisposable
{
    private readonly Lock _lock = new();
    private readonly ManualResetEventSlim _addressConfigured = new(initialState: false);
    private EndPoint? _listenAddress;

    public void Configure(RapidClusterOptions options)
    {
        // Wait for the address to be configured (with timeout to prevent deadlock)
        if (!_addressConfigured.Wait(TimeSpan.FromSeconds(30)))
        {
            throw new InvalidOperationException(
                "Timed out waiting for server address to be configured. " +
                "Ensure the server has started before resolving RapidClusterOptions.");
        }

        lock (_lock)
        {
            options.ListenAddress = _listenAddress!;
        }
    }

    /// <summary>
    /// Waits for the configuration to be set. Can be cancelled via the provided token.
    /// </summary>
    public void WaitForConfiguration(CancellationToken cancellationToken) => _addressConfigured.Wait(cancellationToken);

    public EndPoint ConfigureFromServer(IServer server)
    {
        // Get the first available HTTPS server address for gRPC communication
        // With TLS, both HTTP/1.1 (health) and HTTP/2 (gRPC) work on the same HTTPS endpoint
        var addresses = server.Features.Get<IServerAddressesFeature>()?.Addresses ?? [];

        // Prefer HTTPS addresses for gRPC (required for Http1AndHttp2 with ALPN)
        var address = addresses.FirstOrDefault(a => a.StartsWith("https://", StringComparison.OrdinalIgnoreCase))
            ?? addresses.FirstOrDefault();

        if (string.IsNullOrEmpty(address) || !Uri.TryCreate(address, UriKind.Absolute, out var selectedUri))
        {
            throw new InvalidOperationException(
                "Server addresses are not available. Ensure the server has started before calling this method.");
        }

        // For local Aspire testing, we need to use the actual bound address (localhost)
        // rather than resolving to a network IP, because the server only binds to localhost
        var host = selectedUri.Host;
        EndPoint listenAddress;

        if (IPAddress.TryParse(host, out var ipAddress))
        {
            listenAddress = new IPEndPoint(ipAddress, selectedUri.Port);
        }
        else
        {
            listenAddress = new DnsEndPoint(host, selectedUri.Port);
        }

        lock (_lock)
        {
            _listenAddress = listenAddress;
        }

        _addressConfigured.Set();
        return listenAddress;
    }

    public void Dispose() => _addressConfigured.Dispose();
}
