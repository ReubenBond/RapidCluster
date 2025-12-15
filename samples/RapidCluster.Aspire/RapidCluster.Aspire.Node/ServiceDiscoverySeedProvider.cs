using System.Net;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using RapidCluster.Discovery;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// Seed provider that uses .NET Aspire's service discovery to find cluster replicas.
/// In Aspire, each replica of a service gets its own unique endpoint that can be
/// discovered through configuration-based service discovery.
/// </summary>
/// <remarks>
/// This provider reads the service endpoints from Aspire's configuration system.
/// Aspire injects service endpoint information via environment variables that are
/// mapped to configuration, allowing replicas to discover each other.
/// </remarks>
internal sealed partial class ServiceDiscoverySeedProvider : ISeedProvider
{
    private readonly IConfiguration _configuration;
    private readonly ILogger<ServiceDiscoverySeedProvider> _logger;
    private readonly string _serviceName;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceDiscoverySeedProvider"/> class.
    /// </summary>
    /// <param name="configuration">The configuration containing service discovery information.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="serviceName">The name of the service to discover (typically "cluster").</param>
    public ServiceDiscoverySeedProvider(
        IConfiguration configuration,
        ILogger<ServiceDiscoverySeedProvider> logger,
        string serviceName = "cluster")
    {
        _configuration = configuration;
        _logger = logger;
        _serviceName = serviceName;
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var endpoints = new List<EndPoint>();

        // Aspire uses the services__<servicename>__<index>__<endpoint> configuration pattern
        // For example: services__cluster__0__grpc, services__cluster__1__grpc, etc.
        var servicesSection = _configuration.GetSection("services");
        var serviceSection = servicesSection.GetSection(_serviceName);

        if (!serviceSection.Exists())
        {
            LogNoServiceEndpoints(_logger, _serviceName);
            return ValueTask.FromResult<IReadOnlyList<EndPoint>>(endpoints);
        }

        // Iterate through all replica indices (0, 1, 2, etc.)
        foreach (var replicaSection in serviceSection.GetChildren())
        {
            // Look for the grpc endpoint
            var grpcEndpoint = replicaSection.GetSection("grpc").Value;
            if (string.IsNullOrEmpty(grpcEndpoint))
            {
                // Try alternate naming patterns
                grpcEndpoint = replicaSection.GetValue<string>("0");
            }

            if (!string.IsNullOrEmpty(grpcEndpoint) && TryParseEndpoint(grpcEndpoint, out var endpoint))
            {
                endpoints.Add(endpoint);
                LogDiscoveredSeed(_logger, grpcEndpoint);
            }
        }

        // If no structured config found, try the connection string pattern
        if (endpoints.Count == 0)
        {
            var connectionString = _configuration.GetConnectionString(_serviceName);
            if (!string.IsNullOrEmpty(connectionString) && TryParseEndpoint(connectionString, out var endpoint))
            {
                endpoints.Add(endpoint);
                LogDiscoveredSeed(_logger, connectionString);
            }
        }

        LogTotalSeedsDiscovered(_logger, endpoints.Count, _serviceName);
        return ValueTask.FromResult<IReadOnlyList<EndPoint>>(endpoints);
    }

    private static bool TryParseEndpoint(string value, out EndPoint endpoint)
    {
        endpoint = null!;

        // Handle URI format (http://host:port or https://host:port)
        if (Uri.TryCreate(value, UriKind.Absolute, out var uri))
        {
            endpoint = new DnsEndPoint(uri.Host, uri.Port);
            return true;
        }

        // Handle host:port format
        var lastColon = value.LastIndexOf(':');
        if (lastColon > 0 && int.TryParse(value.AsSpan(lastColon + 1), out var port))
        {
            var host = value[..lastColon];
            if (IPAddress.TryParse(host, out var ipAddress))
            {
                endpoint = new IPEndPoint(ipAddress, port);
            }
            else
            {
                endpoint = new DnsEndPoint(host, port);
            }
            return true;
        }

        return false;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "No service endpoints found for service '{ServiceName}'")]
    private static partial void LogNoServiceEndpoints(ILogger logger, string serviceName);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Discovered seed endpoint: {Endpoint}")]
    private static partial void LogDiscoveredSeed(ILogger logger, string endpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "Discovered {Count} seed(s) for service '{ServiceName}'")]
    private static partial void LogTotalSeedsDiscovered(ILogger logger, int count, string serviceName);
}
