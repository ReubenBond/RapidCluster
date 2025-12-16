using System.Diagnostics.CodeAnalysis;
using System.Net;
using Microsoft.Extensions.ServiceDiscovery;
using RapidCluster.Discovery;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// Seed provider that uses .NET Aspire's service discovery to find cluster nodes.
/// Uses <see cref="ServiceEndpointResolver"/> to dynamically resolve endpoints
/// for each cluster node service.
/// </summary>
/// <remarks>
/// <para>
/// This provider uses Aspire's runtime service discovery mechanism to resolve
/// endpoints for services named with a pattern like "cluster-0", "cluster-1", etc.
/// </para>
/// <para>
/// The service name prefix (default "cluster") combined with the cluster size from
/// configuration determines which services to resolve.
/// </para>
/// </remarks>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Available for use but not currently instantiated")]
internal sealed partial class ServiceDiscoverySeedProvider : ISeedProvider
{
    private readonly ServiceEndpointResolver _resolver;
    private readonly IConfiguration _configuration;
    private readonly ILogger<ServiceDiscoverySeedProvider> _logger;
    private readonly string _serviceNamePrefix;

    /// <summary>
    /// Initializes a new instance of the <see cref="ServiceDiscoverySeedProvider"/> class.
    /// </summary>
    /// <param name="resolver">The service endpoint resolver for discovering service endpoints.</param>
    /// <param name="configuration">The configuration to read cluster size from.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="serviceNamePrefix">The prefix for service names (e.g., "cluster" for cluster-0, cluster-1, etc.).</param>
    public ServiceDiscoverySeedProvider(
        ServiceEndpointResolver resolver,
        IConfiguration configuration,
        ILogger<ServiceDiscoverySeedProvider> logger,
        string serviceNamePrefix = "cluster")
    {
        _resolver = resolver;
        _configuration = configuration;
        _logger = logger;
        _serviceNamePrefix = serviceNamePrefix;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var endpoints = new List<EndPoint>();
        var clusterSize = _configuration.GetValue<int>("CLUSTER_SIZE", 1);

        LogDiscoveringNodes(_logger, clusterSize, _serviceNamePrefix);

        // Resolve each cluster node service (cluster-0, cluster-1, etc.)
        for (var i = 0; i < clusterSize; i++)
        {
            var serviceName = $"{_serviceNamePrefix}-{i}";
            var serviceUri = $"http://{serviceName}";

            try
            {
                var result = await _resolver.GetEndpointsAsync(serviceUri, cancellationToken);

                foreach (var serviceEndpoint in result.Endpoints)
                {
                    var endpoint = ConvertToStandardEndPoint(serviceEndpoint.EndPoint);
                    if (endpoint != null)
                    {
                        endpoints.Add(endpoint);
                        var endpointString = endpoint.ToString()!;
                        LogDiscoveredSeed(_logger, serviceName, endpointString);
                    }
                }
            }
            catch (InvalidOperationException ex)
            {
                // Service discovery may fail if the service is not yet registered
                LogServiceDiscoveryError(_logger, serviceName, ex);
            }
        }

        LogTotalSeedsDiscovered(_logger, endpoints.Count, _serviceNamePrefix);
        return endpoints;
    }

    /// <summary>
    /// Converts an endpoint to a standard .NET EndPoint type (DnsEndPoint or IPEndPoint).
    /// ServiceEndpointResolver returns UriEndPoint which is not supported by RapidCluster.
    /// </summary>
    private static EndPoint? ConvertToStandardEndPoint(EndPoint endpoint)
    {
        return endpoint switch
        {
            // UriEndPoint is returned by Aspire's ServiceEndpointResolver
            UriEndPoint uriEndPoint => ConvertUriEndPoint(uriEndPoint),
            // Already a standard endpoint type
            IPEndPoint or DnsEndPoint => endpoint,
            // Unknown type - skip it
            _ => null
        };
    }

    private static EndPoint ConvertUriEndPoint(UriEndPoint uriEndPoint)
    {
        var uri = uriEndPoint.Uri;
        var port = uri.Port > 0 ? uri.Port : (uri.Scheme == "https" ? 443 : 80);

        // Try to parse as IP address first
        if (IPAddress.TryParse(uri.Host, out var ipAddress))
        {
            return new IPEndPoint(ipAddress, port);
        }

        // Fall back to DNS endpoint
        return new DnsEndPoint(uri.Host, port);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Discovering {ClusterSize} nodes with service prefix '{ServicePrefix}'")]
    private static partial void LogDiscoveringNodes(ILogger logger, int clusterSize, string servicePrefix);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Discovered seed from {ServiceName}: {Endpoint}")]
    private static partial void LogDiscoveredSeed(ILogger logger, string serviceName, string endpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "Discovered {Count} seed(s) for cluster '{ServicePrefix}'")]
    private static partial void LogTotalSeedsDiscovered(ILogger logger, int count, string servicePrefix);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to discover endpoints for service '{ServiceName}'")]
    private static partial void LogServiceDiscoveryError(ILogger logger, string serviceName, Exception ex);
}
