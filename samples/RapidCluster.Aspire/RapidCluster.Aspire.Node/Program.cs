using System.Diagnostics.CodeAnalysis;
using System.Net;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using RapidCluster;
using RapidCluster.Aspire.Node;
using RapidCluster.Discovery;
using RapidCluster.Grpc;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults (OpenTelemetry, health checks, service discovery)
builder.AddServiceDefaults();

// Get the gRPC endpoint configuration from Aspire
// Aspire sets ASPNETCORE_URLS for HTTP endpoints, but for gRPC we need HTTP/2
var grpcPort = builder.Configuration.GetValue<int>("GRPC_PORT", 5000);

// Also check for Aspire-injected port via environment or configuration
var aspireGrpcPort = builder.Configuration.GetValue<int?>("services__cluster__grpc__0");
if (aspireGrpcPort.HasValue)
{
    grpcPort = aspireGrpcPort.Value;
}

// Configure Kestrel for HTTP/2 (required for gRPC)
builder.WebHost.ConfigureKestrel(options =>
{
    // Listen for gRPC on the configured port
    options.ListenAnyIP(grpcPort, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http2;
    });

    // Also listen on a separate port for HTTP/1.1 health checks if needed
    var healthPort = builder.Configuration.GetValue<int>("HEALTH_PORT", 8080);
    options.ListenAnyIP(healthPort, listenOptions =>
    {
        listenOptions.Protocols = HttpProtocols.Http1AndHttp2;
    });
});

// Get the hostname for this replica
var hostname = builder.Configuration["HOSTNAME"] ?? Environment.MachineName;
var listenEndpoint = new DnsEndPoint(hostname, grpcPort);

// Register the service discovery seed provider
builder.Services.AddSingleton<ISeedProvider>(sp =>
    new ServiceDiscoverySeedProvider(
        sp.GetRequiredService<IConfiguration>(),
        sp.GetRequiredService<ILogger<ServiceDiscoverySeedProvider>>(),
        "cluster"));

// Add RapidCluster services
builder.Services.AddRapidCluster(options =>
{
    options.ListenAddress = listenEndpoint;
    // BootstrapExpect = 5 means seeds wait for 5 nodes before forming cluster
    options.BootstrapExpect = builder.Configuration.GetValue<int>("CLUSTER_SIZE", 5);
    options.BootstrapTimeout = TimeSpan.FromMinutes(2);
});

// Add gRPC transport
builder.Services.AddRapidClusterGrpc();

// Add cluster membership health check
builder.Services.AddHealthChecks()
    .AddCheck<ClusterMembershipHealthCheck>(
        "cluster-membership",
        tags: ["ready"]);

// Add background service to log cluster status
builder.Services.AddHostedService<ClusterStatusLogger>();

var app = builder.Build();

// Map health check endpoints
app.MapDefaultEndpoints();

// Map RapidCluster gRPC service
app.MapRapidClusterMembershipService();

app.Run();

/// <summary>
/// Background service that logs cluster status changes.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed partial class ClusterStatusLogger : BackgroundService
{
    private readonly IRapidCluster _cluster;
    private readonly ILogger<ClusterStatusLogger> _logger;

    public ClusterStatusLogger(IRapidCluster cluster, ILogger<ClusterStatusLogger> logger)
    {
        _cluster = cluster;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var view in _cluster.ViewUpdates.WithCancellation(stoppingToken))
            {
                LogViewChange(_logger, view.ConfigurationId, view.Members.Length);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster view changed: ConfigId={ConfigId}, Members={MemberCount}")]
    private static partial void LogViewChange(ILogger logger, long configId, int memberCount);
}
