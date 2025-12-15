using System.Diagnostics.CodeAnalysis;
using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Options;
using RapidCluster;
using RapidCluster.Aspire.Node;
using RapidCluster.Discovery;
using RapidCluster.Grpc;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults (OpenTelemetry, health checks, service discovery)
builder.AddServiceDefaults();

// Configure Kestrel to use HTTP/2 for gRPC support
// We use Http2 protocol to enable gRPC over plain HTTP (H2C - HTTP/2 Cleartext)
// This is necessary because gRPC requires HTTP/2, and without TLS we need to explicitly enable it
builder.WebHost.ConfigureKestrel(options =>
{
    options.ConfigureEndpointDefaults(listenOptions =>
    {
        // Use HTTP/2 only - this enables gRPC over plain HTTP (H2C)
        // Health checks and other HTTP/1.1 clients may not work on this endpoint
        listenOptions.Protocols = HttpProtocols.Http2;
    });
});

// Register the service discovery seed provider
builder.Services.AddSingleton<ISeedProvider>(sp =>
    new ServiceDiscoverySeedProvider(
        sp.GetRequiredService<IConfiguration>(),
        sp.GetRequiredService<ILogger<ServiceDiscoverySeedProvider>>(),
        "cluster"));

// Register our deferred options configurator that will set ListenAddress after the server starts
builder.Services.AddSingleton<DeferredRapidClusterOptionsConfigurator>();
builder.Services.AddSingleton<IConfigureOptions<RapidClusterOptions>>(sp =>
    sp.GetRequiredService<DeferredRapidClusterOptionsConfigurator>());

// Add RapidCluster services with manual lifecycle management
// We use AddRapidClusterManual because the listen address is only known after the server starts
builder.Services.AddRapidClusterManual(options =>
{
    // ListenAddress will be set by DeferredRapidClusterOptionsConfigurator after server starts
    // BootstrapExpect=1 allows single node to bootstrap; others join via seed discovery
    // Note: In Aspire with replicas, each node discovers itself as the seed, so each forms its own cluster
    // For multi-node clusters, you'd need external service discovery (e.g., Redis, DNS, shared database)
    options.BootstrapExpect = 1;
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

// Set up manual lifecycle management for RapidCluster
// We start the cluster after the server is ready (ApplicationStarted)
// and stop it when the application is stopping
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
var configurator = app.Services.GetRequiredService<DeferredRapidClusterOptionsConfigurator>();
var server = app.Services.GetRequiredService<IServer>();
var configuration = app.Services.GetRequiredService<IConfiguration>();

// We need to capture the cluster lifecycle in a variable that can be shared between callbacks
// but we can't resolve it until after the address is configured
IRapidClusterLifecycle? clusterLifecycle = null;

lifetime.ApplicationStarted.Register(() =>
{
    // Configure the listen address from the server's actual bound address
    configurator.ConfigureFromServer(server, configuration);

    // Now it's safe to resolve the cluster lifecycle (options are configured)
    clusterLifecycle = app.Services.GetRequiredService<IRapidClusterLifecycle>();

    // Start the cluster
    _ = Task.Run(async () =>
    {
        try
        {
            await clusterLifecycle.StartAsync(lifetime.ApplicationStopping);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
    });
});

lifetime.ApplicationStopping.Register(() =>
{
    // Stop the cluster gracefully
    clusterLifecycle?.StopAsync(CancellationToken.None).GetAwaiter().GetResult();
});

app.Run();

/// <summary>
/// Configures RapidClusterOptions.ListenAddress after the server has started.
/// This is necessary because Aspire assigns dynamic ports via ASPNETCORE_URLS,
/// and the actual bound address is only available after the server starts.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed class DeferredRapidClusterOptionsConfigurator : IConfigureOptions<RapidClusterOptions>, IDisposable
{
    private EndPoint? _listenAddress;
    private readonly object _lock = new();
    private readonly ManualResetEventSlim _addressConfigured = new(false);

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
    public void WaitForConfiguration(CancellationToken cancellationToken)
    {
        _addressConfigured.Wait(cancellationToken);
    }

    public void ConfigureFromServer(IServer server, IConfiguration configuration)
    {
        var addressFeature = server.Features.Get<IServerAddressesFeature>();
        if (addressFeature == null || addressFeature.Addresses.Count == 0)
        {
            throw new InvalidOperationException(
                "Server addresses are not available. Ensure the server has started.");
        }

        // Prefer the HTTP address for gRPC (HTTP/2 Cleartext)
        // HTTPS requires TLS ALPN negotiation which adds complexity
        var address = addressFeature.Addresses
            .FirstOrDefault(a => a.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
            ?? addressFeature.Addresses.First();

        var uri = new Uri(address);

        // Get the hostname to advertise
        var hostname = configuration["HOSTNAME"]
            ?? Environment.GetEnvironmentVariable("HOSTNAME")
            ?? Environment.MachineName;

        lock (_lock)
        {
            _listenAddress = new DnsEndPoint(hostname, uri.Port);
        }

        _addressConfigured.Set();
    }

    public void Dispose()
    {
        _addressConfigured.Dispose();
    }
}

/// <summary>
/// Background service that logs cluster status changes.
/// Uses IServiceProvider to lazily resolve IRapidCluster after the server has started.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed partial class ClusterStatusLogger : BackgroundService
{
    private readonly IServiceProvider _serviceProvider;
    private readonly DeferredRapidClusterOptionsConfigurator _configurator;
    private readonly ILogger<ClusterStatusLogger> _logger;

    public ClusterStatusLogger(
        IServiceProvider serviceProvider,
        DeferredRapidClusterOptionsConfigurator configurator,
        ILogger<ClusterStatusLogger> logger)
    {
        _serviceProvider = serviceProvider;
        _configurator = configurator;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Wait for the address to be configured before accessing the cluster
            _configurator.WaitForConfiguration(stoppingToken);

            var cluster = _serviceProvider.GetRequiredService<IRapidCluster>();
            await foreach (var view in cluster.ViewUpdates.WithCancellation(stoppingToken))
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
