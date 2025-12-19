using System.Diagnostics.CodeAnalysis;
using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Options;
using RapidCluster;
using RapidCluster.Aspire.Node;
using RapidCluster.Discovery;
using RapidCluster.Grpc;

var builder = WebApplication.CreateBuilder(args);

// Add service defaults (OpenTelemetry, health checks, service discovery)
builder.AddServiceDefaults();

// With HTTPS (TLS), Kestrel uses ALPN negotiation to support both HTTP/1.1 and HTTP/2
// on the same port. This is configured via appsettings.json: Kestrel:EndpointDefaults:Protocols = "Http1AndHttp2"
// - Health checks use HTTP/1.1
// - gRPC uses HTTP/2
// Both work on the same HTTPS endpoint without any special configuration.

// Read cluster configuration from environment variables set by AppHost
var clusterSize = builder.Configuration.GetValue<int>("CLUSTER_SIZE", 1);
var nodeIndex = builder.Configuration.GetValue<int>("CLUSTER_NODE_INDEX", 0);
var bootstrapFilePath = builder.Configuration["CLUSTER_BOOTSTRAP_FILE"]
    ?? Path.Combine(Path.GetTempPath(), "rapidcluster-aspire-bootstrap.json");

// Register the file-based bootstrap provider for coordinated startup
// Node 0 will start a new cluster, others will wait for node 0 and join it
builder.Services.AddSingleton<FileBasedBootstrapProvider>(sp =>
    new FileBasedBootstrapProvider(
        bootstrapFilePath,
        nodeIndex,
        clusterSize,
        sp.GetRequiredService<ILogger<FileBasedBootstrapProvider>>()));
builder.Services.AddSingleton<ISeedProvider>(sp => sp.GetRequiredService<FileBasedBootstrapProvider>());

// Register our deferred options configurator that will set ListenAddress after the server starts
builder.Services.AddSingleton<DeferredRapidClusterOptionsConfigurator>();
builder.Services.AddSingleton<IConfigureOptions<RapidClusterOptions>>(sp =>
    sp.GetRequiredService<DeferredRapidClusterOptionsConfigurator>());

// Add RapidCluster services with manual lifecycle management
// We use AddRapidClusterManual because the listen address is only known after the server starts
builder.Services.AddRapidClusterManual(options =>
{
    // ListenAddress will be set by DeferredRapidClusterOptionsConfigurator after server starts
    // Disable BootstrapExpect since we're using file-based coordination instead
    options.BootstrapExpect = 0;
});

// Add gRPC transport with HTTPS enabled (required for Http1AndHttp2 with TLS)
builder.Services.AddRapidClusterGrpc(options =>
{
    options.UseHttps = true;
});

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
var bootstrapProvider = app.Services.GetRequiredService<FileBasedBootstrapProvider>();
var server = app.Services.GetRequiredService<IServer>();

// We need to capture the cluster lifecycle in a variable that can be shared between callbacks
// but we can't resolve it until after the address is configured
IRapidClusterLifecycle? clusterLifecycle = null;

lifetime.ApplicationStarted.Register(() =>
{
    // Configure the listen address from the server's actual bound HTTPS address
    var listenAddress = configurator.ConfigureFromServer(server);

    // Set the address on the bootstrap provider so it can register in the shared file
    bootstrapProvider.SetMyAddress(listenAddress);

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
                LogViewChange(_logger, view.ConfigurationId.Version, view.Members.Length);
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
