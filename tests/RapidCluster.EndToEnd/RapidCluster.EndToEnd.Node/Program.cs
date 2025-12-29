using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.Extensions.Options;
using RapidCluster;
using RapidCluster.Discovery;
using RapidCluster.EndToEnd.Node;
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
var nodeIndex = builder.Configuration.GetValue("CLUSTER_NODE_INDEX", 0);
var bootstrapFilePath = builder.Configuration["CLUSTER_BOOTSTRAP_FILE"]
    ?? Path.Combine(Path.GetTempPath(), "rapidcluster-e2e-bootstrap.json");

// Register the file-based bootstrap provider for coordinated startup
// Node 0 will start a new cluster, others will wait for node 0 and join it
builder.Services.AddSingleton(sp =>
    new FileBasedBootstrapProvider(
        bootstrapFilePath,
        nodeIndex,
        sp.GetRequiredService<ILogger<FileBasedBootstrapProvider>>()));
builder.Services.AddSingleton<ISeedProvider>(sp => sp.GetRequiredService<FileBasedBootstrapProvider>());

// Register our deferred options configurator that will set ListenAddress after the server starts
builder.Services.AddSingleton<DeferredRapidClusterOptionsConfigurator>();
builder.Services.AddSingleton<IConfigureOptions<RapidClusterOptions>>(sp =>
    sp.GetRequiredService<DeferredRapidClusterOptionsConfigurator>());

// Add RapidCluster services with manual lifecycle management
// We use AddRapidClusterManual because the listen address is only known after the server starts
builder.Services.AddRapidCluster(options =>
    // ListenAddress will be set by DeferredRapidClusterOptionsConfigurator after server starts
    // Disable BootstrapExpect since we're using file-based coordination instead
    options.BootstrapExpect = 0);

// Add gRPC transport with HTTPS enabled (required for Http1AndHttp2 with TLS)
builder.Services.AddRapidClusterGrpc(options => options.UseHttps = true);

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

app.Run();
