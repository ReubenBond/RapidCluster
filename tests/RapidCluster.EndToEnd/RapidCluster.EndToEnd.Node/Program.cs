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

// Use the server-based listen address provider for dynamic port scenarios
// This must be registered BEFORE AddRapidCluster to take precedence
builder.Services.AddRapidClusterServerAddressProvider();

// Register the file-based bootstrap provider for coordinated startup
// Node 0 will start a new cluster, others will wait for node 0 and join it
builder.Services.AddSingleton(sp =>
    new FileBasedBootstrapProvider(
        bootstrapFilePath,
        nodeIndex,
        sp.GetRequiredService<IListenAddressProvider>(),
        sp.GetRequiredService<ILogger<FileBasedBootstrapProvider>>()));
builder.Services.AddSingleton<ISeedProvider>(sp => sp.GetRequiredService<FileBasedBootstrapProvider>());

// Add RapidCluster services with automatic lifecycle management
// ListenAddress will be resolved from the server after it starts
// Disable BootstrapExpect since we're using file-based coordination instead
builder.Services.AddRapidCluster(options => options.BootstrapExpect = 0);

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

app.Run();
