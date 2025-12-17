var builder = DistributedApplication.CreateBuilder(args);

const int clusterSize = 5;

// Create a unique bootstrap file path for this test run
// All nodes will use this file to coordinate bootstrap
var bootstrapFilePath = Path.Combine(Path.GetTempPath(), $"rapidcluster-e2e-{Guid.NewGuid():N}.json");

// Create individual cluster node resources instead of replicas
// This allows each node to have references to all other nodes for peer discovery
//
// We use HTTPS endpoints which enables TLS. With TLS, Kestrel can use ALPN negotiation
// to support both HTTP/1.1 (for health checks) and HTTP/2 (for gRPC) on the same port.
// This is the recommended approach for gRPC services in ASP.NET Core.
var nodes = new IResourceBuilder<ProjectResource>[clusterSize];
for (var i = 0; i < clusterSize; i++)
{
    nodes[i] = builder.AddProject<Projects.RapidCluster_EndToEnd_Node>($"cluster-{i}")
        .WithEnvironment("CLUSTER_SIZE", clusterSize.ToString())
        .WithEnvironment("CLUSTER_NODE_INDEX", i.ToString())
        .WithEnvironment("CLUSTER_BOOTSTRAP_FILE", bootstrapFilePath)
        .WithHttpHealthCheck("/health", endpointName: "https");
}

// Wire up each node to reference all nodes (including itself) for service discovery
foreach (var node in nodes)
{
    foreach (var peer in nodes)
    {
        node.WithReference(peer);
    }
}

builder.Build().Run();
