var builder = DistributedApplication.CreateBuilder(args);

const int ClusterSize = 5;

// Create a unique bootstrap file path for this test run
// All nodes will use this file to coordinate bootstrap
var bootstrapFilePath = Path.Combine(Path.GetTempPath(), $"rapidcluster-aspire-{Guid.NewGuid():N}.json");

builder.AddProject<Projects.RapidCluster_Aspire_Node>("cluster")
    .WithEnvironment("CLUSTER_SIZE", ClusterSize.ToString())
    .WithEnvironment("CLUSTER_BOOTSTRAP_FILE", bootstrapFilePath)
    .WithHttpHealthCheck("/health", endpointName: "https")
    .WithReplicas(ClusterSize);

builder.Build().Run();
