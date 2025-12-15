var builder = DistributedApplication.CreateBuilder(args);

// Create a 5-node RapidCluster cluster using replicas
// Each replica will discover other replicas through Aspire's service discovery
var cluster = builder.AddProject<Projects.RapidCluster_Aspire_Node>("cluster")
    .WithReplicas(5)
    .WithEnvironment("CLUSTER_SIZE", "5")
    .WithHttpHealthCheck("/health");

builder.Build().Run();
