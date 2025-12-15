var builder = DistributedApplication.CreateBuilder(args);

// Create a 5-node RapidCluster cluster using replicas
// Each replica will discover other replicas through the connection string we provide
var cluster = builder.AddProject<Projects.RapidCluster_Aspire_Node>("cluster")
    .WithReplicas(5)
    .WithEnvironment("CLUSTER_SIZE", "5")
    .WithHttpHealthCheck("/health");

// Each replica gets a reference to the cluster service, which provides
// the connection string for service discovery
cluster.WithReference(cluster);

builder.Build().Run();
