# RapidCluster .NET Aspire Sample

This sample demonstrates running a RapidCluster membership cluster using .NET Aspire for orchestration and observability.

## Overview

The sample creates a 5-node cluster where all nodes are seeds. Each replica discovers the others using Aspire's service discovery mechanism. The cluster uses the bootstrap-expect pattern to wait for all 5 nodes before forming.

## Projects

- **RapidCluster.Aspire.AppHost** - Aspire orchestration host that starts 5 replicas
- **RapidCluster.Aspire.Node** - The cluster node application with gRPC transport
- **RapidCluster.Aspire.ServiceDefaults** - Shared configuration for OpenTelemetry and health checks

## Running the Sample

### Prerequisites

- .NET 10 SDK
- .NET Aspire workload: `dotnet workload install aspire`

### Start the Cluster

```bash
cd samples/RapidCluster.Aspire/RapidCluster.Aspire.AppHost
dotnet run
```

This opens the Aspire dashboard (typically at https://localhost:17225) where you can:

- View all 5 cluster node replicas
- Monitor logs from each node
- See distributed traces for cluster operations
- Check health status of each node

## Architecture

### Service Discovery

The `ServiceDiscoverySeedProvider` reads endpoint information from Aspire's configuration system. When Aspire starts replicas, it injects service endpoint configuration that allows nodes to discover each other.

### Health Checks

The `ClusterMembershipHealthCheck` reports:
- **Healthy**: Node is part of a cluster with members
- **Degraded**: Cluster view not yet available (node is joining)
- **Unhealthy**: Node has no cluster membership

### Observability

The sample includes full OpenTelemetry integration:
- **Metrics**: RapidCluster exposes cluster membership metrics
- **Traces**: Distributed tracing for cluster operations
- **Logs**: Structured logging with correlation IDs

## Configuration

Environment variables for customization:

| Variable | Default | Description |
|----------|---------|-------------|
| `GRPC_PORT` | 5000 | Port for gRPC cluster communication |
| `HEALTH_PORT` | 8080 | Port for HTTP health endpoints |
| `CLUSTER_SIZE` | 5 | Expected number of nodes (bootstrap-expect) |
