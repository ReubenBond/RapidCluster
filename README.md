# RapidCluster

[![Build](https://github.com/ReubenBond/RapidCluster/actions/workflows/ci.yml/badge.svg)](https://github.com/ReubenBond/RapidCluster/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/RapidCluster.svg)](https://www.nuget.org/packages/RapidCluster)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What is RapidCluster?

RapidCluster is a distributed membership service for .NET. It allows a set of processes to easily form clusters and receive notifications when the membership changes.

RapidCluster is a .NET port of the [Rapid](https://github.com/lalithsuresh/rapid) protocol by Lalith Suresh, designed for cloud-native distributed systems that require fast, scalable, and stable failure detection and membership tracking.

## Key Features

- **Expander-based monitoring** edge overlay for scalable failure detection
- **Multi-process cut detection** for stability in diverse failure scenarios
- **Practical consensus** using a leaderless Fast Paxos protocol
- **Pluggable failure detectors** via `IEdgeFailureDetectorFactory`
- **Pluggable messaging** via `IMessagingClient` and `IMessagingServer`
- **gRPC-based** communication with Protocol Buffers
- **Native ASP.NET Core integration** with dependency injection and hosted services
- **Reactive event streams** via `IAsyncEnumerable` and `IObservable`

## Requirements

- .NET 10.0 or later

## Installation

```bash
dotnet add package RapidCluster
```

## Quick Start

### 1. Configure Services

Add RapidCluster to your ASP.NET Core application:

```csharp
var builder = WebApplication.CreateBuilder(args);

// Configure Kestrel for gRPC
builder.ConfigureRapidClusterKestrel(port: 5000);

// Add RapidCluster services
builder.Services.AddRapidCluster(options =>
{
    options.ListenAddress = RapidClusterUtils.HostFromString("127.0.0.1:5000");
    options.SeedAddresses = [RapidClusterUtils.HostFromString("127.0.0.1:5000")]; // Same as listen for first node
});

var app = builder.Build();

// Map RapidCluster gRPC endpoints
app.MapRapidClusterMembershipService();

await app.RunAsync();
```

### 2. Access the Cluster

Inject `IRapidCluster` to interact with the membership:

```csharp
public class MyService(IRapidCluster cluster)
{
    public void PrintMembers()
    {
        var members = cluster.GetMemberlist();
        Console.WriteLine($"Cluster has {members.Count} members");
    }
}
```

### 3. Subscribe to Events

RapidCluster provides two ways to subscribe to cluster events:

**Using IAsyncEnumerable:**

```csharp
await foreach (var notification in cluster.EventStream)
{
    switch (notification.Event)
    {
        case ClusterEvents.ViewChange:
            Console.WriteLine($"View changed: {notification.Change.Membership.Count} members");
            break;
        case ClusterEvents.ViewChangeProposal:
            Console.WriteLine($"Proposal detected: {notification.Change}");
            break;
        case ClusterEvents.Kicked:
            Console.WriteLine("This node was kicked from the cluster");
            break;
    }
}
```

**Using IObservable (Rx.NET compatible):**

```csharp
cluster.Events.Subscribe(notification =>
{
    Console.WriteLine($"Event: {notification.Event}");
});
```

### 4. Graceful Shutdown

When shutting down, leave the cluster gracefully:

```csharp
await cluster.LeaveGracefullyAsync();
```

## Configuration

### RapidClusterOptions

| Property | Type | Description |
|----------|------|-------------|
| `ListenAddress` | `Endpoint` | The endpoint this node listens on |
| `SeedAddresses` | `List<Endpoint>?` | The seed nodes to join. If null/empty or contains only `ListenAddress`, starts a new cluster. Nodes are tried in round-robin order until join succeeds. |
| `Metadata` | `Metadata` | Optional metadata for this node |

### RapidClusterProtocolOptions

Protocol-level configuration for tuning failure detection and consensus behavior.

## Architecture

RapidCluster implements the Rapid protocol which provides:

1. **Scalable Failure Detection**: Uses an expander-based monitoring overlay where each node monitors a small subset of other nodes, achieving O(log N) monitoring overhead.

2. **Multi-Process Cut Detection**: Aggregates failure reports from multiple observers before declaring a node as failed, providing stability against transient network issues.

3. **Fast Paxos Consensus**: Uses a leaderless consensus protocol for agreeing on membership changes, optimized for the common case where all nodes observe the same failures.

## Building from Source

```bash
# Clone the repository
git clone https://github.com/ReubenBond/RapidCluster.git
cd RapidCluster

# Build
dotnet build RapidCluster.slnx

# Run tests
cd tests/RapidCluster.Tests
dotnet run -- --timeout 60s
```

## Running Tests

This project uses xUnit v3 with Microsoft Testing Platform:

```bash
cd tests/RapidCluster.Tests

# Run all tests
dotnet run -- --timeout 60s

# Run specific test class
dotnet run -- --timeout 60s --filter-class "*ClusterBasicTests"

# Run specific test method
dotnet run -- --timeout 60s --filter-method "*TestMethodName"

# List all tests
dotnet run -- --list-tests
```

## Examples

See the [samples/RapidCluster.Examples](samples/RapidCluster.Examples) directory for a complete example application.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Based on the [Rapid](https://github.com/lalithsuresh/rapid) protocol by Lalith Suresh
- Paper: [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/conference/atc18/presentation/suresh)
