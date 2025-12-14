# RapidCluster

[![Build](https://github.com/ReubenBond/RapidCluster/actions/workflows/ci.yml/badge.svg)](https://github.com/ReubenBond/RapidCluster/actions/workflows/ci.yml)
[![NuGet](https://img.shields.io/nuget/v/RapidCluster.svg)](https://www.nuget.org/packages/RapidCluster)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What is RapidCluster?

RapidCluster is a distributed membership service for .NET that enables processes to form clusters and receive real-time notifications when membership changes. It provides fast, stable, and scalable failure detection with consistent membership views across all nodes.

RapidCluster is a .NET implementation of the [Rapid](https://github.com/lalithsuresh/rapid) protocol, which was designed for cloud-native distributed systems. The protocol was published in the paper [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/conference/atc18/presentation/suresh) at USENIX ATC '18.

## Key Features

- **Fast Failure Detection** - Detects node failures in seconds using ping/pong probes with configurable thresholds
- **Stable Membership Views** - Multi-process cut detection prevents flapping and ensures all nodes agree on membership changes
- **Scalable Architecture** - K-ring monitoring overlay provides O(log N) monitoring overhead per node
- **Leaderless Consensus** - Fast Paxos protocol achieves consensus without a designated leader, with automatic fallback to Classic Paxos
- **Automatic Rejoin** - Nodes automatically rejoin the cluster after being kicked due to transient failures
- **Graceful Shutdown** - Nodes can leave the cluster gracefully, notifying other members before departing
- **Pluggable Failure Detectors** - Customize failure detection via `IEdgeFailureDetectorFactory`
- **Pluggable Seed Discovery** - Implement `ISeedProvider` for DNS, cloud APIs, or service discovery integration
- **Native ASP.NET Core Integration** - First-class support for dependency injection and hosted services
- **Reactive Event Streams** - Subscribe to cluster events via `IAsyncEnumerable` or `IObservable` (Rx.NET compatible)
- **gRPC Communication** - Efficient Protocol Buffers-based messaging over HTTP/2
- **Built-in Metrics** - OpenTelemetry-compatible metrics for monitoring cluster health

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
    options.SeedAddresses = [RapidClusterUtils.HostFromString("127.0.0.1:5000")];
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
        Console.WriteLine($"Cluster has {members.Count} members:");
        foreach (var member in members)
        {
            Console.WriteLine($"  - {member.Hostname}:{member.Port}");
        }
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
            foreach (var change in notification.Change.NodeChanges)
            {
                var action = change.Status == EdgeStatus.Up ? "joined" : "left";
                Console.WriteLine($"  Node {change.Endpoint.Hostname}:{change.Endpoint.Port} {action}");
            }
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

## How It Works

RapidCluster implements the Rapid protocol, which combines three key mechanisms:

### 1. K-Ring Monitoring Topology

Instead of each node monitoring all other nodes (O(N²) overhead), RapidCluster uses an expander-based monitoring overlay:

- Nodes are arranged in K virtual rings (default K=10)
- Each node monitors K other nodes (its "subjects") and is monitored by K nodes (its "observers")
- This provides O(K) = O(log N) monitoring overhead per node
- The ring structure ensures every node is monitored with high probability even under failures

### 2. Multi-Process Cut Detection

Before declaring a node as failed, RapidCluster aggregates reports from multiple observers:

- **Low Watermark (L)**: A node enters "unstable" mode when L observers report it as failed
- **High Watermark (H)**: A node is confirmed failed when H observers report it as failed
- The system waits for all unstable nodes to become stable before proposing a view change
- This prevents flapping from transient network issues and ensures almost-everywhere agreement

### 3. Fast Paxos Consensus

Membership changes are agreed upon using a leaderless consensus protocol:

- **Fast Round**: Nodes broadcast their proposals directly; if all nodes propose the same change, consensus completes in one round-trip
- **Classic Paxos Fallback**: If proposals conflict, the protocol falls back to Classic Paxos with a coordinator
- This is optimized for the common case where all nodes observe the same failures

## Configuration

### RapidClusterOptions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ListenAddress` | `Endpoint` | (required) | The endpoint this node listens on |
| `SeedAddresses` | `List<Endpoint>?` | `null` | Seed nodes to contact for joining. If empty or only contains self, starts a new cluster |
| `Metadata` | `Metadata` | `new()` | Optional key-value metadata for this node |
| `BootstrapExpect` | `int` | `0` | Number of nodes to wait for before forming initial cluster. Set to 0 to start immediately |
| `BootstrapTimeout` | `TimeSpan` | `5 min` | Timeout for waiting for bootstrap nodes |

### RapidClusterProtocolOptions

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `ObserversPerSubject` | `int` | `10` | Number of observers per node (K). Higher = more reliable detection |
| `HighWatermark` | `int` | `9` | Reports needed to confirm failure (H). Must be < K |
| `LowWatermark` | `int` | `3` | Reports to enter unstable mode (L). Must be <= H |
| `FailureDetectorInterval` | `TimeSpan` | `1s` | Interval between failure detector probes |
| `FailureDetectorConsecutiveFailures` | `int` | `3` | Consecutive probe failures before declaring node down |
| `GrpcTimeout` | `TimeSpan` | `10s` | gRPC request timeout |
| `GrpcProbeTimeout` | `TimeSpan` | `500ms` | Timeout for probe messages |
| `BatchingWindow` | `TimeSpan` | `100ms` | Window for batching alerts before broadcasting |
| `MaxJoinRetries` | `int` | `∞` | Maximum join retry attempts |
| `JoinRetryBaseDelay` | `TimeSpan` | `100ms` | Initial delay between join retries |
| `JoinRetryMaxDelay` | `TimeSpan` | `5s` | Maximum delay between join retries |
| `RejoinBackoffBase` | `TimeSpan` | `1s` | Base delay before rejoining after being kicked |
| `RejoinBackoffMax` | `TimeSpan` | `30s` | Maximum delay before rejoining |

## Seed Discovery

RapidCluster supports pluggable seed discovery through the `ISeedProvider` interface:

### Static Seeds (Default)

Seeds are specified directly in `RapidClusterOptions.SeedAddresses`:

```csharp
builder.Services.AddRapidCluster(options =>
{
    options.ListenAddress = RapidClusterUtils.HostFromString("127.0.0.1:5000");
    options.SeedAddresses = [
        RapidClusterUtils.HostFromString("seed1.example.com:5000"),
        RapidClusterUtils.HostFromString("seed2.example.com:5000"),
    ];
});
```

### Configuration-Based Seeds

Read seeds from `appsettings.json` or other configuration sources:

```csharp
// Register configuration provider before AddRapidCluster
builder.Services.AddRapidClusterConfigurationSeeds("RapidCluster:Seeds");

builder.Services.AddRapidCluster(options =>
{
    options.ListenAddress = RapidClusterUtils.HostFromString("127.0.0.1:5000");
});
```

```json
{
  "RapidCluster": {
    "Seeds": ["seed1.example.com:5000", "seed2.example.com:5000"]
  }
}
```

### Custom Seed Provider

Implement `ISeedProvider` for dynamic discovery (DNS, cloud APIs, etc.):

```csharp
public class DnsSeedProvider : ISeedProvider
{
    public async ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken)
    {
        var hosts = await Dns.GetHostAddressesAsync("seeds.example.com", cancellationToken);
        return hosts.Select(h => new Endpoint { Hostname = h.ToString(), Port = 5000 }).ToList();
    }
}

// Register before AddRapidCluster
builder.Services.AddRapidClusterSeedProvider<DnsSeedProvider>();
```

## Running the Samples

The `samples/RapidCluster.Examples` directory contains a complete example application.

### Build the Sample

```bash
cd samples/RapidCluster.Examples
dotnet build
```

### Start a Single-Node Cluster

Start the first node as both listener and seed:

```bash
dotnet run -- --listen 127.0.0.1:5000 --seed 127.0.0.1:5000
```

### Join Additional Nodes

Start additional nodes pointing to the first node as seed:

```bash
# Terminal 2
dotnet run -- --listen 127.0.0.1:5001 --seed 127.0.0.1:5000

# Terminal 3
dotnet run -- --listen 127.0.0.1:5002 --seed 127.0.0.1:5000
```

### Multiple Seeds for Redundancy

Specify multiple seeds for failover. The node tries each seed in order:

```bash
dotnet run -- --listen 127.0.0.1:5003 --seed 127.0.0.1:5000 --seed 127.0.0.1:5001
```

### Bootstrap a Multi-Node Cluster

Wait for a specific number of nodes before forming the cluster:

```bash
# Terminal 1 - Seed waits for 3 nodes
dotnet run -- --listen 127.0.0.1:5000 --seed 127.0.0.1:5000 --bootstrap-expect 3

# Terminal 2
dotnet run -- --listen 127.0.0.1:5001 --seed 127.0.0.1:5000 --bootstrap-expect 3

# Terminal 3 - Cluster forms when this node joins
dotnet run -- --listen 127.0.0.1:5002 --seed 127.0.0.1:5000 --bootstrap-expect 3
```

### Sample Options

| Option | Default | Description |
|--------|---------|-------------|
| `--listen <address>` | (required) | Address this node listens on |
| `--seed <address>` | (required) | Seed node address (can be specified multiple times) |
| `--bootstrap-expect <n>` | `0` | Wait for N nodes before forming cluster |
| `--bootstrap-timeout <s>` | `300` | Timeout in seconds for bootstrap |

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

## Testing

RapidCluster uses a comprehensive testing strategy combining four complementary approaches to ensure correctness of the distributed protocol implementation.

### Test Types

#### Unit Tests

Located in `tests/RapidCluster.Tests/Unit/`, these tests verify individual components in isolation using mock dependencies. They're fast, deterministic, and test specific logic paths.

```csharp
[Fact]
public void AggregateForProposal_ReturnsProposal_WhenHighWatermarkReached()
{
    var detector = new MultiNodeCutDetector(highWatermark: 3, lowWatermark: 2, membershipView);
    // ... test cut detection logic in isolation
}
```

#### Integration Tests

Located in `tests/RapidCluster.Tests/Integration/`, these tests run real cluster nodes with actual gRPC networking. They verify end-to-end behavior but are slower and require port allocation.

```csharp
[Fact]
public async Task ThreeNodesFormCluster()
{
    var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress);
    var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress);
    await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10));
}
```

#### Property-Based Tests

Using [CsCheck](https://github.com/AnthonyLloyd/CsCheck), these tests verify that invariants hold across many randomly generated inputs. They're excellent for finding edge cases in algorithms.

```csharp
[Fact]
public void Property_CompareTo_Is_Transitive()
{
    Gen.Select(Gen.Int[0, 100], Gen.Int[0, 100], Gen.Int[0, 100])
        .Sample((r1, r2, r3) =>
        {
            var rank1 = new Rank { Round = r1 };
            var rank2 = new Rank { Round = r2 };
            var rank3 = new Rank { Round = r3 };
            // Verify transitivity: if a <= b and b <= c, then a <= c
            if (rank1.CompareTo(rank2) <= 0 && rank2.CompareTo(rank3) <= 0)
                return rank1.CompareTo(rank3) <= 0;
            return true;
        });
}
```

#### Deterministic Simulation Tests

Located in `tests/RapidCluster.Tests/Simulation/`, these tests use the **Clockwork** framework (`src/Clockwork/`) to control all sources of non-determinism (time, randomness, network). This enables:

- **Reproducible failures**: Same seed produces identical execution
- **Fast execution**: No real delays; simulated time advances instantly
- **Chaos testing**: Inject network partitions, message drops, and node crashes
- **Invariant checking**: Verify safety properties hold under all conditions

```csharp
[Fact]
public void ClusterMaintainsConsistencyUnderChaos()
{
    var harness = new RapidSimulationCluster(seed: 12345);
    var nodes = harness.CreateCluster(size: 5);
    
    // Inject chaos: partitions, crashes, message drops
    var chaos = new ChaosInjector(harness);
    chaos.PartitionRate = 0.05;
    chaos.CrashRate = 0.02;
    chaos.RunChaos(steps: 50);
    
    // Verify invariants still hold
    var checker = new InvariantChecker(harness);
    Assert.True(checker.CheckAll());
}
```

Simulation tests are the preferred approach for testing distributed protocol behavior because they can explore edge cases that are difficult to trigger with real networking.

### Running Tests

This project uses xUnit v3 with Microsoft Testing Platform:

```bash
cd tests/RapidCluster.Tests

# Run all tests
dotnet run -- --timeout 60s

# Run specific test class
dotnet run -- --timeout 60s --filter-class "*ClusterBasicTests"

# Run specific test method
dotnet run -- --timeout 60s --filter-method "*ThreeNodeClusterFormation"

# Run only unit tests (fastest)
dotnet run -- --timeout 60s --filter-class "*Tests.Unit*"

# Run only simulation tests
dotnet run -- --timeout 60s --filter-class "*Tests.Simulation*"

# Exclude integration tests
dotnet run -- --timeout 60s --filter-not-class "*Integration*"

# List all tests
dotnet run -- --list-tests

# Run with code coverage
dotnet run -- --coverage --coverage-output-format cobertura --coverage-output coverage.xml --timeout 60s
```

## Cluster Events

RapidCluster publishes the following events:

| Event | Description |
|-------|-------------|
| `ViewChange` | A new cluster membership view has been agreed upon. Contains the new member list and which nodes joined/left. |
| `ViewChangeProposal` | A membership change has been proposed but not yet agreed upon. Useful for monitoring. |
| `Kicked` | This node has been removed from the cluster (usually due to being unreachable). The node will attempt to rejoin. |
| `ViewChangeOneStepFailed` | Fast Paxos could not reach consensus; falling back to Classic Paxos. |

## Metrics

RapidCluster exposes OpenTelemetry-compatible metrics under the `RapidCluster` meter:

| Metric | Type | Description |
|--------|------|-------------|
| `rapidcluster.membership.size` | Gauge | Current number of nodes in the cluster |
| `rapidcluster.membership.view_changes` | Counter | Total membership view changes |
| `rapidcluster.join.latency` | Histogram | Time to join the cluster |
| `rapidcluster.nodes.added` | Counter | Nodes added to the cluster |
| `rapidcluster.nodes.removed` | Counter | Nodes removed from the cluster |
| `rapidcluster.messages.received` | Counter | Messages received by type |
| `rapidcluster.messages.sent` | Counter | Messages sent by type |
| `rapidcluster.consensus.votes.sent` | Counter | Consensus votes sent |
| `rapidcluster.consensus.votes.received` | Counter | Consensus votes received |
| `rapidcluster.cut_detector.reports` | Counter | Cut detector reports received |
| `rapidcluster.cut_detector.cuts` | Counter | Cuts detected |

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Based on the [Rapid](https://github.com/lalithsuresh/rapid) protocol by Lalith Suresh et al.
- Paper: [Stable and Consistent Membership at Scale with Rapid](https://www.usenix.org/conference/atc18/presentation/suresh) (USENIX ATC '18)
