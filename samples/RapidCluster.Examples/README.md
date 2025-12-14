# RapidCluster.Examples

A standalone example application demonstrating RapidCluster cluster formation, membership tracking, and bootstrapping options.

## Building

```bash
dotnet build
```

## Usage

```bash
dotnet run -- --listen <address> --seed <seed-address> [options]
```

### Required Options

| Option | Description |
|--------|-------------|
| `--listen <address>` | The address this node listens on (e.g., `127.0.0.1:5000`) |
| `--seed <address>` | Seed node address(es) for bootstrap. Can be specified multiple times for failover. |

### Optional Options

| Option | Default | Description |
|--------|---------|-------------|
| `--bootstrap-expect <n>` | `0` | Number of nodes expected to form the initial cluster. When set > 0, seed nodes wait for this many nodes before starting consensus. Set to `0` to disable waiting (cluster starts immediately). |
| `--bootstrap-timeout <seconds>` | `300` | Timeout in seconds for waiting for `--bootstrap-expect` nodes to join. |

## Examples

### Start a Single-Node Cluster

Start the first node as both the listener and seed (creates a new cluster):

```bash
dotnet run -- --listen 127.0.0.1:5000 --seed 127.0.0.1:5000
```

### Join an Existing Cluster

Start a second node that joins the existing cluster:

```bash
dotnet run -- --listen 127.0.0.1:5001 --seed 127.0.0.1:5000
```

### Join with Multiple Seeds (Failover)

Specify multiple seeds for redundancy. The node will try each seed in order until one succeeds:

```bash
dotnet run -- --listen 127.0.0.1:5002 --seed 127.0.0.1:5000 --seed 127.0.0.1:5001
```

### Bootstrap a Multi-Node Cluster

Start a 3-node cluster where all seeds wait for 3 nodes before forming the cluster. This ensures the cluster forms atomically with the expected size.

Terminal 1:
```bash
dotnet run -- --listen 127.0.0.1:5000 --seed 127.0.0.1:5000 --bootstrap-expect 3
```

Terminal 2:
```bash
dotnet run -- --listen 127.0.0.1:5001 --seed 127.0.0.1:5000 --bootstrap-expect 3
```

Terminal 3:
```bash
dotnet run -- --listen 127.0.0.1:5002 --seed 127.0.0.1:5000 --bootstrap-expect 3
```

The cluster will form once all 3 nodes have contacted the seed.

### Bootstrap with Custom Timeout

Set a shorter timeout (60 seconds) for bootstrap:

```bash
dotnet run -- --listen 127.0.0.1:5000 --seed 127.0.0.1:5000 --bootstrap-expect 3 --bootstrap-timeout 60
```

## Output

The application logs cluster events to the console:

- **Starting**: Node is starting up
- **Cluster started**: This node created a new cluster (seed-only mode)
- **Cluster joined**: This node joined an existing cluster
- **Bootstrap expect**: Waiting for N nodes to form the cluster
- **Proposal detected**: A membership change has been proposed
- **View change**: The cluster membership has changed
- **Kicked**: This node was removed from the cluster

## Graceful Shutdown

Press `Ctrl+C` to gracefully leave the cluster. The node will notify other members before shutting down.
