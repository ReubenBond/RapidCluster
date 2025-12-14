# 006: Enhance Observability with Metrics

## Problem Description

RapidCluster currently lacks built-in metrics instrumentation. Adding metrics following .NET best practices will enable users to monitor cluster health, diagnose performance issues, and gain visibility into protocol behavior.

## Proposed Solution

Implement metrics instrumentation using `System.Diagnostics.Metrics` APIs following Microsoft's recommended patterns for .NET libraries.

### Key Design Decisions

#### 1. Meter Creation via Dependency Injection

Use `IMeterFactory` for DI-aware meter creation rather than static meters:

```csharp
public class RapidClusterMetrics
{
    private readonly Counter<long> _messagesReceived;
    private readonly Counter<long> _messagesSent;
    private readonly Histogram<double> _consensusLatency;
    // ... more instruments

    public RapidClusterMetrics(IMeterFactory meterFactory)
    {
        var meter = meterFactory.Create("RapidCluster");
        _messagesReceived = meter.CreateCounter<long>("rapidcluster.messages.received");
        // ...
    }
}
```

#### 2. Naming Conventions

Follow OpenTelemetry naming guidelines:
- Meter name: `RapidCluster`
- Instrument names: lowercase dotted hierarchical with underscores between words
- Examples:
  - `rapidcluster.cluster.size`
  - `rapidcluster.messages.received`
  - `rapidcluster.consensus.latency`
  - `rapidcluster.membership.changes`

#### 3. Metrics Categories

Metrics are organized into two categories: **State Metrics** (current values, gauges) and **Event Metrics** (counters for things that happen).

---

### State Metrics (ObservableGauge / UpDownCounter)

These represent the current state of the cluster, node, or system at any point in time.

#### Cluster State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.cluster.size` | ObservableGauge | `{nodes}` | Current number of nodes in the cluster |
| `rapidcluster.cluster.configuration_id` | ObservableGauge | `{id}` | Current configuration ID (monotonically increasing) |
| `rapidcluster.cluster.configuration_epoch` | ObservableGauge | `{epoch}` | Number of configuration changes since cluster start |

#### Node State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.node.state` | ObservableGauge | - | Node state (0=joining, 1=active, 2=leaving, 3=failed) |
| `rapidcluster.node.is_seed` | ObservableGauge | - | Whether this node is a seed node (1 or 0) |
| `rapidcluster.node.uptime` | ObservableGauge | `s` | Time since node joined the cluster |
| `rapidcluster.node.observers_count` | ObservableGauge | `{nodes}` | Number of nodes this node is observing (monitoring) |
| `rapidcluster.node.subjects_count` | ObservableGauge | `{nodes}` | Number of nodes observing (monitoring) this node |

#### Membership View State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.membership.ring_size` | ObservableGauge | `{nodes}` | Current size of the membership ring |
| `rapidcluster.membership.view_hash` | ObservableGauge | - | Hash of current membership view (for consistency checks) |

#### Consensus State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.consensus.pending_proposals` | UpDownCounter | `{proposals}` | Number of proposals currently awaiting consensus |
| `rapidcluster.consensus.current_round` | ObservableGauge | `{round}` | Current consensus round number |
| `rapidcluster.consensus.highest_accepted_round` | ObservableGauge | `{round}` | Highest round number with accepted value |

#### Cut Detector State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.cut_detector.pending_reports` | ObservableGauge | `{reports}` | Number of pending failure reports in cut detector |
| `rapidcluster.cut_detector.proposal_count` | ObservableGauge | `{proposals}` | Number of proposals in current aggregation window |
| `rapidcluster.cut_detector.unstable_nodes` | ObservableGauge | `{nodes}` | Number of nodes with partial failure reports |

#### Failure Detection State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.failure_detection.active_monitors` | ObservableGauge | `{monitors}` | Number of active failure detection monitors |
| `rapidcluster.failure_detection.suspected_nodes` | ObservableGauge | `{nodes}` | Number of currently suspected nodes |

#### Network/Messaging State

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.network.active_connections` | UpDownCounter | `{connections}` | Number of active gRPC connections |
| `rapidcluster.network.pending_messages` | ObservableGauge | `{messages}` | Messages queued for sending |

---

### Event Metrics (Counters)

These track events that occur over time. Use tags to distinguish subtypes.

#### Messaging Events

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.messages.sent` | `message.type` | `{messages}` | Total messages sent by type |
| `rapidcluster.messages.received` | `message.type` | `{messages}` | Total messages received by type |
| `rapidcluster.messages.bytes_sent` | `message.type` | `By` | Total bytes sent |
| `rapidcluster.messages.bytes_received` | `message.type` | `By` | Total bytes received |
| `rapidcluster.messages.dropped` | `message.type`, `reason` | `{messages}` | Messages dropped (queue full, invalid, etc.) |
| `rapidcluster.messages.retries` | `message.type` | `{messages}` | Message send retries |
| `rapidcluster.messages.errors` | `message.type`, `error.type` | `{errors}` | Message errors (serialization, network, timeout) |

**Message types:** `join_request`, `join_response`, `probe_request`, `probe_response`, `alert`, `consensus_proposal`, `consensus_vote`, `phase1a`, `phase1b`, `phase2a`, `phase2b`, `leave_request`, `metadata_update`

**Drop reasons:** `queue_full`, `invalid_format`, `unknown_sender`, `stale_configuration`, `duplicate`

**Error types:** `serialization`, `network`, `timeout`, `rejected`, `unknown`

#### Consensus Events

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.consensus.proposals` | `result` | `{proposals}` | Consensus proposals initiated and their outcomes |
| `rapidcluster.consensus.votes_sent` | `vote.type`, `configuration_id` | `{votes}` | Votes sent in consensus rounds |
| `rapidcluster.consensus.votes_received` | `vote.type`, `configuration_id` | `{votes}` | Votes received in consensus rounds |
| `rapidcluster.consensus.rounds_started` | `protocol` | `{rounds}` | Consensus rounds started |
| `rapidcluster.consensus.rounds_completed` | `protocol`, `result` | `{rounds}` | Consensus rounds completed |
| `rapidcluster.consensus.conflicts` | `configuration_id` | `{conflicts}` | Fast path conflicts requiring classic Paxos fallback |
| `rapidcluster.consensus.timeouts` | `phase` | `{timeouts}` | Consensus phase timeouts |
| `rapidcluster.consensus.nacks` | `phase` | `{nacks}` | Negative acknowledgments received |
| `rapidcluster.consensus.value_changes` | - | `{changes}` | Times a node changed its vote/accepted value |
| `rapidcluster.consensus.leader_elections` | `result` | `{elections}` | Leader election attempts (classic Paxos) |

**Result tags:** `success`, `timeout`, `conflict`, `superseded`, `aborted`

**Vote types:** `fast_vote`, `phase1b`, `phase2b`

**Protocol tags:** `fast_paxos`, `classic_paxos`

**Phase tags:** `fast_round`, `phase1a`, `phase1b`, `phase2a`, `phase2b`

#### Membership Events

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.membership.view_changes` | - | `{changes}` | Total membership view changes |
| `rapidcluster.membership.nodes_added` | - | `{nodes}` | Nodes added to the cluster |
| `rapidcluster.membership.nodes_removed` | `reason` | `{nodes}` | Nodes removed from the cluster |
| `rapidcluster.membership.join_requests` | `result` | `{requests}` | Join requests processed |
| `rapidcluster.membership.join_rejections` | `reason` | `{rejections}` | Join requests rejected |
| `rapidcluster.membership.leave_requests` | `result` | `{requests}` | Graceful leave requests |
| `rapidcluster.membership.metadata_updates` | - | `{updates}` | Node metadata updates |
| `rapidcluster.membership.ring_reconfigurations` | - | `{reconfigurations}` | Ring topology reconfigurations |

**Removal reasons:** `graceful_leave`, `failure_detected`, `kicked`, `timeout`

**Join results:** `success`, `rejected`, `timeout`, `already_member`

**Rejection reasons:** `ring_full`, `configuration_mismatch`, `uuid_collision`, `invalid_metadata`

#### Failure Detection Events

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.failure_detection.probes_sent` | - | `{probes}` | Failure detection probes sent |
| `rapidcluster.failure_detection.probes_received` | - | `{probes}` | Failure detection probes received |
| `rapidcluster.failure_detection.probe_successes` | - | `{probes}` | Successful probe responses |
| `rapidcluster.failure_detection.probe_failures` | `reason` | `{probes}` | Failed probes |
| `rapidcluster.failure_detection.alerts_raised` | `alert.type` | `{alerts}` | Failure alerts raised |
| `rapidcluster.failure_detection.false_positives` | - | `{alerts}` | False positive detections (node recovered) |
| `rapidcluster.failure_detection.monitor_assignments` | - | `{assignments}` | Monitor relationship changes |

**Probe failure reasons:** `timeout`, `connection_error`, `rejected`, `node_not_found`

**Alert types:** `suspect`, `confirm`, `clear`

#### Cut Detector Events

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.cut_detector.reports_received` | `report.type` | `{reports}` | Reports received by cut detector |
| `rapidcluster.cut_detector.reports_aggregated` | - | `{reports}` | Reports successfully aggregated |
| `rapidcluster.cut_detector.cuts_detected` | - | `{cuts}` | Stable cuts detected |
| `rapidcluster.cut_detector.cuts_proposed` | `result` | `{cuts}` | Cuts proposed for consensus |
| `rapidcluster.cut_detector.duplicate_reports` | - | `{reports}` | Duplicate reports ignored |
| `rapidcluster.cut_detector.stale_reports` | - | `{reports}` | Reports for old configurations ignored |

**Report types:** `join`, `fail`, `leave`

**Cut proposal results:** `accepted`, `rejected`, `superseded`

#### gRPC/Network Events

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.grpc.connections_opened` | - | `{connections}` | gRPC connections opened |
| `rapidcluster.grpc.connections_closed` | `reason` | `{connections}` | gRPC connections closed |
| `rapidcluster.grpc.connection_errors` | `error.type` | `{errors}` | gRPC connection errors |
| `rapidcluster.grpc.calls_started` | `method` | `{calls}` | gRPC calls started |
| `rapidcluster.grpc.calls_completed` | `method`, `status` | `{calls}` | gRPC calls completed |

**Connection close reasons:** `graceful`, `error`, `timeout`, `remote_closed`

**gRPC methods:** `SendMessage`, `Join`, `Probe`, etc.

---

### Latency/Duration Metrics (Histograms)

| Metric | Tags | Unit | Description |
|--------|------|------|-------------|
| `rapidcluster.consensus.latency` | `protocol`, `result` | `s` | Time to reach consensus |
| `rapidcluster.consensus.round_duration` | `protocol`, `phase` | `s` | Duration of individual consensus phases |
| `rapidcluster.join.latency` | `result` | `s` | Time for a node to complete join |
| `rapidcluster.leave.latency` | - | `s` | Time for graceful leave to complete |
| `rapidcluster.message.send_duration` | `message.type` | `s` | Time to send a message |
| `rapidcluster.message.roundtrip_time` | `message.type` | `s` | Message round-trip time (request-response) |
| `rapidcluster.message.processing_time` | `message.type` | `s` | Time to process received messages |
| `rapidcluster.probe.latency` | `result` | `s` | Failure detection probe round-trip time |
| `rapidcluster.cut_detector.aggregation_time` | - | `s` | Time to aggregate reports into a cut |
| `rapidcluster.grpc.call_duration` | `method`, `status` | `s` | gRPC call duration |
| `rapidcluster.view_change.duration` | - | `s` | Time to apply a membership view change |

---

### Histogram Bucket Boundaries

Different latency metrics need different bucket boundaries based on expected ranges:

```csharp
// Fast operations (sub-millisecond to tens of milliseconds)
// For: probe.latency, message.send_duration, message.processing_time
private static readonly double[] FastOperationBuckets = 
    [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1];

// Medium operations (milliseconds to seconds)
// For: consensus.latency, consensus.round_duration, message.roundtrip_time
private static readonly double[] MediumOperationBuckets = 
    [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

// Slow operations (seconds to tens of seconds)
// For: join.latency, leave.latency, view_change.duration
private static readonly double[] SlowOperationBuckets = 
    [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120];
```

#### 4. Tags (Dimensions) Summary

All tags used across metrics, with their possible values:

| Tag | Values | Used In |
|-----|--------|---------|
| `message.type` | `join_request`, `join_response`, `probe_request`, `probe_response`, `alert`, `consensus_proposal`, `consensus_vote`, `phase1a`, `phase1b`, `phase2a`, `phase2b`, `leave_request`, `metadata_update` | Messaging metrics |
| `error.type` | `serialization`, `network`, `timeout`, `rejected`, `unknown` | Error metrics |
| `reason` | (varies by metric - see individual metric tables) | Multiple |
| `result` | `success`, `timeout`, `conflict`, `superseded`, `aborted`, `rejected` | Consensus, membership metrics |
| `vote.type` | `fast_vote`, `phase1b`, `phase2b` | Consensus vote metrics |
| `protocol` | `fast_paxos`, `classic_paxos` | Consensus metrics |
| `phase` | `fast_round`, `phase1a`, `phase1b`, `phase2a`, `phase2b` | Consensus timing metrics |
| `alert.type` | `suspect`, `confirm`, `clear` | Failure detection metrics |
| `report.type` | `join`, `fail`, `leave` | Cut detector metrics |
| `configuration_id` | (numeric) | Consensus metrics (low cardinality in practice) |
| `method` | gRPC method names | gRPC metrics |
| `status` | gRPC status codes | gRPC metrics |

**Cardinality Warning:** Avoid adding high-cardinality tags like `node.id` to high-frequency metrics. Reserve such tags for debugging/tracing scenarios only.

#### 5. Units

Follow UCUM standard for units:
- Time measurements: `s` (seconds as floating point)
- Bytes: `By`
- Counts: `{messages}`, `{nodes}`, `{rounds}`, `{proposals}`, etc.

#### 6. Example Code

```csharp
public class RapidClusterMetrics
{
    private readonly Counter<long> _messagesSent;
    private readonly Counter<long> _messagesReceived;
    private readonly Counter<long> _consensusRoundsCompleted;
    private readonly Counter<long> _membershipViewChanges;
    private readonly Histogram<double> _consensusLatency;
    private readonly Histogram<double> _joinLatency;
    
    // State accessors for observable gauges
    private readonly Func<int> _getClusterSize;
    private readonly Func<long> _getConfigurationId;

    public RapidClusterMetrics(IMeterFactory meterFactory, IMembershipViewAccessor viewAccessor)
    {
        var meter = meterFactory.Create("RapidCluster");
        
        // Event counters
        _messagesSent = meter.CreateCounter<long>(
            name: "rapidcluster.messages.sent",
            unit: "{messages}",
            description: "Total messages sent");
            
        _messagesReceived = meter.CreateCounter<long>(
            name: "rapidcluster.messages.received",
            unit: "{messages}",
            description: "Total messages received");
            
        _consensusRoundsCompleted = meter.CreateCounter<long>(
            name: "rapidcluster.consensus.rounds_completed",
            unit: "{rounds}",
            description: "Consensus rounds completed");
            
        _membershipViewChanges = meter.CreateCounter<long>(
            name: "rapidcluster.membership.view_changes",
            unit: "{changes}",
            description: "Total membership view changes");
        
        // Histograms with custom buckets
        _consensusLatency = meter.CreateHistogram<double>(
            name: "rapidcluster.consensus.latency",
            unit: "s",
            description: "Time to reach consensus",
            advice: new InstrumentAdvice<double> 
            { 
                HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] 
            });
            
        _joinLatency = meter.CreateHistogram<double>(
            name: "rapidcluster.join.latency",
            unit: "s",
            description: "Time for a node to complete join",
            advice: new InstrumentAdvice<double> 
            { 
                HistogramBucketBoundaries = [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120] 
            });
        
        // Observable gauges for state
        _getClusterSize = () => viewAccessor.GetCurrentView()?.Count ?? 0;
        _getConfigurationId = () => viewAccessor.GetCurrentConfigurationId();
        
        meter.CreateObservableGauge(
            name: "rapidcluster.cluster.size",
            observeValue: _getClusterSize,
            unit: "{nodes}",
            description: "Current number of nodes in the cluster");
            
        meter.CreateObservableGauge(
            name: "rapidcluster.cluster.configuration_id",
            observeValue: _getConfigurationId,
            unit: "{id}",
            description: "Current configuration ID");
    }

    // Recording methods
    public void RecordMessageSent(string messageType) =>
        _messagesSent.Add(1, new KeyValuePair<string, object?>("message.type", messageType));
        
    public void RecordMessageReceived(string messageType) =>
        _messagesReceived.Add(1, new KeyValuePair<string, object?>("message.type", messageType));
        
    public void RecordConsensusCompleted(string protocol, string result, double latencySeconds)
    {
        _consensusRoundsCompleted.Add(1,
            new KeyValuePair<string, object?>("protocol", protocol),
            new KeyValuePair<string, object?>("result", result));
        _consensusLatency.Record(latencySeconds,
            new KeyValuePair<string, object?>("protocol", protocol),
            new KeyValuePair<string, object?>("result", result));
    }
    
    public void RecordMembershipViewChange() => _membershipViewChanges.Add(1);
    
    public void RecordJoinLatency(string result, double latencySeconds) =>
        _joinLatency.Record(latencySeconds, new KeyValuePair<string, object?>("result", result));
}
```

### Implementation Plan

1. **Create `RapidClusterMetrics` class** in `RapidCluster.Core/Monitoring/`
   - Define all instruments (counters, histograms, observable gauges)
   - Accept `IMeterFactory` via constructor
   - Accept `IMembershipViewAccessor` for observable gauge callbacks
   - Expose strongly-typed recording methods for each metric category
   - Use `TagList` for metrics with multiple tags to avoid allocations

2. **Create metric constants** in `RapidCluster.Core/Monitoring/MetricNames.cs`
   - Define all metric names as constants
   - Define all tag names and values as constants
   - Helps prevent typos and enables IDE completion

3. **Register metrics in DI**
   - Update `RapidClusterServiceCollectionExtensions` to register `RapidClusterMetrics`
   - Ensure `AddMetrics()` is called to register `IMeterFactory`
   - Make metrics optional (check for null before recording)

4. **Instrument components** (inject `RapidClusterMetrics` and call recording methods)

   | Component | Metrics |
   |-----------|---------|
   | `MembershipService` | cluster.size, membership.*, join.latency, leave.latency |
   | `ConsensusCoordinator` | consensus.* (proposals, rounds, latency) |
   | `FastPaxos` | consensus.votes_*, consensus.conflicts, consensus.timeouts |
   | `Paxos` | consensus.votes_*, consensus.rounds_*, consensus.leader_elections |
   | `MultiNodeCutDetector` | cut_detector.* |
   | `SimpleCutDetector` | cut_detector.* |
   | `PingPongFailureDetector` | failure_detection.*, probe.latency |
   | `GrpcClient` | messages.sent, messages.bytes_sent, grpc.*, message.send_duration |
   | `MembershipServiceImpl` | messages.received, messages.bytes_received, message.processing_time |
   | `UnicastToAllBroadcaster` | messages.sent (aggregated) |

5. **Add tests**
   - Unit tests using `MetricCollector<T>` to verify each metric
   - Test tag values are correct
   - Test histogram bucket boundaries
   - Integration tests to verify metrics flow through the system

6. **Documentation**
   - Add metrics reference to README
   - Document how to collect metrics with OpenTelemetry
   - Document how to configure Prometheus/Grafana dashboards
   - Provide example Grafana dashboard JSON

### Best Practices to Follow

From Microsoft docs:
- Use `IMeterFactory` for DI scenarios (not static meters)
- Meters created by `IMeterFactory` are automatically disposed with the DI container
- Keep tag cardinality low (< 1000 combinations per instrument)
- Use consistent tag names across instruments
- Callbacks for observable instruments should be fast (no blocking)
- For high-frequency measurements (>1M/sec), consider `ObservableCounter`
- Use appropriate numeric types (`long` for counters, `double` for histograms)

### References

- [Microsoft Docs: Creating Metrics](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation)
- [OpenTelemetry Metrics Naming Conventions](https://github.com/open-telemetry/semantic-conventions/blob/main/docs/general/metrics.md)
- [UCUM Units](https://ucum.org/)

## Implementation Notes

(To be updated during implementation)
