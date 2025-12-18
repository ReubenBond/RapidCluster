# New session - 2025-12-18T14:24:34.090Z

**Session ID:** ses_4ce26d6d5ffeVkqzsRxo6O19v0
**Created:** 12/18/2025, 6:24:34 AM
**Updated:** 12/18/2025, 7:19:43 AM

---

## User

If the set of seed nodes is dynamic (eg, discovered via DNS or some other mechanism), then they might not be the same across all nodes. What can we do to mitigate the chance of divergent clusters being formed? Do Consul or Etcd have mitigations for this scenario?

---

## Assistant

---

## User

continue

---

## Assistant

This is an excellent question about a real challenge in distributed systems. Let me analyze the problem and research how other systems handle it.

## The Problem

When seed nodes are discovered dynamically (DNS, service discovery, etc.), different nodes might see different seed lists at startup due to:
- DNS propagation delays
- Service discovery timing
- Network partitions during discovery
- Caching differences

This could lead to **split-brain scenarios** where multiple independent clusters form, each thinking they're the "correct" cluster.

## Current RapidCluster Behavior

Let me first check how RapidCluster currently handles seed discovery:

```
Tool: task
```

```
Tool: webfetch
```

```
Tool: webfetch
```

---

## Assistant

Now I have comprehensive information. Let me synthesize this into a clear answer.

---

## Analysis: Dynamic Seed Discovery and Split-Brain Prevention

### The Problem

When seed nodes are discovered dynamically (DNS, service discovery, etc.), different nodes might see different seed lists due to:
- DNS propagation delays
- Service discovery timing/caching
- Network partitions during discovery
- Load balancer randomization

This risks **split-brain**: multiple independent clusters forming, each believing they're legitimate.

---

## How Consul and etcd Handle This

### **Consul's Approach**

Consul uses Raft consensus and has several mitigations:

1. **Bootstrap Expect**: Requires `bootstrap_expect` number of servers to form a cluster. Servers won't start until enough peers are discovered.

2. **Cluster ID / Token**: Uses `initial-cluster-token` to generate unique cluster IDs. Nodes with different tokens refuse to join each other - they get a "cluster ID mismatch" error and exit.

3. **Single Bootstrap Node**: Only one server runs in "bootstrap" mode to self-elect as leader. Other servers must join explicitly.

4. **Datacenter Isolation**: Each datacenter has an independent Raft peer set, reducing cross-datacenter split-brain risk.

### **etcd's Approach**

etcd provides multiple discovery mechanisms with safeguards:

1. **Discovery Service**: Uses an external etcd cluster (or `discovery.etcd.io`) to coordinate bootstrap:
   - Nodes register themselves at a unique discovery URL
   - The URL has a **size configuration** specifying expected cluster size
   - Nodes wait until all expected members register before forming the cluster
   - **Discovery URL is single-use** - a new URL is required for each new cluster

2. **Cluster Token (`initial-cluster-token`)**: Generates unique cluster/member IDs even with identical configurations. Nodes with mismatched tokens refuse to communicate.

3. **Strict Member Validation**: If a node attempts to join with different configuration, etcd reports "conflicting cluster ID" and exits rather than forming a separate cluster.

4. **DNS SRV Records**: When using DNS discovery, the resolved addresses must match what's in SRV records - provides some consistency.

---

## Current RapidCluster Mitigations

RapidCluster already has several protections (in `MembershipService.cs`):

| Mitigation | Location | Description |
|------------|----------|-------------|
| **Seed Normalization** | Lines 263-269 | Seeds are sorted deterministically by `hostname:port` |
| **BootstrapExpect Truncation** | Lines 265-268 | If `BootstrapExpect > 0`, takes only first N seeds after sorting |
| **Deterministic Coordinator** | Lines 274-276 | First seed in sorted order becomes bootstrap coordinator |
| **Self-Filtering** | Lines 279-281 | Self-address filtered from seed list |
| **Duplicate Deduplication** | Lines 257-261 | Uses `DistinctBy` on address string |
| **File-Based Bootstrap** | Aspire samples | Shared file where all nodes register and wait for expected count |

---

## Recommended Additional Mitigations

### 1. **Cluster Token / UUID** (High Priority)

Like Consul/etcd, add a `ClusterToken` that gets hashed into the `ConfigurationId`:

```csharp
public class RapidClusterOptions
{
    // Existing
    public int BootstrapExpect { get; set; }
    
    // New: Prevents split-brain with dynamic seeds
    public string? ClusterToken { get; set; }
}
```

During bootstrap, nodes exchange tokens and refuse to form a cluster if they differ. This is the **primary defense** against divergent clusters.

### 2. **Minimum Overlap Validation** (Medium Priority)

Before bootstrapping, require that joining nodes see at least N% overlap with existing cluster members:

```csharp
// Pseudo-code
if (discoveredSeeds.Intersect(existingMembers).Count() < MinimumOverlap)
{
    throw new ClusterFormationException("Insufficient seed overlap - possible split-brain");
}
```

### 3. **Seed Discovery Retry with Backoff** (Medium Priority)

When seeds are discovered dynamically, retry discovery multiple times with exponential backoff before forming a cluster:

```csharp
// Retry seed discovery to allow DNS propagation
for (int i = 0; i < RetryCount; i++)
{
    var seeds = await _seedProvider.GetSeedsAsync();
    if (seeds.Count >= BootstrapExpect)
        break;
    await Task.Delay(BackoffDelay * (1 << i));
}
```

### 4. **External Discovery Service** (For Large Deployments)

Like etcd's discovery service, use an external coordination point:

```csharp
public interface IClusterDiscoveryService
{
    // Register self and get list of all registered members
    Task<IReadOnlyList<EndPoint>> RegisterAndDiscoverAsync(
        EndPoint selfAddress,
        int expectedClusterSize,
        string clusterToken,
        CancellationToken ct);
}
```

### 5. **Join Validation Against Running Cluster**

When a node tries to join, validate that the cluster it's joining has a compatible token/configuration:

```csharp
// In join response, include cluster token
message JoinResponse {
    // ... existing fields
    string cluster_token = N;
}

// Joiner validates token matches expected
if (response.ClusterToken != expectedToken)
{
    throw new ClusterMismatchException("Attempted to join wrong cluster");
}
```

---

## Summary of Best Practices

| Practice | Consul | etcd | RapidCluster Status |
|----------|--------|------|---------------------|
| Cluster Token/ID | Yes | Yes | **Not implemented** |
| Bootstrap Expect | Yes | Yes | Yes |
| Deterministic Seed Ordering | N/A | Yes | Yes |
| Discovery Service | N/A | Yes | Partial (file-based) |
| Cluster ID Mismatch Detection | Yes | Yes | **Not implemented** |
| Single-use Discovery URLs | N/A | Yes | N/A |

**Recommended Priority:**
1. **Add Cluster Token** - Primary defense, relatively simple to implement
2. **Add Cluster ID mismatch detection** - Reject joining clusters with different tokens
3. **Consider discovery service** for large-scale dynamic deployments

Would you like me to create a work item to implement the Cluster Token feature?

---

## User

We can detect split brains when nodes on either side contact each other, since we have the ClusterId field in ConfigurationId.

---

## Assistant

You're absolutely right. Let me check how `ConfigurationId` and cluster detection currently work:

```
Tool: read
```

```
Tool: grep
```

---

## Assistant

You're right - RapidCluster already has this covered. The `ClusterId` (a hash of the sorted initial seed endpoints) is:

1. **Computed at bootstrap** from the sorted seed list (`ConfigurationId.CreateInitial()`)
2. **Included in all messages** via the protobuf `ConfigurationId`
3. **Validated on message receipt** - probes and alerts from different clusters are rejected

Looking at `MembershipService.cs:1304-1312`, when nodes with different `ClusterId`s contact each other:

```csharp
if (senderConfigId.ClusterId == localConfigId.ClusterId && ...)
{
    // Process normally
}
else if (senderConfigId.ClusterId != localConfigId.ClusterId && probeMessage.Sender != null)
{
    _log.ProbeFromDifferentCluster(probeMessage.Sender, senderConfigId.ClusterId, localConfigId.ClusterId);
}
```

So split-brain **detection** is already in place. The question then becomes: **what mitigations can we add to prevent split-brain formation in the first place**, since detection after the fact means two clusters have already formed and diverged.

## Prevention vs Detection

| Approach | Status | When it helps |
|----------|--------|---------------|
| **Detection** (ClusterId mismatch) | Already implemented | After split-brain occurs, prevents cross-contamination |
| **Prevention** | Partially implemented | Before clusters form |

## Remaining Preventive Measures

Given that detection exists, the preventive measures worth considering are:

1. **`BootstrapExpect` + deterministic seed ordering** - Already implemented. Ensures all nodes agreeing on the same sorted seed list will compute the same `ClusterId`.

2. **Require minimum quorum before bootstrap** - The `BootstrapExpect` option already does this via `WaitForBootstrapSeedsAsync`.

3. **Warn/fail on low seed overlap** - If a node discovers seeds that would produce a different `ClusterId` than expected, it could warn loudly or refuse to start. This would be useful for operational visibility.

4. **Cluster Token (user-provided)** - An optional user-supplied token that gets mixed into the `ClusterId` hash. This would let operators intentionally distinguish clusters even if they happen to have overlapping seeds.

The existing `ClusterId` mechanism provides good protection. The main gap is **operational**: when split-brain is detected, it's currently just logged at Debug level. For dynamic seed scenarios, it might be worth:

- Logging at Warning/Error level when cross-cluster contact is detected
- Emitting a metric/event that operators can alert on
- Optionally exposing a "cluster health" status that indicates split-brain detection

Would you like me to look into improving the observability around split-brain detection, or is the current behavior sufficient for your use case?

---

## User

We should definitely log a split brain at an error level. We should also consider mitigations, such as terminating nodes on one side of the partition (eg, the ones on the smaller side). To prevent or greatly reduce the chance of a split brain forming, perhaps we can come up with a modification to bootstrapping. Eg, don't assume nodes have the same set of seeds, have them start with a special 'seed' ClusterId (= 0), and use a gossipping or voting mechanism to share seeds until we have BootstrapExpect seeds, then form a cluster from the shared set of seeds (which we have verified are identical accross the seed nodes unanimously)

---

## Assistant

This is a great idea. Let me think through this carefully.

## Current Problem

Today, `ClusterId` is computed locally from each node's seed list. If nodes have different seed lists (due to DNS timing, etc.), they compute different `ClusterId`s and form separate clusters.

## Proposed Solution: Seed Gossip Protocol

Instead of assuming all nodes have the same seeds, we:

1. **Start with `ClusterId = 0`** (special "forming" state)
2. **Gossip discovered seeds** between nodes until we have unanimous agreement
3. **Only then compute the final `ClusterId`** and form the cluster

### Protocol Sketch

```
Phase 1: Seed Discovery & Gossip
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Node A discovers: [A, B, C]     Node B discovers: [A, B, D]       │
│                                                                     │
│  Nodes exchange seed sets via gossip:                              │
│    A → B: "I know [A, B, C]"                                       │
│    B → A: "I know [A, B, D]"                                       │
│                                                                     │
│  After gossip:                                                      │
│    A knows: [A, B, C, D]                                           │
│    B knows: [A, B, C, D]                                           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Phase 2: Seed Confirmation (Voting)
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  Once a node has BootstrapExpect seeds, it proposes the sorted set │
│                                                                     │
│  A proposes: [A, B, C, D] (sorted)                                 │
│  B, C, D must ACK with same set                                    │
│                                                                     │
│  If all BootstrapExpect nodes agree → proceed to Phase 3           │
│  If disagreement → continue gossiping                              │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

Phase 3: Cluster Formation
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│  ClusterId = Hash([A, B, C, D])  (computed identically by all)     │
│  Form cluster with deterministic membership view                   │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Properties

1. **Unanimous agreement required** - All `BootstrapExpect` nodes must agree on the exact seed set before forming
2. **Union of discovered seeds** - Gossip merges seed sets, so partial discovery still works
3. **Deterministic outcome** - Once agreed, all nodes compute identical `ClusterId`
4. **Split-brain prevention** - Two groups with different seeds can't independently form clusters (they'd need to reach each other to get to `BootstrapExpect`)

### Edge Cases to Consider

| Scenario | Behavior |
|----------|----------|
| Node A sees [A,B,C], Node D sees [D,E,F] (disjoint) | Neither group reaches `BootstrapExpect`, no cluster forms |
| Node A sees [A,B], Node B sees [B,C], Node C sees [C,A] | Gossip merges to [A,B,C], all agree, cluster forms |
| DNS returns different IPs for same hostname | Need to handle hostname resolution carefully |
| One node crashes during Phase 2 | Timeout and retry, or wait for replacement |

### Questions for Design

1. **Should we take the union or intersection of seeds?**
   - Union: More tolerant of partial discovery, but could grow unbounded
   - Intersection: Stricter, but might fail if any node has stale data
   - **Recommendation**: Union, capped at some reasonable max (e.g., 2x `BootstrapExpect`)

2. **How long to gossip before giving up?**
   - Use existing `BootstrapTimeout` option

3. **What if we get MORE than `BootstrapExpect` seeds?**
   - Take first N after sorting (current behavior), but only after unanimous agreement

4. **Message protocol changes needed?**
   - New message type for seed gossip during bootstrap phase
   - Or extend existing `ProbeMessage` with optional seed set

Let me look at the current bootstrap flow to see where this would fit:

```
Tool: read
```

---

## Assistant

Now let me see the current probe message structure:

```
Tool: read
```

---

## Assistant

Now I have a complete picture. Let me draft a design document for this feature:

---

## Design: Safe Bootstrap Protocol with Seed Gossip

### Goals

1. **Prevent split-brain formation** when nodes have different initial seed lists
2. **Detect and respond to split-brain** if it somehow occurs
3. **Maintain backward compatibility** with existing bootstrap modes

### Overview

The key insight is that today's bootstrap assumes all nodes have identical seed lists. With dynamic discovery, this assumption can be violated. The solution is to:

1. **Gossip seeds during bootstrap** so all nodes converge on the same set
2. **Require unanimous agreement** before computing `ClusterId` and forming the cluster
3. **Log errors and optionally terminate** if split-brain is detected post-formation

### Protocol Changes

#### New Message Types

```protobuf
// Bootstrap protocol messages for seed gossip
message BootstrapSeedExchange
{
    Endpoint sender = 1;
    repeated Endpoint known_seeds = 2;      // Seeds this node knows about
    bytes proposed_seed_hash = 3;           // Hash of sorted seed list (for quick comparison)
    BootstrapPhase phase = 4;
}

enum BootstrapPhase {
    DISCOVERING = 0;    // Still gathering seeds
    PROPOSING = 1;      // Proposing final seed set
    CONFIRMED = 2;      // Confirmed agreement
}

message BootstrapSeedExchangeResponse
{
    Endpoint sender = 1;
    repeated Endpoint known_seeds = 2;
    bytes proposed_seed_hash = 3;
    bool agrees_with_proposal = 4;          // True if hash matches
    BootstrapPhase phase = 5;
}
```

#### Bootstrap State Machine

```
                    ┌─────────────────┐
                    │   INITIALIZED   │
                    │  (ClusterId=0)  │
                    └────────┬────────┘
                             │ Start bootstrap
                             ▼
                    ┌─────────────────┐
              ┌────▶│   DISCOVERING   │◀────┐
              │     │ Gossip seeds    │     │
              │     └────────┬────────┘     │
              │              │ Have BootstrapExpect seeds
              │              ▼              │
              │     ┌─────────────────┐     │
              │     │    PROPOSING    │     │
              │     │ Propose hash    │─────┘ Disagreement
              │     └────────┬────────┘
              │              │ All agree
              │              ▼
              │     ┌─────────────────┐
              │     │   CONFIRMED     │
              │     │ Form cluster    │
              │     └────────┬────────┘
              │              │
              │              ▼
              │     ┌─────────────────┐
              └─────│    FORMED       │
                    │ (ClusterId≠0)   │
                    └─────────────────┘
```

#### Algorithm

```
function BootstrapWithSeedGossip():
    knownSeeds = myDiscoveredSeeds ∪ {self}
    phase = DISCOVERING
    
    while not formed:
        // Gossip with all known seeds
        for each seed in knownSeeds (except self):
            response = SendSeedExchange(seed, knownSeeds, phase)
            knownSeeds = knownSeeds ∪ response.knownSeeds
        
        // Check if we have enough seeds
        if |knownSeeds| >= BootstrapExpect:
            sortedSeeds = Sort(knownSeeds)
            proposedHash = Hash(sortedSeeds[:BootstrapExpect])
            phase = PROPOSING
            
            // Try to get unanimous agreement
            agreements = 0
            for each seed in sortedSeeds[:BootstrapExpect] (except self):
                response = SendSeedExchange(seed, sortedSeeds, phase, proposedHash)
                if response.agrees_with_proposal and response.phase >= PROPOSING:
                    agreements++
            
            if agreements == BootstrapExpect - 1:  // All others agree
                phase = CONFIRMED
                ClusterId = Hash(sortedSeeds[:BootstrapExpect])
                FormCluster(sortedSeeds[:BootstrapExpect])
                return
        
        // Wait before next round
        Sleep(probeInterval)
```

### Split-Brain Detection & Response

When a node detects contact from a different `ClusterId`:

```csharp
public enum SplitBrainResponse
{
    LogOnly,           // Just log at Error level (default)
    TerminateSmaller,  // Terminate if we're in the smaller cluster
    TerminateAlways    // Always terminate on detection
}
```

**Determining "smaller" cluster:**
- Include cluster size in probe messages/responses
- The cluster with fewer members should terminate
- If equal, the cluster with lexicographically smaller `ClusterId` terminates (deterministic tiebreaker)

### Configuration Options

```csharp
public class RapidClusterOptions
{
    // Existing
    public int BootstrapExpect { get; set; }
    public TimeSpan BootstrapTimeout { get; set; }
    
    // New
    /// <summary>
    /// When true, uses seed gossip protocol during bootstrap to prevent
    /// split-brain when different nodes discover different seed lists.
    /// Default: true when BootstrapExpect > 0
    /// </summary>
    public bool UseSeedGossip { get; set; } = true;
    
    /// <summary>
    /// Action to take when split-brain is detected (contact from different ClusterId).
    /// </summary>
    public SplitBrainResponse SplitBrainResponse { get; set; } = SplitBrainResponse.LogOnly;
}
```

### Implementation Plan

| Phase | Task | Complexity |
|-------|------|------------|
| 1 | Upgrade split-brain logging from Debug to Error | Low |
| 2 | Add cluster size to probe messages for split-brain comparison | Low |
| 3 | Implement `SplitBrainResponse.TerminateSmaller` | Medium |
| 4 | Add `BootstrapSeedExchange` message types | Low |
| 5 | Implement seed gossip in `WaitForBootstrapSeedsAsync` | Medium |
| 6 | Implement unanimous agreement check before cluster formation | Medium |
| 7 | Add simulation tests for dynamic seed scenarios | Medium |

### Backward Compatibility

- **Default behavior unchanged** for static seed lists - if all nodes have identical seeds, gossip quickly converges and confirms
- **New behavior opt-in** via `UseSeedGossip` (though defaulting to true when `BootstrapExpect > 0`)
- **No breaking changes** to existing message formats - new messages are additive

---

Would you like me to create a work item for this and start implementation? I'd suggest starting with Phase 1-3 (improved detection and response) since they're lower risk, then Phase 4-7 (seed gossip prevention).

---

## User

We need to design this before implementing it.
Take the union of seeds, and cap it (sort the nodes and take the first N nodes)
2. Yes, use BootstrapTimeout
3. Yes, sort and take BootstrapExpect
4. It seems that JoinMessage and JoinResponse could be used instead of new messages, eg with a ConfigurationId.ClusterId = 0 and special JoinStatusCode to indicate that we are still bootstrapping?
Do not retain backwards compatibility, always use seed gossip when the set of seed nodes is not known to be static - Change the return type of ISeedProvider.GetSeedsAsync so that it includes a field indicating if the result is static and well-known (will never change, and identical accross seeds) or not.
It's important that this is CLEANLY SEPARATED from non-bootstrapping code and does not modify regular membership code, where possible.

---

## Assistant

Excellent points. Let me revise the design with these constraints.

---

## Design: Safe Bootstrap Protocol with Seed Gossip (v2)

### Goals

1. **Prevent split-brain formation** when nodes have different initial seed lists
2. **Cleanly separated** from regular membership protocol code
3. **Reuse existing message types** where possible

### Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Seed aggregation | Union, then sort and take first `BootstrapExpect` | Tolerates partial discovery while bounding size |
| Timeout | Use existing `BootstrapTimeout` | Consistency with current options |
| Messages | Reuse `JoinMessage`/`JoinResponse` with `ClusterId=0` | Avoid protocol bloat |
| Backward compat | None - require seed gossip for dynamic seeds | Simpler, safer |
| Separation | New `BootstrapCoordinator` class | Keep `MembershipService` clean |

### ISeedProvider Changes

```csharp
/// <summary>
/// Result of seed discovery, indicating whether the seeds are static or dynamic.
/// </summary>
public readonly record struct SeedDiscoveryResult
{
    /// <summary>
    /// The discovered seed endpoints.
    /// </summary>
    public required IReadOnlyList<EndPoint> Seeds { get; init; }
    
    /// <summary>
    /// When true, the seed list is static and guaranteed to be identical across all nodes.
    /// Bootstrap can proceed without seed gossip.
    /// When false, seeds may differ between nodes (e.g., DNS, service discovery),
    /// and seed gossip is required to prevent split-brain.
    /// </summary>
    public required bool IsStatic { get; init; }
}

public interface ISeedProvider
{
    /// <summary>
    /// Discovers seed nodes for cluster formation.
    /// </summary>
    ValueTask<SeedDiscoveryResult> GetSeedsAsync(CancellationToken cancellationToken = default);
}
```

### Bootstrap Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        MembershipService.InitializeAsync                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   seeds = await _seedProvider.GetSeedsAsync()                          │
│                                                                         │
│   if (seeds.IsStatic && BootstrapExpect > 0)                           │
│       // Current behavior: deterministic bootstrap from known seeds    │
│       await BootstrapClusterAsync()                                    │
│                                                                         │
│   else if (!seeds.IsStatic && BootstrapExpect > 0)                     │
│       // NEW: Seed gossip required                                     │
│       agreedSeeds = await _bootstrapCoordinator.NegotiateSeedsAsync()  │
│       await BootstrapClusterAsync(agreedSeeds)                         │
│                                                                         │
│   else                                                                  │
│       // Traditional join (no BootstrapExpect)                         │
│       await JoinClusterAsync()                                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### Protocol: Reusing JoinMessage/JoinResponse

During bootstrap negotiation, nodes communicate using existing message types with special semantics:

**Indicator of bootstrap phase:** `ConfigurationId.ClusterId = 0`

**New JoinStatusCode:**
```protobuf
enum JoinStatusCode {
    HOSTNAME_ALREADY_IN_RING = 0;
    SAFE_TO_JOIN = 1;
    CONFIG_CHANGED = 2;
    MEMBERSHIP_REJECTED = 3;
    BOOTSTRAP_IN_PROGRESS = 4;  // NEW: Still negotiating seeds
};
```

**Message semantics during bootstrap (`ClusterId = 0`):**

| Field | Meaning during bootstrap |
|-------|--------------------------|
| `JoinMessage.sender` | Node's own address |
| `JoinMessage.configurationId` | `{Version=0, ClusterId=0}` signals bootstrap mode |
| `JoinMessage.metadata` | Could carry known seeds (or use a dedicated field) |
| `JoinResponse.statusCode` | `BOOTSTRAP_IN_PROGRESS` or `SAFE_TO_JOIN` (agreement reached) |
| `JoinResponse.endpoints` | Responder's known seeds |
| `JoinResponse.configurationId` | Responder's proposed final config (if agreed) |

**Alternative: Add dedicated field for seed exchange**

Since `JoinMessage.metadata` is a generic map, we could use it:
```csharp
// In bootstrap mode, metadata contains:
metadata["bootstrap_seeds"] = SerializedEndpointList
metadata["bootstrap_seed_hash"] = Hash of sorted, truncated seed list
```

Or cleaner - add optional fields to the proto:

```protobuf
message JoinMessage
{
   Endpoint sender = 1;
   repeated int32 ringNumber = 2;
   ConfigurationId configurationId = 3;
   Metadata metadata = 4;
   
   // Bootstrap seed negotiation (only used when configurationId.cluster_id = 0)
   repeated Endpoint bootstrap_known_seeds = 5;
   bytes bootstrap_seed_hash = 6;  // Hash of proposed final seed set
}

message JoinResponse
{
   Endpoint sender = 1;
   JoinStatusCode statusCode = 2;
   ConfigurationId configurationId = 3;
   repeated Endpoint endpoints = 4;
   repeated Endpoint metadataKeys = 5;
   repeated Metadata metadataValues = 6;
   int64 max_node_id = 7;
   
   // Bootstrap seed negotiation (only used when configurationId.cluster_id = 0)
   repeated Endpoint bootstrap_known_seeds = 8;
   bytes bootstrap_seed_hash = 9;
   bool bootstrap_agrees = 10;  // True if responder agrees with sender's hash
}
```

### BootstrapCoordinator Class

New class that encapsulates all seed gossip logic, cleanly separated from `MembershipService`:

```csharp
/// <summary>
/// Coordinates bootstrap seed negotiation when seeds are dynamically discovered.
/// Ensures all bootstrap nodes agree on the same seed set before cluster formation.
/// </summary>
internal sealed class BootstrapCoordinator
{
    private readonly Endpoint _myAddr;
    private readonly int _bootstrapExpect;
    private readonly TimeSpan _bootstrapTimeout;
    private readonly IBroadcaster _broadcaster;
    private readonly ILogger<BootstrapCoordinator> _logger;
    private readonly TimeProvider _timeProvider;
    
    // Accumulated seeds from gossip
    private readonly HashSet<Endpoint> _knownSeeds;
    
    public BootstrapCoordinator(
        Endpoint myAddr,
        IReadOnlyList<Endpoint> initialSeeds,
        int bootstrapExpect,
        TimeSpan bootstrapTimeout,
        IBroadcaster broadcaster,
        ILogger<BootstrapCoordinator> logger,
        TimeProvider timeProvider)
    {
        _myAddr = myAddr;
        _bootstrapExpect = bootstrapExpect;
        _bootstrapTimeout = bootstrapTimeout;
        _broadcaster = broadcaster;
        _logger = logger;
        _timeProvider = timeProvider;
        
        _knownSeeds = new HashSet<Endpoint>(initialSeeds, EndpointComparer.Instance);
        _knownSeeds.Add(myAddr);
    }
    
    /// <summary>
    /// Negotiates seed set with other bootstrap nodes until unanimous agreement.
    /// Returns the agreed-upon seed list (sorted, truncated to BootstrapExpect).
    /// </summary>
    /// <exception cref="TimeoutException">If agreement not reached within timeout.</exception>
    /// <exception cref="OperationCanceledException">If cancelled.</exception>
    public async Task<IReadOnlyList<Endpoint>> NegotiateSeedsAsync(CancellationToken ct)
    {
        // Implementation follows the algorithm below
    }
    
    /// <summary>
    /// Handles incoming bootstrap negotiation message from another node.
    /// Called by MembershipService when it receives a JoinMessage with ClusterId=0.
    /// </summary>
    public JoinResponse HandleBootstrapMessage(JoinMessage message)
    {
        // Merge their seeds into our known set
        // Return our known seeds and whether we agree on the hash
    }
}
```

### Algorithm

```
function NegotiateSeedsAsync():
    startTime = Now()
    
    while (Now() - startTime) < BootstrapTimeout:
        // Get current candidate set
        sortedSeeds = Sort(_knownSeeds)
        candidateSeeds = sortedSeeds.Take(BootstrapExpect)
        candidateHash = Hash(candidateSeeds)
        
        // Gossip with all known seeds
        agreementCount = 1  // Self agrees
        
        for each seed in _knownSeeds (except self):
            response = await SendBootstrapMessage(seed, _knownSeeds, candidateHash)
            
            if response.StatusCode == BOOTSTRAP_IN_PROGRESS:
                // Merge their seeds
                _knownSeeds.UnionWith(response.BootstrapKnownSeeds)
                
                if response.BootstrapAgrees:
                    agreementCount++
                    
            else if response.StatusCode == SAFE_TO_JOIN:
                // They've already formed - we're late, join normally
                throw new BootstrapAlreadyCompleteException()
        
        // Recompute after merging (seeds may have changed)
        sortedSeeds = Sort(_knownSeeds)
        candidateSeeds = sortedSeeds.Take(BootstrapExpect)
        
        // Check for unanimous agreement among candidate seeds
        if agreementCount == BootstrapExpect:
            _logger.BootstrapAgreementReached(candidateSeeds, candidateHash)
            return candidateSeeds
        
        // Not yet - wait and retry
        await Delay(ProbeInterval)
    
    throw new TimeoutException("Bootstrap seed negotiation timed out")
```

### Handling Race Conditions

**Scenario: Node A and B gossip, agree on [A,B,C], but C hasn't responded yet**

- Agreement requires all `BootstrapExpect` nodes to confirm
- A and B will keep trying until C responds and agrees
- Only when all 3 agree does cluster formation proceed

**Scenario: Two groups [A,B] and [C,D] discover each other late**

- Initially A knows [A,B], C knows [C,D]
- When A contacts C (or vice versa), seeds merge: [A,B,C,D]
- After sorting/truncating (say to 3): [A,B,C]
- All nodes converge to same candidate set
- Agreement reached, single cluster forms

**Scenario: Node joins after cluster already formed**

- Late node sends bootstrap message with `ClusterId=0`
- Existing cluster members respond with their real `ClusterId` and `SAFE_TO_JOIN` (or `CONFIG_CHANGED`)
- Late node detects cluster exists, switches to normal join protocol

### Split-Brain Detection Improvements

Separate from bootstrap, improve detection in `MembershipService`:

```csharp
// When probe/message received from different ClusterId:
if (senderConfigId.ClusterId != localConfigId.ClusterId && 
    senderConfigId.ClusterId != 0 &&  // Ignore nodes still bootstrapping
    localConfigId.ClusterId != 0)
{
    _logger.SplitBrainDetected(
        senderConfigId.ClusterId, 
        localConfigId.ClusterId,
        senderClusterSize,    // New: include in probe
        localClusterSize);
    
    if (_options.SplitBrainResponse == SplitBrainResponse.TerminateSmaller)
    {
        if (ShouldTerminate(localClusterSize, senderClusterSize, 
                           localConfigId.ClusterId, senderConfigId.ClusterId))
        {
            _logger.TerminatingDueToSplitBrain();
            _lifecycleManager.RequestShutdown();
        }
    }
}
```

### File Structure

```
src/RapidCluster.Core/
├── Discovery/
│   ├── ISeedProvider.cs           # Modified: new return type
│   ├── SeedDiscoveryResult.cs     # NEW
│   └── ConfigurationSeedProvider.cs  # Modified: returns IsStatic=true
├── Bootstrap/                      # NEW directory
│   ├── BootstrapCoordinator.cs    # NEW: seed negotiation logic
│   └── BootstrapException.cs      # NEW: specific exceptions
├── MembershipService.cs           # Modified: delegate to BootstrapCoordinator
└── Protos/
    └── rapid_types.proto          # Modified: new fields, status code
```

### Test Scenarios

| Scenario | Expected Behavior |
|----------|-------------------|
| All nodes have identical static seeds | Bypass gossip, form immediately |
| Nodes have overlapping dynamic seeds | Gossip merges, unanimous agreement, single cluster |
| Nodes have disjoint seeds initially | Gossip discovers all, merges, single cluster |
| One node very slow to respond | Others wait (within timeout) until it agrees |
| Node joins after cluster formed | Detects via response, falls back to normal join |
| Timeout before agreement | Throws `TimeoutException`, node does not form cluster |
| Two isolated groups (network partition) | Neither reaches `BootstrapExpect`, neither forms |

---

Does this design address your concerns? Key changes from v1:

1. **Clean separation**: All gossip logic in `BootstrapCoordinator`, `MembershipService` just delegates
2. **Reuse messages**: `JoinMessage`/`JoinResponse` with `ClusterId=0` and new optional fields
3. **ISeedProvider change**: Returns `SeedDiscoveryResult` with `IsStatic` flag
4. **No backward compat**: Dynamic seeds always use gossip

---

## User

Let's use new message types instead of overloading JoinMessage and JoinResponse - how would that look?

---

## Assistant

Good call - cleaner separation. Here's the design with dedicated message types:

---

## Protocol Messages for Bootstrap Seed Negotiation

### New Proto Definitions

```protobuf
// ******* Bootstrap seed negotiation *******
// Used when seeds are dynamically discovered to ensure all bootstrap nodes
// agree on the same seed set before cluster formation.

message BootstrapSeedGossip
{
    Endpoint sender = 1;
    
    // All seeds this node currently knows about
    repeated Endpoint known_seeds = 2;
    
    // Hash of the proposed final seed set (sorted, truncated to BootstrapExpect)
    // Used for quick agreement check without comparing full lists
    int64 proposed_seed_hash = 3;
    
    // Number of nodes expected for bootstrap (BootstrapExpect setting)
    int32 bootstrap_expect = 4;
}

message BootstrapSeedGossipResponse
{
    Endpoint sender = 1;
    
    // All seeds this node currently knows about
    repeated Endpoint known_seeds = 2;
    
    // Hash of this node's proposed final seed set
    int64 proposed_seed_hash = 3;
    
    // Status of this node's bootstrap process
    BootstrapStatus status = 4;
    
    // If status is ALREADY_FORMED, this contains the cluster's ConfigurationId
    ConfigurationId configuration_id = 5;
}

enum BootstrapStatus
{
    // Still discovering/negotiating seeds
    NEGOTIATING = 0;
    
    // Agrees with the sender's proposed_seed_hash
    AGREED = 1;
    
    // This node has already formed/joined a cluster
    // The late node should use normal join protocol instead
    ALREADY_FORMED = 2;
}
```

### Updated RapidClusterRequest/Response

```protobuf
message RapidClusterRequest
{
   oneof content {
        PreJoinMessage preJoinMessage = 1;
        JoinMessage joinMessage = 2;
        BatchedAlertMessage batchedAlertMessage = 3;
        ProbeMessage probeMessage = 4;
        FastRoundPhase2bMessage fastRoundPhase2bMessage = 5;
        Phase1aMessage phase1aMessage = 6;
        Phase1bMessage phase1bMessage = 7;
        Phase2aMessage phase2aMessage = 8;
        Phase2bMessage phase2bMessage = 9;
        LeaveMessage leaveMessage = 10;
        MembershipViewRequest membershipViewRequest = 11;
        BootstrapSeedGossip bootstrapSeedGossip = 12;  // NEW
   }
}

message RapidClusterResponse
{
   oneof content {
        JoinResponse joinResponse = 1;
        Response response = 2;
        ConsensusResponse consensusResponse = 3;
        ProbeResponse probeResponse = 4;
        MembershipViewResponse membershipViewResponse = 5;
        BootstrapSeedGossipResponse bootstrapSeedGossipResponse = 6;  // NEW
   }
}
```

### Message Flow

```
┌─────────────┐                              ┌─────────────┐
│   Node A    │                              │   Node B    │
│ knows:[A,B] │                              │ knows:[B,C] │
└──────┬──────┘                              └──────┬──────┘
       │                                            │
       │  BootstrapSeedGossip                       │
       │  {sender:A, known:[A,B], hash:H1}          │
       │───────────────────────────────────────────▶│
       │                                            │
       │                                            │ Merge: [A,B,C]
       │                                            │ Compute new hash H2
       │                                            │
       │  BootstrapSeedGossipResponse               │
       │  {sender:B, known:[A,B,C], hash:H2,        │
       │   status:NEGOTIATING}                      │
       │◀───────────────────────────────────────────│
       │                                            │
       │ Merge: [A,B,C]                             │
       │ Compute new hash H2                        │
       │                                            │
       │         ... continue until all agree ...   │
       │                                            │
       │  BootstrapSeedGossip                       │
       │  {sender:A, known:[A,B,C], hash:H2}        │
       │───────────────────────────────────────────▶│
       │                                            │
       │  BootstrapSeedGossipResponse               │
       │  {sender:B, known:[A,B,C], hash:H2,        │
       │   status:AGREED}                           │
       │◀───────────────────────────────────────────│
       │                                            │
       ▼                                            ▼
   Form cluster with                         Form cluster with
   ClusterId = H2                            ClusterId = H2
```

### Late Joiner Scenario

```
┌─────────────┐                              ┌─────────────┐
│   Node D    │                              │   Node A    │
│  (late)     │                              │  (formed)   │
│ knows:[A,D] │                              │ ClusterId=X │
└──────┬──────┘                              └──────┬──────┘
       │                                            │
       │  BootstrapSeedGossip                       │
       │  {sender:D, known:[A,D], hash:H1}          │
       │───────────────────────────────────────────▶│
       │                                            │
       │  BootstrapSeedGossipResponse               │
       │  {sender:A, status:ALREADY_FORMED,         │
       │   configuration_id:{version:N, cluster:X}} │
       │◀───────────────────────────────────────────│
       │                                            │
       │ Abort bootstrap,                           │
       │ switch to normal JoinClusterAsync()        │
       ▼                                            │
```

### BootstrapCoordinator Interface

```csharp
/// <summary>
/// Coordinates bootstrap seed negotiation when seeds are dynamically discovered.
/// Ensures all bootstrap nodes agree on the same seed set before cluster formation.
/// </summary>
internal interface IBootstrapCoordinator
{
    /// <summary>
    /// Negotiates seed set with other bootstrap nodes until unanimous agreement.
    /// </summary>
    /// <returns>The agreed-upon seed list (sorted, truncated to BootstrapExpect).</returns>
    /// <exception cref="BootstrapAlreadyCompleteException">
    /// A cluster has already formed. Caller should use normal join protocol.
    /// </exception>
    /// <exception cref="TimeoutException">Agreement not reached within timeout.</exception>
    Task<IReadOnlyList<Endpoint>> NegotiateSeedsAsync(CancellationToken ct);
    
    /// <summary>
    /// Handles incoming bootstrap gossip message.
    /// </summary>
    BootstrapSeedGossipResponse HandleGossip(BootstrapSeedGossip message);
}

internal sealed class BootstrapCoordinator : IBootstrapCoordinator
{
    private readonly Endpoint _myAddr;
    private readonly int _bootstrapExpect;
    private readonly TimeSpan _bootstrapTimeout;
    private readonly TimeSpan _gossipInterval;
    private readonly IBroadcaster _broadcaster;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<BootstrapCoordinator> _logger;
    
    private readonly object _lock = new();
    private readonly HashSet<Endpoint> _knownSeeds;
    private bool _clusterFormed;
    private ConfigurationId _formedConfigurationId;
    
    public BootstrapCoordinator(
        Endpoint myAddr,
        IReadOnlyList<Endpoint> initialSeeds,
        int bootstrapExpect,
        RapidClusterOptions options,
        IBroadcaster broadcaster,
        TimeProvider timeProvider,
        ILogger<BootstrapCoordinator> logger)
    {
        _myAddr = myAddr;
        _bootstrapExpect = bootstrapExpect;
        _bootstrapTimeout = options.BootstrapTimeout;
        _gossipInterval = TimeSpan.FromSeconds(1); // Or from options
        _broadcaster = broadcaster;
        _timeProvider = timeProvider;
        _logger = logger;
        
        _knownSeeds = [..initialSeeds, myAddr];
    }
    
    public async Task<IReadOnlyList<Endpoint>> NegotiateSeedsAsync(CancellationToken ct)
    {
        var stopwatch = Stopwatch.StartNew();
        
        while (stopwatch.Elapsed < _bootstrapTimeout)
        {
            ct.ThrowIfCancellationRequested();
            
            // Snapshot current state
            List<Endpoint> currentSeeds;
            long proposedHash;
            lock (_lock)
            {
                currentSeeds = [.._knownSeeds];
                proposedHash = ComputeSeedHash(currentSeeds);
            }
            
            // Gossip with all known seeds
            var agreementCount = 1; // Self agrees
            var candidateSeeds = GetCandidateSeeds(currentSeeds);
            
            var gossipTasks = currentSeeds
                .Where(s => !EndpointEquals(s, _myAddr))
                .Select(seed => GossipWithSeedAsync(seed, currentSeeds, proposedHash, ct))
                .ToList();
            
            var responses = await Task.WhenAll(gossipTasks).ConfigureAwait(false);
            
            foreach (var response in responses.Where(r => r != null))
            {
                // Check if cluster already formed elsewhere
                if (response.Status == BootstrapStatus.AlreadyFormed)
                {
                    throw new BootstrapAlreadyCompleteException(
                        ConfigurationId.FromProtobuf(response.ConfigurationId));
                }
                
                // Merge their seeds
                lock (_lock)
                {
                    foreach (var seed in response.KnownSeeds)
                    {
                        _knownSeeds.Add(seed);
                    }
                }
                
                if (response.Status == BootstrapStatus.Agreed &&
                    response.ProposedSeedHash == proposedHash)
                {
                    agreementCount++;
                }
            }
            
            // Recompute candidates after merge
            List<Endpoint> finalCandidates;
            long finalHash;
            lock (_lock)
            {
                finalCandidates = GetCandidateSeeds([.._knownSeeds]);
                finalHash = ComputeSeedHash([.._knownSeeds]);
            }
            
            // Check for unanimous agreement among candidates
            if (agreementCount >= _bootstrapExpect)
            {
                _logger.BootstrapAgreementReached(finalCandidates, finalHash);
                return finalCandidates;
            }
            
            _logger.BootstrapNegotiationProgress(
                _knownSeeds.Count, 
                agreementCount, 
                _bootstrapExpect);
            
            await Task.Delay(_gossipInterval, _timeProvider, ct).ConfigureAwait(false);
        }
        
        throw new TimeoutException(
            $"Bootstrap seed negotiation timed out after {_bootstrapTimeout}. " +
            $"Known seeds: {_knownSeeds.Count}, expected: {_bootstrapExpect}");
    }
    
    public BootstrapSeedGossipResponse HandleGossip(BootstrapSeedGossip message)
    {
        lock (_lock)
        {
            // If we've already formed a cluster, tell them
            if (_clusterFormed)
            {
                return new BootstrapSeedGossipResponse
                {
                    Sender = _myAddr,
                    Status = BootstrapStatus.AlreadyFormed,
                    ConfigurationId = _formedConfigurationId.ToProtobuf()
                };
            }
            
            // Merge their seeds into ours
            foreach (var seed in message.KnownSeeds)
            {
                _knownSeeds.Add(seed);
            }
            
            // Compute our hash and check agreement
            var ourHash = ComputeSeedHash([.._knownSeeds]);
            var agrees = ourHash == message.ProposedSeedHash;
            
            return new BootstrapSeedGossipResponse
            {
                Sender = _myAddr,
                KnownSeeds = { _knownSeeds },
                ProposedSeedHash = ourHash,
                Status = agrees ? BootstrapStatus.Agreed : BootstrapStatus.Negotiating
            };
        }
    }
    
    /// <summary>
    /// Called by MembershipService after cluster formation to update state.
    /// </summary>
    public void MarkClusterFormed(ConfigurationId configurationId)
    {
        lock (_lock)
        {
            _clusterFormed = true;
            _formedConfigurationId = configurationId;
        }
    }
    
    private List<Endpoint> GetCandidateSeeds(List<Endpoint> seeds)
    {
        return seeds
            .OrderBy(e => $"{e.Hostname.ToStringUtf8()}:{e.Port}", StringComparer.Ordinal)
            .Take(_bootstrapExpect)
            .ToList();
    }
    
    private static long ComputeSeedHash(List<Endpoint> seeds)
    {
        // Same logic as ConfigurationId.ComputeClusterId
        // Hash the sorted, truncated seed list
    }
    
    private async Task<BootstrapSeedGossipResponse?> GossipWithSeedAsync(
        Endpoint seed,
        List<Endpoint> knownSeeds,
        long proposedHash,
        CancellationToken ct)
    {
        try
        {
            var request = new RapidClusterRequest
            {
                BootstrapSeedGossip = new BootstrapSeedGossip
                {
                    Sender = _myAddr,
                    KnownSeeds = { knownSeeds },
                    ProposedSeedHash = proposedHash,
                    BootstrapExpect = _bootstrapExpect
                }
            };
            
            var response = await _broadcaster
                .SendAsync(seed, request, ct)
                .ConfigureAwait(false);
                
            return response.BootstrapSeedGossipResponse;
        }
        catch (Exception ex)
        {
            _logger.BootstrapGossipFailed(seed, ex);
            return null;
        }
    }
}
```

### Exception Types

```csharp
namespace RapidCluster.Exceptions;

/// <summary>
/// Thrown when bootstrap negotiation discovers that a cluster has already formed.
/// The node should switch to normal join protocol.
/// </summary>
public sealed class BootstrapAlreadyCompleteException : Exception
{
    public ConfigurationId ExistingClusterConfiguration { get; }
    
    public BootstrapAlreadyCompleteException(ConfigurationId configurationId)
        : base($"A cluster has already formed with configuration {configurationId}. Use normal join protocol.")
    {
        ExistingClusterConfiguration = configurationId;
    }
}
```

### MembershipService Integration

```csharp
// In MembershipService.InitializeAsync:

private async Task InitializeAsync(CancellationToken cancellationToken)
{
    var seedResult = await _seedProvider.GetSeedsAsync(cancellationToken);
    ProcessSeeds(seedResult.Seeds);
    
    if (_seedAddresses.Count == 0)
    {
        // No seeds - start new single-node cluster
        await StartNewClusterAsync(cancellationToken);
    }
    else if (_bootstrapExpect > 0)
    {
        if (seedResult.IsStatic)
        {
            // Static seeds - use current deterministic bootstrap
            await BootstrapClusterAsync(cancellationToken);
        }
        else
        {
            // Dynamic seeds - negotiate first
            await BootstrapWithNegotiationAsync(cancellationToken);
        }
    }
    else
    {
        // No BootstrapExpect - traditional join
        await JoinClusterAsync(cancellationToken);
    }
}

private async Task BootstrapWithNegotiationAsync(CancellationToken cancellationToken)
{
    _bootstrapCoordinator = new BootstrapCoordinator(
        _myAddr,
        _seedAddresses,
        _bootstrapExpect,
        _options,
        _broadcaster,
        _sharedResources.TimeProvider,
        _loggerFactory.CreateLogger<BootstrapCoordinator>());
    
    try
    {
        var agreedSeeds = await _bootstrapCoordinator
            .NegotiateSeedsAsync(cancellationToken);
        
        // Replace seed list with agreed set
        _seedAddresses.Clear();
        _seedAddresses.AddRange(agreedSeeds.Where(s => !EndpointEquals(s, _myAddr)));
        
        // Now bootstrap with agreed seeds
        await BootstrapClusterAsync(cancellationToken);
        
        // Mark coordinator as formed
        _bootstrapCoordinator.MarkClusterFormed(_view.ConfigurationId);
    }
    catch (BootstrapAlreadyCompleteException ex)
    {
        _log.ClusterAlreadyFormedDuringBootstrap(ex.ExistingClusterConfiguration);
        
        // Switch to normal join
        await JoinClusterAsync(cancellationToken);
    }
}

// In message handler:
private RapidClusterResponse HandleRequest(RapidClusterRequest request)
{
    return request.ContentCase switch
    {
        // ... existing cases ...
        
        RapidClusterRequest.ContentOneofCase.BootstrapSeedGossip =>
            HandleBootstrapGossip(request.BootstrapSeedGossip),
            
        _ => new RapidClusterResponse { Response = new Response() }
    };
}

private RapidClusterResponse HandleBootstrapGossip(BootstrapSeedGossip gossip)
{
    // If we have a coordinator, delegate to it
    if (_bootstrapCoordinator != null)
    {
        return new RapidClusterResponse
        {
            BootstrapSeedGossipResponse = _bootstrapCoordinator.HandleGossip(gossip)
        };
    }
    
    // No coordinator means we either:
    // 1. Already formed (static seeds)
    // 2. Joined normally
    // Either way, tell them we're formed
    return new RapidClusterResponse
    {
        BootstrapSeedGossipResponse = new BootstrapSeedGossipResponse
        {
            Sender = _myAddr,
            Status = BootstrapStatus.AlreadyFormed,
            ConfigurationId = _view.ConfigurationId.ToProtobuf()
        }
    };
}
```

### File Structure

```
src/RapidCluster.Core/
├── Discovery/
│   ├── ISeedProvider.cs              # Modified
│   ├── SeedDiscoveryResult.cs        # NEW
│   └── ConfigurationSeedProvider.cs  # Modified (IsStatic = true)
├── Bootstrap/                         # NEW directory
│   ├── IBootstrapCoordinator.cs      # NEW
│   ├── BootstrapCoordinator.cs       # NEW
│   └── BootstrapAlreadyCompleteException.cs  # NEW
├── Logging/
│   └── BootstrapCoordinatorLogger.cs # NEW
├── MembershipService.cs              # Modified (minimal changes)
└── Protos/
    └── rapid_types.proto             # Modified (new messages)
```

---

This keeps the bootstrap negotiation protocol completely separate from the existing join/membership protocol. The only touchpoints with `MembershipService` are:

1. Calling `NegotiateSeedsAsync()` before bootstrap
2. Routing incoming `BootstrapSeedGossip` messages to the coordinator
3. Calling `MarkClusterFormed()` after successful bootstrap

Does this look cleaner?

---

## User

ConfigurationSeedProvider is not static, since configuration can change at runtime in .NET

---

## Assistant

You're right - `IOptionsMonitor` supports runtime configuration changes, so `ConfigurationSeedProvider` cannot guarantee static seeds. Let me look at the current implementation:

```
Tool: read
```

---

## Assistant

Right. So the question is: how do we determine if seeds are static? Options:

1. **Explicit flag on `ISeedProvider`** - Provider declares whether it's static
2. **Explicit flag on `RapidClusterOptions`** - User declares whether seeds are static
3. **Assume all seeds are dynamic** - Always use gossip when `BootstrapExpect > 0`

Option 3 is simplest and safest - gossip converges quickly when seeds are identical anyway, so there's minimal overhead for the static case. The only downside is slightly more network traffic during bootstrap.

Option 2 puts the burden on the user but gives them control.

What's your preference? I'm leaning toward:

- **Option 3 as default** (always gossip when `BootstrapExpect > 0`)
- **Option 2 as override** (user can set `AssumeStaticSeeds = true` to skip gossip if they're confident)

```csharp
public class RapidClusterOptions
{
    // ...existing...
    
    /// <summary>
    /// When true, assumes all nodes have identical seed lists and skips
    /// bootstrap seed negotiation. Only set this if you are certain that
    /// all nodes will discover the exact same seeds (e.g., hardcoded list).
    /// Default: false (use seed negotiation to prevent split-brain).
    /// </summary>
    public bool AssumeStaticSeeds { get; set; } = false;
}
```

Or we could flip it - default to assuming static (current behavior) and require opt-in for gossip:

```csharp
/// <summary>
/// When true, uses seed gossip protocol during bootstrap to handle
/// dynamic seed discovery (DNS, service discovery, etc.) and prevent split-brain.
/// Default: false (assumes static seed list).
/// </summary>
public bool UseDynamicSeedDiscovery { get; set; } = false;
```

Which approach do you prefer?

---

## User

Let's just assume it is static and put the onus on the developer

---

## Assistant

Makes sense. So the design simplifies to:

### ISeedProvider Changes

```csharp
/// <summary>
/// Result of seed discovery, indicating whether the seeds are static or dynamic.
/// </summary>
public readonly record struct SeedDiscoveryResult
{
    /// <summary>
    /// The discovered seed endpoints.
    /// </summary>
    public required IReadOnlyList<EndPoint> Seeds { get; init; }
    
    /// <summary>
    /// When true, the seed list is static and guaranteed to be identical across all nodes.
    /// Bootstrap can proceed without seed gossip.
    /// When false, seeds may differ between nodes (e.g., DNS, service discovery),
    /// and seed gossip is required to prevent split-brain.
    /// </summary>
    public required bool IsStatic { get; init; }
}

public interface ISeedProvider
{
    /// <summary>
    /// Discovers seed nodes for cluster formation.
    /// </summary>
    ValueTask<SeedDiscoveryResult> GetSeedsAsync(CancellationToken cancellationToken = default);
}
```

### Provider Implementations

| Provider | `IsStatic` | Rationale |
|----------|------------|-----------|
| `ConfigurationSeedProvider` | `true` | Developer's responsibility to ensure config is identical |
| `ServiceDiscoverySeedProvider` (Aspire) | `false` | DNS/service discovery can vary |
| `FileBasedBootstrapProvider` (Aspire) | `true` | Already coordinates via shared file |

### MembershipService Logic

```csharp
var seedResult = await _seedProvider.GetSeedsAsync(cancellationToken);

if (_bootstrapExpect > 0)
{
    if (seedResult.IsStatic)
    {
        // Current behavior - deterministic bootstrap
        await BootstrapClusterAsync(cancellationToken);
    }
    else
    {
        // Seed gossip required
        await BootstrapWithNegotiationAsync(cancellationToken);
    }
}
```

---

This puts the decision squarely on the `ISeedProvider` implementation author, who knows whether their discovery mechanism is deterministic or not.

Should I create a work item for this and start implementation?

---

## User

continue

---

## Assistant

```
Tool: todowrite
```

---

## Assistant

```
Tool: todowrite
```

---

## Assistant

Let me start by reading the current ISeedProvider interface:

```
Tool: read
```

---

## Assistant

```
Tool: list
```

---

## Assistant

Now let me create the SeedDiscoveryResult and update the interface:

```
Tool: write
```

---

## Assistant

```
Tool: write
```

---

## Assistant

```
Tool: todowrite
```

---

## Assistant

Now I need to update all the providers and the MembershipService. Let me read the files that need updating:

```
Tool: read
```

```
Tool: read
```

```
Tool: read
```

```
Tool: read
```

---

## Assistant

Now I have a complete picture. Let me update all the providers:

```
Tool: write
```

---

## Assistant

```
Tool: todowrite
```

---

## User

Let's not make assumptions about ConfigurationSeedProvider - force it to use bootstrapping. We should create a separate options class for it, RapidClusterSeedOptions. We can create helper methods to configure static seeds using a new StaticSeedProvider

---

## Assistant

Good idea. Let me restructure this properly:

1. `ConfigurationSeedProvider` reads from `RapidClusterSeedOptions` which includes an `IsStatic` flag
2. New `StaticSeedProvider` for hardcoded seeds that are guaranteed static
3. Helper extension methods to configure each scenario

```
Tool: write
```

---

## Assistant

```
Tool: write
```

---

## Assistant

```
Tool: read
```

---

## Assistant

```
Tool: write
```

---

## Assistant

Now let me create the StaticSeedProvider:

```
Tool: write
```

---

## User

The IsStatic option is good - we don't need a separate seed provider in that case

---

## Assistant

You're right - `StaticSeedProvider` is redundant. Users can just set `IsStatic = true` on `RapidClusterSeedOptions`. Let me remove it and continue fixing the remaining files.

```
Tool: bash
```

---

## Assistant

```
Tool: read
```

---

## Assistant

```
Tool: edit
```

---

## Assistant

Now let me update the MembershipService to handle the new `SeedDiscoveryResult`:

```
Tool: read
```

---

## Assistant

Now I need to see more context around how `RefreshSeedsAsync` is used and where `_seedsAreStatic` should be stored:

```
Tool: read
```

---

## Assistant

Let me also check the field declarations at the top of the class:

```
Tool: read
```

---

## Assistant

Now I understand the structure. I need to:
1. Add a `_seedsAreStatic` field
2. Update `RefreshSeedsAsync` to use `SeedDiscoveryResult`
3. Update `InitializeAsync` to branch on `_seedsAreStatic`

```
Tool: edit
```

---

## Assistant

```
Tool: read
```

---

## Assistant

```
Tool: edit
```

---

## Assistant

Now let me update the Aspire sample providers. First the ServiceDiscoverySeedProvider (dynamic):

```
Tool: edit
```

---

## Assistant

Now update the Aspire FileBasedBootstrapProvider (static - it coordinates via file):

```
Tool: edit
```

---

## User

---

## Assistant

## Summary: Implementing Safe Bootstrap Protocol with Seed Gossip for RapidCluster

### What We Did

We designed and started implementing a **seed gossip protocol** to prevent split-brain scenarios when cluster seed nodes are discovered dynamically (via DNS, service discovery, etc.). The problem is that different nodes might see different seed lists at startup, leading to multiple independent clusters forming.

### Key Design Decisions

1. **`ISeedProvider` interface changed** to return `SeedDiscoveryResult` which includes:
   - `Seeds` - the discovered endpoints
   - `IsStatic` - boolean indicating if seeds are guaranteed identical across all nodes

2. **New `RapidClusterSeedOptions` class** with `IsStatic` flag (defaults to `false` for safety)

3. **When `IsStatic = true`**: Use current fast deterministic bootstrap
4. **When `IsStatic = false`**: Use seed gossip protocol to ensure unanimous agreement before forming cluster

5. **New proto messages** (not yet implemented):
   - `BootstrapSeedGossip` - for exchanging known seeds between nodes
   - `BootstrapSeedGossipResponse` - with `BootstrapStatus` enum (NEGOTIATING, AGREED, ALREADY_FORMED)

6. **New `BootstrapCoordinator` class** (not yet implemented) - cleanly separated from `MembershipService`

### Files Modified So Far

- `src/RapidCluster.Core/Discovery/ISeedProvider.cs` - Changed return type to `ValueTask<SeedDiscoveryResult>`
- `src/RapidCluster.Core/Discovery/SeedDiscoveryResult.cs` - **NEW** - Record with `Seeds` and `IsStatic`
- `src/RapidCluster.Core/Discovery/RapidClusterSeedOptions.cs` - **NEW** - Options with `SeedAddresses` and `IsStatic`
- `src/RapidCluster.Core/Discovery/ConfigurationSeedProvider.cs` - Updated to use `RapidClusterSeedOptions`
- `src/RapidCluster.Core/MembershipService.cs` - Added `_seedsAreStatic` field, updated `RefreshSeedsAsync`
- `samples/RapidCluster.Aspire/RapidCluster.Aspire.Node/ServiceDiscoverySeedProvider.cs` - Returns `IsStatic = false`
- `samples/RapidCluster.Aspire/RapidCluster.Aspire.Node/FileBasedBootstrapProvider.cs` - Returns `IsStatic = true`

### Files Still Need Updating (Have Compile Errors)

- `tests/RapidCluster.EndToEnd/RapidCluster.EndToEnd.Node/FileBasedBootstrapProvider.cs` - Needs same update as Aspire version
- `tests/RapidCluster.Unit.Tests/SeedProviderTests.cs` - Tests need updating for new return type
- `tests/RapidCluster.Simulation.Tests/SeedDiscoveryTests.cs` - Tests need updating for new return type

### What Needs To Be Done Next

1. **Fix remaining compile errors** in test files and E2E FileBasedBootstrapProvider

2. **Update `MembershipService.InitializeAsync`** to branch on `_seedsAreStatic`:
   ```csharp
   if (_bootstrapExpect > 0)
   {
       if (_seedsAreStatic)
           await BootstrapClusterAsync(cancellationToken);  // Current behavior
       else
           await BootstrapWithNegotiationAsync(cancellationToken);  // NEW
   }
   ```

3. **Add proto messages** to `src/RapidCluster.Core/Protos/rapid_types.proto`:
   - `BootstrapSeedGossip`
   - `BootstrapSeedGossipResponse`
   - `BootstrapStatus` enum

4. **Create `BootstrapCoordinator`** class in new `src/RapidCluster.Core/Bootstrap/` directory

5. **Create `BootstrapAlreadyCompleteException`**

6. **Upgrade split-brain detection logging** from Debug to Error level in `MembershipService`

7. **Add simulation tests** for dynamic seed scenarios

### Current TODO List State

```
1. [COMPLETED] Create SeedDiscoveryResult record and update ISeedProvider interface
2. [COMPLETED] Update ConfigurationSeedProvider to return SeedDiscoveryResult
3. [PENDING] Add BootstrapSeedGossip and BootstrapSeedGossipResponse to proto
4. [PENDING] Create BootstrapCoordinator class for seed negotiation
5. [PENDING] Create BootstrapAlreadyCompleteException
6. [PENDING] Add BootstrapCoordinator logger
7. [PENDING] Integrate BootstrapCoordinator into MembershipService
8. [COMPLETED] Update ServiceDiscoverySeedProvider (Aspire) to return IsStatic=false
9. [COMPLETED] Update FileBasedBootstrapProvider (Aspire) to return IsStatic=true
10. [PENDING] Upgrade split-brain detection logging from Debug to Error
11. [PENDING] Add simulation tests for dynamic seed scenarios
12. [IN_PROGRESS] Update unit tests for ISeedProvider changes
13. [IN_PROGRESS] Update MembershipService.RefreshSeedsAsync to use SeedDiscoveryResult
```

### Seed Gossip Algorithm (For Reference)

```
1. Start with ClusterId = 0 (special "forming" state)
2. Each node gossips its known seeds to other known seeds
3. Nodes merge received seeds (union), sort, take first BootstrapExpect
4. Once a node has BootstrapExpect seeds, propose the hash of the sorted set
5. All nodes must agree on the same hash before forming
6. Only then compute final ClusterId and form cluster
```

---

## User

---

## Assistant

Let me check the current state of the codebase and the todo directory to see what work items remain.

```
Tool: list
```

```
Tool: bash
```

---

## Assistant

The todo directory is empty and the build succeeds. Let me check what was actually completed by looking at the done directory and checking the current state of the seed gossip implementation:

```
Tool: list
```

```
Tool: glob
```

```
Tool: glob
```

```
Tool: glob
```

---

## Assistant

Let me check the most recent done item (007_EnhancedClusterBootstrapping.md) to see what was completed and what might remain:

```
Tool: read
```

```
Tool: read
```

```
Tool: read
```

---

## Assistant

Good - the foundation is in place. Now let me check the current state of MembershipService to see what seed gossip implementation work remains:

```
Tool: read
```

---

## Assistant

The file is long - let me check the rest and also check if the seed gossip protocol has been implemented:

```
Tool: read
```

```
Tool: glob
```

```
Tool: grep
```

---

## Assistant

Based on my analysis, I can see the current state:

1. **The foundation is in place**: `ISeedProvider` returns `SeedDiscoveryResult` with `IsStatic` flag, and `_seedsAreStatic` is tracked in `MembershipService`.

2. **But the seed gossip protocol is NOT implemented yet**: The `_seedsAreStatic` flag is stored but never used to branch behavior. Currently, `BootstrapClusterAsync` always uses deterministic bootstrap regardless of whether seeds are static.

3. **No `BootstrapCoordinator` class exists** - the proto messages for seed gossip haven't been added.

The summary I provided earlier was the **design/plan** that was discussed but not fully implemented. The actual implementation stopped at:
- Creating the `SeedDiscoveryResult` and updating `ISeedProvider`
- Updating providers to return `IsStatic`
- Storing `_seedsAreStatic` in `MembershipService`

**What remains to be done:**
1. Add branching logic in `BootstrapClusterAsync` based on `_seedsAreStatic`
2. Implement the seed gossip protocol for dynamic seeds
3. Add proto messages for bootstrap negotiation
4. Create `BootstrapCoordinator` class

However, since there are no work items in the `todo/` directory, this work was likely deprioritized or considered complete for now (the deterministic bootstrap works for static seeds).

Let me check if there's anything else pending or if the project is in a working state:

```
Tool: bash
```

---

## Assistant

```
Tool: bash
```

---

## Assistant

There are compile errors in the unit tests. This is the remaining work that needs to be done. Let me fix these test files:

```
Tool: todowrite
```

```
Tool: read
```

```
Tool: read
```

---

## Assistant

I see the issue. The `ConfigurationSeedProvider` now takes `IOptionsMonitor<RapidClusterSeedOptions>` instead of `IOptionsMonitor<RapidClusterOptions>`. Let me also check the `RapidClusterSeedOptions` class:

```
Tool: read
```

---

## Assistant

Now I understand the changes. The tests need to be updated to:
1. Use `RapidClusterSeedOptions` instead of `RapidClusterOptions`
2. Access `result.Seeds` instead of `result` directly (and use `.Count`, `[index]` on the `Seeds` property)
3. Assert `IsStatic` where appropriate

Let me update the test file:

```
Tool: write
```

---

## Assistant

Good progress. Now I need to fix the simulation tests and the E2E FileBasedBootstrapProvider. Let me fix those:

```
Tool: read
```

```
Tool: read
```

---

## Assistant

Now I need to update both files. Let me fix the E2E FileBasedBootstrapProvider first:

```
Tool: edit
```

---

## Assistant

Now let me read more of the simulation tests file to see the full context and fix it:

```
Tool: read
```

---

## Assistant

```
Tool: edit
```

---

## Assistant

Now let me check if there's a using statement for `RapidClusterSeedOptions` needed in the simulation tests file:

```
Tool: read
```

---

## Assistant

The file already has `using RapidCluster.Discovery;` which includes `RapidClusterSeedOptions`. Now let me build to verify all errors are fixed:

```
Tool: bash
```

---

## Assistant

There's one more file to fix - `RapidSimulationNode.cs`:

```
Tool: read
```

---

## Assistant

I need to create a `RapidClusterSeedOptions` from the seed addresses and pass that to `ConfigurationSeedProvider`:

```
Tool: edit
```

---

## Assistant

```
Tool: read
```

---

## Assistant

The file already has the `using RapidCluster.Discovery;` statement. Let me try the edit again by reading more context:

```
Tool: read
```

---

## Assistant

The edit was actually applied. The diagnostic error might be stale. Let me build again to verify:

```
Tool: bash
```

---

## Assistant

Build succeeded. Now let me run the unit tests to verify everything works:

```
Tool: todowrite
```

```
Tool: bash
```

---

## Assistant

Unit tests pass. Now let me run a quick check on the simulation tests to ensure the seed provider tests work:

```
Tool: bash
```

---

## Assistant

All seed discovery tests pass. Let me also run a broader set of simulation tests to make sure the infrastructure works:

```
Tool: bash
```

---

## Assistant

All tests pass. Let me update the todos and provide a summary:

```
Tool: todowrite
```

---

## Assistant

## Summary

The project had some **compile errors** in test files that needed fixing after the `ISeedProvider` interface was updated to return `SeedDiscoveryResult` instead of `IReadOnlyList<EndPoint>`. I fixed these:

### Files Fixed:

1. **`tests/RapidCluster.Unit.Tests/SeedProviderTests.cs`**
   - Updated to use `RapidClusterSeedOptions` instead of `RapidClusterOptions`
   - Changed assertions to access `result.Seeds` property
   - Added new tests for `IsStatic` property behavior

2. **`tests/RapidCluster.EndToEnd/RapidCluster.EndToEnd.Node/FileBasedBootstrapProvider.cs`**
   - Updated return type from `ValueTask<IReadOnlyList<EndPoint>>` to `ValueTask<SeedDiscoveryResult>`
   - Returns `SeedDiscoveryResult.Static([...])` since file-based bootstrap is deterministic

3. **`tests/RapidCluster.Simulation.Tests/SeedDiscoveryTests.cs`**
   - Updated to use `RapidClusterSeedOptions` instead of `RapidClusterOptions`
   - Changed assertions to access `result.Seeds` property

4. **`tests/RapidCluster.Simulation.Tests/Infrastructure/RapidSimulationNode.cs`**
   - Created separate `RapidClusterSeedOptions` for the seed provider with `IsStatic = true`

### Current Project State:
- **Build**: Passing
- **Unit tests**: 509 passing
- **Simulation tests**: Passing (verified `SeedDiscoveryTests` and `ClusterBasicTests`)

### What Was Partially Implemented (Design Only):
The **seed gossip protocol** for dynamic seeds was designed but not fully implemented:
- `SeedDiscoveryResult` with `IsStatic` flag exists
- `_seedsAreStatic` is tracked in `MembershipService`
- But the branching logic to use seed gossip when `IsStatic=false` is NOT implemented
- The `BootstrapCoordinator` class and proto messages were never created

The current implementation uses **deterministic bootstrap** for all cases (works correctly for static seeds). The seed gossip protocol would be a future enhancement for environments with dynamic seed discovery (DNS, service discovery, etc.).

---

## User

Continue

---

## Assistant

Let me check if there are any remaining work items or issues to address:

```
Tool: list
```

```
Tool: bash
```

---

## Assistant

```
Tool: bash
```

---

## User

We must continue implementing the bootstrap seed gossip protocol. Create a work item in the 'todo' directory to detail the work and track progress.

---

## Assistant

```
Tool: write
```

---

## User

Here is some more detail from earlier in our session: Summary: Implementing Safe Bootstrap Protocol with Seed Gossip for RapidCluster
What We Did
We designed and started implementing a seed gossip protocol to prevent split-brain scenarios when cluster seed nodes are discovered dynamically (via DNS, service discovery, etc.). The problem is that different nodes might see different seed lists at startup, leading to multiple independent clusters forming.
Key Design Decisions
1. ISeedProvider interface changed to return SeedDiscoveryResult which includes:
   - Seeds - the discovered endpoints
   - IsStatic - boolean indicating if seeds are guaranteed identical across all nodes
2. New RapidClusterSeedOptions class with IsStatic flag (defaults to false for safety)
3. When IsStatic = true: Use current fast deterministic bootstrap
4. When IsStatic = false: Use seed gossip protocol to ensure unanimous agreement before forming cluster
5. New proto messages (not yet implemented):
   - BootstrapSeedGossip - for exchanging known seeds between nodes
   - BootstrapSeedGossipResponse - with BootstrapStatus enum (NEGOTIATING, AGREED, ALREADY_FORMED)
6. New BootstrapCoordinator class (not yet implemented) - cleanly separated from MembershipService
Files Modified So Far
- src/RapidCluster.Core/Discovery/ISeedProvider.cs - Changed return type to ValueTask<SeedDiscoveryResult>
- src/RapidCluster.Core/Discovery/SeedDiscoveryResult.cs - NEW - Record with Seeds and IsStatic
- src/RapidCluster.Core/Discovery/RapidClusterSeedOptions.cs - NEW - Options with SeedAddresses and IsStatic
- src/RapidCluster.Core/Discovery/ConfigurationSeedProvider.cs - Updated to use RapidClusterSeedOptions
- src/RapidCluster.Core/MembershipService.cs - Added _seedsAreStatic field, updated RefreshSeedsAsync
- samples/RapidCluster.Aspire/RapidCluster.Aspire.Node/ServiceDiscoverySeedProvider.cs - Returns IsStatic = false
- samples/RapidCluster.Aspire/RapidCluster.Aspire.Node/FileBasedBootstrapProvider.cs - Returns IsStatic = true
Files Still Need Updating (Have Compile Errors)
- tests/RapidCluster.EndToEnd/RapidCluster.EndToEnd.Node/FileBasedBootstrapProvider.cs - Needs same update as Aspire version
- tests/RapidCluster.Unit.Tests/SeedProviderTests.cs - Tests need updating for new return type
- tests/RapidCluster.Simulation.Tests/SeedDiscoveryTests.cs - Tests need updating for new return type
What Needs To Be Done Next
1. Fix remaining compile errors in test files and E2E FileBasedBootstrapProvider
2. Update MembershipService.InitializeAsync to branch on _seedsAreStatic:
      if (_bootstrapExpect > 0)
   {
       if (_seedsAreStatic)
           await BootstrapClusterAsync(cancellationToken);  // Current behavior
       else
           await BootstrapWithNegotiationAsync(cancellationToken);  // NEW
   }
   
3. Add proto messages to src/RapidCluster.Core/Protos/rapid_types.proto:
   - BootstrapSeedGossip
   - BootstrapSeedGossipResponse
   - BootstrapStatus enum
4. Create BootstrapCoordinator class in new src/RapidCluster.Core/Bootstrap/ directory
5. Create BootstrapAlreadyCompleteException
6. Upgrade split-brain detection logging from Debug to Error level in MembershipService
7. Add simulation tests for dynamic seed scenarios
Current TODO List State
1. [COMPLETED] Create SeedDiscoveryResult record and update ISeedProvider interface
2. [COMPLETED] Update ConfigurationSeedProvider to return SeedDiscoveryResult
3. [PENDING] Add BootstrapSeedGossip and BootstrapSeedGossipResponse to proto
4. [PENDING] Create BootstrapCoordinator class for seed negotiation
5. [PENDING] Create BootstrapAlreadyCompleteException
6. [PENDING] Add BootstrapCoordinator logger
7. [PENDING] Integrate BootstrapCoordinator into MembershipService
8. [COMPLETED] Update ServiceDiscoverySeedProvider (Aspire) to return IsStatic=false
9. [COMPLETED] Update FileBasedBootstrapProvider (Aspire) to return IsStatic=true
10. [PENDING] Upgrade split-brain detection logging from Debug to Error
11. [PENDING] Add simulation tests for dynamic seed scenarios
12. [IN_PROGRESS] Update unit tests for ISeedProvider changes
13. [IN_PROGRESS] Update MembershipService.RefreshSeedsAsync to use SeedDiscoveryResult
Seed Gossip Algorithm (For Reference)
1. Start with ClusterId = 0 (special "forming" state)
2. Each node gossips its known seeds to other known seeds
3. Nodes merge received seeds (union), sort, take first BootstrapExpect
4. Once a node has BootstrapExpect seeds, propose the hash of the sorted set
5. All nodes must agree on the same hash before forming
6. Only then compute final ClusterId and form cluster 

---

