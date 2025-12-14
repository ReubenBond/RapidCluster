# 008: Clean Public API Surface - Remove Protobuf Types

## Problem Description

RapidCluster currently exposes protobuf-generated types (`Endpoint`, `Metadata`, `NodeId`, `EdgeStatus`, `JoinStatusCode`) in its public API surface. This is problematic because:

1. **Leaky abstraction**: Internal implementation details (protobuf) are exposed to consumers
2. **Poor developer experience**: Protobuf types like `ByteString` for hostnames are awkward to use
3. **Coupling**: Changes to wire protocol require API changes
4. **Non-idiomatic**: .NET developers expect standard types like `System.Net.EndPoint`

Additionally, the `IRapidCluster` interface has accumulated methods that should be internal or removed:
- `LeaveGracefullyAsync()` - lifecycle managed by `RapidClusterHostedService`
- `GetMembershipSize()` / `GetMemberlist()` - available via `ViewAccessor`
- `EventStream` / `Events` - internal testing concerns

## Proposed Solution

Create a clean public API that:
1. Uses `System.Net.EndPoint` for network addresses
2. Uses a custom `ClusterMetadata` wrapper for metadata
3. Introduces `NodeStatus` enum (`Available`/`Unavailable`) to replace `EdgeStatus`
4. Creates `ClusterMembershipView` as the public-facing view type
5. Makes `IRapidCluster` a simplified public interface mirroring `IMembershipViewAccessor`
6. Makes `IMembershipViewAccessor` internal-only (uses protobuf types internally)

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Endpoint type | `System.Net.EndPoint` | Standard .NET type, supports both `IPEndPoint` and `DnsEndPoint` |
| Metadata type | Custom `ClusterMetadata` class | Wraps `IReadOnlyDictionary<string, ReadOnlyMemory<byte>>` |
| NodeId exposure | Internal only | May change implementation later |
| Status enum | New `NodeStatus` enum | `Available`/`Unavailable` - cleaner than `EdgeStatus.UP/DOWN` |
| MembershipView | New `ClusterMembershipView` | Minimal public surface with `EndPoint` types |
| Breaking changes | Clean break | No `[Obsolete]` attributes or transition period |
| IsSafeToJoin | Internal only | Protocol implementation detail |
| Utility methods | Remove | Users create `EndPoint` instances directly |
| BroadcastChannel | Remain public | Useful generic type for consumers |
| ConfigurationId | Remain public | Already a proper .NET type |
| Exceptions | Remain public | Users may want to catch them |

## API Design

### New Public Types

#### ClusterMetadata

```csharp
/// <summary>
/// Represents metadata associated with a cluster node.
/// </summary>
public sealed class ClusterMetadata : IReadOnlyDictionary<string, ReadOnlyMemory<byte>>
{
    private readonly Dictionary<string, ReadOnlyMemory<byte>> _data;
    
    internal ClusterMetadata(Dictionary<string, ReadOnlyMemory<byte>> data);
    
    // IReadOnlyDictionary implementation
    public ReadOnlyMemory<byte> this[string key] { get; }
    public IEnumerable<string> Keys { get; }
    public IEnumerable<ReadOnlyMemory<byte>> Values { get; }
    public int Count { get; }
    public bool ContainsKey(string key);
    public bool TryGetValue(string key, out ReadOnlyMemory<byte> value);
    public IEnumerator<KeyValuePair<string, ReadOnlyMemory<byte>>> GetEnumerator();
    
    // Convenience methods
    public string? GetString(string key);
    public static ClusterMetadata Empty { get; }
}
```

#### NodeStatus

```csharp
/// <summary>
/// Represents the status of a node in the cluster.
/// </summary>
public enum NodeStatus
{
    /// <summary>The node has joined and is available in the cluster.</summary>
    Available,
    
    /// <summary>The node has left or been removed from the cluster.</summary>
    Unavailable
}
```

#### ClusterMembershipView

```csharp
/// <summary>
/// An immutable snapshot of the cluster membership at a point in time.
/// </summary>
public sealed class ClusterMembershipView : 
    IEquatable<ClusterMembershipView>, 
    IComparable<ClusterMembershipView>
{
    /// <summary>
    /// An empty membership view with no members.
    /// </summary>
    public static ClusterMembershipView Empty { get; }
    
    /// <summary>
    /// Gets the set of member endpoints in the cluster.
    /// </summary>
    public ImmutableHashSet<EndPoint> Members { get; }
    
    /// <summary>
    /// Gets the configuration identifier for this view.
    /// </summary>
    public ConfigurationId ConfigurationId { get; }
    
    /// <summary>
    /// Gets the metadata for all members in the cluster.
    /// </summary>
    public IReadOnlyDictionary<EndPoint, ClusterMetadata> Metadata { get; }
    
    // IComparable<ClusterMembershipView> - compares by ConfigurationId
    public int CompareTo(ClusterMembershipView? other);
    
    // IEquatable<ClusterMembershipView> - equals by ConfigurationId
    public bool Equals(ClusterMembershipView? other);
    public override bool Equals(object? obj);
    public override int GetHashCode();
    
    // Operators
    public static bool operator ==(ClusterMembershipView? left, ClusterMembershipView? right);
    public static bool operator !=(ClusterMembershipView? left, ClusterMembershipView? right);
    public static bool operator <(ClusterMembershipView? left, ClusterMembershipView? right);
    public static bool operator >(ClusterMembershipView? left, ClusterMembershipView? right);
    public static bool operator <=(ClusterMembershipView? left, ClusterMembershipView? right);
    public static bool operator >=(ClusterMembershipView? left, ClusterMembershipView? right);
}
```

### Updated IRapidCluster Interface

```csharp
/// <summary>
/// Provides access to the Rapid cluster membership information.
/// </summary>
public interface IRapidCluster
{
    /// <summary>
    /// Gets the current membership view.
    /// </summary>
    ClusterMembershipView CurrentView { get; }
    
    /// <summary>
    /// Gets a reader for subscribing to view changes.
    /// Yields a new ClusterMembershipView each time consensus is reached.
    /// </summary>
    BroadcastChannelReader<ClusterMembershipView> ViewUpdates { get; }
}
```

### Updated RapidClusterOptions

```csharp
public sealed class RapidClusterOptions
{
    /// <summary>
    /// The endpoint this node listens on.
    /// Use DnsEndPoint for hostname:port or IPEndPoint for IP addresses.
    /// </summary>
    public EndPoint ListenAddress { get; set; } = null!;
    
    /// <summary>
    /// The seed node endpoints to join.
    /// </summary>
    public IReadOnlyList<EndPoint>? SeedAddresses { get; set; }
    
    /// <summary>
    /// Metadata for this node.
    /// </summary>
    public Dictionary<string, byte[]> Metadata { get; set; } = new();
    
    // BootstrapExpect and BootstrapTimeout remain unchanged
}
```

### Updated ISeedProvider Interface

```csharp
public interface ISeedProvider
{
    ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default);
}
```

### Internal IMembershipViewAccessor (unchanged, becomes internal)

```csharp
internal interface IMembershipViewAccessor
{
    MembershipView CurrentView { get; }
    BroadcastChannelReader<MembershipView> Updates { get; }
}
```

## Files to Modify

### New Files to Create

| File | Description |
|------|-------------|
| `src/RapidCluster.Core/ClusterMetadata.cs` | Public metadata wrapper class |
| `src/RapidCluster.Core/NodeStatus.cs` | Public enum for node status |
| `src/RapidCluster.Core/ClusterMembershipView.cs` | Public membership view type |
| `src/RapidCluster.Core/EndPointComparer.cs` | IEqualityComparer for EndPoint (needed for dictionaries) |
| `src/RapidCluster.Core/EndPointConversions.cs` | Internal conversion helpers |

### Files to Modify

| File | Changes |
|------|---------|
| `src/RapidCluster.Core/IRapidCluster.cs` | Simplify to `CurrentView` + `ViewUpdates` only |
| `src/RapidCluster.Core/IMembershipViewAccessor.cs` | Make internal |
| `src/RapidCluster.Core/RapidClusterOptions.cs` | Change to use `EndPoint`, `Dictionary<string, byte[]>` |
| `src/RapidCluster.Core/Discovery/ISeedProvider.cs` | Return `IReadOnlyList<EndPoint>` |
| `src/RapidCluster.Core/Discovery/ConfigurationSeedProvider.cs` | Return `EndPoint` types |
| `src/RapidCluster.Core/RapidClusterUtils.cs` | Remove `HostFromString`/`HostFromParts`, keep internal conversion helpers |
| `src/RapidCluster.Core/MembershipService.cs` | Internal conversion between protobuf and public types |
| `src/RapidCluster.Core/MembershipViewAccessor.cs` | Publish `ClusterMembershipView` alongside internal view |
| `src/RapidCluster.Core/RapidClusterServiceCollectionExtensions.cs` | Update DI registration |
| `src/RapidCluster.Core/RapidClusterHostingExtensions.cs` | Update if needed |

### Files to Make Internal

| File | Action |
|------|--------|
| `src/RapidCluster.Core/NodeStatusChange.cs` | Make internal |
| `src/RapidCluster.Core/ClusterStatusChange.cs` | Make internal |
| `src/RapidCluster.Core/ClusterEvents.cs` | Make internal (enum + record) |

### Exception Updates

| File | Changes |
|------|---------|
| `src/RapidCluster.Core/Exceptions/NodeNotInRingException.cs` | Accept `EndPoint` instead of `Endpoint` |
| `src/RapidCluster.Core/Exceptions/NodeAlreadyInRingException.cs` | Accept `EndPoint` instead of `Endpoint` |
| `src/RapidCluster.Core/Exceptions/UuidAlreadySeenException.cs` | Accept `EndPoint`, remove `NodeId` parameter |

### Test Updates

| Directory | Changes |
|-----------|---------|
| `tests/RapidCluster.Tests/Unit/` | Update to use new public API types |
| `tests/RapidCluster.Tests/Integration/` | Update to use `EndPoint`, view updates |
| `tests/RapidCluster.Tests/Simulation/` | Update infrastructure to use new types |

### Sample Updates

| File | Changes |
|------|---------|
| `samples/RapidCluster.Examples/Program.cs` | Use `DnsEndPoint`, subscribe to `ViewUpdates` |

## Internal Conversion Helpers

Create internal extension methods for converting between protobuf and public types:

```csharp
internal static class EndPointConversions
{
    internal static Endpoint ToProtobuf(this EndPoint endPoint)
    {
        return endPoint switch
        {
            DnsEndPoint dns => new Endpoint 
            { 
                Hostname = ByteString.CopyFromUtf8(dns.Host), 
                Port = dns.Port 
            },
            IPEndPoint ip => new Endpoint 
            { 
                Hostname = ByteString.CopyFromUtf8(ip.Address.ToString()), 
                Port = ip.Port 
            },
            _ => throw new ArgumentException($"Unsupported EndPoint type: {endPoint.GetType()}")
        };
    }
    
    internal static EndPoint ToEndPoint(this Endpoint endpoint)
    {
        var host = endpoint.Hostname.ToStringUtf8();
        return new DnsEndPoint(host, endpoint.Port);
    }
}

internal static class MetadataConversions
{
    internal static ClusterMetadata ToClusterMetadata(this Metadata metadata)
    {
        var dict = metadata.Metadata_
            .ToDictionary(
                kvp => kvp.Key, 
                kvp => (ReadOnlyMemory<byte>)kvp.Value.ToByteArray());
        return new ClusterMetadata(dict);
    }
    
    internal static Metadata ToProtobuf(this Dictionary<string, byte[]> metadata)
    {
        var pb = new Metadata();
        foreach (var kvp in metadata)
        {
            pb.Metadata_.Add(kvp.Key, ByteString.CopyFrom(kvp.Value));
        }
        return pb;
    }
}
```

## Implementation Phases

| Phase | Description | Effort |
|-------|-------------|--------|
| 1 | Create new public types (`ClusterMetadata`, `NodeStatus`, `ClusterMembershipView`, `EndPointComparer`) | Medium |
| 2 | Create internal conversion helpers | Small |
| 3 | Update `IRapidCluster` and make `IMembershipViewAccessor` internal | Small |
| 4 | Update `RapidClusterOptions` and `ISeedProvider` | Small |
| 5 | Update `MembershipService` and `MembershipViewAccessor` | Medium |
| 6 | Update exceptions | Small |
| 7 | Make event types internal (`NodeStatusChange`, `ClusterStatusChange`, `ClusterEvents`) | Small |
| 8 | Remove/internalize `RapidClusterUtils` public methods | Small |
| 9 | Update unit tests | Medium |
| 10 | Update integration tests | Medium |
| 11 | Update simulation tests | Large |
| 12 | Update sample application | Small |

## Testing Strategy

### Unit Tests

#### New Tests to Create

| Test Class | Tests |
|------------|-------|
| `ClusterMetadataTests.cs` | - Empty metadata returns empty dictionary<br>- Indexer returns correct value<br>- TryGetValue returns false for missing key<br>- GetString decodes UTF-8 correctly<br>- GetString returns null for missing key<br>- Count returns correct value<br>- Keys/Values enumerables work correctly |
| `ClusterMembershipViewTests.cs` | - Empty view has no members<br>- Equals compares by ConfigurationId<br>- CompareTo orders by ConfigurationId<br>- Operators (==, !=, <, >, <=, >=) work correctly<br>- GetHashCode is consistent with Equals<br>- Members returns immutable set<br>- Metadata dictionary is accessible |
| `EndPointComparerTests.cs` | - DnsEndPoint equality by host and port<br>- IPEndPoint equality by address and port<br>- Different types are not equal<br>- GetHashCode is consistent<br>- Null handling |
| `EndPointConversionsTests.cs` | - DnsEndPoint converts to protobuf correctly<br>- IPEndPoint converts to protobuf correctly<br>- Protobuf Endpoint converts to DnsEndPoint<br>- Round-trip conversion preserves values<br>- Unsupported EndPoint type throws |

#### Existing Tests to Update

| Test Class | Changes |
|------------|---------|
| `RapidClusterOptionsTests.cs` | Update to use `EndPoint` types |
| `RapidClusterUtilsTests.cs` | Remove tests for removed methods, add conversion tests |
| `SeedProviderTests.cs` | Update to use `EndPoint` types |
| `MembershipViewTests.cs` | Keep internal tests, may need to access via InternalsVisibleTo |

### Integration Tests

#### New Tests to Add

| Test | Description |
|------|-------------|
| `ClusterFormation_WithDnsEndPoint` | Verify cluster forms using `DnsEndPoint` for options |
| `ViewUpdates_ReceivedOnMembershipChange` | Verify `IRapidCluster.ViewUpdates` yields views on join/leave |
| `Metadata_PropagatedThroughPublicApi` | Verify metadata is accessible via `ClusterMembershipView.Metadata` |
| `CurrentView_ReflectsClusterState` | Verify `IRapidCluster.CurrentView` reflects current membership |

#### Existing Tests to Update

| Test Class | Changes |
|------------|---------|
| `ClusterIntegrationTests.cs` | Replace `Endpoint` with `EndPoint`, use `ViewUpdates` |
| `SubscriptionsTests.cs` | Update to use public API types |
| `TestCluster.cs` | Update helper methods to use `EndPoint` |

### Simulation Tests

#### Infrastructure Updates

| File | Changes |
|------|---------|
| `RapidSimulationNode.cs` | Use `EndPoint` in public interfaces |
| `RapidSimulationCluster.cs` | Update to use new types |
| `SimulationNetwork.cs` | May need internal access to protobuf types |

#### Test Updates

All simulation tests that access membership information need updates to use the new public API types. The internal `MembershipView` can still be used for protocol-level assertions via `InternalsVisibleTo`.

## Backward Compatibility

**This is a breaking change.** There is no deprecation period.

Users must:
1. Replace `Endpoint` with `System.Net.DnsEndPoint` or `System.Net.IPEndPoint`
2. Replace `RapidClusterUtils.HostFromString()` with `new DnsEndPoint(host, port)`
3. Replace `cluster.EventStream` with `cluster.ViewUpdates`
4. Replace `cluster.GetMemberlist()` with `cluster.CurrentView.Members`
5. Replace `cluster.GetClusterMetadata()` with `cluster.CurrentView.Metadata`
6. Replace `cluster.LeaveGracefullyAsync()` with stopping the hosted service
7. Update metadata access to use `ClusterMetadata` type

## Migration Example

**Before:**
```csharp
var listen = RapidClusterUtils.HostFromString("127.0.0.1:5000");
var seed = RapidClusterUtils.HostFromString("127.0.0.1:5001");

services.AddRapidCluster(options =>
{
    options.ListenAddress = listen;
    options.SeedAddresses = [seed];
    options.Metadata = new Metadata();
});

// In consumer code
var members = cluster.GetMemberlist();
await foreach (var evt in cluster.EventStream)
{
    if (evt.Event == ClusterEvents.ViewChange)
    {
        Console.WriteLine($"Members: {evt.Change.Membership.Count}");
    }
}
```

**After:**
```csharp
// Use IPEndPoint for IP addresses, DnsEndPoint for hostnames
var listen = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5000);
var seed = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 5001);

services.AddRapidCluster(options =>
{
    options.ListenAddress = listen;
    options.SeedAddresses = [seed];
    options.Metadata = new Dictionary<string, byte[]>();
});

// In consumer code
var members = cluster.CurrentView.Members;
await foreach (var view in cluster.ViewUpdates)
{
    Console.WriteLine($"Members: {view.Members.Count}");
}
```

## Implementation Notes

(To be updated during implementation)
