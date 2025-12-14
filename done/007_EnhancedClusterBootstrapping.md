# 007: Enhanced Cluster Bootstrapping

## Problem Description

RapidCluster currently only supports static seed addresses via `RapidClusterOptions.SeedAddresses`. This limits flexibility in dynamic environments where seed nodes may not be known at compile time or may change during the cluster lifecycle.

## Research Summary

We researched bootstrapping mechanisms in etcd and Consul:

**etcd:**
- Static `--initial-cluster` with explicit member list
- etcd Discovery Service (uses existing etcd cluster)
- DNS SRV queries for `_etcd-server._tcp.domain`

**Consul:**
- `bootstrap_expect`: Waits for N servers before electing leader
- Static `retry-join` with retry logic
- Cloud Auto-Join: Uses cloud provider APIs (AWS, Azure, GCP, K8s) to discover by tags

**.NET Service Discovery (`Microsoft.Extensions.ServiceDiscovery`):**
- Configuration-based endpoint resolution from `IConfiguration`
- Pass-through resolver (returns service name as DnsEndPoint)
- DNS SRV provider in separate package

## Proposed Solution

Implement a pluggable seed provider architecture with multiple built-in providers and an optional integration package for `Microsoft.Extensions.ServiceDiscovery`.

### Key Design Decisions

1. **Pluggable `ISeedProvider` abstraction** - Core interface for seed discovery
2. **Backward compatible** - Existing `SeedAddresses` configuration continues to work
3. **Optional Service Discovery package** - `Microsoft.Extensions.ServiceDiscovery` in separate NuGet package
4. **Refresh on failure** - Re-query providers when rejoin attempts fail
5. **BootstrapExpect** - Wait for N nodes before starting consensus
6. **No Kubernetes/DNS SRV initially** - Can be added later via custom providers

## API Design

### Phase 1: ISeedProvider Abstraction

```csharp
// Core interface
public interface ISeedProvider
{
    /// <summary>
    /// Gets the current list of seed endpoints.
    /// Called on startup and when rejoin fails.
    /// </summary>
    ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default);
}

// Static provider (default, backward compatible)
public sealed class StaticSeedProvider : ISeedProvider
{
    private readonly IReadOnlyList<Endpoint> _seeds;
    
    public StaticSeedProvider(IReadOnlyList<Endpoint> seeds) => _seeds = seeds;
    public StaticSeedProvider(IOptions<RapidClusterOptions> options) 
        => _seeds = options.Value.SeedAddresses;
    
    public ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
        => ValueTask.FromResult(_seeds);
}
```

### Phase 2: Configuration-based Provider

```csharp
// Reads seeds from IConfiguration
public sealed class ConfigurationSeedProvider : ISeedProvider
{
    private readonly IConfiguration _configuration;
    private readonly string _sectionName;
    
    public ConfigurationSeedProvider(IConfiguration configuration, string sectionName = "RapidCluster:Seeds")
    {
        _configuration = configuration;
        _sectionName = sectionName;
    }
    
    public ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var section = _configuration.GetSection(_sectionName);
        var seeds = section.Get<List<string>>() ?? [];
        return ValueTask.FromResult<IReadOnlyList<Endpoint>>(
            seeds.Select(Endpoint.Parse).ToList());
    }
}
```

**appsettings.json example:**
```json
{
  "RapidCluster": {
    "Seeds": [
      "192.168.1.10:5000",
      "192.168.1.11:5000",
      "192.168.1.12:5000"
    ]
  }
}
```

### Phase 3: BootstrapExpect

```csharp
// Updated RapidClusterOptions
public class RapidClusterOptions
{
    // Existing properties...
    
    /// <summary>
    /// Number of nodes expected to form the initial cluster.
    /// The cluster will wait until this many nodes have joined before
    /// starting consensus. Set to 0 to disable (default).
    /// </summary>
    public int BootstrapExpect { get; set; } = 0;
    
    /// <summary>
    /// Timeout for waiting for BootstrapExpect nodes to join.
    /// Default: 5 minutes.
    /// </summary>
    public TimeSpan BootstrapTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
```

### Phase 4: Service Discovery Integration (Separate Package)

**New project: `RapidCluster.ServiceDiscovery`**

```csharp
// In RapidCluster.ServiceDiscovery package
public sealed class ServiceDiscoverySeedProvider : ISeedProvider
{
    private readonly ServiceEndpointResolver _resolver;
    private readonly string _serviceName;
    
    public ServiceDiscoverySeedProvider(
        ServiceEndpointResolver resolver,
        string serviceName = "rapidcluster")
    {
        _resolver = resolver;
        _serviceName = serviceName;
    }
    
    public async ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var endpoints = await _resolver.GetEndpointsAsync(_serviceName, cancellationToken);
        return endpoints.Endpoints
            .Select(e => new Endpoint { Host = e.EndPoint.Host, Port = e.EndPoint.Port })
            .ToList();
    }
}

// Extension method for easy registration
public static class ServiceDiscoveryExtensions
{
    public static IServiceCollection AddRapidClusterServiceDiscovery(
        this IServiceCollection services,
        string serviceName = "rapidcluster")
    {
        services.AddServiceDiscovery();
        services.AddSingleton<ISeedProvider>(sp =>
            new ServiceDiscoverySeedProvider(
                sp.GetRequiredService<ServiceEndpointResolver>(),
                serviceName));
        return services;
    }
}
```

### Phase 5: MembershipService Updates

```csharp
// In MembershipService - update rejoin logic
private async Task RejoinClusterAsync(CancellationToken cancellationToken)
{
    var attempts = 0;
    while (attempts < _options.MaxRejoinAttempts)
    {
        try
        {
            // Existing rejoin logic...
            return;
        }
        catch (Exception ex)
        {
            attempts++;
            _logger.LogWarning(ex, "Rejoin attempt {Attempt} failed", attempts);
            
            if (attempts >= _options.MaxRejoinAttempts)
            {
                // Re-query seed provider for fresh seeds
                _logger.LogInformation("Max rejoin attempts reached, refreshing seeds");
                var freshSeeds = await _seedProvider.GetSeedsAsync(cancellationToken);
                _currentSeeds = freshSeeds;
                attempts = 0; // Reset attempts with new seeds
            }
            
            await Task.Delay(_options.RejoinDelay, cancellationToken);
        }
    }
}
```

## File Structure

```
src/
  RapidCluster.Core/
    Discovery/
      ISeedProvider.cs
      StaticSeedProvider.cs
      ConfigurationSeedProvider.cs
      CompositeSeedProvider.cs  # Combines multiple providers
    RapidClusterOptions.cs      # Add BootstrapExpect
    MembershipService.cs        # Update to use ISeedProvider
    
  RapidCluster.ServiceDiscovery/
    RapidCluster.ServiceDiscovery.csproj
    ServiceDiscoverySeedProvider.cs
    ServiceDiscoveryExtensions.cs
```

## DI Registration

```csharp
// Default registration (backward compatible)
services.AddRapidCluster(options => {
    options.SeedAddresses = [...];
});
// Automatically registers StaticSeedProvider

// Configuration-based
services.AddRapidCluster()
    .AddConfigurationSeeds("RapidCluster:Seeds");

// Service Discovery (requires RapidCluster.ServiceDiscovery package)
services.AddRapidCluster()
    .AddRapidClusterServiceDiscovery("my-cluster");

// Custom provider
services.AddRapidCluster()
    .AddSeedProvider<MyCustomSeedProvider>();
```

## Testing Strategy

### Unit Tests
- `StaticSeedProvider` returns configured seeds
- `ConfigurationSeedProvider` reads from IConfiguration
- `CompositeSeedProvider` aggregates from multiple providers
- `BootstrapExpect` validation (must be >= 0)

### Simulation Tests
- Cluster forms correctly with BootstrapExpect
- Cluster waits for expected nodes before consensus
- Seeds refresh correctly when rejoin fails
- Multiple seed providers aggregate correctly

### Integration Tests
- Configuration reload updates seeds
- Service discovery integration works end-to-end

## Backward Compatibility

- `RapidClusterOptions.SeedAddresses` continues to work unchanged
- If no `ISeedProvider` is registered, `StaticSeedProvider` is used automatically
- Existing code requires no changes

## Implementation Phases

| Phase | Description | Effort |
|-------|-------------|--------|
| 1 | `ISeedProvider` + `StaticSeedProvider` | Small |
| 2 | `ConfigurationSeedProvider` | Small |
| 3 | `BootstrapExpect` feature | Medium |
| 4 | `RapidCluster.ServiceDiscovery` package | Medium |
| 5 | MembershipService refresh on failure | Small |

## Implementation Notes

### Completed Implementation (Phases 1, 2, 3, 5)

#### Phase 1: ISeedProvider Abstraction
- Created `ISeedProvider` interface in `src/RapidCluster.Core/Discovery/ISeedProvider.cs`
- Created `StaticSeedProvider` in `src/RapidCluster.Core/Discovery/StaticSeedProvider.cs`
  - Default provider for backward compatibility
  - Reads from `RapidClusterOptions.SeedAddresses`
  - Two constructors: one with explicit seed list, one with `IOptions<RapidClusterOptions>`

#### Phase 2: ConfigurationSeedProvider
- Created `ConfigurationSeedProvider` in `src/RapidCluster.Core/Discovery/ConfigurationSeedProvider.cs`
  - Reads seeds from `IConfiguration` (e.g., appsettings.json)
  - Parses endpoint strings in format "hostname:port"
  - Uses `ByteString.CopyFromUtf8(hostname)` for Endpoint.Hostname (protobuf bytes type)

#### Phase 3: BootstrapExpect
- Added `BootstrapExpect` property to `RapidClusterOptions` (int, default 0)
- Added `BootstrapTimeout` property to `RapidClusterOptions` (TimeSpan, default 5 minutes)
- Added XML documentation explaining the feature
- Note: Actual wait-for-N-nodes logic not yet implemented in MembershipService

#### Phase 5: MembershipService Updates
- Updated `MembershipService` constructor to accept `ISeedProvider seedProvider` parameter
- Added `RefreshSeedsAsync()` method to fetch seeds from provider
- Updated `InitializeAsync()` to call `RefreshSeedsAsync()` before join
- Refactored `RejoinClusterAsync()` to:
  - Extract `TryRejoinWithCurrentSeedsAsync()` helper method
  - Call `RefreshSeedsAsync()` and retry if initial rejoin fails
- Added log messages: `SeedsRefreshed`, `RefreshingSeedsForRejoin`

#### DI Registration
- Updated `RapidClusterServiceCollectionExtensions.cs`:
  - Default registration: `services.TryAddSingleton<ISeedProvider, StaticSeedProvider>()`
  - `AddRapidClusterSeedProvider<T>()` - register custom provider type
  - `AddRapidClusterSeedProvider(ISeedProvider)` - register provider instance
  - `AddRapidClusterConfigurationSeeds(string sectionName)` - register ConfigurationSeedProvider

#### Test Infrastructure
- Updated `RapidSimulationNode.cs` to create `StaticSeedProvider` for simulation tests

### Not Implemented (Deferred)

#### Phase 4: Service Discovery Integration
- `RapidCluster.ServiceDiscovery` package not created
- `ServiceDiscoverySeedProvider` not implemented
- Can be added later as a separate NuGet package

#### BootstrapExpect Wait Logic
- `BootstrapExpect` and `BootstrapTimeout` options are defined but the actual wait-for-N-nodes logic is not yet implemented in `MembershipService.InitializeAsync()`
- This can be added in a future iteration if needed

### Usage Examples

```csharp
// Default (backward compatible) - uses SeedAddresses from options
services.AddRapidCluster(options => {
    options.SeedAddresses = [...];
});

// Configuration-based seeds from appsettings.json
services.AddRapidCluster()
    .AddRapidClusterConfigurationSeeds("RapidCluster:Seeds");

// Custom seed provider
services.AddRapidCluster()
    .AddRapidClusterSeedProvider<MyCustomSeedProvider>();
```
