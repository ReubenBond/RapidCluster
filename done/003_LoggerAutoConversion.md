# Centralize Logger Helper Structs and Simplify Call Sites

## Status: COMPLETED

## Problem

Logger classes required callers to manually wrap application-level types in internal logging structs:

```csharp
// Current - verbose and repetitive
_log.PaxosInitialized(new PaxosLogger.LoggableEndpoint(myAddr), configurationId, membershipSize);
_log.DecidedValue(new PaxosLogger.LoggableEndpoints(proposal.Members.Select(m => m.Endpoint)));
_log.MembershipServiceInitialized(
    new MembershipServiceLogger.LoggableEndpoint(_myAddr),
    new MembershipServiceLogger.CurrentConfigId(_membershipView),
    new MembershipServiceLogger.MembershipSize(_membershipView));
```

Issues:
1. **Duplication** - `LoggableEndpoint` and `LoggableEndpoints` were defined identically in 8 different files
2. **Verbose call sites** - Every log call required `new XxxLogger.LoggableEndpoint(...)` wrapper
3. **Manual extraction** - For `MembershipProposal`, callers had to do `.Members.Select(m => m.Endpoint)` before wrapping
4. **Static utility method** - `RapidClusterUtils.Loggable(Endpoint)` should be an instance method on `Endpoint`

## Implementation Summary

### Approach Chosen: Helper Method Pattern

Instead of removing the wrapper structs entirely, we added **public helper methods** to each logger class that accept raw types and handle the wrapping internally:

```csharp
// Private method with [LoggerMessage] attribute uses wrapper types
[LoggerMessage(EventName = nameof(PaxosInitialized), Level = LogLevel.Debug, Message = "...")]
private partial void PaxosInitializedCore(LoggableEndpoint myAddr, long configurationId, int membershipSize);

// Public method accepts raw types - callers use this
public void PaxosInitialized(Endpoint myAddr, long configurationId, int membershipSize) =>
    PaxosInitializedCore(new(myAddr), configurationId, membershipSize);
```

This approach:
- **Simplifies call sites** - No more manual `new Loggable*()` wrappers
- **Maintains type safety** - Raw types automatically converted to loggable versions
- **Preserves event names** - Using `EventName = nameof(PublicMethod)` ensures logged event names match public API
- **Backward compatible** - Existing tests and external code using `RapidClusterUtils.Loggable()` continues to work

### Files Modified

#### New File
- `src/RapidCluster.Core/Logging/LoggingHelpers.cs` - Shared wrapper structs

#### Endpoint Partial Class  
- `src/RapidCluster.Core/Endpoint.IComparable.cs` - Added `GetNetworkAddressString()` method

#### Logger Classes (added helper methods)
- `src/RapidCluster.Core/Logging/PaxosLogger.cs`
- `src/RapidCluster.Core/Logging/FastPaxosLogger.cs`
- `src/RapidCluster.Core/Logging/ConsensusCoordinatorLogger.cs`
- `src/RapidCluster.Core/Logging/MembershipServiceLogger.cs`

#### Call Sites Updated
- `src/RapidCluster.Core/Paxos.cs`
- `src/RapidCluster.Core/FastPaxos.cs`
- `src/RapidCluster.Core/ConsensusCoordinator.cs`
- `src/RapidCluster.Core/MembershipService.cs`

#### Bug Fix
- `tests/RapidCluster.Tests/Unit/RapidClusterOptionsTests.cs` - Updated tests to use `SeedAddresses` (plural) instead of old `SeedAddress` (singular)

### Not Changed

The following were intentionally left unchanged:
- `RapidClusterUtils.Loggable()` methods - Kept for backward compatibility with tests and external code
- Private `LoggableEndpoint` structs in other classes - Not part of the logger infrastructure

## Example Before/After

**Before:**
```csharp
_log.PaxosInitialized(new PaxosLogger.LoggableEndpoint(_myAddr), _configurationId, _membershipSize);
_log.DecidedValue(new PaxosLogger.LoggableEndpoints(decision.Members.Select(m => m.Endpoint)));
```

**After:**
```csharp
_log.PaxosInitialized(_myAddr, _configurationId, _membershipSize);
_log.DecidedValue(decision);
```

## Testing

All tests pass:
- Build: `dotnet build RapidCluster.slnx` - Success
- Tests: `dotnet run -- --timeout 60s --filter-not-class "*Integration*"` - 829+ tests pass, 3 skipped

## Benefits Achieved

1. **Cleaner call sites** - No more manual wrapper construction
2. **Automatic extraction** - `LoggableMembershipProposal` handles `.Members.Select(m => m.Endpoint)` internally  
3. **Consistent naming** - All wrapper structs follow `Loggable*` naming convention
4. **Instance method** - `Endpoint.GetNetworkAddressString()` is more discoverable than static utility
5. **Backward compatible** - Existing code using `RapidClusterUtils.Loggable()` continues to work
