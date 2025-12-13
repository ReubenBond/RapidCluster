# AGENTS.md - RapidCluster

> **Note**: Keep this file updated as you learn more about the environment, CLI tools, and project conventions.

## Build & Test Commands
```bash
dotnet build RapidCluster/RapidCluster.slnx                    # Build

# Run tests using dotnet run (xUnit v3 with Microsoft.Testing.Platform)
cd RapidCluster/tests/RapidCluster.Tests
dotnet run -- --timeout 60s                                        # Run all tests with 60s timeout
dotnet run -- --timeout 60s --filter-class "*ClusterBasicTests"    # Single test class
dotnet run -- --timeout 60s --filter-method "*TestMethodName"      # Single test method
dotnet run -- --timeout 60s --filter-not-class "*Integration*"     # Exclude integration tests
dotnet run -- --list-tests                                         # List all tests
dotnet run -- --help                                               # Show all options

# Code coverage (using Microsoft.Testing.Extensions.CodeCoverage)
dotnet run -- --coverage --coverage-output-format cobertura --coverage-output coverage.xml --timeout 60s
```

Note: This project uses xUnit v3 with Microsoft.Testing.Platform (MTP). Use `dotnet run --` to run tests, not `dotnet test`. The `--timeout` parameter sets a global test execution timeout (format: `<value>[h|m|s]`).

## Code Coverage
Code coverage is collected using `Microsoft.Testing.Extensions.CodeCoverage`. Use the `--coverage` flag when running tests:
- `--coverage` - Enable code coverage collection
- `--coverage-output-format` - Output format: `coverage`, `xml`, or `cobertura`
- `--coverage-output` - Path to the output coverage file

## CLI Tools
- **Shell**: Use `pwsh` (modern PowerShell), not `cmd` or legacy `powershell`
- **Platform**: Windows
- **ripgrep (rg)**: Use `rg` for searching, not `grep`. Example: `rg "pattern" --type cs`
- **sed**: Available for batch text replacements. Example: `sed -i 's/old/new/g' file.cs`

## Code Style (enforced via .editorconfig)
- **Framework**: .NET 10, nullable enabled, warnings as errors
- **Namespaces**: File-scoped (`namespace Foo;`)
- **Types**: Use `var` always, prefer pattern matching, use collection expressions
- **Braces**: Always use braces, newline before open brace (Allman style)
- **Naming**: `_camelCase` for private fields, `PascalCase` for public/constants, `IPrefix` for interfaces
- **Imports**: Sort System directives first, remove unused usings
- **Async**: Forward CancellationToken, avoid ConfigureAwait
- **Error handling**: Use throw helpers (ArgumentNullException.ThrowIfNull), rethrow to preserve stack
- **Regions**: Do not use `#region`/`#endregion` directives

## Project Structure
- `RapidCluster/src/RapidCluster.Core/` - Main library
- `RapidCluster/src/Clockwork/` - Deterministic simulation testing framework
- `RapidCluster/tests/RapidCluster.Tests/` - xUnit v3 tests (underscores allowed in test names)
  - `Unit/` - Unit tests
  - `Integration/` - Integration tests with real networking
  - `Simulation/` - Deterministic simulation tests using Clockwork

## Work Item Management

Pending work items are stored as markdown files in the `todo/` directory. When completing a work item:

1. **Update the work item file** with completion notes, implementation details, or any relevant findings
2. **Move the file** from `todo/` to `done/` (create the `done/` directory if it doesn't exist)
3. **Check for remaining work items** in the `todo/` directory and start work on them if any exist

Work item files should be named descriptively (e.g., `001_PaxosMonotonicIds.md`) and contain:
- Problem description
- Proposed solution or approach
- Implementation notes (updated during/after completion)

## Testing Infrastructure

This project uses multiple testing techniques to ensure correctness. **New functionality should ideally be tested using multiple techniques** (Unit, Integration, Simulation, and Property-Based tests where applicable).

### Test Types

#### 1. Unit Tests (`tests/RapidCluster.Tests/Unit/`)

Isolated tests for individual components without external dependencies.

**Patterns:**
- Use mock/stub implementations for dependencies (e.g., `TestBroadcaster`)
- Test single classes or methods in isolation
- Fast execution, no I/O or network

**Example:**
```csharp
[Fact]
public async Task DeclaresSuccess_BeforeAllVotesReceived()
{
    var broadcaster = new TestBroadcaster();
    var fastPaxos = new FastPaxos(myAddr, configurationId: 1, membershipSize: 5, broadcaster, NullLogger<FastPaxos>.Instance);
    // ... test logic
}
```

#### 2. Integration Tests (`tests/RapidCluster.Tests/Integration/`)

End-to-end tests using real networking (gRPC over HTTP/2).

**Key Classes:**
- `TestCluster` - Manages node lifecycle, port allocation, logging
- `TestClusterPortAllocator` - Allocates unique ports for test isolation

**Patterns:**
- Use `TestCluster` helper to create and manage nodes
- Use `WaitForClusterSizeAsync()` for convergence checks
- Tests implement `IAsyncDisposable` for cleanup
- Logs are written to files and attached to test context

**Example:**
```csharp
public sealed class ClusterIntegrationTests(ITestOutputHelper outputHelper) : IAsyncDisposable
{
    private readonly TestCluster _cluster = new(outputHelper);

    [Fact]
    public async Task ThreeNodesFormCluster()
    {
        var (seedApp, seed) = await _cluster.CreateSeedNodeAsync(seedAddress, TestContext.Current.CancellationToken);
        var (joiner1App, joiner1) = await _cluster.CreateJoinerNodeAsync(joiner1Address, seedAddress, TestContext.Current.CancellationToken);
        await TestCluster.WaitForClusterSizeAsync(seed, 3, TimeSpan.FromSeconds(10));
    }
}
```

#### 3. Simulation Tests (`tests/RapidCluster.Tests/Simulation/`)

Deterministic tests using the **Clockwork** framework (`src/Clockwork/`). These tests control all sources of non-determinism (time, randomness, network) for reproducible distributed system testing.

**Key Classes:**
- `RapidSimulationCluster` - Main test harness extending `SimulationCluster<T>`
- `RapidSimulationNode` - Simulated cluster node
- `SimulationNetwork` - In-memory network with fault injection
- `ChaosInjector` - Automated fault injection (crashes, partitions)
- `InvariantChecker` - Validates cluster invariants

**Clockwork Features:**
- **Deterministic execution**: Same seed produces identical results
- **Controlled time**: No real delays, time advances instantly
- **Network simulation**: Partitions, message drops, latency injection
- **Chaos testing**: Random fault injection with safety constraints

**Patterns:**
- Tests implement `IAsyncLifetime` with harness setup/teardown
- Use fixed seeds for reproducibility (e.g., `const int TestSeed = 12345`)
- Use `_harness.Run()` to drive async operations to completion
- Use `_harness.WaitForConvergence()` for cluster state assertions
- Use `_harness.RunForDuration()` to advance simulated time

**Example:**
```csharp
public sealed class ClusterBasicTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 12345;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    [Fact]
    public void ThreeNodeClusterFormation()
    {
        var nodes = _harness.CreateCluster(size: 3);
        Assert.All(nodes, node => Assert.Equal(3, node.MembershipSize));
    }

    [Fact]
    public void InvariantsHoldUnderChaos()
    {
        var nodes = _harness.CreateCluster(size: 3);
        var chaos = new ChaosInjector(_harness);
        chaos.PartitionRate = 0.02;
        chaos.RunChaos(steps: 20);
        
        var checker = new InvariantChecker(_harness);
        Assert.True(checker.CheckAll());
    }
}
```

#### 4. Property-Based Tests (using CsCheck)

Tests that verify properties hold across many randomly generated inputs. Uses the [CsCheck](https://github.com/AnthonyLloworthy/CsCheck) library.

**Patterns:**
- Use `Gen.*` to create data generators
- Use `.Sample()` to run property checks
- Properties should return `bool` indicating if the property holds
- Create custom generators for domain types (e.g., `GenUniqueNodes`)

**Example:**
```csharp
using CsCheck;

[Fact]
public void Property_CompareTo_Is_Transitive()
{
    Gen.Select(Gen.Int[0, 100], Gen.Int[0, 100], Gen.Int[0, 100],
               Gen.Int[0, 100], Gen.Int[0, 100], Gen.Int[0, 100])
        .Sample((r1, n1, r2, n2, r3, n3) =>
        {
            var rank1 = new Rank { Round = r1, NodeIndex = n1 };
            var rank2 = new Rank { Round = r2, NodeIndex = n2 };
            var rank3 = new Rank { Round = r3, NodeIndex = n3 };

            // If rank1 <= rank2 and rank2 <= rank3, then rank1 <= rank3
            if (rank1.CompareTo(rank2) <= 0 && rank2.CompareTo(rank3) <= 0)
            {
                return rank1.CompareTo(rank3) <= 0;
            }
            return true;
        });
}

// Custom generator for unique nodes
private static Gen<(int[], int[], long[])> GenUniqueNodes(int minCount, int maxCount) =>
    Gen.Int[minCount, maxCount].SelectMany(count =>
        Gen.Select(
            Gen.Int[1, 255].Array[count].Where(a => a.Distinct().Count() == count),
            Gen.Int[1000, 65535].Array[count],
            Gen.Long.Array[count].Where(a => a.Distinct().Count() == count)));
```

### Testing Guidelines

1. **New features should have multiple test types**: Unit tests for logic, simulation tests for distributed behavior, integration tests for real networking, and property-based tests for invariants.

2. **Simulation tests are preferred** for distributed protocol testing because they are:
   - Deterministic and reproducible
   - Fast (no real I/O delays)
   - Comprehensive (can test edge cases like partitions)

3. **Use meaningful seeds** and log them for reproduction:
   ```csharp
   _harness.LogSeedForReproduction();
   ```

4. **Test naming**: Use underscores in test names for readability (e.g., `Property_CompareTo_Is_Transitive`).

5. **Invariant checking**: Use `InvariantChecker` in simulation tests to verify safety properties under chaos.

## Debugging Tips
- **Log files**: Simulation tests produce log files that can be very large (multi-MB). When reading log files, always use tools to limit the amount of text read at once (e.g., `Get-Content -Tail 100` or `Get-Content -Head 100` in PowerShell, or use offset/limit parameters with Read tool).
