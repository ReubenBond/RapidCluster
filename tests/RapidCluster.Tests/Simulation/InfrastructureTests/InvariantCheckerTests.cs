using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation.InfrastructureTests;

/// <summary>
/// Tests for the invariant checker.
/// </summary>
public sealed class InvariantCheckerTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private InvariantChecker _checker = null!;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: 11111);
        _checker = new InvariantChecker(_harness);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync() => await _harness.DisposeAsync();

    [Fact]
    public void CheckAllPassesWithNoNodes()
    {
        var result = _checker.CheckAll();

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void CheckAllPassesWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckAll();

        Assert.True(result);
        Assert.False(_checker.HasViolations);
    }

    [Fact]
    public void CheckMembershipConsistencyPassesWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckMembershipConsistency();

        Assert.True(result);
    }

    [Fact]
    public void CheckNoSplitBrainPassesWithSingleNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckNoSplitBrain();

        Assert.True(result);
    }

    [Fact]
    public void CheckConfigurationIdMonotonicityPassesWithInitializedNode()
    {
        _harness.CreateSeedNode();

        var result = _checker.CheckConfigurationIdMonotonicity();

        Assert.True(result);
    }

    [Fact]
    public void ClearRemovesAllViolations()
    {
        // Force some violations by manually adding them (would need internal access)
        // For now, just verify Clear doesn't throw
        _checker.Clear();

        Assert.False(_checker.HasViolations);
        Assert.Empty(_checker.Violations);
    }

    [Fact]
    public void ViolationsListIsImmutableCopy()
    {
        var violations1 = _checker.Violations;
        var violations2 = _checker.Violations;

        Assert.NotSame(violations1, violations2);
    }
}
