using Microsoft.Extensions.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure.Logging;

/// <summary>
/// High-performance logger for SimulationHarness using LoggerMessage source generators.
/// </summary>
internal sealed partial class SimulationHarnessLogger(ILogger<RapidSimulationCluster> logger)
{
    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger { get; } = logger;

    [LoggerMessage(Level = LogLevel.Debug, Message = "Harness created with seed {Seed}")]
    public partial void HarnessCreated(int seed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {NodeId} created (uninitialized)")]
    public partial void UninitializedNodeCreated(int nodeId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Seed node {NodeId} created")]
    public partial void SeedNodeCreated(int nodeId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {NodeId} joining via seed")]
    public partial void NodeJoining(int nodeId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {NodeId} joined cluster")]
    public partial void NodeJoined(int nodeId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} joining via seed (parallel batch)")]
    public partial void NodeJoiningParallel(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} joined cluster (parallel batch)")]
    public partial void NodeJoinedParallel(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node crashed")]
    public partial void NodeCrashed();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node beginning graceful leave")]
    public partial void NodeLeaving();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node left gracefully")]
    public partial void NodeLeft();

    [LoggerMessage(Level = LogLevel.Debug, Message = "{Count} nodes beginning parallel graceful leave")]
    public partial void NodesLeavingParallel(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} initiating leave (parallel batch)")]
    public partial void NodeLeavingParallel(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} left gracefully (parallel batch)")]
    public partial void NodeLeftParallel(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Parallel leave completed: {Count} nodes removed in {ConfigChanges} configuration changes")]
    public partial void ParallelLeaveCompleted(int count, int configChanges);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node isolated")]
    public partial void NodeIsolated();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node reconnected")]
    public partial void NodeReconnected();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition created between nodes")]
    public partial void PartitionCreated();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Partition healed between nodes")]
    public partial void PartitionHealed();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Teardown cancellation requested - exiting simulation loop")]
    public partial void TeardownCancellationRequested();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Condition met after {Iterations} iterations")]
    public partial void ConditionMet(int iterations);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Simulation is idle with no pending work - condition cannot be met. Iterations: {Iterations}, Simulated time: {SimulatedTime}")]
    public partial void SimulationIdleNoPendingWork(int iterations, string simulatedTime);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Simulation appears stuck: exceeded max simulated time ({MaxTime}). Start: {StartTime}, Current: {CurrentTime}, Next scheduled time delta: {TimeDelta}")]
    public partial void SimulationStuckMaxTime(string maxTime, string startTime, string currentTime, string timeDelta);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Simulation appears stuck: {TimeAdvanceCount} consecutive time advances without task execution")]
    public partial void SimulationStuckConsecutiveTimeAdvances(int timeAdvanceCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Max iterations ({MaxIterations}) reached")]
    public partial void MaxIterationsReached(int maxIterations);

    [LoggerMessage(Level = LogLevel.Information, Message = "Max iterations ({MaxIterations}) reached. Diagnostics:\n{Diagnostics}")]
    public partial void MaxIterationsReachedWithDiagnostics(int maxIterations, string diagnostics);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Simulation reached idle state")]
    public partial void SimulationReachedIdleState();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Advancing time by {Delta}")]
    public partial void TimeAdvancing(string delta);
}
