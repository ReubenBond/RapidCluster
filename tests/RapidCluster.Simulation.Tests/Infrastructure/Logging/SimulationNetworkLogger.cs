using Microsoft.Extensions.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure.Logging;

/// <summary>
/// High-performance logger for SimulationNetwork using LoggerMessage source generators.
/// </summary>
internal sealed partial class SimulationNetworkLogger(ILogger<SimulationNetwork> logger)
{
    private readonly ILogger _logger = logger;

    [LoggerMessage(Level = LogLevel.Information, Message = "Created partition: {Source} -> {Target}")]
    public partial void PartitionCreated(string source, string target);

    [LoggerMessage(Level = LogLevel.Information, Message = "Creating bidirectional partition between {Node1} and {Node2}")]
    public partial void BidirectionalPartitionCreating(string node1, string node2);

    [LoggerMessage(Level = LogLevel.Information, Message = "Healed partition: {Source} -> {Target}")]
    public partial void PartitionHealed(string source, string target);

    [LoggerMessage(Level = LogLevel.Information, Message = "Healing bidirectional partition between {Node1} and {Node2}")]
    public partial void BidirectionalPartitionHealing(string node1, string node2);

    [LoggerMessage(Level = LogLevel.Information, Message = "Healed all {Count} partitions")]
    public partial void AllPartitionsHealed(int count);

    [LoggerMessage(Level = LogLevel.Information, Message = "Isolating node {Node}")]
    public partial void NodeIsolating(string node);

    [LoggerMessage(Level = LogLevel.Information, Message = "Reconnecting isolated node {Node}")]
    public partial void NodeReconnecting(string node);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Message from {Source} to {Target} blocked (partition)")]
    public partial void MessageBlockedByPartition(string source, string target);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Message from {Source} to {Target} dropped (random)")]
    public partial void MessageDroppedRandom(string source, string target);
}
