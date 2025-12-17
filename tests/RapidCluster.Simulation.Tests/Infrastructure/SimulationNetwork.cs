using System.Diagnostics;
using Clockwork;
using Microsoft.Extensions.Logging;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Simulates a network for in-memory transport between nodes in a RapidCluster simulation.
/// Extends <see cref="Clockwork.SimulationNetwork"/> with RapidCluster-specific logging.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class SimulationNetwork : Clockwork.SimulationNetwork
{
    private SimulationNetworkLogger _log;

    internal SimulationNetwork(RapidSimulationCluster harness, SimulationRandom random)
        : base(() => harness.Nodes, random)
    {
        _log = new SimulationNetworkLogger(Microsoft.Extensions.Logging.Abstractions.NullLogger<SimulationNetwork>.Instance);
    }

    internal void SetLogger(ILogger<SimulationNetwork> logger)
    {
        _log = new SimulationNetworkLogger(logger);
        base.SetLogger(logger);
    }

    #region Override logging hooks for RapidCluster-specific logging

    /// <inheritdoc />
    protected override void OnPartitionCreated(string source, string target)
    {
        _log.PartitionCreated(source, target);
    }

    /// <inheritdoc />
    protected override void OnBidirectionalPartitionCreating(string node1, string node2)
    {
        _log.BidirectionalPartitionCreating(node1, node2);
    }

    /// <inheritdoc />
    protected override void OnPartitionHealed(string source, string target)
    {
        _log.PartitionHealed(source, target);
    }

    /// <inheritdoc />
    protected override void OnBidirectionalPartitionHealing(string node1, string node2)
    {
        _log.BidirectionalPartitionHealing(node1, node2);
    }

    /// <inheritdoc />
    protected override void OnAllPartitionsHealed(int count)
    {
        _log.AllPartitionsHealed(count);
    }

    /// <inheritdoc />
    protected override void OnNodeIsolating(string nodeAddress)
    {
        _log.NodeIsolating(nodeAddress);
    }

    /// <inheritdoc />
    protected override void OnNodeReconnecting(string nodeAddress)
    {
        _log.NodeReconnecting(nodeAddress);
    }

    /// <inheritdoc />
    protected override void OnMessageBlockedByPartition(string source, string target)
    {
        _log.MessageBlockedByPartition(source, target);
    }

    /// <inheritdoc />
    protected override void OnMessageDroppedRandom(string source, string target)
    {
        _log.MessageDroppedRandom(source, target);
    }

    #endregion

    private string DebuggerDisplay => $"SimulationNetwork(DropRate={MessageDropRate:P0})";
}
