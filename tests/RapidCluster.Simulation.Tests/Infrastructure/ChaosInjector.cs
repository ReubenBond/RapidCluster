using Clockwork;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Injects random faults into the RapidCluster simulation for chaos testing.
/// Extends <see cref="ChaosInjector{TNode, TCluster}"/> with RapidCluster-specific operations.
/// </summary>
/// <remarks>
/// Creates a new chaos injector for the specified harness.
/// </remarks>
internal sealed class ChaosInjector(RapidSimulationCluster harness) : ChaosInjector<RapidSimulationNode, RapidSimulationCluster>(harness)
{
    /// <inheritdoc />
    protected override void CrashNode(RapidSimulationNode node) => Cluster.CrashNode(node);

    /// <inheritdoc />
    protected override void PartitionNodes(RapidSimulationNode node1, RapidSimulationNode node2) => Cluster.PartitionNodes(node1, node2);

    /// <inheritdoc />
    protected override void HealPartition(RapidSimulationNode node1, RapidSimulationNode node2) => Cluster.HealPartition(node1, node2);

    /// <inheritdoc />
    protected override void IsolateNode(RapidSimulationNode node) => Cluster.IsolateNode(node);

    /// <inheritdoc />
    protected override void ReconnectNode(RapidSimulationNode node) => Cluster.ReconnectNode(node);

    /// <inheritdoc />
    protected override void HealAllPartitions() => Cluster.Network.HealAllPartitions();
}
