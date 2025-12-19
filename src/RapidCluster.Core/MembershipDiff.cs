using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Represents the difference between two membership views.
/// </summary>
public sealed class MembershipDiff(
    IReadOnlyList<Endpoint> addedNodes,
    int removedCount,
    IReadOnlyList<Endpoint> removedNodes)
{
    /// <summary>
    /// Nodes that were added in the new view.
    /// </summary>
    public IReadOnlyList<Endpoint> AddedNodes { get; } = addedNodes;

    /// <summary>
    /// The number of nodes that were removed.
    /// </summary>
    public int RemovedCount { get; } = removedCount;

    /// <summary>
    /// The nodes that were removed.
    /// </summary>
    public IReadOnlyList<Endpoint> RemovedNodes { get; } = removedNodes;
}
