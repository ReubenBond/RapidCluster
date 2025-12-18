using System.Net;

namespace RapidCluster.Discovery;

/// <summary>
/// Configuration options for seed discovery.
/// </summary>
public sealed class RapidClusterSeedOptions
{
    /// <summary>
    /// The seed addresses for cluster discovery.
    /// </summary>
    public IReadOnlyList<EndPoint>? SeedAddresses { get; set; }

    /// <summary>
    /// Indicates whether the seed list is static and guaranteed to be identical across all nodes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When <c>true</c>, bootstrap can proceed without seed gossip because all nodes
    /// are guaranteed to have the same seed list and will compute the same ClusterId.
    /// </para>
    /// <para>
    /// When <c>false</c> (the default), seeds may differ between nodes, and seed gossip
    /// is required to ensure all bootstrap nodes agree on the same seed set before
    /// forming the cluster. This prevents split-brain scenarios.
    /// </para>
    /// <para>
    /// Only set this to <c>true</c> if you are certain that all nodes will discover
    /// exactly the same seeds (e.g., hardcoded list, identically deployed configuration).
    /// </para>
    /// </remarks>
    public bool IsStatic { get; set; } = false;
}
