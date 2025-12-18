using System.Net;

namespace RapidCluster.Discovery;

/// <summary>
/// Result of seed discovery, indicating the discovered seeds and whether they are static.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="IsStatic"/> property is critical for preventing split-brain scenarios
/// during cluster bootstrap. When seeds are static (identical across all nodes), the
/// cluster can form immediately using deterministic bootstrap. When seeds are dynamic
/// (may differ between nodes due to DNS propagation, service discovery timing, etc.),
/// a seed gossip protocol is used to ensure all nodes agree on the same seed set before
/// forming the cluster.
/// </para>
/// <para>
/// Implementers of <see cref="ISeedProvider"/> must carefully consider whether their
/// seed source is truly static. A source is static only if:
/// <list type="bullet">
/// <item>All nodes are guaranteed to receive exactly the same seed list</item>
/// <item>The seed list will not change between nodes starting</item>
/// </list>
/// </para>
/// <para>
/// When in doubt, return <see cref="IsStatic"/> = <c>false</c> to use the safer
/// seed gossip protocol.
/// </para>
/// </remarks>
public readonly record struct SeedDiscoveryResult
{
    /// <summary>
    /// The discovered seed endpoints.
    /// </summary>
    public required IReadOnlyList<EndPoint> Seeds { get; init; }

    /// <summary>
    /// Indicates whether the seed list is static and guaranteed to be identical across all nodes.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When <c>true</c>, bootstrap can proceed without seed gossip because all nodes
    /// are guaranteed to have the same seed list and will compute the same ClusterId.
    /// </para>
    /// <para>
    /// When <c>false</c>, seeds may differ between nodes (e.g., due to DNS propagation
    /// delays, service discovery timing, load balancer randomization), and seed gossip
    /// is required to ensure all bootstrap nodes agree on the same seed set before
    /// forming the cluster. This prevents split-brain scenarios where different groups
    /// of nodes form separate clusters.
    /// </para>
    /// </remarks>
    public required bool IsStatic { get; init; }

    /// <summary>
    /// Creates a static seed discovery result.
    /// </summary>
    /// <param name="seeds">The seed endpoints.</param>
    /// <returns>A result indicating static seeds.</returns>
    public static SeedDiscoveryResult Static(IReadOnlyList<EndPoint> seeds) =>
        new() { Seeds = seeds, IsStatic = true };

    /// <summary>
    /// Creates a dynamic seed discovery result.
    /// </summary>
    /// <param name="seeds">The seed endpoints.</param>
    /// <returns>A result indicating dynamic seeds that require gossip.</returns>
    public static SeedDiscoveryResult Dynamic(IReadOnlyList<EndPoint> seeds) =>
        new() { Seeds = seeds, IsStatic = false };
}
