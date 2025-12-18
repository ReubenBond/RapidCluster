namespace RapidCluster.Discovery;

/// <summary>
/// Interface for providing seed node endpoints for cluster discovery.
/// Implementations can provide seeds from various sources such as configuration,
/// DNS, cloud provider APIs, or service discovery systems.
/// </summary>
/// <remarks>
/// <para>
/// Seed providers must indicate whether their seeds are static (identical across all nodes)
/// or dynamic (may vary between nodes). This distinction is critical for preventing
/// split-brain scenarios during cluster bootstrap.
/// </para>
/// <para>
/// Examples of static seed sources:
/// <list type="bullet">
/// <item>Hardcoded IP addresses in code</item>
/// <item>Configuration files deployed identically to all nodes</item>
/// <item>Environment variables set identically on all nodes</item>
/// </list>
/// </para>
/// <para>
/// Examples of dynamic seed sources:
/// <list type="bullet">
/// <item>DNS resolution (propagation delays can cause different results)</item>
/// <item>Service discovery (Consul, etcd, Kubernetes)</item>
/// <item>Cloud provider APIs (instance lists may update asynchronously)</item>
/// <item>Load balancers (may return different backends)</item>
/// </list>
/// </para>
/// </remarks>
public interface ISeedProvider
{
    /// <summary>
    /// Gets the current list of seed endpoints and indicates whether they are static.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Called on startup and when rejoin fails to refresh seeds.
    /// </para>
    /// <para>
    /// Implementers must set <see cref="SeedDiscoveryResult.IsStatic"/> appropriately:
    /// <list type="bullet">
    /// <item><c>true</c> - Seeds are guaranteed identical across all nodes. Enables fast deterministic bootstrap.</item>
    /// <item><c>false</c> - Seeds may differ between nodes. Triggers seed gossip protocol to prevent split-brain.</item>
    /// </list>
    /// </para>
    /// </remarks>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The discovered seeds and whether they are static.</returns>
    ValueTask<SeedDiscoveryResult> GetSeedsAsync(CancellationToken cancellationToken = default);
}
