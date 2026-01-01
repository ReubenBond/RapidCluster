namespace RapidCluster;

/// <summary>
/// Interface for controlling the RapidCluster lifecycle.
/// Use this interface when you need manual control over when the cluster starts and stops,
/// for example when the listen address is only known after the server starts.
/// </summary>
/// <remarks>
/// For typical scenarios, use AddRapidCluster() which automatically registers a hosted service
/// to manage the lifecycle. For scenarios requiring manual control (e.g., Aspire with dynamic ports),
/// use AddRapidClusterManual() and call <see cref="StartAsync"/> after the server is ready.
/// </remarks>
public interface IRapidClusterLifecycle
{
    /// <summary>
    /// Starts the RapidCluster service, joining or creating a cluster.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the cluster has been joined or created.</returns>
    Task StartAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Stops the RapidCluster service, gracefully leaving the cluster.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when the cluster has been left.</returns>
    Task StopAsync(CancellationToken cancellationToken = default);
}
