using System.Net;

namespace RapidCluster.Discovery;

/// <summary>
/// Interface for providing seed node endpoints for cluster discovery.
/// Implementations can provide seeds from various sources such as configuration,
/// DNS, cloud provider APIs, or service discovery systems.
/// </summary>
public interface ISeedProvider
{
    /// <summary>
    /// Gets the current list of seed endpoints.
    /// Called on startup and when rejoin fails to refresh seeds.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A list of seed endpoints to contact for joining the cluster.</returns>
    ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default);
}
