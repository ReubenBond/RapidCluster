using Microsoft.Extensions.DependencyInjection;

namespace RapidCluster;

/// <summary>
/// Builder for configuring RapidCluster services.
/// </summary>
public interface IRapidClusterBuilder
{
    /// <summary>
    /// Gets the service collection being configured.
    /// </summary>
    IServiceCollection Services { get; }
}
