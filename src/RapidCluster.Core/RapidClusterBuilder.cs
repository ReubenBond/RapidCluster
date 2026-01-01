using Microsoft.Extensions.DependencyInjection;

namespace RapidCluster;

/// <summary>
/// Default implementation of <see cref="IRapidClusterBuilder"/>.
/// </summary>
internal sealed class RapidClusterBuilder : IRapidClusterBuilder
{
    /// <summary>
    /// Initializes a new instance of the <see cref="RapidClusterBuilder"/> class.
    /// </summary>
    /// <param name="services">The service collection being configured.</param>
    public RapidClusterBuilder(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        Services = services;
    }

    /// <inheritdoc />
    public IServiceCollection Services { get; }
}
