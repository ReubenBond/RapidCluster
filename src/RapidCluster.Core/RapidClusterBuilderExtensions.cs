using System.Net;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RapidCluster.Monitoring;

namespace RapidCluster;

/// <summary>
/// Extension methods for <see cref="IRapidClusterBuilder"/>.
/// </summary>
public static class RapidClusterBuilderExtensions
{
    /// <summary>
    /// Configures the cluster to use a static listen address.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="listenAddress">The endpoint this node listens on.</param>
    /// <returns>The builder for chaining.</returns>
    public static IRapidClusterBuilder UseListenAddress(this IRapidClusterBuilder builder, EndPoint listenAddress)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(listenAddress);

        builder.Services.RemoveAll<IListenAddressProvider>();
        builder.Services.AddSingleton<IListenAddressProvider>(new StaticListenAddressProvider(listenAddress));

        return builder;
    }

    /// <summary>
    /// Configures the protocol options.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="configure">A delegate to configure the protocol options.</param>
    /// <returns>The builder for chaining.</returns>
    public static IRapidClusterBuilder ConfigureProtocol(this IRapidClusterBuilder builder, Action<RapidClusterProtocolOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);

        return builder;
    }

    /// <summary>
    /// Configures the failure detector options.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="configure">A delegate to configure the failure detector options.</param>
    /// <returns>The builder for chaining.</returns>
    public static IRapidClusterBuilder ConfigureFailureDetector(this IRapidClusterBuilder builder, Action<PingPongFailureDetectorOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        builder.Services.Configure(configure);

        return builder;
    }
}
