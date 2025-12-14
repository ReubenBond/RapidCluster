using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RapidCluster.Discovery;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;

namespace RapidCluster;

/// <summary>
/// Extension methods for adding RapidCluster services to dependency injection.
/// </summary>
public static class RapidClusterServiceCollectionExtensions
{
    /// <summary>
    /// Adds RapidCluster services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for Rapid options.</param>
    /// <param name="configureProtocol">Optional configuration action for protocol options.</param>
    /// <param name="timeProvider">Optional TimeProvider for testing and time control.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidCluster(
        this IServiceCollection services,
        Action<RapidClusterOptions> configure,
        Action<RapidClusterProtocolOptions>? configureProtocol = null,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Configure options
        services.Configure(configure);

        // Configure protocol options
        if (configureProtocol != null)
        {
            services.Configure(configureProtocol);
        }
        else
        {
            services.Configure<RapidClusterProtocolOptions>(_ => { });
        }

        // Add validation
        services.AddSingleton<Microsoft.Extensions.Options.IValidateOptions<RapidClusterProtocolOptions>, RapidClusterProtocolOptionsValidator>();

        // Register TimeProvider
        var provider = timeProvider ?? TimeProvider.System;
        services.AddSingleton(provider);

        // Add core services
        services.AddGrpc();
        services.AddSingleton(sp =>
        {
            var lifetime = sp.GetService<IHostApplicationLifetime>();
            var shuttingDownToken = lifetime?.ApplicationStopping ?? default;
            return new SharedResources(sp.GetRequiredService<TimeProvider>(), shuttingDownToken: shuttingDownToken);
        });

        // Register metrics
        services.AddSingleton<RapidClusterMetrics>();

        // Register messaging infrastructure
        // GrpcClient is registered as a hosted service so it shuts down AFTER RapidClusterService
        // (hosted services are stopped in reverse registration order)
        services.AddSingleton<GrpcClient>();
        services.AddSingleton<IMessagingClient>(sp => sp.GetRequiredService<GrpcClient>());
        services.AddHostedService(sp => sp.GetRequiredService<GrpcClient>());
        services.AddSingleton<IBroadcasterFactory, UnicastToAllBroadcasterFactory>();

        // Register failure detector factory
        services.AddSingleton<IEdgeFailureDetectorFactory>(sp =>
        {
            var options = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RapidClusterOptions>>().Value;
            var protocolOptions = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<RapidClusterProtocolOptions>>();
            var client = sp.GetRequiredService<IMessagingClient>();
            var sharedResources = sp.GetRequiredService<SharedResources>();
            var metrics = sp.GetRequiredService<RapidClusterMetrics>();
            var logger = sp.GetRequiredService<ILogger<PingPongFailureDetector>>();
            return new PingPongFailureDetectorFactory(options.ListenAddress.ToProtobuf(), client, sharedResources, protocolOptions, metrics, logger);
        });

        // Register ConsensusCoordinator factory
        services.AddSingleton<IConsensusCoordinatorFactory, ConsensusCoordinatorFactory>();

        // Register CutDetector factory
        services.AddSingleton<ICutDetectorFactory, CutDetectorFactory>();

        // Register seed provider (default to ConfigurationSeedProvider if not already registered)
        services.TryAddSingleton<ISeedProvider, ConfigurationSeedProvider>();

        // Register MembershipViewAccessor as singleton (used by both MembershipService and consumers)
        services.AddSingleton<MembershipViewAccessor>();
        services.AddSingleton<IMembershipViewAccessor>(sp => sp.GetRequiredService<MembershipViewAccessor>());

        // Register MetadataManager as singleton (shared between MembershipService and RapidClusterImpl)
        services.AddSingleton<MetadataManager>();

        // Register MembershipService directly (InitializeAsync is called by RapidClusterService)
        services.AddSingleton<MembershipService>();

        // Register the membership service handler
        services.AddSingleton<IMembershipServiceHandler>(sp => sp.GetRequiredService<MembershipService>());

        // Register the gRPC service implementation
        services.AddSingleton<MembershipServiceImpl>();

        // Register the cluster service as a hosted service
        services.AddSingleton<RapidClusterHostedService>();
        services.AddHostedService(sp => sp.GetRequiredService<RapidClusterHostedService>());

        // Register the cluster interface for application access
        services.AddSingleton<IRapidCluster, RapidClusterImpl>();

        return services;
    }

    /// <summary>
    /// Adds RapidCluster gRPC services to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterGrpc(this IServiceCollection services)
    {
        services.AddGrpc();
        return services;
    }
}
