using System.Diagnostics.Metrics;
using Microsoft.Extensions.Configuration;
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
            return new PingPongFailureDetectorFactory(options.ListenAddress, client, sharedResources, protocolOptions, metrics, logger);
        });

        // Register ConsensusCoordinator factory
        services.AddSingleton<IConsensusCoordinatorFactory, ConsensusCoordinatorFactory>();

        // Register CutDetector factory
        services.AddSingleton<ICutDetectorFactory, CutDetectorFactory>();

        // Register MembershipViewAccessor as singleton (used by both MembershipService and consumers)
        services.AddSingleton<MembershipViewAccessor>();
        services.AddSingleton<IMembershipViewAccessor>(sp => sp.GetRequiredService<MembershipViewAccessor>());

        // Register metrics - AddMetrics() registers IMeterFactory if not already present
        services.AddMetrics();
        services.AddSingleton<RapidClusterMetrics>(sp =>
        {
            var meterFactory = sp.GetRequiredService<IMeterFactory>();
            var viewAccessor = sp.GetService<IMembershipViewAccessor>();
            return new RapidClusterMetrics(meterFactory, viewAccessor);
        });

        // Register default seed provider if none is registered
        // Uses TryAdd so custom providers can be registered before calling AddRapidCluster
        services.TryAddSingleton<ISeedProvider, StaticSeedProvider>();

        // Register MembershipService directly (InitializeAsync is called by RapidClusterService)
        services.AddSingleton<MembershipService>();

        // Register the membership service handler
        services.AddSingleton<IMembershipServiceHandler>(sp => sp.GetRequiredService<MembershipService>());

        // Register the gRPC service implementation
        services.AddSingleton<MembershipServiceImpl>();

        // Register the cluster service as a hosted service
        services.AddSingleton<RapidClusterService>();
        services.AddHostedService(sp => sp.GetRequiredService<RapidClusterService>());

        // Register the cluster interface for application access
        services.AddSingleton<IRapidCluster, RapidCluster>();

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

    /// <summary>
    /// Registers a custom seed provider for RapidCluster.
    /// Must be called before <see cref="AddRapidCluster"/>.
    /// </summary>
    /// <typeparam name="T">The seed provider type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterSeedProvider<T>(this IServiceCollection services)
        where T : class, ISeedProvider
    {
        ArgumentNullException.ThrowIfNull(services);

        // Remove any existing ISeedProvider registration
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(ISeedProvider));
        if (descriptor != null)
        {
            services.Remove(descriptor);
        }

        services.AddSingleton<ISeedProvider, T>();
        return services;
    }

    /// <summary>
    /// Registers a seed provider instance for RapidCluster.
    /// Must be called before <see cref="AddRapidCluster"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="seedProvider">The seed provider instance.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterSeedProvider(this IServiceCollection services, ISeedProvider seedProvider)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(seedProvider);

        // Remove any existing ISeedProvider registration
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(ISeedProvider));
        if (descriptor != null)
        {
            services.Remove(descriptor);
        }

        services.AddSingleton(seedProvider);
        return services;
    }

    /// <summary>
    /// Registers a configuration-based seed provider for RapidCluster.
    /// Seeds are read from the specified configuration section.
    /// Must be called before <see cref="AddRapidCluster"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="sectionName">The configuration section name containing the seed list. Default is "RapidCluster:Seeds".</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterConfigurationSeeds(this IServiceCollection services, string sectionName = "RapidCluster:Seeds")
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);

        // Remove any existing ISeedProvider registration
        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(ISeedProvider));
        if (descriptor != null)
        {
            services.Remove(descriptor);
        }

        services.AddSingleton<ISeedProvider>(sp =>
        {
            var configuration = sp.GetRequiredService<IConfiguration>();
            return new ConfigurationSeedProvider(configuration, sectionName);
        });
        return services;
    }
}
