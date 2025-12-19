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
    /// Adds RapidCluster core services to the service collection with automatic lifecycle management.
    /// The cluster will start automatically when the host starts and stop when the host stops.
    /// Note: You must also call AddRapidClusterGrpc() from the RapidCluster.Grpc package
    /// (or provide your own IMessagingClient implementation) for the cluster to communicate.
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
        // Add core services
        AddRapidClusterCore(services, configure, configureProtocol, timeProvider);

        // Register the cluster service as a hosted service for automatic lifecycle management
        services.AddHostedService(sp => sp.GetRequiredService<RapidClusterLifecycleService>());

        return services;
    }

    /// <summary>
    /// Adds RapidCluster core services to the service collection with manual lifecycle management.
    /// Use this when you need to control when the cluster starts, for example when the listen address
    /// is only known after the server starts (e.g., with Aspire's dynamic port assignment).
    /// </summary>
    /// <remarks>
    /// When using this method, you must manually call <see cref="IRapidClusterLifecycle.StartAsync"/>
    /// after the server has started and <see cref="IRapidClusterLifecycle.StopAsync"/> before shutdown.
    /// Typically, you would hook into <see cref="IHostApplicationLifetime.ApplicationStarted"/> and
    /// <see cref="IHostApplicationLifetime.ApplicationStopping"/> to manage the lifecycle.
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for Rapid options.</param>
    /// <param name="configureProtocol">Optional configuration action for protocol options.</param>
    /// <param name="timeProvider">Optional TimeProvider for testing and time control.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterManual(
        this IServiceCollection services,
        Action<RapidClusterOptions> configure,
        Action<RapidClusterProtocolOptions>? configureProtocol = null,
        TimeProvider? timeProvider = null)
    {
        // Add core services without the hosted service registration
        AddRapidClusterCore(services, configure, configureProtocol, timeProvider);

        // No hosted service registration - caller must manage lifecycle manually via IRapidClusterLifecycle

        return services;
    }

    /// <summary>
    /// Adds RapidCluster core services without lifecycle management.
    /// </summary>
    private static void AddRapidClusterCore(
        IServiceCollection services,
        Action<RapidClusterOptions> configure,
        Action<RapidClusterProtocolOptions>? configureProtocol,
        TimeProvider? timeProvider)
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
        services.AddSingleton(sp =>
        {
            var lifetime = sp.GetService<IHostApplicationLifetime>();
            var shuttingDownToken = lifetime?.ApplicationStopping ?? default;
            return new SharedResources(sp.GetRequiredService<TimeProvider>(), shuttingDownToken: shuttingDownToken);
        });

        // Register metrics
        services.AddSingleton<RapidClusterMetrics>();

        // Register broadcaster factory (requires IMessagingClient to be registered)
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

        // Register MembershipService directly (InitializeAsync is called by RapidClusterLifecycleService)
        services.AddSingleton<MembershipService>();

        // Register the membership service handler
        services.AddSingleton<IMembershipServiceHandler>(sp => sp.GetRequiredService<MembershipService>());

        // Register the lifecycle service (for both manual and automatic scenarios)
        services.AddSingleton<RapidClusterLifecycleService>();
        services.AddSingleton<IRapidClusterLifecycle>(sp => sp.GetRequiredService<RapidClusterLifecycleService>());

        // Register the cluster interface for application access
        services.AddSingleton<IRapidCluster, RapidClusterImpl>();
    }
}
