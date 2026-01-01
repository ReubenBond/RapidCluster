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
    /// </summary>
    /// <remarks>
    /// <para>
    /// You must configure a listen address using one of the builder extension methods:
    /// <list type="bullet">
    /// <item><description><see cref="RapidClusterBuilderExtensions.UseListenAddress"/> for static addresses</description></item>
    /// <item><description>UseServerListenAddress() from RapidCluster.Grpc for ASP.NET Core server-derived addresses</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// You must also add a transport implementation such as AddRapidClusterGrpc() from the RapidCluster.Grpc package.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>A builder for further configuration.</returns>
    public static IRapidClusterBuilder AddRapidCluster(this IServiceCollection services)
    {
        return services.AddRapidCluster(_ => { });
    }

    /// <summary>
    /// Adds RapidCluster core services to the service collection with automatic lifecycle management.
    /// The cluster will start automatically when the host starts and stop when the host stops.
    /// </summary>
    /// <remarks>
    /// <para>
    /// You must configure a listen address using one of the builder extension methods:
    /// <list type="bullet">
    /// <item><description><see cref="RapidClusterBuilderExtensions.UseListenAddress"/> for static addresses</description></item>
    /// <item><description>UseServerListenAddress() from RapidCluster.Grpc for ASP.NET Core server-derived addresses</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// You must also add a transport implementation such as AddRapidClusterGrpc() from the RapidCluster.Grpc package.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for Rapid options.</param>
    /// <param name="timeProvider">Optional TimeProvider for testing and time control.</param>
    /// <returns>A builder for further configuration.</returns>
    public static IRapidClusterBuilder AddRapidCluster(
        this IServiceCollection services,
        Action<RapidClusterOptions> configure,
        TimeProvider? timeProvider = null)
    {
        // Add core services
        AddRapidClusterCore(services, configure, timeProvider);

        // Register the cluster service as a hosted service for automatic lifecycle management
        services.AddHostedService(sp => sp.GetRequiredService<RapidClusterLifecycleService>());

        return new RapidClusterBuilder(services);
    }

    /// <summary>
    /// Adds RapidCluster core services to the service collection with manual lifecycle management.
    /// Use this when you need to control when the cluster starts, for example when the listen address
    /// is only known after the server starts (e.g., with Aspire's dynamic port assignment).
    /// </summary>
    /// <remarks>
    /// <para>
    /// When using this method, you must manually call <see cref="IRapidClusterLifecycle.StartAsync"/>
    /// after the server has started and <see cref="IRapidClusterLifecycle.StopAsync"/> before shutdown.
    /// Typically, you would hook into <see cref="IHostApplicationLifetime.ApplicationStarted"/> and
    /// <see cref="IHostApplicationLifetime.ApplicationStopping"/> to manage the lifecycle.
    /// </para>
    /// <para>
    /// You must configure a listen address using one of the builder extension methods:
    /// <list type="bullet">
    /// <item><description><see cref="RapidClusterBuilderExtensions.UseListenAddress"/> for static addresses</description></item>
    /// <item><description>UseServerListenAddress() from RapidCluster.Grpc for ASP.NET Core server-derived addresses</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// You must also add a transport implementation such as AddRapidClusterGrpc() from the RapidCluster.Grpc package.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <returns>A builder for further configuration.</returns>
    public static IRapidClusterBuilder AddRapidClusterManual(this IServiceCollection services)
    {
        return services.AddRapidClusterManual(_ => { });
    }

    /// <summary>
    /// Adds RapidCluster core services to the service collection with manual lifecycle management.
    /// Use this when you need to control when the cluster starts, for example when the listen address
    /// is only known after the server starts (e.g., with Aspire's dynamic port assignment).
    /// </summary>
    /// <remarks>
    /// <para>
    /// When using this method, you must manually call <see cref="IRapidClusterLifecycle.StartAsync"/>
    /// after the server has started and <see cref="IRapidClusterLifecycle.StopAsync"/> before shutdown.
    /// Typically, you would hook into <see cref="IHostApplicationLifetime.ApplicationStarted"/> and
    /// <see cref="IHostApplicationLifetime.ApplicationStopping"/> to manage the lifecycle.
    /// </para>
    /// <para>
    /// You must configure a listen address using one of the builder extension methods:
    /// <list type="bullet">
    /// <item><description><see cref="RapidClusterBuilderExtensions.UseListenAddress"/> for static addresses</description></item>
    /// <item><description>UseServerListenAddress() from RapidCluster.Grpc for ASP.NET Core server-derived addresses</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// You must also add a transport implementation such as AddRapidClusterGrpc() from the RapidCluster.Grpc package.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Configuration action for Rapid options.</param>
    /// <param name="timeProvider">Optional TimeProvider for testing and time control.</param>
    /// <returns>A builder for further configuration.</returns>
    public static IRapidClusterBuilder AddRapidClusterManual(
        this IServiceCollection services,
        Action<RapidClusterOptions> configure,
        TimeProvider? timeProvider = null)
    {
        // Add core services without the hosted service registration
        AddRapidClusterCore(services, configure, timeProvider);

        // No hosted service registration - caller must manage lifecycle manually via IRapidClusterLifecycle
        return new RapidClusterBuilder(services);
    }

    /// <summary>
    /// Adds RapidCluster core services without lifecycle management.
    /// </summary>
    private static void AddRapidClusterCore(
        IServiceCollection services,
        Action<RapidClusterOptions> configure,
        TimeProvider? timeProvider)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        // Configure options
        services.Configure(configure);

        // Configure default protocol options (can be overridden via builder)
        services.TryAddSingleton(_ => Microsoft.Extensions.Options.Options.Create(new RapidClusterProtocolOptions()));
        services.Configure<RapidClusterProtocolOptions>(_ => { });

        // Configure default failure detector options (can be overridden via builder)
        services.TryAddSingleton(_ => Microsoft.Extensions.Options.Options.Create(new PingPongFailureDetectorOptions()));
        services.Configure<PingPongFailureDetectorOptions>(_ => { });

        // Add validation
        services.AddSingleton<Microsoft.Extensions.Options.IValidateOptions<RapidClusterProtocolOptions>, RapidClusterProtocolOptionsValidator>();

        // Register TimeProvider
        var provider = timeProvider ?? TimeProvider.System;
        services.TryAddSingleton(provider);

        // Add core services
        services.TryAddSingleton(sp =>
        {
            var lifetime = sp.GetService<IHostApplicationLifetime>();
            var shuttingDownToken = lifetime?.ApplicationStopping ?? default;
            return new SharedResources(sp.GetRequiredService<TimeProvider>(), shuttingDownToken: shuttingDownToken);
        });

        // Register metrics
        services.TryAddSingleton<RapidClusterMetrics>();

        // Register internal wrapper that validates the listen address is not null
        // Note: IListenAddressProvider must be registered via UseListenAddress() or UseServerListenAddress()
        services.TryAddSingleton(sp => new ListenAddressProvider(sp.GetRequiredService<IListenAddressProvider>()));

        // Register broadcaster factory (requires IMessagingClient to be registered)
        services.TryAddSingleton<IBroadcasterFactory, UnicastToAllBroadcasterFactory>();

        // Register failure detector factory
        services.TryAddSingleton<IEdgeFailureDetectorFactory>(sp =>
        {
            var listenAddressProvider = sp.GetRequiredService<IListenAddressProvider>();
            var failureDetectorOptions = sp.GetRequiredService<Microsoft.Extensions.Options.IOptions<PingPongFailureDetectorOptions>>();
            var client = sp.GetRequiredService<IMessagingClient>();
            var sharedResources = sp.GetRequiredService<SharedResources>();
            var metrics = sp.GetRequiredService<RapidClusterMetrics>();
            var logger = sp.GetRequiredService<ILogger<PingPongFailureDetector>>();
            return new PingPongFailureDetectorFactory(listenAddressProvider, client, sharedResources, failureDetectorOptions, metrics, logger);
        });

        // Register ConsensusCoordinator factory
        services.TryAddSingleton<IConsensusCoordinatorFactory, ConsensusCoordinatorFactory>();

        // Register seed provider (default to ConfigurationSeedProvider if not already registered)
        services.TryAddSingleton<ISeedProvider, ConfigurationSeedProvider>();

        // Register MembershipViewAccessor as singleton (used by both MembershipService and consumers)
        services.TryAddSingleton<MembershipViewAccessor>();
        services.TryAddSingleton<IMembershipViewAccessor>(sp => sp.GetRequiredService<MembershipViewAccessor>());

        // Register MembershipService directly (InitializeAsync is called by RapidClusterLifecycleService)
        services.TryAddSingleton<MembershipService>();

        // Register the membership service handler
        services.TryAddSingleton<IMembershipServiceHandler>(sp => sp.GetRequiredService<MembershipService>());

        // Register the lifecycle service (for both manual and automatic scenarios)
        services.TryAddSingleton<RapidClusterLifecycleService>();
        services.TryAddSingleton<IRapidClusterLifecycle>(sp => sp.GetRequiredService<RapidClusterLifecycleService>());

        // Register the cluster interface for application access
        services.TryAddSingleton<IRapidCluster, RapidClusterImpl>();
    }
}
