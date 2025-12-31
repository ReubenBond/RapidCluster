using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using RapidCluster.Messaging;

namespace RapidCluster.Grpc;

/// <summary>
/// Extension methods for configuring RapidCluster gRPC transport with ASP.NET Core hosting.
/// </summary>
public static class RapidClusterGrpcExtensions
{
    /// <summary>
    /// Adds RapidCluster gRPC transport services to the service collection.
    /// Call this after <see cref="RapidClusterServiceCollectionExtensions.AddRapidCluster"/> to add gRPC transport support.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterGrpc(this IServiceCollection services) => services.AddRapidClusterGrpc(_ => { }, _ => { });

    /// <summary>
    /// Adds RapidCluster gRPC transport services to the service collection with gRPC options configuration.
    /// Call this after <see cref="RapidClusterServiceCollectionExtensions.AddRapidCluster"/> to add gRPC transport support.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureGrpcOptions">A delegate to configure the gRPC-specific options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterGrpc(this IServiceCollection services, Action<RapidClusterGrpcOptions> configureGrpcOptions)
        => services.AddRapidClusterGrpc(_ => { }, configureGrpcOptions);

    /// <summary>
    /// Adds RapidCluster gRPC transport services to the service collection with configuration.
    /// Call this after <see cref="RapidClusterServiceCollectionExtensions.AddRapidCluster"/> to add gRPC transport support.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureProtocolOptions">A delegate to configure the protocol options.</param>
    /// <param name="configureGrpcOptions">A delegate to configure the gRPC-specific options.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterGrpc(this IServiceCollection services, Action<RapidClusterProtocolOptions> configureProtocolOptions, Action<RapidClusterGrpcOptions> configureGrpcOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureProtocolOptions);
        ArgumentNullException.ThrowIfNull(configureGrpcOptions);

        // Configure protocol options
        services.Configure(configureProtocolOptions);

        // Configure gRPC-specific options
        services.Configure(configureGrpcOptions);

        // Add gRPC infrastructure
        services.AddGrpc();

        // Register GrpcClient as the messaging client implementation
        // GrpcClient is registered as a hosted service so it shuts down AFTER RapidClusterService
        // (hosted services are stopped in reverse registration order)
        services.AddSingleton<GrpcClient>();
        services.AddSingleton<IMessagingClient>(sp => sp.GetRequiredService<GrpcClient>());
        services.AddHostedService(sp => sp.GetRequiredService<GrpcClient>());

        // Register the gRPC service implementation
        services.AddSingleton<MembershipServiceImpl>();

        return services;
    }

    /// <summary>
    /// Registers a <see cref="ServerListenAddressProvider"/> that reads the listen address from the ASP.NET Core server.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this method when the listen address is not known until the server starts (e.g., with dynamic port assignment
    /// in Aspire or when binding to port 0). This must be called <strong>before</strong> <see cref="RapidClusterServiceCollectionExtensions.AddRapidCluster"/>
    /// to ensure the server-based provider takes precedence over the default options-based provider.
    /// </para>
    /// <para>
    /// The provider reads from <see cref="IServer.Features"/> which is only populated after the server starts.
    /// RapidCluster's lifecycle service waits for ApplicationStarted before initializing, so the address
    /// will be available by the time it's needed.
    /// </para>
    /// </remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="preferHttps">Whether to prefer HTTPS addresses over HTTP. Default is true.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddRapidClusterServerAddressProvider(this IServiceCollection services, bool preferHttps = true)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Replace any existing IListenAddressProvider registration with the server-based one
        services.RemoveAll<IListenAddressProvider>();
        services.AddSingleton<IListenAddressProvider>(sp =>
        {
            var server = sp.GetRequiredService<IServer>();
            return new ServerListenAddressProvider(server, preferHttps);
        });

        return services;
    }

    /// <summary>
    /// Configures Kestrel to listen on the RapidCluster port with HTTP/2.
    /// Call this when you need to configure the port for Rapid gRPC services.
    /// </summary>
    /// <param name="builder">The WebApplicationBuilder.</param>
    /// <param name="port">The port to listen on.</param>
    /// <returns>The builder for chaining.</returns>
    public static WebApplicationBuilder ConfigureRapidClusterKestrel(
        this WebApplicationBuilder builder,
        int port)
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.WebHost.ConfigureKestrel(options => options.ListenAnyIP(port, listenOptions => listenOptions.Protocols = HttpProtocols.Http2));

        return builder;
    }

    /// <summary>
    /// Maps RapidCluster gRPC endpoints to the application.
    /// </summary>
    /// <param name="app">The endpoint route builder (WebApplication or IEndpointRouteBuilder).</param>
    /// <returns>The gRPC service endpoint convention builder.</returns>
    public static GrpcServiceEndpointConventionBuilder MapRapidClusterMembershipService(this IEndpointRouteBuilder app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return app.MapGrpcService<MembershipServiceImpl>();
    }
}
