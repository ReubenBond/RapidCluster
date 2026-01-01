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
    /// Adds RapidCluster gRPC transport services.
    /// Call this after AddRapidCluster() to add gRPC transport support.
    /// </summary>
    /// <param name="builder">The RapidCluster builder.</param>
    /// <returns>The builder for chaining.</returns>
    public static IRapidClusterBuilder UseGrpcTransport(this IRapidClusterBuilder builder)
    {
        return builder.UseGrpcTransport(_ => { });
    }

    /// <summary>
    /// Adds RapidCluster gRPC transport services with configuration.
    /// Call this after AddRapidCluster() to add gRPC transport support.
    /// </summary>
    /// <param name="builder">The RapidCluster builder.</param>
    /// <param name="configure">A delegate to configure the gRPC-specific options.</param>
    /// <returns>The builder for chaining.</returns>
    public static IRapidClusterBuilder UseGrpcTransport(this IRapidClusterBuilder builder, Action<RapidClusterGrpcOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var services = builder.Services;

        // Configure gRPC-specific options
        services.Configure(configure);

        // Add gRPC infrastructure
        services.AddGrpc();

        // Register GrpcClient as the messaging client implementation
        // GrpcClient is registered as a hosted service so it shuts down AFTER RapidClusterService
        // (hosted services are stopped in reverse registration order)
        services.TryAddSingleton<GrpcClient>();
        services.TryAddSingleton<IMessagingClient>(sp => sp.GetRequiredService<GrpcClient>());
        services.AddHostedService(sp => sp.GetRequiredService<GrpcClient>());

        // Register the gRPC service implementation
        services.TryAddSingleton<MembershipServiceImpl>();

        return builder;
    }

    /// <summary>
    /// Configures the cluster to use the ASP.NET Core server's listen address.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Use this method when the listen address is not known until the server starts (e.g., with dynamic port assignment
    /// in Aspire or when binding to port 0).
    /// </para>
    /// <para>
    /// The provider reads from <see cref="IServer.Features"/> which is only populated after the server starts.
    /// RapidCluster's lifecycle service waits for ApplicationStarted before initializing, so the address
    /// will be available by the time it's needed.
    /// </para>
    /// </remarks>
    /// <param name="builder">The RapidCluster builder.</param>
    /// <param name="preferHttps">Whether to prefer HTTPS addresses over HTTP. Default is true.</param>
    /// <returns>The builder for chaining.</returns>
    public static IRapidClusterBuilder UseServerListenAddress(this IRapidClusterBuilder builder, bool preferHttps = true)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var services = builder.Services;

        // Replace any existing IListenAddressProvider registration with the server-based one
        services.RemoveAll<IListenAddressProvider>();
        services.AddSingleton<IListenAddressProvider>(sp =>
        {
            var server = sp.GetRequiredService<IServer>();
            return new ServerListenAddressProvider(server, preferHttps);
        });

        return builder;
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
