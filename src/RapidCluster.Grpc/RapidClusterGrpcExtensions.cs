using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;

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
    public static IServiceCollection AddRapidClusterGrpc(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

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

        builder.WebHost.ConfigureKestrel(options =>
        {
            options.ListenAnyIP(port, listenOptions =>
            {
                listenOptions.Protocols = HttpProtocols.Http2;
            });
        });

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
