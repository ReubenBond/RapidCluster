using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Routing;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using RapidCluster.Messaging;

namespace RapidCluster;

/// <summary>
/// Extension methods for configuring RapidCluster with ASP.NET Core hosting.
/// </summary>
public static class RapidClusterHostingExtensions
{
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
