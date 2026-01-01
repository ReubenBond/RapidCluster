using System.CommandLine;
using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RapidCluster.Grpc;

namespace RapidCluster.Examples;

/// <summary>
/// RapidCluster example application using modern ASP.NET hosting.
/// </summary>
internal sealed partial class Program
{
    [LoggerMessage(Level = LogLevel.Information, Message = "Starting RapidCluster agent on {Listen}")]
    private static partial void LogStarting(ILogger logger, string Listen);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster started on {Listen}")]
    private static partial void LogClusterStarted(ILogger logger, string Listen);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster joined. Seed: {Seed}")]
    private static partial void LogClusterJoined(ILogger logger, string Seed);

    [LoggerMessage(Level = LogLevel.Information, Message = "Current membership size: {Size}")]
    private static partial void LogMembershipSize(ILogger logger, int Size);

    [LoggerMessage(Level = LogLevel.Information, Message = "View change: Config={ConfigId}, Members={MemberCount}")]
    private static partial void LogViewChange(ILogger logger, long ConfigId, int MemberCount);

    private static async Task<int> Main(string[] args)
    {
        var listenOption = new Option<string>(
            "listen",
            "l")
        { Description = "The listening address (e.g., 127.0.0.1:1234)" };

        var seedOption = new Option<string>(
            "seed", "s")
        { Description = "The seed node's address for bootstrap (e.g., 127.0.0.1:1234)" };

        var rootCommand = new RootCommand("RapidCluster Standalone Node");
        rootCommand.Options.Add(listenOption);
        rootCommand.Options.Add(seedOption);

        rootCommand.SetAction((parseResult) =>
        {
            var listen = parseResult.GetValue(listenOption);
            var seed = parseResult.GetValue(seedOption);
            RunAgentAsync(listen!, seed!).Wait();
        });

        return await rootCommand.Parse(args).InvokeAsync();
    }

    private static async Task RunAgentAsync(string listenAddress, string seedAddress)
    {
        var builder = WebApplication.CreateBuilder();

        // Configure logging
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        var (listen, listenPort) = ParseEndPoint(listenAddress);
        var (seed, _) = ParseEndPoint(seedAddress);

        // Configure Kestrel for gRPC
        builder.ConfigureRapidClusterKestrel(listenPort);

        // Add Rapid services
        builder.Services
            .AddRapidCluster(options => options.SeedAddresses = [seed])
            .UseGrpcTransport()
            .UseListenAddress(listen);

        // Add background service to monitor cluster
        builder.Services.AddHostedService<ClusterMonitorService>();

        var app = builder.Build();

        // Map Rapid gRPC endpoints
        app.MapRapidClusterMembershipService();

        LogStarting(app.Logger, listenAddress);

        if (listen.Equals(seed))
        {
            LogClusterStarted(app.Logger, listenAddress);
        }
        else
        {
            LogClusterJoined(app.Logger, seedAddress);
        }

        await app.RunAsync();
    }

    /// <summary>
    /// Background service to monitor the cluster and subscribe to view updates.
    /// </summary>
#pragma warning disable CA1812 // Avoid uninstantiated internal classes - Instantiated by DI
    private sealed class ClusterMonitorService(
        IRapidCluster cluster,
        ILogger<ClusterMonitorService> logger) : BackgroundService
#pragma warning restore CA1812
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await foreach (var view in cluster.ViewUpdates.WithCancellation(stoppingToken))
                {
                    LogViewChange(logger, view.ConfigurationId.Version, view.Members.Length);
                    LogMembershipSize(logger, view.Members.Length);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }
    }

    /// <summary>
    /// Parses an address string in "host:port" format to an EndPoint.
    /// </summary>
    private static (EndPoint endpoint, int port) ParseEndPoint(string address)
    {
        var colonIndex = address.LastIndexOf(':');
        if (colonIndex < 0)
        {
            throw new ArgumentException($"Invalid address format: {address}. Expected 'host:port'.", nameof(address));
        }

        var host = address[..colonIndex];
        var portString = address[(colonIndex + 1)..];

        if (!int.TryParse(portString, out var port))
        {
            throw new ArgumentException($"Invalid port: {portString}.", nameof(address));
        }

        // Use IPEndPoint if it's an IP address, otherwise use DnsEndPoint
        if (IPAddress.TryParse(host, out var ipAddress))
        {
            return (new IPEndPoint(ipAddress, port), port);
        }

        return (new DnsEndPoint(host, port), port);
    }
}
