using System.CommandLine;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RapidCluster;

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

    [LoggerMessage(Level = LogLevel.Information, Message = "Proposal detected: {Change}")]
    private static partial void LogProposalDetected(ILogger logger, ClusterStatusChange Change);

    [LoggerMessage(Level = LogLevel.Information, Message = "View change: Config={ConfigId}, Members={MemberCount}")]
    private static partial void LogViewChange(ILogger logger, long ConfigId, int MemberCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kicked from cluster: {Change}")]
    private static partial void LogKicked(ILogger logger, ClusterStatusChange Change);

    private static async Task<int> Main(string[] args)
    {
        var listenOption = new Option<string>(
            "--listen",
            "The listening address (e.g., 127.0.0.1:1234)")
        { Required = true };

        var seedOption = new Option<string>(
            "--seed",
            "The seed node's address for bootstrap (e.g., 127.0.0.1:1234)")
        { Required = true };

        var rootCommand = new RootCommand("RapidCluster Standalone Node");
        rootCommand.Options.Add(listenOption);
        rootCommand.Options.Add(seedOption);

        rootCommand.SetAction((parseResult) =>
        {
            var listen = parseResult.GetValue(listenOption);
            var seed = parseResult.GetValue(seedOption);
            RunAgentAsync(listen!, seed!).Wait();
        });

        return rootCommand.Parse(args).InvokeAsync().GetAwaiter().GetResult();
    }

    private static async Task RunAgentAsync(string listenAddress, string seedAddress)
    {
        var builder = WebApplication.CreateBuilder();

        // Configure logging
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        var listen = RapidClusterUtils.HostFromString(listenAddress);
        var seed = RapidClusterUtils.HostFromString(seedAddress);

        // Configure Kestrel for gRPC
        builder.ConfigureRapidClusterKestrel(listen.Port);

        // Add Rapid services
        builder.Services.AddRapidCluster(options =>
        {
            options.ListenAddress = listen;
            options.SeedAddress = seed;
        });

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
    /// Background service to monitor the cluster and subscribe to events.
    /// </summary>
#pragma warning disable CA1812 // Avoid uninstantiated internal classes - Instantiated by DI
    private sealed class ClusterMonitorService(
        IRapidCluster cluster,
        ILogger<Program.ClusterMonitorService> logger,
        IHostApplicationLifetime lifetime) : BackgroundService
#pragma warning restore CA1812
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            try
            {
                await foreach (var notification in cluster.EventStream.WithCancellation(stoppingToken))
                {
                    switch (notification.Event)
                    {
                        case ClusterEvents.ViewChangeProposal:
                            LogProposalDetected(logger, notification.Change);
                            break;
                        case ClusterEvents.ViewChange:
                            LogViewChange(logger, notification.Change.ConfigurationId, notification.Change.Membership.Count);
                            LogMembershipSize(logger, notification.Change.Membership.Count);
                            break;
                        case ClusterEvents.Kicked:
                            LogKicked(logger, notification.Change);
                            break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }

            // Leave gracefully
            await cluster.LeaveGracefullyAsync();

            // Signal shutdown
            lifetime.StopApplication();
        }
    }
}
