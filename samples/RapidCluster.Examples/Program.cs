using System.CommandLine;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using RapidCluster;

/// <summary>
/// RapidCluster example application using modern ASP.NET hosting.
/// Demonstrates cluster bootstrapping with multiple seed nodes.
/// </summary>
internal sealed partial class Program
{

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting RapidCluster agent on {Listen}")]
    private static partial void LogStarting(ILogger logger, string Listen);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster started on {Listen}")]
    private static partial void LogClusterStarted(ILogger logger, string Listen);

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster joined. Seeds: {Seeds}")]
    private static partial void LogClusterJoined(ILogger logger, string Seeds);

    [LoggerMessage(Level = LogLevel.Information, Message = "Current membership size: {Size}")]
    private static partial void LogMembershipSize(ILogger logger, int Size);

    [LoggerMessage(Level = LogLevel.Information, Message = "Proposal detected: {Change}")]
    private static partial void LogProposalDetected(ILogger logger, ClusterStatusChange Change);

    [LoggerMessage(Level = LogLevel.Information, Message = "View change: Config={ConfigId}, Members={MemberCount}")]
    private static partial void LogViewChange(ILogger logger, long ConfigId, int MemberCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Kicked from cluster: {Change}")]
    private static partial void LogKicked(ILogger logger, ClusterStatusChange Change);

    [LoggerMessage(Level = LogLevel.Information, Message = "Bootstrap expect: waiting for {ExpectedNodes} nodes (timeout: {Timeout})")]
    private static partial void LogBootstrapExpect(ILogger logger, int ExpectedNodes, TimeSpan Timeout);

    private static int Main(string[] args)
    {
        var listenOption = new Option<string>("--listen")
        {
            Description = "The listening address (e.g., 127.0.0.1:1234)",
            Required = true
        };

        var seedsOption = new Option<string[]>("--seed")
        {
            Description = "Seed node address(es) for bootstrap. Can be specified multiple times. (e.g., --seed 127.0.0.1:1234 --seed 127.0.0.1:1235)",
            Required = true,
            AllowMultipleArgumentsPerToken = true
        };

        var bootstrapExpectOption = new Option<int>("--bootstrap-expect")
        {
            Description = "Number of nodes expected to form the initial cluster. Set to 0 to disable waiting (default).",
            DefaultValueFactory = _ => 0
        };

        var bootstrapTimeoutOption = new Option<int>("--bootstrap-timeout")
        {
            Description = "Timeout in seconds for waiting for bootstrap-expect nodes to join. Default: 300 (5 minutes).",
            DefaultValueFactory = _ => 300
        };

        var rootCommand = new RootCommand("RapidCluster Standalone Node");
        rootCommand.Options.Add(listenOption);
        rootCommand.Options.Add(seedsOption);
        rootCommand.Options.Add(bootstrapExpectOption);
        rootCommand.Options.Add(bootstrapTimeoutOption);

        rootCommand.SetAction((parseResult) =>
        {
            var listen = parseResult.GetValue(listenOption)!;
            var seeds = parseResult.GetValue(seedsOption)!;
            var bootstrapExpect = parseResult.GetValue(bootstrapExpectOption);
            var bootstrapTimeout = TimeSpan.FromSeconds(parseResult.GetValue(bootstrapTimeoutOption));

            RunAgentAsync(listen, seeds, bootstrapExpect, bootstrapTimeout).Wait();
        });

        return rootCommand.Parse(args).InvokeAsync().GetAwaiter().GetResult();
    }

    private static async Task RunAgentAsync(
        string listenAddress,
        string[] seedAddresses,
        int bootstrapExpect,
        TimeSpan bootstrapTimeout)
    {
        var builder = WebApplication.CreateBuilder();

        // Configure logging
        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(LogLevel.Information);

        var listen = RapidClusterUtils.HostFromString(listenAddress);
        var seeds = seedAddresses.Select(RapidClusterUtils.HostFromString).ToList();

        // Configure Kestrel for gRPC
        builder.ConfigureRapidClusterKestrel(listen.Port);

        // Add Rapid services with bootstrapping options
        builder.Services.AddRapidCluster(options =>
        {
            options.ListenAddress = listen;
            options.SeedAddresses = seeds;
            options.BootstrapExpect = bootstrapExpect;
            options.BootstrapTimeout = bootstrapTimeout;
        });

        // Add background service to monitor cluster
        builder.Services.AddHostedService<ClusterMonitorService>();

        var app = builder.Build();

        // Map Rapid gRPC endpoints
        app.MapRapidClusterMembershipService();

        LogStarting(app.Logger, listenAddress);

        // Check if this node is a seed (listen address is in the seed list)
        var isSeed = seeds.Any(s => s.Equals(listen));
        if (isSeed && seeds.Count == 1 && seeds[0].Equals(listen))
        {
            // Single seed - this node starts a new cluster
            LogClusterStarted(app.Logger, listenAddress);
        }
        else
        {
            // Joining existing cluster or multi-seed bootstrap
            var seedsList = string.Join(", ", seedAddresses);
            LogClusterJoined(app.Logger, seedsList);
        }

        if (bootstrapExpect > 0)
        {
            LogBootstrapExpect(app.Logger, bootstrapExpect, bootstrapTimeout);
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
