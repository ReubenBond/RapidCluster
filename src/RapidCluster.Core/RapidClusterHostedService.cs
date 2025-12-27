using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;

namespace RapidCluster;

/// <summary>
/// Service that manages the RapidCluster lifecycle.
/// Implements both <see cref="IRapidClusterLifecycle"/> for manual control
/// and <see cref="BackgroundService"/> for automatic lifecycle management.
/// </summary>
/// <remarks>
/// This service waits for <see cref="IHostApplicationLifetime.ApplicationStarted"/> before
/// initializing the cluster, ensuring the gRPC server is listening before attempting to
/// join or accept connections from other nodes.
/// </remarks>
internal sealed partial class RapidClusterLifecycleService(
    IOptions<RapidClusterOptions> options,
    MembershipService membershipService,
    IHostApplicationLifetime applicationLifetime,
    ILogger<RapidClusterLifecycleService> logger) : BackgroundService, IRapidClusterLifecycle
{
    private readonly RapidClusterOptions _options = options.Value;
    private readonly ILogger<RapidClusterLifecycleService> _logger = logger;
    private bool _started;

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting RapidCluster service on {ListenAddress}")]
    private partial void LogStarting(LoggableEndpoint ListenAddress);

    [LoggerMessage(Level = LogLevel.Information, Message = "RapidCluster service started successfully")]
    private partial void LogStarted();

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopping RapidCluster service")]
    private partial void LogStopping();

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in RapidCluster service")]
    private partial void LogError(Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Waiting for application to start before initializing cluster")]
    private partial void LogWaitingForApplicationStart();

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Wait for the application (including Kestrel) to fully start before initializing the cluster.
        // This ensures our gRPC server is listening before we attempt to join or accept connections.
        LogWaitingForApplicationStart();

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            stoppingToken,
            applicationLifetime.ApplicationStarted);

        // Wait for either ApplicationStarted or stoppingToken to be triggered
        // Use SuppressThrowing so we don't throw when the token is cancelled
        await Task.Delay(Timeout.Infinite, linkedCts.Token)
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        // If we're stopping, don't initialize
        if (stoppingToken.IsCancellationRequested)
        {
            return;
        }

        await InitializeClusterAsync(stoppingToken).ConfigureAwait(true);

        // Keep running until stoppingToken is cancelled
        await Task.Delay(Timeout.Infinite, stoppingToken)
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    /// <inheritdoc />
    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        await base.StopAsync(cancellationToken).ConfigureAwait(true);

        LogStopping();

        if (_started)
        {
            await membershipService.StopAsync(cancellationToken).ConfigureAwait(true);
        }
    }

    /// <inheritdoc />
    async Task IRapidClusterLifecycle.StartAsync(CancellationToken cancellationToken) => await InitializeClusterAsync(cancellationToken).ConfigureAwait(true);

    /// <inheritdoc />
    Task IRapidClusterLifecycle.StopAsync(CancellationToken cancellationToken) => StopAsync(cancellationToken);

    private async Task InitializeClusterAsync(CancellationToken cancellationToken)
    {
        try
        {
            LogStarting(new LoggableEndpoint(_options.ListenAddress));

            // Initialize the membership service (start new cluster or join existing)
            await membershipService.InitializeAsync(cancellationToken).ConfigureAwait(true);
            _started = true;

            LogStarted();
        }
        catch (Exception ex)
        {
            if (ex is OperationCanceledException && cancellationToken.IsCancellationRequested)
            {
                // Ignore
            }
            else
            {
                LogError(ex);
                throw;
            }
        }
    }
}
