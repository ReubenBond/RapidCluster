using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;

namespace RapidCluster;

/// <summary>
/// Service that manages the RapidCluster lifecycle.
/// Implements both <see cref="IRapidClusterLifecycle"/> for manual control
/// and <see cref="IHostedService"/> for automatic lifecycle management.
/// </summary>
internal sealed partial class RapidClusterLifecycleService(
    IOptions<RapidClusterOptions> options,
    MembershipService membershipService,
    ILogger<RapidClusterLifecycleService> logger) : IRapidClusterLifecycle, IHostedService
{
    private readonly RapidClusterOptions _options = options.Value;
    private readonly ILogger<RapidClusterLifecycleService> _logger = logger;

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting RapidCluster service on {ListenAddress}")]
    private partial void LogStarting(LoggableEndpoint ListenAddress);

    [LoggerMessage(Level = LogLevel.Information, Message = "RapidCluster service started successfully")]
    private partial void LogStarted();

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopping RapidCluster service")]
    private partial void LogStopping();

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in RapidCluster service")]
    private partial void LogError(Exception ex);

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        LogStopping();

        await membershipService.StopAsync(cancellationToken).ConfigureAwait(true);
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        try
        {
            LogStarting(new LoggableEndpoint(_options.ListenAddress));

            // Initialize the membership service (start new cluster or join existing)
            await membershipService.InitializeAsync(cancellationToken).ConfigureAwait(true);

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

    // IHostedService implementation delegates to the same methods
    Task IHostedService.StartAsync(CancellationToken cancellationToken) => StartAsync(cancellationToken);
    Task IHostedService.StopAsync(CancellationToken cancellationToken) => StopAsync(cancellationToken);
}
