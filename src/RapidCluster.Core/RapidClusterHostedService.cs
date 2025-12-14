using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;

namespace RapidCluster;

/// <summary>
/// Hosted service that manages the RapidCluster lifecycle.
/// </summary>
internal sealed partial class RapidClusterHostedService(
    IOptions<RapidClusterOptions> options,
    MembershipService membershipService,
    ILogger<RapidClusterHostedService> logger) : IHostedService
{
    private readonly RapidClusterOptions _options = options.Value;
    private readonly ILogger<RapidClusterHostedService> _logger = logger;

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting RapidCluster service on {ListenAddress}")]
    private partial void LogStarting(LoggableEndpoint ListenAddress);

    [LoggerMessage(Level = LogLevel.Information, Message = "RapidCluster service started successfully")]
    private partial void LogStarted();

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopping RapidCluster service")]
    private partial void LogStopping();

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in RapidCluster service")]
    private partial void LogError(Exception ex);

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        LogStopping();

        await membershipService.StopAsync(cancellationToken).ConfigureAwait(true);
    }

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
}
