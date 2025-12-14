using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;

namespace RapidCluster;

/// <summary>
/// Hosted service that manages the RapidCluster lifecycle.
/// </summary>
internal sealed partial class RapidClusterService(
    IOptions<RapidClusterOptions> options,
    MembershipService membershipService,
    ILogger<RapidClusterService> logger) : BackgroundService, IAsyncDisposable
{
    private readonly RapidClusterOptions _options = options.Value;
    private readonly ILogger<RapidClusterService> _logger = logger;
    private int _disposed;

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting RapidCluster service on {ListenAddress}")]
    private partial void LogStarting(LoggableEndpoint ListenAddress);

    [LoggerMessage(Level = LogLevel.Information, Message = "RapidCluster service started successfully")]
    private partial void LogStarted();

    [LoggerMessage(Level = LogLevel.Information, Message = "Stopping RapidCluster service")]
    private partial void LogStopping();

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in RapidCluster service")]
    private partial void LogError(Exception ex);

    public MembershipService MembershipService { get; } = membershipService;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            LogStarting(new LoggableEndpoint(_options.ListenAddress));

            // Initialize the membership service (start new cluster or join existing)
            await MembershipService.InitializeAsync(stoppingToken).ConfigureAwait(true);

            LogStarted();

            // Keep running until cancellation
            await Task.Delay(Timeout.Infinite, stoppingToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (Exception ex)
        {
            LogError(ex);
            throw;
        }
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        LogStopping();

        await MembershipService.StopAsync(cancellationToken).ConfigureAwait(true);

        await base.StopAsync(cancellationToken).ConfigureAwait(true);
    }

    public override void Dispose()
    {
        DisposeAsync().AsTask().GetAwaiter().GetResult();
        base.Dispose();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return; // Already disposed
        }

        await MembershipService.DisposeAsync().ConfigureAwait(true);

        base.Dispose();
    }
}
