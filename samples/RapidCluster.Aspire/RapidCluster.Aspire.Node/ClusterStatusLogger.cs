using System.Diagnostics.CodeAnalysis;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// Background service that logs cluster status changes.
/// Uses IServiceProvider to lazily resolve IRapidCluster after the server has started.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed partial class ClusterStatusLogger(
    IServiceProvider serviceProvider,
    ILogger<ClusterStatusLogger> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            // Get the cluster - the lifecycle service ensures it's initialized after ApplicationStarted
            var cluster = serviceProvider.GetRequiredService<IRapidCluster>();
            await foreach (var view in cluster.ViewUpdates.WithCancellation(stoppingToken))
            {
                LogViewChange(logger, view.ConfigurationId.Version, view.Members.Length);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Cluster view changed: ConfigId={ConfigId}, Members={MemberCount}")]
    private static partial void LogViewChange(ILogger logger, long configId, int memberCount);
}
