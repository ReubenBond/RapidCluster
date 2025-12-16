using MartinCostello.Logging.XUnit;
using Microsoft.Extensions.Logging;

namespace RapidCluster.Aspire.Tests;

/// <summary>
/// Integration tests for the RapidCluster Aspire sample.
/// These tests use Aspire.Hosting.Testing to spin up the AppHost
/// and verify that the cluster nodes start and become healthy.
/// </summary>
public sealed class ClusterIntegrationTests(ITestOutputHelper outputHelper) : IAsyncLifetime
{
    private const int ClusterSize = 5;
    private DistributedApplication? _app;
    private CancellationTokenSource? _logWatchCts;
    private readonly List<Task> _logWatchTasks = [];
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(2);
    private static readonly Uri HealthUri = new("/health", UriKind.Relative);

    public async ValueTask InitializeAsync()
    {
        var appHost = await DistributedApplicationTestingBuilder
            .CreateAsync<Projects.RapidCluster_Aspire_AppHost>();

        appHost.Services.AddLogging(logging =>
        {
            logging.SetMinimumLevel(LogLevel.Debug);
            logging.AddFilter(appHost.Environment.ApplicationName, LogLevel.Debug);
            logging.AddFilter("Aspire.", LogLevel.Warning);
            logging.AddXUnit(outputHelper);
        });

        appHost.Services.ConfigureHttpClientDefaults(clientBuilder =>
        {
            clientBuilder.AddStandardResilienceHandler();
        });

        _app = await appHost.BuildAsync();

        // Start watching resource logs before starting the app
        _logWatchCts = new CancellationTokenSource();

        // Watch logs for all cluster nodes
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            _logWatchTasks.Add(WatchResourceLogsAsync(_app, resourceName, _logWatchCts.Token));
        }

        await _app.StartAsync();
    }

    /// <summary>
    /// Watches logs from a specific resource and outputs them to the test output.
    /// </summary>
    private async Task WatchResourceLogsAsync(DistributedApplication app, string resourceName, CancellationToken cancellationToken)
    {
        try
        {
            var resourceLoggerService = app.Services.GetRequiredService<ResourceLoggerService>();

            await foreach (var logBatch in resourceLoggerService.WatchAsync(resourceName).WithCancellation(cancellationToken))
            {
                foreach (var log in logBatch)
                {
                    var prefix = log.IsErrorMessage ? "[ERR]" : "[INF]";
                    outputHelper.WriteLine($"[{resourceName}] {prefix} {log.Content}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
        catch (InvalidOperationException ex)
        {
            // Can occur if test output is disposed before this finishes
            outputHelper.WriteLine($"Error watching resource logs for {resourceName}: {ex}");
        }
    }

    public async ValueTask DisposeAsync()
    {
        // Cancel log watching
        if (_logWatchCts is not null)
        {
            await _logWatchCts.CancelAsync();
            _logWatchCts.Dispose();
        }

        // Wait for all log watch tasks to complete
        foreach (var task in _logWatchTasks)
        {
            try
            {
                await task;
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
        }

        if (_app is not null)
        {
            await _app.DisposeAsync();
        }
    }

    [Fact]
    public async Task ClusterResourceIsRunning()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for all cluster nodes to be in Running state
        var waitTasks = new Task[ClusterSize];
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            waitTasks[i] = _app.ResourceNotifications.WaitForResourceAsync(
                resourceName,
                KnownResourceStates.Running,
                cts.Token);
        }

        await Task.WhenAll(waitTasks);
    }

    [Fact]
    public async Task ClusterResourceBecomesHealthy()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for all cluster nodes to become healthy
        // This indicates the health check is passing, meaning the node
        // has joined the cluster and has members
        var waitTasks = new Task[ClusterSize];
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            waitTasks[i] = _app.ResourceNotifications.WaitForResourceHealthyAsync(
                resourceName,
                cts.Token);
        }

        await Task.WhenAll(waitTasks);
    }

    [Fact]
    public async Task ClusterHealthEndpointReturnsOk()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for the first node to be healthy
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster-0",
            cts.Token);

        // Create an HTTP client for the first cluster node
        using var httpClient = _app.CreateHttpClient("cluster-0");

        // Hit the health endpoint
        using var response = await httpClient.GetAsync(HealthUri, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
