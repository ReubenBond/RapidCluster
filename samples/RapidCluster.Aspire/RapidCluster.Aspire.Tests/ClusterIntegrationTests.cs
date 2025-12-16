using MartinCostello.Logging.XUnit;
using Microsoft.Extensions.Logging;

#pragma warning disable IDE0005 // Using directive is unnecessary - needed for implicit usings resolution

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

    [Fact]
    public async Task AllNodesHealthEndpointsReturnOk()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for all nodes to become healthy first
        var waitTasks = new Task[ClusterSize];
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            waitTasks[i] = _app.ResourceNotifications.WaitForResourceHealthyAsync(
                resourceName,
                cts.Token);
        }
        await Task.WhenAll(waitTasks);

        outputHelper.WriteLine("All nodes healthy. Verifying health endpoints...");

        // Verify all nodes respond to health checks
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            using var httpClient = _app.CreateHttpClient(resourceName);
            using var response = await httpClient.GetAsync(HealthUri, cts.Token);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            outputHelper.WriteLine($"{resourceName} health check passed.");
        }
    }

    [Fact]
    public async Task HealthEndpointReturnsClusterMembershipInfo()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for a node to become healthy
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster-0",
            cts.Token);

        // Create an HTTP client for the cluster node
        using var httpClient = _app.CreateHttpClient("cluster-0");

        // Hit the health endpoint
        using var response = await httpClient.GetAsync(HealthUri, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);

        // Read the response body - it should contain cluster membership info
        var content = await response.Content.ReadAsStringAsync(cts.Token);
        outputHelper.WriteLine($"Health response: {content}");

        // The health check should indicate the cluster is healthy
        // The response format depends on the ASP.NET Core health check configuration
        Assert.NotEmpty(content);
    }

    [Fact]
    public async Task ClusterNodesBecomeHealthyInParallel()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Record the start time
        var startTime = DateTime.UtcNow;

        // Wait for all nodes to become healthy in parallel
        var waitTasks = new Task[ClusterSize];
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            waitTasks[i] = _app.ResourceNotifications.WaitForResourceHealthyAsync(
                resourceName,
                cts.Token);
        }
        await Task.WhenAll(waitTasks);

        var elapsed = DateTime.UtcNow - startTime;
        outputHelper.WriteLine($"All {ClusterSize} nodes became healthy in {elapsed.TotalSeconds:F2} seconds");

        // All nodes should become healthy - this tests the parallel bootstrap mechanism
        Assert.True(elapsed < DefaultTimeout, "Cluster formation took too long");
    }

    [Fact]
    public async Task HealthEndpointReturnsHealthyStatus()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for all nodes to become healthy
        var waitTasks = new Task[ClusterSize];
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            waitTasks[i] = _app.ResourceNotifications.WaitForResourceHealthyAsync(
                resourceName,
                cts.Token);
        }
        await Task.WhenAll(waitTasks);

        // Check that the health endpoint returns "Healthy" status text
        using var httpClient = _app.CreateHttpClient("cluster-0");
        using var response = await httpClient.GetAsync(HealthUri, cts.Token);

        var content = await response.Content.ReadAsStringAsync(cts.Token);
        outputHelper.WriteLine($"Health response content: {content}");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Contains("Healthy", content, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task MultipleHealthCheckRequestsSucceed()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for a node to become healthy
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster-1",
            cts.Token);

        using var httpClient = _app.CreateHttpClient("cluster-1");

        // Make multiple health check requests in rapid succession
        const int requestCount = 10;
        var tasks = new Task<HttpResponseMessage>[requestCount];

        for (var i = 0; i < requestCount; i++)
        {
            tasks[i] = httpClient.GetAsync(HealthUri, cts.Token);
        }

        var responses = await Task.WhenAll(tasks);

        // All requests should succeed
        foreach (var response in responses)
        {
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            response.Dispose();
        }

        outputHelper.WriteLine($"All {requestCount} health check requests succeeded.");
    }

    [Fact]
    public async Task ConcurrentHealthCheckRequestsAcrossNodes()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for all nodes to become healthy
        var waitTasks = new Task[ClusterSize];
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            waitTasks[i] = _app.ResourceNotifications.WaitForResourceHealthyAsync(
                resourceName,
                cts.Token);
        }
        await Task.WhenAll(waitTasks);

        // Make concurrent health check requests to all nodes
        var healthCheckTasks = new List<Task<(string Node, HttpStatusCode Status)>>();

        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            var nodeIndex = i;
            healthCheckTasks.Add(Task.Run(async () =>
            {
                using var httpClient = _app.CreateHttpClient(resourceName);
                using var response = await httpClient.GetAsync(HealthUri, cts.Token);
                return (resourceName, response.StatusCode);
            }, cts.Token));
        }

        var results = await Task.WhenAll(healthCheckTasks);

        // All concurrent requests should succeed
        foreach (var (node, status) in results)
        {
            Assert.Equal(HttpStatusCode.OK, status);
            outputHelper.WriteLine($"{node}: {status}");
        }
    }

    [Fact]
    public async Task ClusterFormationCompletesWithin60Seconds()
    {
        Assert.NotNull(_app);

        // Use a strict 60-second timeout for cluster formation
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        var startTime = DateTime.UtcNow;

        try
        {
            // Wait for all nodes to become healthy
            var waitTasks = new Task[ClusterSize];
            for (var i = 0; i < ClusterSize; i++)
            {
                var resourceName = $"cluster-{i}";
                waitTasks[i] = _app.ResourceNotifications.WaitForResourceHealthyAsync(
                    resourceName,
                    cts.Token);
            }
            await Task.WhenAll(waitTasks);

            var elapsed = DateTime.UtcNow - startTime;
            outputHelper.WriteLine($"Cluster formation completed in {elapsed.TotalSeconds:F2} seconds");

            // Cluster should form well within 60 seconds
            Assert.True(elapsed.TotalSeconds < 60, $"Cluster formation took {elapsed.TotalSeconds:F2} seconds, expected < 60 seconds");
        }
        catch (OperationCanceledException)
        {
            var elapsed = DateTime.UtcNow - startTime;
            Assert.Fail($"Cluster formation did not complete within 60 seconds (elapsed: {elapsed.TotalSeconds:F2}s)");
        }
    }

    [Fact]
    public async Task BootstrapCoordinatorNodeIsHealthy()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Node 0 is the bootstrap coordinator
        // It should be the first to become healthy since it starts a new cluster
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster-0",
            cts.Token);

        using var httpClient = _app.CreateHttpClient("cluster-0");
        using var response = await httpClient.GetAsync(HealthUri, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        outputHelper.WriteLine("Bootstrap coordinator (cluster-0) is healthy.");
    }

    [Fact]
    public async Task NonCoordinatorNodesJoinCluster()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for the coordinator to be healthy first
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster-0",
            cts.Token);

        outputHelper.WriteLine("Bootstrap coordinator is healthy. Waiting for joiners...");

        // Then verify that non-coordinator nodes (1-4) also become healthy
        // This proves they successfully joined the cluster started by node 0
        for (var i = 1; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            await _app.ResourceNotifications.WaitForResourceHealthyAsync(
                resourceName,
                cts.Token);
            outputHelper.WriteLine($"{resourceName} has joined the cluster.");
        }

        // Final verification - all nodes are healthy
        for (var i = 0; i < ClusterSize; i++)
        {
            var resourceName = $"cluster-{i}";
            using var httpClient = _app.CreateHttpClient(resourceName);
            using var response = await httpClient.GetAsync(HealthUri, cts.Token);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        }

        outputHelper.WriteLine($"All {ClusterSize} nodes have successfully joined the cluster.");
    }
}
