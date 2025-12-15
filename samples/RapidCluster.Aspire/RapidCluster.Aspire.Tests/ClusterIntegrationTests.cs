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
    private DistributedApplication? _app;
    private static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(2);
    private static readonly Uri HealthUri = new("/health", UriKind.Relative);
    private static readonly Uri ReadyUri = new("/health/ready", UriKind.Relative);

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
        await _app.StartAsync();
    }

    public async ValueTask DisposeAsync()
    {
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

        // Wait for the cluster resource to be in Running state
        await _app.ResourceNotifications.WaitForResourceAsync(
            "cluster",
            KnownResourceStates.Running,
            cts.Token);
    }

    [Fact]
    public async Task ClusterResourceBecomesHealthy()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for the cluster resource to become healthy
        // This indicates the health check is passing, meaning the node
        // has joined the cluster and has members
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster",
            cts.Token);
    }

    [Fact]
    public async Task ClusterHealthEndpointReturnsOk()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for the resource to be healthy first
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster",
            cts.Token);

        // Create an HTTP client for the cluster resource
        using var httpClient = _app.CreateHttpClient("cluster");

        // Hit the health endpoint
        using var response = await httpClient.GetAsync(HealthUri, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ClusterReadyEndpointReturnsOk()
    {
        Assert.NotNull(_app);

        using var cts = new CancellationTokenSource(DefaultTimeout);

        // Wait for the resource to be healthy first
        await _app.ResourceNotifications.WaitForResourceHealthyAsync(
            "cluster",
            cts.Token);

        // Create an HTTP client for the cluster resource
        using var httpClient = _app.CreateHttpClient("cluster");

        // Hit the ready endpoint (includes the cluster-membership health check)
        using var response = await httpClient.GetAsync(ReadyUri, cts.Token);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
