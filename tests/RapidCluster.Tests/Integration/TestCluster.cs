using System.Net;
using System.Text;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RapidCluster.Grpc;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Integration;

/// <summary>
/// Represents a test cluster that tracks and manages nodes for integration tests.
/// Automatically shuts down all nodes when disposed.
/// </summary>
internal sealed class TestCluster : IAsyncDisposable
{
    private readonly List<WebApplication> _apps = [];
    private readonly ILoggerFactory _loggerFactory;
    private readonly TestClusterPortAllocator _portAllocator = new();
    private readonly string _logFilePath;

    public TestCluster(ITestOutputHelper outputHelper)
    {
        var context = TestContext.Current;
        var testName = context.Test?.TestDisplayName ?? "unknown_test";
        _logFilePath = GenerateLogFilePath(testName);

        _loggerFactory = LoggerFactory.Create(builder => builder
            .AddProvider(new FileLoggerProvider(_logFilePath))
            .AddFilter("Microsoft.AspNetCore", LogLevel.Warning)
            .AddFilter("Grpc.AspNetCore", LogLevel.Warning)
            .SetMinimumLevel(LogLevel.Debug));

        // Log the file path to xUnit so users know where to find detailed logs
        outputHelper.WriteLine($"Integration test logs: {_logFilePath}");
    }

    /// <summary>
    /// Gets the next available port for a node.
    /// </summary>
    public int GetNextPort() => _portAllocator.AllocatePort();

    /// <summary>
    /// Creates a seed node at the specified address.
    /// </summary>
    public async Task<(WebApplication App, IRapidCluster Cluster)> CreateSeedNodeAsync(EndPoint address, CancellationToken cancellationToken = default)
    {
        var port = ((IPEndPoint)address).Port;
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_loggerFactory);
        builder.ConfigureRapidClusterKestrel(port);

        builder.Services.AddRapidCluster(options =>
        {
            options.ListenAddress = address;
            options.SeedAddresses = [address]; // Same as listen = seed node
        });
        builder.Services.AddRapidClusterGrpc();

        var app = builder.Build();
        app.MapRapidClusterMembershipService();

        await app.StartAsync(cancellationToken).ConfigureAwait(true);
        _apps.Add(app);

        var cluster = app.Services.GetRequiredService<IRapidCluster>();

        // Wait for the cluster to be initialized
        await WaitForClusterInitializedAsync(cluster, cancellationToken).ConfigureAwait(true);

        return (app, cluster);
    }

    /// <summary>
    /// Creates a seed node with custom configuration.
    /// </summary>
    public async Task<(WebApplication App, IRapidCluster Cluster)> CreateSeedNodeAsync(
        EndPoint address,
        Action<RapidClusterOptions> configureOptions,
        CancellationToken cancellationToken = default)
    {
        var port = ((IPEndPoint)address).Port;
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_loggerFactory);
        builder.ConfigureRapidClusterKestrel(port);

        builder.Services.AddRapidCluster(options =>
        {
            options.ListenAddress = address;
            options.SeedAddresses = [address];
            configureOptions(options);
        });
        builder.Services.AddRapidClusterGrpc();

        var app = builder.Build();
        app.MapRapidClusterMembershipService();

        await app.StartAsync(cancellationToken).ConfigureAwait(true);
        _apps.Add(app);

        var cluster = app.Services.GetRequiredService<IRapidCluster>();

        // Wait for the cluster to be initialized
        await WaitForClusterInitializedAsync(cluster, cancellationToken).ConfigureAwait(true);

        return (app, cluster);
    }

    /// <summary>
    /// Creates a joiner node that joins through the specified seed.
    /// </summary>
    public async Task<(WebApplication App, IRapidCluster Cluster)> CreateJoinerNodeAsync(
        EndPoint address,
        EndPoint seedAddress,
        CancellationToken cancellationToken = default)
    {
        var port = ((IPEndPoint)address).Port;
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_loggerFactory);
        builder.ConfigureRapidClusterKestrel(port);

        builder.Services.AddRapidCluster(options =>
        {
            options.ListenAddress = address;
            options.SeedAddresses = [seedAddress];
        });
        builder.Services.AddRapidClusterGrpc();

        var app = builder.Build();
        app.MapRapidClusterMembershipService();

        await app.StartAsync(cancellationToken).ConfigureAwait(true);
        _apps.Add(app);

        var cluster = app.Services.GetRequiredService<IRapidCluster>();

        // Wait for the cluster to be initialized
        await WaitForClusterInitializedAsync(cluster, cancellationToken).ConfigureAwait(true);

        return (app, cluster);
    }

    /// <summary>
    /// Creates a joiner node with custom configuration.
    /// </summary>
    public async Task<(WebApplication App, IRapidCluster Cluster)> CreateJoinerNodeAsync(
        EndPoint address,
        EndPoint seedAddress,
        Action<RapidClusterOptions> configureOptions,
        CancellationToken cancellationToken = default)
    {
        var port = ((IPEndPoint)address).Port;
        var builder = WebApplication.CreateBuilder();
        builder.Logging.ClearProviders();
        builder.Services.AddSingleton(_loggerFactory);
        builder.ConfigureRapidClusterKestrel(port);

        builder.Services.AddRapidCluster(options =>
        {
            options.ListenAddress = address;
            options.SeedAddresses = [seedAddress];
            configureOptions(options);
        });
        builder.Services.AddRapidClusterGrpc();

        var app = builder.Build();
        app.MapRapidClusterMembershipService();

        await app.StartAsync(cancellationToken).ConfigureAwait(true);
        _apps.Add(app);

        var cluster = app.Services.GetRequiredService<IRapidCluster>();

        // Wait for the cluster to be initialized
        await WaitForClusterInitializedAsync(cluster, cancellationToken).ConfigureAwait(true);

        return (app, cluster);
    }

    /// <summary>
    /// Waits for a cluster to be initialized (has at least one member).
    /// </summary>
    private static async Task WaitForClusterInitializedAsync(IRapidCluster cluster, CancellationToken cancellationToken)
    {
        while (cluster.CurrentView.Members.Length == 0)
        {
            await Task.Delay(10, cancellationToken).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Waits for a cluster to reach at least the expected size.
    /// </summary>
    public static async Task WaitForClusterSizeAsync(IRapidCluster cluster, int expectedSize, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (cluster.CurrentView.Members.Length >= expectedSize)
                return;
            await Task.Delay(10).ConfigureAwait(true);
        }
        throw new TimeoutException($"Cluster did not reach expected size {expectedSize} within {timeout}");
    }

    /// <summary>
    /// Waits for a cluster to reach exactly the expected size.
    /// </summary>
    public static async Task WaitForClusterSizeExactAsync(IRapidCluster cluster, int expectedSize, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (cluster.CurrentView.Members.Length == expectedSize)
                return;
            await Task.Delay(10).ConfigureAwait(true);
        }
        throw new TimeoutException($"Cluster did not reach expected size {expectedSize} within {timeout}. Current size: {cluster.CurrentView.Members.Length}");
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var app in _apps)
        {
#pragma warning disable CA1031
            try
            {
                await app.StopAsync().ConfigureAwait(true);
                await app.DisposeAsync().ConfigureAwait(true);
            }
            catch
            {
                // Ignore disposal errors in tests
            }
#pragma warning restore CA1031
        }
        _apps.Clear();
        _loggerFactory.Dispose();
        _portAllocator.Dispose();

        // Attach log file to test context
        var context = TestContext.Current;
        if (File.Exists(_logFilePath))
        {
            var logFileName = Path.GetFileName(_logFilePath);
            context.AddAttachment(logFileName, _logFilePath);
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Generates a unique log file path for integration tests.
    /// </summary>
    private static string GenerateLogFilePath(string testName)
    {
        var sanitizedTestName = SanitizeFileName(testName);
        var uniqueId = Guid.NewGuid().ToString("N")[..8];

        var assemblyLocation = typeof(TestCluster).Assembly.Location;
        var baseDirectory = Path.GetDirectoryName(assemblyLocation) ?? AppContext.BaseDirectory;
        var logsDirectory = Path.Combine(baseDirectory, "logs");

        return Path.Combine(logsDirectory, $"rapidcluster_integration_{sanitizedTestName}_{uniqueId}.log");
    }

    /// <summary>
    /// Sanitizes a string to be used as a file name.
    /// </summary>
    private static string SanitizeFileName(string name)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = new StringBuilder();
        foreach (var c in name)
        {
            sanitized.Append(invalidChars.Contains(c) ? '_' : c);
        }
        var result = sanitized.ToString();
        return result.Length > 100 ? result[..100] : result;
    }
}
