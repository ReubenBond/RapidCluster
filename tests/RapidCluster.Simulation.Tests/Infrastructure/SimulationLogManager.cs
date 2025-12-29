using System.Globalization;
using Clockwork;
using Microsoft.Extensions.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Manages logging for simulation tests, including logger factory creation
/// and log attachment to test contexts.
/// </summary>
internal sealed class SimulationLogManager : IDisposable
{
    private readonly InMemoryLoggerProvider _inMemoryLoggerProvider;
    private readonly int _seed;
    private bool _disposed;

    /// <summary>
    /// Maximum size in bytes for full log attachment (100 MB).
    /// If logs exceed this size, only Information level and above will be attached.
    /// </summary>
    private const long MaxFullLogSizeBytes = 100 * 1024 * 1024;

    /// <summary>
    /// Creates a new log manager for a simulation.
    /// </summary>
    /// <param name="timeProvider">The time provider for timestamps.</param>
    /// <param name="seed">The simulation seed (used in log file names).</param>
    public SimulationLogManager(TimeProvider timeProvider, int seed)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        _seed = seed;

        // Create logger factory with in-memory provider for attachment to test context
        _inMemoryLoggerProvider = new InMemoryLoggerProvider(timeProvider);
        LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
        {
            builder.AddProvider(_inMemoryLoggerProvider);
            builder.SetMinimumLevel(LogLevel.Debug);
        });
    }

    /// <summary>
    /// Gets the logger factory for creating loggers.
    /// </summary>
    public ILoggerFactory LoggerFactory { get; }

    /// <summary>
    /// Gets the in-memory log buffer for inspecting log entries.
    /// </summary>
    public InMemoryLogBuffer LogBuffer => _inMemoryLoggerProvider.Buffer;

    /// <summary>
    /// Attaches buffered logs to the test context.
    /// If the full log exceeds the size limit, a warning is added and only Information level and above are attached.
    /// </summary>
    /// <param name="testContext">The test context to attach logs to.</param>
    public void AttachLogsToTestContext(ITestContext? testContext)
    {
        if (testContext == null)
        {
            return;
        }

        var buffer = _inMemoryLoggerProvider.Buffer;
        var (fullContent, fullSizeBytes) = buffer.FormatAllEntriesWithSize();

        string logContent;
        string logFileName;

        if (fullSizeBytes <= MaxFullLogSizeBytes)
        {
            // Full log is under limit - attach it directly
            logContent = fullContent;
            logFileName = GenerateLogFileName(testContext.Test?.TestDisplayName, _seed);
        }
        else
        {
            // Full log exceeds limit - warn and attach only Information and above
            testContext.TestOutputHelper?.WriteLine(
                string.Create(CultureInfo.InvariantCulture, $"Warning: Full simulation log ({fullSizeBytes:N0} bytes) exceeds {MaxFullLogSizeBytes:N0} byte limit. ") +
                "Attaching only Information level and above.");

            var (filteredContent, _) = buffer.FormatEntriesWithSize(LogLevel.Information);
            logContent = filteredContent;
            logFileName = GenerateLogFileName(testContext.Test?.TestDisplayName, _seed, filtered: true);
        }

        // Attach log content directly to the test context
        testContext.AddAttachment(logFileName, logContent);
    }

    /// <summary>
    /// Generates a unique log file name for a simulation.
    /// </summary>
    private static string GenerateLogFileName(string? testName, int seed, bool filtered = false)
    {
        var sanitizedTestName = SanitizeFileName(testName ?? "unknown_test");
        var uniqueId = Guid.NewGuid().ToString("N")[..8];
        var suffix = filtered ? "_info_and_above" : "";

        return string.Create(CultureInfo.InvariantCulture, $"rapid_sim_{sanitizedTestName}_{seed}_{uniqueId}{suffix}.log");
    }

    /// <summary>
    /// Sanitizes a string to be used as a file name by removing invalid characters.
    /// </summary>
    private static string SanitizeFileName(string name)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = new System.Text.StringBuilder();
        foreach (var c in name)
        {
            sanitized.Append(invalidChars.Contains(c) ? '_' : c);
        }
        // Truncate to a reasonable length to avoid path length issues
        var result = sanitized.ToString();
        return result.Length > 100 ? result[..100] : result;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Dispose the logger factory first (removes reference to provider)
        LoggerFactory.Dispose();

        // Explicitly dispose the in-memory logger provider
        _inMemoryLoggerProvider.Dispose();
    }
}
