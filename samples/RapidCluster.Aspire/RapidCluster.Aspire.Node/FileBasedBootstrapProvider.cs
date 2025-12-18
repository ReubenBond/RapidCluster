using System.Net;
using System.Text.Json;
using RapidCluster.Discovery;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// A seed provider that uses a shared file for bootstrap coordination.
/// All nodes write their addresses to the file, and once the expected cluster size
/// is reached, returns a deterministically ordered list of seeds where the first
/// seed becomes the bootstrap coordinator.
/// </summary>
/// <remarks>
/// <para>
/// This provider solves the "simultaneous startup" problem in dynamic environments
/// like Aspire where all nodes start at once and need to elect a single bootstrap
/// coordinator without prior knowledge of each other's addresses.
/// </para>
/// <para>
/// The bootstrap process works as follows:
/// 1. Each node registers its address in the shared file
/// 2. Each node waits until all expected nodes have registered
/// 3. All nodes receive the same sorted list of seeds
/// 4. The node whose address is first in the sorted list becomes the bootstrap coordinator
///    (it will see itself as first and start a new cluster)
/// 5. Other nodes will join through that coordinator
/// </para>
/// <para>
/// This approach doesn't require a separate "bootstrap coordinator" designation - the
/// ordering is determined automatically based on address sorting. The first node in
/// the sorted list starts the cluster; others join it.
/// </para>
/// <para>
/// The file format is a JSON object mapping unique node identifiers to endpoint strings.
/// Each node writes its own entry atomically using file locking.
/// </para>
/// </remarks>
internal sealed partial class FileBasedBootstrapProvider : ISeedProvider, IDisposable
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    private readonly string _filePath;
    private readonly int _clusterSize;
    private readonly ILogger<FileBasedBootstrapProvider> _logger;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _startupTimeout;
    private EndPoint? _myAddress;
    private bool _registered;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileBasedBootstrapProvider"/> class.
    /// </summary>
    /// <param name="filePath">Path to the shared bootstrap file.</param>
    /// <param name="clusterSize">Expected number of nodes in the cluster.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="pollInterval">How often to poll the file for updates. Default: 500ms.</param>
    /// <param name="startupTimeout">How long to wait for all nodes to register. Default: 60s.</param>
    public FileBasedBootstrapProvider(
        string filePath,
        int clusterSize,
        ILogger<FileBasedBootstrapProvider> logger,
        TimeSpan? pollInterval = null,
        TimeSpan? startupTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(filePath);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfLessThan(clusterSize, 1);

        _filePath = filePath;
        _clusterSize = clusterSize;
        _logger = logger;
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(500);
        _startupTimeout = startupTimeout ?? TimeSpan.FromSeconds(60);
    }

    /// <summary>
    /// Sets this node's listen address. Must be called before GetSeedsAsync.
    /// </summary>
    public void SetMyAddress(EndPoint address)
    {
        _myAddress = address;
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        if (_myAddress == null)
        {
            throw new InvalidOperationException("SetMyAddress must be called before GetSeedsAsync");
        }

        // Register ourselves in the file
        if (!_registered)
        {
            await RegisterNodeAsync(cancellationToken);
            _registered = true;
        }

        // Wait for all expected nodes to register
        var allSeeds = await WaitForAllSeedsAsync(cancellationToken);
        if (allSeeds == null || allSeeds.Count == 0)
        {
            LogSeedsTimeout(_logger, _clusterSize, _startupTimeout.TotalSeconds);
            return [];
        }

        // Return all seeds in sorted order - MembershipService will use this to determine
        // bootstrap coordinator (first in list) vs joiners (others in list)
        // The sorting ensures all nodes see the same order, so they agree on who coordinates
        var seedsList = string.Join(", ", allSeeds.Select(EndpointToString));
        LogAllSeedsDiscovered(_logger, allSeeds.Count, seedsList);
        return allSeeds;
    }

    private async Task RegisterNodeAsync(CancellationToken cancellationToken)
    {
        var endpointStr = EndpointToString(_myAddress!);
        LogRegisteringNode(_logger, endpointStr, _filePath);

        var maxRetries = 10;
        for (var retry = 0; retry < maxRetries; retry++)
        {
            try
            {
                // Ensure directory exists
                var directory = Path.GetDirectoryName(_filePath);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                // Read-modify-write with file locking
                await using var stream = new FileStream(
                    _filePath,
                    FileMode.OpenOrCreate,
                    FileAccess.ReadWrite,
                    FileShare.None);

                var entries = await ReadEntriesAsync(stream, cancellationToken);

                // Use the endpoint string as the key (each unique address is a unique node)
                entries[endpointStr] = endpointStr;
                await WriteEntriesAsync(stream, entries, cancellationToken);

                LogNodeRegistered(_logger, endpointStr, entries.Count);
                return;
            }
            catch (IOException ex) when (retry < maxRetries - 1)
            {
                // File locked by another process, retry
                LogFileAccessRetry(_logger, _filePath, ex.Message);
                await Task.Delay(TimeSpan.FromMilliseconds(100 * (retry + 1)), cancellationToken);
            }
        }
    }

    private async Task<List<EndPoint>?> WaitForAllSeedsAsync(CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_startupTimeout);

        try
        {
            while (!timeoutCts.Token.IsCancellationRequested)
            {
                var entries = await ReadEntriesFromFileAsync(timeoutCts.Token);
                LogWaitingForSeeds(_logger, entries.Count, _clusterSize);

                if (entries.Count >= _clusterSize)
                {
                    // All nodes registered - return sorted list for deterministic ordering
                    // Sorting ensures all nodes agree on who is first (the bootstrap coordinator)
                    var seeds = entries.Values
                        .Select(ParseEndpoint)
                        .Where(e => e != null)
                        .Cast<EndPoint>()
                        .OrderBy(EndpointToString, StringComparer.Ordinal)
                        .ToList();

                    if (seeds.Count >= _clusterSize)
                    {
                        return seeds;
                    }
                }

                await Task.Delay(_pollInterval, timeoutCts.Token);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            // Timeout, not external cancellation
        }

        return null;
    }

    private async Task<Dictionary<string, string>> ReadEntriesFromFileAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (!File.Exists(_filePath))
            {
                return [];
            }

            await using var stream = new FileStream(
                _filePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite);

            return await ReadEntriesAsync(stream, cancellationToken);
        }
        catch (IOException)
        {
            // File may be locked or deleted
            return [];
        }
    }

    private static async Task<Dictionary<string, string>> ReadEntriesAsync(FileStream stream, CancellationToken cancellationToken)
    {
        if (stream.Length == 0)
        {
            return [];
        }

        stream.Position = 0;
        try
        {
            var result = await JsonSerializer.DeserializeAsync<Dictionary<string, string>>(stream, cancellationToken: cancellationToken);
            return result ?? [];
        }
        catch (JsonException)
        {
            return [];
        }
    }

    private static async Task WriteEntriesAsync(FileStream stream, Dictionary<string, string> entries, CancellationToken cancellationToken)
    {
        stream.Position = 0;
        stream.SetLength(0);
        await JsonSerializer.SerializeAsync(stream, entries, s_jsonOptions, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    private static string EndpointToString(EndPoint endpoint)
    {
        return endpoint switch
        {
            IPEndPoint ip => $"{ip.Address}:{ip.Port}",
            DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
            _ => endpoint.ToString() ?? ""
        };
    }

    private static EndPoint? ParseEndpoint(string str)
    {
        var parts = str.Split(':');
        if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
        {
            return null;
        }

        if (IPAddress.TryParse(parts[0], out var ip))
        {
            return new IPEndPoint(ip, port);
        }

        return new DnsEndPoint(parts[0], port);
    }

    public void Dispose()
    {
        // No resources to dispose
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Registering node address {Address} to file {FilePath}")]
    private static partial void LogRegisteringNode(ILogger logger, string address, string filePath);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} registered, {TotalNodes} node(s) now in file")]
    private static partial void LogNodeRegistered(ILogger logger, string address, int totalNodes);

    [LoggerMessage(Level = LogLevel.Debug, Message = "File {FilePath} locked, retrying: {Message}")]
    private static partial void LogFileAccessRetry(ILogger logger, string filePath, string message);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Waiting for seeds: {CurrentCount}/{ExpectedCount} registered")]
    private static partial void LogWaitingForSeeds(ILogger logger, int currentCount, int expectedCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "All {Count} seeds discovered: {Seeds}")]
    private static partial void LogAllSeedsDiscovered(ILogger logger, int count, string seeds);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Timed out waiting for {ExpectedCount} seeds after {TimeoutSeconds}s")]
    private static partial void LogSeedsTimeout(ILogger logger, int expectedCount, double timeoutSeconds);
}
