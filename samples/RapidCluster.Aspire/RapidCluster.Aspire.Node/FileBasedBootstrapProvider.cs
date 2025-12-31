using System.Globalization;
using System.Net;
using System.Text.Json;
using RapidCluster.Discovery;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// A seed provider that uses a shared file for bootstrap coordination.
/// Multiple nodes write their addresses to the file, and the node with index 0
/// becomes the bootstrap coordinator (starts a new cluster).
/// Other nodes wait until index 0's address appears, then use it as their seed.
/// </summary>
/// <remarks>
/// <para>
/// This provider solves the "simultaneous startup" problem in dynamic environments
/// like Aspire where all nodes start at once and need to elect a single bootstrap
/// coordinator without prior knowledge of each other's addresses.
/// </para>
/// <para>
/// The file format is a JSON object mapping node index to endpoint string.
/// Each node writes its own entry atomically using file locking.
/// </para>
/// </remarks>
internal sealed partial class FileBasedBootstrapProvider : ISeedProvider, IDisposable
{
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };

    private readonly string _filePath;
    private readonly int _nodeIndex;
    private readonly ILogger<FileBasedBootstrapProvider> _logger;
    private readonly IListenAddressProvider _listenAddressProvider;
    private readonly TimeSpan _pollInterval;
    private readonly TimeSpan _startupTimeout;
    private bool _registered;

    /// <summary>
    /// Initializes a new instance of the <see cref="FileBasedBootstrapProvider"/> class.
    /// </summary>
    /// <param name="filePath">Path to the shared bootstrap file.</param>
    /// <param name="nodeIndex">This node's index (0-based). Node 0 becomes bootstrap coordinator.</param>
    /// <param name="listenAddressProvider">Provider for this node's listen address.</param>
    /// <param name="logger">The logger.</param>
    /// <param name="pollInterval">How often to poll the file for updates. Default: 500ms.</param>
    /// <param name="startupTimeout">How long to wait for bootstrap coordinator to appear. Default: 60s.</param>
    public FileBasedBootstrapProvider(
        string filePath,
        int nodeIndex,
        IListenAddressProvider listenAddressProvider,
        ILogger<FileBasedBootstrapProvider> logger,
        TimeSpan? pollInterval = null,
        TimeSpan? startupTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(filePath);
        ArgumentNullException.ThrowIfNull(listenAddressProvider);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentOutOfRangeException.ThrowIfNegative(nodeIndex);

        _filePath = filePath;
        _nodeIndex = nodeIndex;
        _listenAddressProvider = listenAddressProvider;
        _logger = logger;
        _pollInterval = pollInterval ?? TimeSpan.FromMilliseconds(500);
        _startupTimeout = startupTimeout ?? TimeSpan.FromSeconds(60);
    }

    /// <inheritdoc/>
    public async ValueTask<IReadOnlyList<EndPoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var myAddress = _listenAddressProvider.ListenAddress;

        // Register ourselves in the file
        if (!_registered)
        {
            await RegisterNodeAsync(myAddress, cancellationToken);
            _registered = true;
        }

        // If we're node 0, we're the bootstrap coordinator - return empty list to start new cluster
        if (_nodeIndex == 0)
        {
            var myAddressStr = EndpointToString(myAddress);
            LogBootstrapCoordinator(_logger, _nodeIndex, myAddressStr);
            return [];
        }

        // Otherwise, wait for node 0 to register and use its address as seed
        var node0Address = await WaitForBootstrapCoordinatorAsync(cancellationToken);
        if (node0Address != null)
        {
            var node0AddressStr = EndpointToString(node0Address);
            LogUsingBootstrapCoordinator(_logger, _nodeIndex, node0AddressStr);
            return [node0Address];
        }

        // Timeout waiting for bootstrap coordinator - return empty list
        // This will cause the node to fail to join, which is the correct behavior
        LogBootstrapCoordinatorTimeout(_logger, _nodeIndex, _startupTimeout.TotalSeconds);
        return [];
    }

    private async Task RegisterNodeAsync(EndPoint myAddress, CancellationToken cancellationToken)
    {
        var endpointStr = EndpointToString(myAddress);
        LogRegisteringNode(_logger, _nodeIndex, endpointStr, _filePath);

        const int maxRetries = 10;
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
                entries[_nodeIndex.ToString(CultureInfo.InvariantCulture)] = endpointStr;
                await WriteEntriesAsync(stream, entries, cancellationToken);

                LogNodeRegistered(_logger, _nodeIndex, entries.Count);
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

    private async Task<EndPoint?> WaitForBootstrapCoordinatorAsync(CancellationToken cancellationToken)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(_startupTimeout);

        try
        {
            while (!timeoutCts.Token.IsCancellationRequested)
            {
                var entries = await ReadEntriesFromFileAsync(timeoutCts.Token);
                if (entries.TryGetValue("0", out var node0Str) && !string.IsNullOrEmpty(node0Str))
                {
                    return ParseEndpoint(node0Str);
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
        await JsonSerializer.SerializeAsync(stream, entries, JsonOptions, cancellationToken);
        await stream.FlushAsync(cancellationToken);
    }

    private static string EndpointToString(EndPoint endpoint)
    {
        return endpoint switch
        {
            IPEndPoint ip => string.Create(CultureInfo.InvariantCulture, $"{ip.Address}:{ip.Port}"),
            DnsEndPoint dns => string.Create(CultureInfo.InvariantCulture, $"{dns.Host}:{dns.Port}"),
            _ => endpoint.ToString() ?? "",
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

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {NodeIndex} is the bootstrap coordinator at {Address}")]
    private static partial void LogBootstrapCoordinator(ILogger logger, int nodeIndex, string address);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {NodeIndex} will join using bootstrap coordinator at {Address}")]
    private static partial void LogUsingBootstrapCoordinator(ILogger logger, int nodeIndex, string address);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {NodeIndex} timed out after {TimeoutSeconds}s waiting for bootstrap coordinator")]
    private static partial void LogBootstrapCoordinatorTimeout(ILogger logger, int nodeIndex, double timeoutSeconds);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {NodeIndex} registering address {Address} to file {FilePath}")]
    private static partial void LogRegisteringNode(ILogger logger, int nodeIndex, string address, string filePath);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {NodeIndex} registered, {TotalNodes} node(s) now in file")]
    private static partial void LogNodeRegistered(ILogger logger, int nodeIndex, int totalNodes);

    [LoggerMessage(Level = LogLevel.Debug, Message = "File {FilePath} locked, retrying: {Message}")]
    private static partial void LogFileAccessRetry(ILogger logger, string filePath, string message);
}
