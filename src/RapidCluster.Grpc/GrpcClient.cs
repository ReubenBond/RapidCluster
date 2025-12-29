using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Grpc;

/// <summary>
/// gRPC-based messaging client for Rapid.
/// Implements IHostedService to ensure proper shutdown ordering.
/// </summary>
internal sealed partial class GrpcClient(
    IOptions<RapidClusterProtocolOptions> options,
    RapidClusterMetrics metrics,
    ILogger<GrpcClient> logger) : IMessagingClient, IHostedService
{
    private readonly RapidClusterProtocolOptions _options = options.Value;
    private readonly RapidClusterMetrics _metrics = metrics;
#pragma warning disable CA1823 // Avoid unused private fields
    private readonly ILogger<GrpcClient> _logger = logger;
#pragma warning restore CA1823 // Avoid unused private fields
    private readonly ConcurrentDictionary<string, Pb.MembershipService.MembershipServiceClient> _clients = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<int, Task> _pendingTasks = new();
    private readonly CancellationTokenSource _stoppingCts = new();
    private int _taskIdCounter;
    private bool _disposed;

    private const string SendRequestMethod = "SendRequest";

    /// <summary>
    /// Wrapper for logging a single Endpoint address.
    /// </summary>
    private readonly struct LoggableEndpoint(Endpoint endpoint)
    {
        private readonly string _display = endpoint.GetNetworkAddressString();

        public override readonly string ToString() => _display;
    }

    [LoggerMessage(Level = LogLevel.Error, Message = "RPC failed to {Remote}")]
    private partial void LogRpcFailed(Exception ex, LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "RPC unavailable to {Remote} (node may be shutting down)")]
    private partial void LogRpcUnavailable(LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GrpcClient stopping, waiting for {Count} pending tasks")]
    private partial void LogStopping(int Count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: RPC error {StatusCode} - {Error}")]
    private partial void LogOneWayDeliveryFailedRpc(LoggableEndpoint Remote, StatusCode StatusCode, string Error);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: Timeout after {Timeout}")]
    private partial void LogOneWayDeliveryFailedTimeout(LoggableEndpoint Remote, TimeSpan Timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}")]
    private partial void LogOneWayDeliveryFailedUnexpected(Exception exception, LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Trace, Message = "One-way message delivered successfully to {Remote}")]
    private partial void LogOneWayDeliverySucceeded(LoggableEndpoint Remote);

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Cancel all pending operations immediately
        await _stoppingCts.CancelAsync();

        // Wait for all pending tasks to complete (they should finish quickly now)
        var pendingTasks = _pendingTasks.Values.ToArray();
        if (pendingTasks.Length > 0)
        {
            LogStopping(pendingTasks.Length);
            try
            {
                await Task.WhenAll(pendingTasks).WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                // Timeout waiting for pending tasks
            }
        }
    }

    public async Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request,
        CancellationToken cancellationToken)
    {
        var client = GetOrCreateClient(remote);
        var stopwatch = Stopwatch.StartNew();
        _metrics.RecordGrpcCallStarted(SendRequestMethod);

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stoppingCts.Token);
            cts.CancelAfter(_options.GrpcTimeout);

            var response = await client.SendRequestAsync(request, cancellationToken: cts.Token);
            stopwatch.Stop();
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "OK");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "OK", stopwatch);
            return response;
        }
        catch (RpcException ex)
        {
            stopwatch.Stop();

            // Log at Debug level for expected shutdown-related errors, Error level for others
            if (ex.StatusCode is StatusCode.Unavailable or StatusCode.Cancelled)
            {
                LogRpcUnavailable(new LoggableEndpoint(remote));
            }
            else
            {
                LogRpcFailed(ex, new LoggableEndpoint(remote));
            }

            _metrics.RecordGrpcCallCompleted(SendRequestMethod, ex.StatusCode.ToString());
            _metrics.RecordGrpcCallDuration(SendRequestMethod, ex.StatusCode.ToString(), stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Network);
            throw;
        }
    }

    public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        var taskId = Interlocked.Increment(ref _taskIdCounter);
        var task = SendOneWayMessageInternalAsync(remote, request, rank, taskId, onDeliveryFailure, cancellationToken);
        _pendingTasks.TryAdd(taskId, task);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "One-way messages ignore failures but may invoke callback")]
    private async Task SendOneWayMessageInternalAsync(Endpoint remote, RapidClusterRequest request, Rank? rank, int taskId, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stoppingCts.Token);
        cts.CancelAfter(_options.GrpcTimeout);

        var client = GetOrCreateClient(remote);
        var stopwatch = Stopwatch.StartNew();
        _metrics.RecordGrpcCallStarted(SendRequestMethod);

        try
        {
            await client.SendRequestAsync(request, cancellationToken: cts.Token);
            stopwatch.Stop();
            LogOneWayDeliverySucceeded(new LoggableEndpoint(remote));
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "OK");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "OK", stopwatch);
        }
        catch (RpcException ex)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedRpc(new LoggableEndpoint(remote), ex.StatusCode, ex.Message);
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, ex.StatusCode.ToString());
            _metrics.RecordGrpcCallDuration(SendRequestMethod, ex.StatusCode.ToString(), stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Network);
            onDeliveryFailure?.Invoke(remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedTimeout(new LoggableEndpoint(remote), _options.GrpcTimeout);
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "DeadlineExceeded");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "DeadlineExceeded", stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Timeout);
            onDeliveryFailure?.Invoke(remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
        catch (OperationCanceledException)
        {
            stopwatch.Stop();

            // User cancellation - don't invoke callback but still record metrics
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "Cancelled");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "Cancelled", stopwatch);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedUnexpected(ex, new LoggableEndpoint(remote));
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "Unknown");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "Unknown", stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Unknown);
            onDeliveryFailure?.Invoke(remote, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided."));
        }
        finally
        {
            _pendingTasks.TryRemove(taskId, out _);
        }
    }

    private Pb.MembershipService.MembershipServiceClient GetOrCreateClient(Endpoint remote)
    {
        var key = string.Create(CultureInfo.InvariantCulture, $"{remote.Hostname.ToStringUtf8()}:{remote.Port}");
        return _clients.GetOrAdd(key, static (key, options) =>
        {
            var scheme = options.UseHttps ? "https" : "http";
            var handler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
            };
            var channel = GrpcChannel.ForAddress($"{scheme}://{key}", new GrpcChannelOptions
            {
                HttpHandler = handler,
                HttpVersion = new Version(2, 0),
                HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact, // Force HTTP/2 prior knowledge (for HTTP) or ALPN (for HTTPS)
                ThrowOperationCanceledOnCancellation = true,
            });
            return new Pb.MembershipService.MembershipServiceClient(channel);
        }, _options);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Cancel any remaining pending operations (may already be cancelled by StopAsync)
        await _stoppingCts.CancelAsync();

        // Wait for all pending tasks to complete (with a timeout)
        var pendingTasks = _pendingTasks.Values.ToArray();
        if (pendingTasks.Length > 0)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await Task.WhenAll(pendingTasks).WaitAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                // Timeout waiting for pending tasks
            }
        }

        _stoppingCts.Dispose();
        _clients.Clear();
    }
}
