using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using Grpc.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Messaging;

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
    private readonly ILogger<GrpcClient> _logger = logger;
    private readonly ConcurrentDictionary<string, Pb.MembershipService.MembershipServiceClient> _clients = new();
    private readonly ConcurrentDictionary<int, Task> _pendingTasks = new();
    private int _taskIdCounter;
    private bool _disposed;

    private const string SendRequestMethod = "SendRequest";

    [LoggerMessage(Level = LogLevel.Error, Message = "RPC failed to {Remote}")]
    private partial void LogRpcFailed(Exception ex, LoggableEndpoint Remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "GrpcClient stopping, waiting for {Count} pending tasks")]
    private partial void LogStopping(int Count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: RPC error {StatusCode} - {Error}")]
    private partial void LogOneWayDeliveryFailedRpc(LoggableEndpoint Remote, StatusCode StatusCode, string Error);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: Timeout after {Timeout}")]
    private partial void LogOneWayDeliveryFailedTimeout(LoggableEndpoint Remote, TimeSpan Timeout);

    [LoggerMessage(Level = LogLevel.Debug, Message = "One-way message delivery failed to {Remote}: Unexpected error - {Error}")]
    private partial void LogOneWayDeliveryFailedUnexpected(LoggableEndpoint Remote, string Error);

    [LoggerMessage(Level = LogLevel.Trace, Message = "One-way message delivered successfully to {Remote}")]
    private partial void LogOneWayDeliverySucceeded(LoggableEndpoint Remote);

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Wait for all pending tasks to complete (with a timeout)
        var pendingTasks = _pendingTasks.Values.ToArray();
        if (pendingTasks.Length > 0)
        {
            LogStopping(pendingTasks.Length);
            try
            {
                await Task.WhenAll(pendingTasks).WaitAsync(cancellationToken).ConfigureAwait(true);
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
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
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
            LogRpcFailed(ex, new LoggableEndpoint(remote));
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, ex.StatusCode.ToString());
            _metrics.RecordGrpcCallDuration(SendRequestMethod, ex.StatusCode.ToString(), stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Network);
            throw;
        }
    }

    public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        var taskId = Interlocked.Increment(ref _taskIdCounter);
        var task = SendOneWayMessageInternalAsync(remote, request, taskId, onDeliveryFailure, cancellationToken);
        _pendingTasks.TryAdd(taskId, task);
    }

    [SuppressMessage("Design", "CA1031:Do not catch general exception types", Justification = "One-way messages ignore failures but may invoke callback")]
    private async Task SendOneWayMessageInternalAsync(Endpoint remote, RapidClusterRequest request, int taskId, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
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
            onDeliveryFailure?.Invoke(remote);
        }
        catch (OperationCanceledException) when (cts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            stopwatch.Stop();
            LogOneWayDeliveryFailedTimeout(new LoggableEndpoint(remote), _options.GrpcTimeout);
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "DeadlineExceeded");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "DeadlineExceeded", stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Timeout);
            onDeliveryFailure?.Invoke(remote);
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
            LogOneWayDeliveryFailedUnexpected(new LoggableEndpoint(remote), ex.Message);
            _metrics.RecordGrpcCallCompleted(SendRequestMethod, "Unknown");
            _metrics.RecordGrpcCallDuration(SendRequestMethod, "Unknown", stopwatch);
            _metrics.RecordGrpcConnectionError(MetricNames.ErrorTypes.Unknown);
            onDeliveryFailure?.Invoke(remote);
        }
        finally
        {
            _pendingTasks.TryRemove(taskId, out _);
        }
    }

    private Pb.MembershipService.MembershipServiceClient GetOrCreateClient(Endpoint remote)
    {
        var key = $"{remote.Hostname.ToStringUtf8()}:{remote.Port}";
        return _clients.GetOrAdd(key, _ =>
        {
            var channel = Grpc.Net.Client.GrpcChannel.ForAddress($"http://{key}");
            return new Pb.MembershipService.MembershipServiceClient(channel);
        });
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Wait for all pending tasks to complete (with a timeout)
        var pendingTasks = _pendingTasks.Values.ToArray();
        if (pendingTasks.Length > 0)
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try
            {
                await Task.WhenAll(pendingTasks).WaitAsync(cts.Token).ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
                // Timeout waiting for pending tasks
            }
        }

        _clients.Clear();
    }
}
