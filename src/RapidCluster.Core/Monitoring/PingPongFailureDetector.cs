using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster.Monitoring;

/// <summary>
/// Simple ping-pong failure detector factory.
/// </summary>
public sealed partial class PingPongFailureDetectorFactory(
    Endpoint localEndpoint,
    IMessagingClient client,
    SharedResources sharedResources,
    IOptions<RapidClusterProtocolOptions> protocolOptions,
    ILogger<PingPongFailureDetector> logger) : IEdgeFailureDetectorFactory
{
    private readonly Endpoint _localEndpoint = localEndpoint;
    private readonly IMessagingClient _client = client;
    private readonly SharedResources _sharedResources = sharedResources;
    private readonly RapidClusterProtocolOptions _protocolOptions = protocolOptions.Value;
    private readonly ILogger<PingPongFailureDetector> _logger = logger;

    /// <summary>
    /// Gets or sets the callback invoked when a probe response indicates
    /// the local node has a stale view (remote has higher config ID).
    /// This is the Paxos "learner" role - requesting missed consensus decisions.
    /// The callback receives the learned membership view and can determine if the
    /// local node was kicked (not in view) or just missed consensus rounds (still in view).
    /// Parameters: (remoteEndpoint, remoteConfigId, localConfigId)
    /// </summary>
    public Action<Endpoint, long, long>? OnStaleViewDetected { get; set; }

    /// <summary>
    /// Gets or sets a function that returns the current local configuration ID.
    /// Used to detect stale views.
    /// </summary>
    public Func<long>? GetLocalConfigurationId { get; set; }

    public IEdgeFailureDetector CreateInstance(Endpoint subject, Action notifier) =>
        new PingPongFailureDetector(
            subject,
            _localEndpoint,
            _client,
            _sharedResources,
            notifier,
            _protocolOptions.FailureDetectorConsecutiveFailures,
            _protocolOptions.FailureDetectorInterval,
            OnStaleViewDetected,
            GetLocalConfigurationId,
            _logger);
}

/// <summary>
/// Simple ping-pong failure detector that probes a subject endpoint.
/// Requires multiple consecutive probe failures before declaring a node down.
/// </summary>
public sealed partial class PingPongFailureDetector : IEdgeFailureDetector
{
    private readonly Endpoint _subject;
    private readonly Endpoint _observer;
#pragma warning disable CA2213 // SharedResources is owned by DI container, not disposed by this class
    private readonly IMessagingClient _client;
    private readonly SharedResources _sharedResources;
#pragma warning restore CA2213
    private readonly Action _notifier;
    private readonly int _consecutiveFailuresThreshold;
    private readonly TimeSpan _probeInterval;
    private readonly Action<Endpoint, long, long>? _onStaleViewDetected;
    private readonly Func<long>? _getLocalConfigurationId;
    private readonly ILogger<PingPongFailureDetector> _logger;
    private readonly CancellationTokenSource _cts = new();
    private int _disposed;
    private Task? _probeTask;
    private int _consecutiveFailures;

    /// <summary>
    /// Creates a new ping-pong failure detector.
    /// </summary>
    /// <param name="subject">The endpoint to monitor.</param>
    /// <param name="observer">The local endpoint (observer).</param>
    /// <param name="client">The messaging client for sending probes.</param>
    /// <param name="sharedResources">Shared resources including TimeProvider.</param>
    /// <param name="notifier">Action to invoke when the subject is detected as failed.</param>
    /// <param name="consecutiveFailuresThreshold">Number of consecutive failures required before declaring node down.</param>
    /// <param name="probeInterval">Interval between failure detector probes.</param>
    /// <param name="onStaleViewDetected">Optional callback when stale view is detected (learner role).</param>
    /// <param name="getLocalConfigurationId">Optional function to get local configuration ID.</param>
    /// <param name="logger">Optional logger.</param>
    public PingPongFailureDetector(
        Endpoint subject,
        Endpoint observer,
        IMessagingClient client,
        SharedResources sharedResources,
        Action notifier,
        int consecutiveFailuresThreshold = 3,
        TimeSpan probeInterval = default,
        Action<Endpoint, long, long>? onStaleViewDetected = null,
        Func<long>? getLocalConfigurationId = null,
        ILogger<PingPongFailureDetector>? logger = null)
    {
        _subject = subject;
        _observer = observer;
        _client = client;
        _sharedResources = sharedResources;
        _notifier = notifier;
        _consecutiveFailuresThreshold = consecutiveFailuresThreshold;
        _probeInterval = probeInterval == default ? TimeSpan.FromSeconds(1) : probeInterval;
        _onStaleViewDetected = onStaleViewDetected;
        _getLocalConfigurationId = getLocalConfigurationId;
        _logger = logger ?? NullLogger<PingPongFailureDetector>.Instance;
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Probe failed for {Subject} (consecutive failures: {ConsecutiveFailures}/{Threshold})")]
    private partial void LogProbeFailed(LoggableEndpoint Subject, int ConsecutiveFailures, int Threshold);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Probe exception for {Subject} (consecutive failures: {ConsecutiveFailures}/{Threshold})")]
    private partial void LogProbeException(Exception ex, LoggableEndpoint Subject, int ConsecutiveFailures, int Threshold);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {Subject} declared down after {ConsecutiveFailures} consecutive probe failures")]
    private partial void LogNodeDeclaredDown(LoggableEndpoint Subject, int ConsecutiveFailures);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Probe succeeded for {Subject}, resetting consecutive failure count")]
    private partial void LogProbeSucceeded(LoggableEndpoint Subject);

    [LoggerMessage(Level = LogLevel.Information, Message = "Stale view detected from {Subject}: remote config {RemoteConfigId} > local config {LocalConfigId}")]
    private partial void LogStaleViewDetected(LoggableEndpoint Subject, long RemoteConfigId, long LocalConfigId);

    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed == 1, this);
        _probeTask = ProbeAsync();
    }

    private async Task ProbeAsync()
    {
        while (_disposed == 0)
        {
            await Task.Delay(_probeInterval, _sharedResources.TimeProvider, _cts.Token).ConfigureAwait(true);
            await ProbeOnceAsync().ConfigureAwait(true);
        }
    }

    private async Task ProbeOnceAsync()
    {
        // Check disposed state without throwing - just return if disposed
        if (_disposed == 1)
        {
            return;
        }
#pragma warning disable CA1031
        try
        {
            var localConfigId = _getLocalConfigurationId?.Invoke() ?? 0;
            var request = new ProbeMessage { Sender = _observer, ConfigurationId = localConfigId }.ToRapidClusterRequest();
            var response = await _client.SendMessageAsync(_subject, request, _cts.Token).ConfigureAwait(true);

            if (response.ProbeResponse == null)
            {
                _consecutiveFailures++;
                LogProbeFailed(new LoggableEndpoint(_subject), _consecutiveFailures, _consecutiveFailuresThreshold);
                CheckAndNotifyFailure();
            }
            else
            {
                if (_consecutiveFailures > 0)
                {
                    LogProbeSucceeded(new LoggableEndpoint(_subject));
                }
                _consecutiveFailures = 0;

                // Check if we've been kicked or have a stale view
                CheckForStaleView(response.ProbeResponse);
            }
        }
        catch (Exception ex)
        {
            _consecutiveFailures++;
            LogProbeException(ex, new LoggableEndpoint(_subject), _consecutiveFailures, _consecutiveFailuresThreshold);
            CheckAndNotifyFailure();
        }
#pragma warning restore CA1031
    }

    private void CheckForStaleView(ProbeResponse probeResponse)
    {
        // Check if we have a stale view (remote has higher config ID)
        // The callback will determine if we were kicked (not in new view) or just missed consensus
        if (_onStaleViewDetected != null && _getLocalConfigurationId != null)
        {
            var localConfigId = _getLocalConfigurationId();
            var remoteConfigId = probeResponse.ConfigurationId;

            if (remoteConfigId > localConfigId)
            {
                LogStaleViewDetected(new LoggableEndpoint(_subject), remoteConfigId, localConfigId);
                _onStaleViewDetected(_subject, remoteConfigId, localConfigId);
            }
        }
    }

    private void CheckAndNotifyFailure()
    {
        if (_consecutiveFailures >= _consecutiveFailuresThreshold)
        {
            LogNodeDeclaredDown(new LoggableEndpoint(_subject), _consecutiveFailures);
            _notifier();
            Dispose();
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return; // Already disposed
        }

        // Cancel the token before disposing to stop the probe loop gracefully
        _cts.SafeCancel(_logger);

        _probeTask?.Ignore();
        _cts.Dispose();
    }
}
