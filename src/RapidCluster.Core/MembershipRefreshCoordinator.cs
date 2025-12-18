using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Request to refresh the membership view from a remote endpoint.
/// </summary>
internal readonly record struct MembershipRefreshRequest(Endpoint RemoteEndpoint, long RemoteConfigVersion);

/// <summary>
/// Coordinates membership view refreshes triggered by stale view detection.
/// This class implements the Paxos "learner" role - when a node detects it has
/// missed consensus decisions (via probe responses with higher config IDs),
/// it requests the current membership view from another node.
/// </summary>
/// <remarks>
/// <para>
/// The coordinator uses a channel to queue refresh requests and processes them
/// serially, ensuring only the latest view is fetched. Requests for config versions
/// that are no longer newer than the local view are skipped.
/// </para>
/// <para>
/// Rate limiting prevents excessive refresh requests during rapid view changes
/// or network instability.
/// </para>
/// </remarks>
internal sealed class MembershipRefreshCoordinator : IAsyncDisposable
{
    private readonly MembershipRefreshCoordinatorLogger _log;
    private readonly IMembershipViewAccessor _viewAccessor;
    private readonly IMessagingClient _messagingClient;
    private readonly SharedResources _sharedResources;
    private readonly Endpoint _myAddr;
    private readonly Channel<MembershipRefreshRequest> _refreshChannel;
    private readonly CancellationTokenSource _stoppingCts;
    private long _localConfigVersion;
    private Task? _refreshTask;
    private int _disposed;

    // Handler to notify MembershipService of learned views
    private readonly ILearnedViewHandler _learnedViewHandler;

    /// <summary>
    /// Creates a new MembershipRefreshCoordinator.
    /// </summary>
    /// <param name="options">Cluster options containing listen address.</param>
    /// <param name="viewAccessor">Accessor for the current membership view.</param>
    /// <param name="messagingClient">Client for sending messages to remote nodes.</param>
    /// <param name="sharedResources">Shared resources including TimeProvider and TaskScheduler.</param>
    /// <param name="learnedViewHandler">Handler to invoke when a new view is learned from a remote node.</param>
    /// <param name="logger">Logger for diagnostic output.</param>
    public MembershipRefreshCoordinator(
        IOptions<RapidClusterOptions> options,
        IMembershipViewAccessor viewAccessor,
        IMessagingClient messagingClient,
        SharedResources sharedResources,
        ILearnedViewHandler learnedViewHandler,
        ILogger<MembershipRefreshCoordinator> logger)
        : this(
            options.Value.ListenAddress.ToProtobuf(),
            viewAccessor,
            messagingClient,
            sharedResources,
            learnedViewHandler,
            logger)
    {
    }

    /// <summary>
    /// Creates a new MembershipRefreshCoordinator with raw parameter values.
    /// Used internally by MembershipService which already has unwrapped options.
    /// </summary>
    internal MembershipRefreshCoordinator(
        Endpoint myAddr,
        IMembershipViewAccessor viewAccessor,
        IMessagingClient messagingClient,
        SharedResources sharedResources,
        ILearnedViewHandler learnedViewHandler,
        ILogger<MembershipRefreshCoordinator> logger)
    {
        _myAddr = myAddr;
        _viewAccessor = viewAccessor;
        _messagingClient = messagingClient;
        _sharedResources = sharedResources;
        _learnedViewHandler = learnedViewHandler;
        _log = new MembershipRefreshCoordinatorLogger(logger);

        // Unbounded channel - we'll deduplicate/skip stale requests in the processor
        _refreshChannel = Channel.CreateUnbounded<MembershipRefreshRequest>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false,
        });

        // Link to shared resources shutdown token
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(sharedResources.ShuttingDownToken);
    }

    /// <summary>
    /// Starts the background refresh task.
    /// </summary>
    public void Start()
    {
        ObjectDisposedException.ThrowIf(_disposed != 0, this);
        _log.Starting();
        _refreshTask = Task.Factory.StartNew(
            RefreshLoopAsync,
            _stoppingCts.Token,
            TaskCreationOptions.None,
            _sharedResources.TaskScheduler).Unwrap();
    }

    /// <summary>
    /// Queues a refresh request when a stale view is detected.
    /// Called by the failure detector when a probe response indicates
    /// the local node has a stale view.
    /// </summary>
    /// <param name="remoteEndpoint">The endpoint that reported the higher config ID.</param>
    /// <param name="remoteConfigVersion">The configuration version from the probe response.</param>
    public void OnStaleViewDetected(Endpoint remoteEndpoint, long remoteConfigVersion)
    {
        if (_disposed != 0)
        {
            return;
        }

        if (remoteConfigVersion <= _localConfigVersion)
        {
            return;
        }

        _localConfigVersion = _viewAccessor.CurrentView.ConfigurationId.Version;

        if (remoteConfigVersion <= _localConfigVersion)
        {
            return;
        }

        // Write to channel - will be processed by the refresh loop
        // TryWrite is non-blocking and always succeeds for unbounded channels
        _refreshChannel.Writer.TryWrite(new MembershipRefreshRequest(remoteEndpoint, remoteConfigVersion));
    }

    /// <summary>
    /// Main refresh loop that processes queued refresh requests.
    /// </summary>
    private async Task RefreshLoopAsync()
    {
        var stoppingToken = _stoppingCts.Token;
        var reader = _refreshChannel.Reader;

        try
        {
            // Use a simple while loop instead of ReadAllAsync to avoid complex async
            // iteration machinery that can post continuations to the sync context
            // when cancelled.
            while (!stoppingToken.IsCancellationRequested)
            {
                // Wait for data or completion
                if (!await reader.WaitToReadAsync(stoppingToken).ConfigureAwait(true))
                {
                    // Channel completed
                    break;
                }

                // Drain all available items and find the max version
                var localVersion = _viewAccessor.CurrentView.ConfigurationId.Version;
                var maxSeen = localVersion;
                MembershipRefreshRequest? maxRequest = default;
                while (reader.TryRead(out var candidate))
                {
                    (_, var candidateVersion) = candidate;
                    if (candidateVersion > maxSeen)
                    {
                        maxRequest = candidate;
                        maxSeen = candidateVersion;
                    }
                }

                if (maxRequest is { } refreshRequest)
                {
                    _log.StaleViewDetected(refreshRequest.RemoteEndpoint, refreshRequest.RemoteConfigVersion, localVersion);
                    await ProcessRefreshRequestAsync(maxRequest.Value, stoppingToken).ConfigureAwait(true);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }

        _log.RefreshLoopExiting();
    }

    /// <summary>
    /// Processes a single refresh request.
    /// </summary>
    private async Task ProcessRefreshRequestAsync(MembershipRefreshRequest request, CancellationToken cancellationToken)
    {
        _log.ProcessingRefreshRequest(request.RemoteConfigVersion, request.RemoteEndpoint);

        try
        {
            await RefreshMembershipViewAsync(request.RemoteEndpoint, request.RemoteConfigVersion, cancellationToken).ConfigureAwait(true);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.MembershipViewRefreshFailed(request.RemoteEndpoint, ex.Message);
        }
    }

    /// <summary>
    /// Requests an updated membership view from a remote node.
    /// </summary>
    private async Task RefreshMembershipViewAsync(Endpoint remoteEndpoint, long expectedConfigVersion, CancellationToken cancellationToken)
    {
        _log.RequestingMembershipView(remoteEndpoint);

        var request = new MembershipViewRequest
        {
            Sender = _myAddr,
            CurrentConfigurationId = _viewAccessor.CurrentView.ConfigurationId.ToProtobuf()
        };

        var response = await _messagingClient.SendMessageAsync(
            remoteEndpoint,
            request.ToRapidClusterRequest(),
            cancellationToken).ConfigureAwait(true);

        var viewResponse = response.MembershipViewResponse;
        if (viewResponse == null)
        {
            _log.MembershipViewRefreshFailed(remoteEndpoint, "No MembershipViewResponse in reply");
            return;
        }

        // Only apply if the response is newer than our current view
        var responseConfigId = ConfigurationId.FromProtobuf(viewResponse.ConfigurationId);
        var localConfigId = _viewAccessor.CurrentView.ConfigurationId;

        // Use IsOlderThanOrDifferentCluster to handle the bootstrap case where local ClusterId is 0
        if (!localConfigId.IsOlderThanOrDifferentCluster(responseConfigId))
        {
            _log.SkippingRefresh(responseConfigId.Version, localConfigId.Version);
            return;
        }

        // Notify MembershipService to apply the learned view
        await _learnedViewHandler.ApplyLearnedMembershipViewAsync(viewResponse).ConfigureAwait(true);

        _log.MembershipViewRefreshed(remoteEndpoint, viewResponse.ConfigurationId.Version, viewResponse.Endpoints.Count);
    }

    /// <summary>
    /// Stops the coordinator gracefully.
    /// </summary>
    public async Task StopAsync()
    {
        _log.Stopping();

        // Signal the channel to complete
        _refreshChannel.Writer.TryComplete();

        // Cancel the stopping token
        _stoppingCts.SafeCancel(_log.Logger);

        // Wait for the refresh task to complete
        if (_refreshTask != null)
        {
            await _refreshTask.WaitAsync(CancellationToken.None).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
        }
    }

    /// <summary>
    /// Disposes the coordinator.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _log.Disposed();

        // Complete the channel
        _refreshChannel.Writer.TryComplete();

        // Cancel background task
        _stoppingCts.SafeCancel(_log.Logger);

        // Wait for background task
        if (_refreshTask != null)
        {
            await _refreshTask.WaitAsync(CancellationToken.None).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
        }

        _stoppingCts.Dispose();
    }
}
