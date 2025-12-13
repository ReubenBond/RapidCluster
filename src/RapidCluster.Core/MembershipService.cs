using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

using Microsoft.Extensions.Options;
using RapidCluster.Exceptions;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Membership server class that implements the Rapid protocol.
///
/// Note: This class is not thread-safe yet. RpcServer.start() uses a single threaded messagingExecutor during the server
/// initialization to make sure that only a single thread runs the process* methods.
/// </summary>
internal sealed class MembershipService : IMembershipServiceHandler, IAsyncDisposable
{
    private readonly MembershipServiceLogger _log;
    private ICutDetector _cutDetection = null!;
    private readonly ICutDetectorFactory _cutDetectorFactory;
    private readonly Endpoint _myAddr;
    private readonly IBroadcaster _broadcaster;
    private readonly Dictionary<Endpoint, Channel<TaskCompletionSource<RapidClusterResponse>>> _joinersToRespondTo = new(EndpointAddressComparer.Instance);
    private readonly Dictionary<Endpoint, NodeId> _joinerUuid = new(EndpointAddressComparer.Instance);
    private readonly Dictionary<Endpoint, Metadata> _joinerMetadata = new(EndpointAddressComparer.Instance);
    private readonly Dictionary<Endpoint, NodeId> _memberNodeIds = new(EndpointAddressComparer.Instance);  // Maps current members to their NodeIds
    private readonly IMessagingClient _messagingClient;
    private readonly MetadataManager _metadataManager;
    private readonly IConsensusCoordinatorFactory _consensusCoordinatorFactory;
    private MembershipView _membershipView = null!;

    // Event subscriptions (IAsyncEnumerable and IObservable-based using BroadcastChannel)
    private readonly BroadcastChannel<ClusterEventNotification> _eventChannel;

    // Fields used by batching logic.
    private readonly Channel<AlertMessage> _sendQueue;
    private readonly SharedResources _sharedResources;
    private readonly List<IDisposable> _failureDetectors = [];
    private int _disposed;

    // Failure detector
    private readonly IEdgeFailureDetectorFactory _fdFactory;

    // Fields used by consensus protocol
    private readonly Lock _membershipUpdateLock = new();
    private readonly RapidClusterProtocolOptions _options;
    private bool _announcedProposal;
    private ConsensusCoordinator _consensusInstance = null!;

    // Initialization state
    private bool _initialized;

    // Configuration for join - stored from RapidClusterOptions
    private readonly Endpoint? _seedAddress;
    private readonly Metadata _nodeMetadata;

    // Buffer for consensus messages from future configurations
    // Key: configurationId, Value: list of messages waiting for that config
    private readonly Dictionary<long, List<RapidClusterRequest>> _pendingConsensusMessages = [];

    // View change accessor for publishing updates
    private readonly MembershipViewAccessor _viewAccessor;

    // Flag to track if a rejoin is in progress
    private bool _isRejoining;

    // Flag to track if a stale view refresh is in progress (to prevent concurrent refreshes)
    private bool _isRefreshingView;

    // Last time a stale view refresh was completed (for rate limiting)
    private long _lastStaleViewRefreshTicks;

    // Background task tracking for graceful shutdown
    private readonly List<Task> _backgroundTasks = [];
    private readonly Lock _backgroundTasksLock = new();

    // Internal cancellation for background tasks - linked to SharedResources.ShuttingDownToken
    // Cancelled by StopAsync or when SharedResources signals shutdown
    private readonly CancellationTokenSource _stoppingCts;

    /// <summary>
    /// Result of a single join attempt. Used to avoid exception-based control flow for retryable conditions.
    /// </summary>
    private enum JoinAttemptStatus
    {
        /// <summary>Join succeeded - response contains valid membership data.</summary>
        Success,
        /// <summary>Join failed but should be retried (transient error like config change, network issue).</summary>
        RetryNeeded,
        /// <summary>Join failed permanently - should not retry.</summary>
        Failed
    }

    /// <summary>
    /// Result of a single join attempt.
    /// </summary>
    private readonly struct JoinAttemptResult(JoinAttemptStatus status, JoinResponse? response, string? failureReason)
    {
        public JoinAttemptStatus Status { get; } = status;
        public JoinResponse? Response { get; } = response;
        public string? FailureReason { get; } = failureReason;

        public static JoinAttemptResult Success(JoinResponse response) => new(JoinAttemptStatus.Success, response, null);
        public static JoinAttemptResult RetryNeeded(string reason) => new(JoinAttemptStatus.RetryNeeded, null, reason);
        public static JoinAttemptResult Failed(string reason) => new(JoinAttemptStatus.Failed, null, reason);
    }

    /// <summary>
    /// Creates a new MembershipService instance.
    /// Call <see cref="InitializeAsync"/> after construction to start or join a cluster.
    /// </summary>
    public MembershipService(
        IOptions<RapidClusterOptions> RapidClusterOptions,
        IOptions<RapidClusterProtocolOptions> protocolOptions,
        IMessagingClient messagingClient,
        IBroadcasterFactory broadcasterFactory,
        IEdgeFailureDetectorFactory edgeFailureDetector,
        IConsensusCoordinatorFactory consensusCoordinatorFactory,
        ICutDetectorFactory cutDetectorFactory,
        MembershipViewAccessor viewAccessor,
        SharedResources sharedResources,
        ILogger<MembershipService> logger)
    {
        var opts = RapidClusterOptions.Value;
        _myAddr = opts.ListenAddress;
        _seedAddress = opts.SeedAddress;
        _nodeMetadata = opts.Metadata;
        _options = protocolOptions.Value;
        _membershipView = MembershipView.Empty;
        _cutDetectorFactory = cutDetectorFactory;
        _sharedResources = sharedResources;
        _metadataManager = new MetadataManager();
        _messagingClient = messagingClient;
        _broadcaster = broadcasterFactory.Create();
        _fdFactory = edgeFailureDetector;
        _consensusCoordinatorFactory = consensusCoordinatorFactory;
        _viewAccessor = viewAccessor;
        _log = new MembershipServiceLogger(logger);
        _sendQueue = Channel.CreateUnbounded<AlertMessage>();
        _eventChannel = new BroadcastChannel<ClusterEventNotification>();

        // Create linked CTS so background tasks stop on either StopAsync or SharedResources shutdown
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(sharedResources.ShuttingDownToken);

        // Configure the failure detector factory to detect stale views (learner role - missed consensus decisions)
        if (edgeFailureDetector is PingPongFailureDetectorFactory pingPongFactory)
        {
            pingPongFactory.OnStaleViewDetected = OnStaleViewDetected;
            pingPongFactory.GetLocalConfigurationId = () => _membershipView.ConfigurationId;
        }
    }

    /// <summary>
    /// Initializes the membership service by either starting a new cluster or joining an existing one.
    /// This must be called after construction before the service can handle messages.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_initialized)
        {
            throw new InvalidOperationException("MembershipService is already initialized");
        }

        if (_seedAddress == null || _myAddr.Equals(_seedAddress))
        {
            // Start a new cluster
            StartNewCluster();
        }
        else
        {
            // Join an existing cluster
            await JoinClusterAsync(cancellationToken).ConfigureAwait(true);
        }

        // Start background jobs after initialization
        var alertBatcherTask = Task.Factory.StartNew(AlertBatcherAsync, _stoppingCts.Token, TaskCreationOptions.None, _sharedResources.TaskScheduler).Unwrap();
        TrackBackgroundTask(alertBatcherTask);

        _initialized = true;
    }

    /// <summary>
    /// Starts a new cluster with this node as the only member.
    /// </summary>
    private void StartNewCluster()
    {
        _log.StartingNewCluster(new MembershipServiceLogger.LoggableEndpoint(_myAddr));

        var nodeId = RapidClusterUtils.NodeIdFromUuid(_sharedResources.NewGuid());

        // Create endpoint with monotonic ID = 1 for the seed node
        var seedEndpoint = new Endpoint
        {
            Hostname = _myAddr.Hostname,
            Port = _myAddr.Port,
            MonotonicNodeId = 1
        };

        _membershipView = new MembershipViewBuilder(_options.ObserversPerSubject, [nodeId], [seedEndpoint], maxMonotonicId: 1).Build();
        _cutDetection = _cutDetectorFactory.Create(_membershipView);

        // Initialize member NodeId tracking
        _memberNodeIds[seedEndpoint] = nodeId;

        _metadataManager.Add(seedEndpoint, _nodeMetadata);

        FinalizeInitialization();
    }

    /// <summary>
    /// Joins an existing cluster through the configured seed node.
    /// </summary>
    private async Task JoinClusterAsync(CancellationToken cancellationToken)
    {
        _log.JoiningCluster(new MembershipServiceLogger.LoggableEndpoint(_seedAddress!), new MembershipServiceLogger.LoggableEndpoint(_myAddr));

        var maxRetries = _options.MaxJoinRetries;
        var retryDelay = _options.JoinRetryBaseDelay;
        JoinResponse? successfulResponse = null;
        string? lastFailureReason = null;

        for (var attempt = 0; attempt <= maxRetries; attempt++)
        {
            // Check if we've already joined via the learner protocol (stale view detection).
            // This can happen when failure detection probes reveal we're behind and we learn
            // the current membership view from another node. If our address is already in the
            // membership, we're effectively joined and can skip the join protocol.
            if (_membershipView.IsHostPresent(_myAddr))
            {
                _log.JoinCompletedViaLearnerProtocol(new MembershipServiceLogger.LoggableEndpoint(_myAddr), new MembershipServiceLogger.CurrentConfigId(_membershipView));
                return;
            }

            var result = await TryJoinClusterAsync(cancellationToken).ConfigureAwait(true);

            switch (result.Status)
            {
                case JoinAttemptStatus.Success:
                    successfulResponse = result.Response;
                    break;

                case JoinAttemptStatus.RetryNeeded when attempt < maxRetries:
                    _log.JoinRetry(attempt + 1, result.FailureReason!, retryDelay.TotalMilliseconds);
                    await Task.Delay(retryDelay, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
                    retryDelay = TimeSpan.FromTicks((long)(retryDelay.Ticks * _options.JoinRetryBackoffMultiplier));

                    // Cap at maximum delay
                    if (retryDelay > _options.JoinRetryMaxDelay)
                    {
                        retryDelay = _options.JoinRetryMaxDelay;
                    }
                    continue;

                case JoinAttemptStatus.RetryNeeded:
                    // Last attempt failed with retryable error - treat as failure
                    lastFailureReason = result.FailureReason;
                    break;

                case JoinAttemptStatus.Failed:
                    // Permanent failure - don't retry
                    throw new JoinException(result.FailureReason ?? "Join failed");
            }

            // Either succeeded or exhausted retries
            break;
        }

        if (successfulResponse == null)
        {
            _log.JoinFailed(maxRetries + 1);
            throw new JoinException(lastFailureReason ?? $"Failed to join cluster after {maxRetries + 1} attempts");
        }

        // Initialize membership from response
        var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
        for (var i = 0; i < successfulResponse.MetadataKeys.Count && i < successfulResponse.MetadataValues.Count; i++)
        {
            var endpoint = successfulResponse.MetadataKeys[i];
            var metadata = successfulResponse.MetadataValues[i];
            metadataMap[endpoint] = metadata;
        }

        // Pass maxMonotonicId to ensure it's preserved even if the node with that ID was removed
        _membershipView = new MembershipViewBuilder(
            _options.ObserversPerSubject,
            [.. successfulResponse.Identifiers],
            [.. successfulResponse.Endpoints],
            successfulResponse.MaxMonotonicId).BuildWithConfigurationId(new ConfigurationId(successfulResponse.ConfigurationId));
        _cutDetection = _cutDetectorFactory.Create(_membershipView);
        _metadataManager.AddMetadata(metadataMap);

        // Initialize member NodeId tracking for all initial members
        _memberNodeIds.Clear();
        for (var i = 0; i < successfulResponse.Endpoints.Count && i < successfulResponse.Identifiers.Count; i++)
        {
            _memberNodeIds[successfulResponse.Endpoints[i]] = successfulResponse.Identifiers[i];
        }

        FinalizeInitialization();
    }

    /// <summary>
    /// Attempts a single join operation. Generates a new NodeId and handles UUID collisions internally.
    /// Returns a result indicating success, retry needed, or permanent failure.
    /// </summary>
    private async Task<JoinAttemptResult> TryJoinClusterAsync(CancellationToken cancellationToken)
    {
        var currentIdentifier = RapidClusterUtils.NodeIdFromUuid(_sharedResources.NewGuid());

        // Phase 1: Contact seed for observers (with retry on UUID collision)
        JoinResponse joinResponse;
        while (true)
        {
            var preJoinMessage = new PreJoinMessage
            {
                Sender = _myAddr,
                NodeId = currentIdentifier
            };

            RapidClusterResponse preJoinResponse;
            try
            {
                preJoinResponse = await _messagingClient.SendMessageAsync(
                    _seedAddress!,
                    preJoinMessage.ToRapidClusterRequest(),
                    cancellationToken).ConfigureAwait(true);
            }
            catch (TimeoutException)
            {
                return JoinAttemptResult.RetryNeeded("Timeout contacting seed node");
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                // Network errors are typically transient
                return JoinAttemptResult.RetryNeeded($"Network error contacting seed: {ex.Message}");
            }

            joinResponse = preJoinResponse.JoinResponse;

            if (joinResponse.StatusCode == JoinStatusCode.UuidAlreadyInRing)
            {
                // UUID collision - generate a new identifier and retry (matches Java behavior)
                currentIdentifier = RapidClusterUtils.NodeIdFromUuid(_sharedResources.NewGuid());
                continue;
            }

            break;
        }

        if (joinResponse.StatusCode != JoinStatusCode.SafeToJoin &&
            joinResponse.StatusCode != JoinStatusCode.HostnameAlreadyInRing)
        {
            // Most status codes indicate permanent failure
            return JoinAttemptResult.Failed($"Join failed with status: {joinResponse.StatusCode}");
        }

        var observers = joinResponse.Endpoints.ToList();
        if (observers.Count == 0)
        {
            return JoinAttemptResult.Failed("No observers returned from seed");
        }

        // Phase 2: Contact observers
        var ringNumbersPerObserver = new Dictionary<Endpoint, List<int>>(EndpointAddressComparer.Instance);
        for (var ringNumber = 0; ringNumber < observers.Count; ringNumber++)
        {
            var observer = observers[ringNumber];
            if (!ringNumbersPerObserver.TryGetValue(observer, out var value))
            {
                ringNumbersPerObserver[observer] = value = [];
            }
            ringNumbersPerObserver[observer].Add(ringNumber);
        }

        var tasks = ringNumbersPerObserver.Select(async entry =>
        {
            var joinMessageForObserver = new JoinMessage
            {
                Sender = _myAddr,
                NodeId = currentIdentifier,
                Metadata = _nodeMetadata,
                ConfigurationId = joinResponse.ConfigurationId
            };
            joinMessageForObserver.RingNumber.AddRange(entry.Value);

            return await _messagingClient.SendMessageAsync(
                entry.Key,
                joinMessageForObserver.ToRapidClusterRequest(),
                cancellationToken).WithDefaultOnException().ConfigureAwait(true);
        });

        var responses = await Task.WhenAll(tasks).ConfigureAwait(true);
        var successfulResponse = responses.FirstOrDefault(r => r?.JoinResponse?.StatusCode == JoinStatusCode.SafeToJoin)?.JoinResponse;

        if (successfulResponse == null)
        {
            // Check if we got a ConfigChanged response - configuration changed during join, retry
            if (responses.Any(r => r?.JoinResponse?.StatusCode == JoinStatusCode.ConfigChanged))
            {
                return JoinAttemptResult.RetryNeeded("Configuration changed during join");
            }

            // No successful response from any observer - transient, should retry
            return JoinAttemptResult.RetryNeeded("Failed to get successful response from any observer");
        }

        return JoinAttemptResult.Success(successfulResponse);
    }

    /// <summary>
    /// Finalizes initialization after the membership view is established.
    /// Sets up broadcaster, consensus, failure detectors, and publishes initial view.
    /// </summary>
    private void FinalizeInitialization()
    {
        lock (_membershipUpdateLock)
        {
            // Get metadata map from the manager (was populated by StartNewCluster or JoinClusterAsync)
            var metadataMap = new Dictionary<Endpoint, Metadata>(_metadataManager.GetAllMetadata());

            // SetMembershipView handles all the setup - for initial join, nodeStatusChanges is null
            // which causes GetInitialViewChange() to be used (all nodes marked as Up)
            SetMembershipView(_membershipView, metadataMap, nodeStatusChanges: null, addedNodes: null);
        }

        _log.MembershipServiceInitialized(new MembershipServiceLogger.LoggableEndpoint(_myAddr), new MembershipServiceLogger.CurrentConfigId(_membershipView), new MembershipServiceLogger.MembershipSize(_membershipView));
    }

    /// <summary>
    /// Centralized method for updating the membership view. All view changes flow through here.
    /// This method handles:
    /// - Updating the membership view
    /// - Recreating the cut detector
    /// - Updating the broadcaster
    /// - Disposing old and creating new failure detectors  
    /// - Creating new consensus instance
    /// - Publishing the view to the accessor
    /// - Publishing VIEW_CHANGE event
    /// </summary>
    /// <param name="newView">The new membership view to apply.</param>
    /// <param name="metadataMap">Metadata for nodes in the view. If null, existing metadata is preserved.</param>
    /// <param name="nodeStatusChanges">The status changes to publish. If null, all nodes are treated as Up (initial join).</param>
    /// <param name="addedNodes">Nodes that were added (for notifying waiting joiners). Can be null.</param>
    /// <returns>The previous consensus instance that should be disposed by the caller.</returns>
    private ConsensusCoordinator? SetMembershipView(
        MembershipView newView,
        Dictionary<Endpoint, Metadata>? metadataMap,
        List<NodeStatusChange>? nodeStatusChanges,
        List<Endpoint>? addedNodes)
    {
        // Must be called under _membershipUpdateLock
        // Always capture the previous consensus instance to ensure it gets disposed.
        // This handles the case where SetMembershipView is called multiple times
        // during initialization (e.g., via ApplyLearnedMembershipView during join).
        var previousConsensusInstance = _consensusInstance;

        // Update the view
        _membershipView = newView;

        // Update metadata if provided
        if (metadataMap != null)
        {
            _metadataManager.Clear();
            _metadataManager.AddMetadata(metadataMap);
        }

        // Recreate cut detector for the new cluster size
        _cutDetection = _cutDetectorFactory.Create(_membershipView);

        // Update broadcaster membership
        _broadcaster.SetMembership([.. _membershipView.Members]);

        // Dispose old failure detectors and create new ones
        foreach (var fd in _failureDetectors)
        {
            fd.Dispose();
        }
        _failureDetectors.Clear();

        // Create new consensus instance
        _consensusInstance = _consensusCoordinatorFactory.Create(_myAddr, _membershipView.ConfigurationId, _membershipView.Size, _broadcaster);
        RegisterConsensusDecidedContinuation(_consensusInstance, _membershipView.ConfigurationId);
        _announcedProposal = false;

        // Replay any buffered consensus messages for this configuration
        ReplayBufferedConsensusMessages(_membershipView.ConfigurationId, _stoppingCts.Token);

        // Create new failure detectors
        CreateFailureDetectorsForCurrentConfiguration();

        // Notify waiting joiners if any nodes were added
        if (addedNodes != null)
        {
            NotifyWaitingJoiners(addedNodes);
        }

        // Publish the new view to the accessor
        _viewAccessor.PublishView(_membershipView);

        // Publish VIEW_CHANGE event
        var statusChanges = nodeStatusChanges ?? GetInitialViewChange();
        var currentMembership = _membershipView.Members;
        var clusterStatusChange = new ClusterStatusChange(_membershipView.ConfigurationId, [.. currentMembership], statusChanges);

        _log.PublishingViewChange(new MembershipServiceLogger.CurrentConfigId(_membershipView), new MembershipServiceLogger.MembershipSize(_membershipView));
        PublishEvent(ClusterEvents.ViewChange, clusterStatusChange);

        // Clear pending joiner data that's no longer needed
        _pendingConsensusMessages.Keys
            .Where(k => k < _membershipView.ConfigurationId)
            .ToList()
            .ForEach(k => _pendingConsensusMessages.Remove(k));

        return previousConsensusInstance;
    }

    /// <summary>
    /// Notifies joiners waiting for their join to complete.
    /// </summary>
    private void NotifyWaitingJoiners(List<Endpoint> addedNodes)
    {
        foreach (var node in addedNodes)
        {
            if (_joinersToRespondTo.Remove(node, out var channel))
            {
                // Prevent new attempts from writing to the channel
                channel.Writer.TryComplete();

                var waitingCount = 0;
                var config = _membershipView.Configuration;
                var response = new JoinResponse
                {
                    Sender = _myAddr,
                    StatusCode = JoinStatusCode.SafeToJoin,
                    ConfigurationId = _membershipView.ConfigurationId,
                    MaxMonotonicId = _membershipView.MaxMonotonicId
                };
                response.Endpoints.AddRange(config.Endpoints);
                response.Identifiers.AddRange(config.NodeIds);
                var allMetadata = _metadataManager.GetAllMetadata();
                response.MetadataKeys.AddRange(allMetadata.Keys);
                response.MetadataValues.AddRange(allMetadata.Values);

                var RapidClusterResponse = response.ToRapidClusterResponse();

                // Send response to all waiting tasks
                while (channel.Reader.TryRead(out var tcs))
                {
                    waitingCount++;
                    tcs.TrySetResult(RapidClusterResponse);
                }

                _log.NotifyingJoiners(waitingCount, new MembershipServiceLogger.LoggableEndpoint(node));
            }
        }
    }

    /// <summary>
    /// Entry point for all messages.
    /// </summary>
    public async Task<RapidClusterResponse> HandleMessageAsync(RapidClusterRequest msg, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(msg);

        if (IsTraceMessage(msg.ContentCase))
        {
            _log.HandleMessageReceivedTrace(msg.ContentCase);
        }
        else
        {
            _log.HandleMessageReceivedDebug(msg.ContentCase);
        }

        return msg.ContentCase switch
        {
            RapidClusterRequest.ContentOneofCase.PreJoinMessage => HandlePreJoinMessage(msg.PreJoinMessage, cancellationToken),
            RapidClusterRequest.ContentOneofCase.JoinMessage => await HandleJoinMessageAsync(msg.JoinMessage, cancellationToken).ConfigureAwait(true),
            RapidClusterRequest.ContentOneofCase.BatchedAlertMessage => HandleBatchedAlertMessage(msg.BatchedAlertMessage, cancellationToken),
            RapidClusterRequest.ContentOneofCase.ProbeMessage => HandleProbeMessage(msg.ProbeMessage, cancellationToken),
            RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage or
            RapidClusterRequest.ContentOneofCase.Phase1AMessage or
            RapidClusterRequest.ContentOneofCase.Phase1BMessage or
            RapidClusterRequest.ContentOneofCase.Phase2AMessage or
            RapidClusterRequest.ContentOneofCase.Phase2BMessage => HandleConsensusMessages(msg, cancellationToken),
            RapidClusterRequest.ContentOneofCase.LeaveMessage => HandleLeaveMessage(msg, cancellationToken),
            RapidClusterRequest.ContentOneofCase.MembershipViewRequest => HandleMembershipViewRequest(msg.MembershipViewRequest, cancellationToken),
            _ => throw new ArgumentException($"Unidentified RapidClusterRequest type {msg.ContentCase}")
        };

        static bool IsTraceMessage(RapidClusterRequest.ContentOneofCase contentCase)
            => contentCase == RapidClusterRequest.ContentOneofCase.ProbeMessage;
    }

    /// <summary>
    /// This is invoked by a new node joining the network at a seed node.
    /// The seed responds with the current configuration ID and a list of observers
    /// for the joiner, who then moves on to phase 2 of the protocol with its observers.
    /// </summary>
    private RapidClusterResponse HandlePreJoinMessage(PreJoinMessage msg, CancellationToken cancellationToken)
    {
        lock (_membershipUpdateLock)
        {
            var joiningEndpoint = msg.Sender;
            var statusCode = _membershipView.IsSafeToJoin(joiningEndpoint, msg.NodeId);
            var builder = new JoinResponse
            {
                Sender = _myAddr,
                ConfigurationId = _membershipView.ConfigurationId,
                StatusCode = statusCode
            };

            _log.JoinAtSeed(new MembershipServiceLogger.LoggableEndpoint(_myAddr), new MembershipServiceLogger.LoggableEndpoint(msg.Sender),
                new MembershipServiceLogger.CurrentConfigId(_membershipView), new MembershipServiceLogger.MembershipSize(_membershipView));

            var observersCount = 0;
            if (statusCode == JoinStatusCode.SafeToJoin || statusCode == JoinStatusCode.HostnameAlreadyInRing)
            {
                var observers = _membershipView.GetExpectedObserversOf(joiningEndpoint);
                builder.Endpoints.AddRange(observers);
                observersCount = observers.Length;
            }

            _log.HandlePreJoinResult(new MembershipServiceLogger.LoggableEndpoint(joiningEndpoint), statusCode, observersCount);

            return builder.ToRapidClusterResponse();
        }
    }

    /// <summary>
    /// Invoked by gatekeepers of a joining node. They perform any failure checking
    /// required before propagating a AlertMessage with the status UP. After the cut detection
    /// and full agreement succeeds, the observer informs the joiner about the new configuration it
    /// is now a part of.
    /// </summary>
    private async Task<RapidClusterResponse> HandleJoinMessageAsync(JoinMessage joinMessage, CancellationToken cancellationToken)
    {
        _log.HandleJoinMessage(new MembershipServiceLogger.LoggableEndpoint(joinMessage.Sender), joinMessage.ConfigurationId);

        Task<RapidClusterResponse> resultTask;
        lock (_membershipUpdateLock)
        {
            var currentConfiguration = _membershipView.ConfigurationId;
            if (currentConfiguration == joinMessage.ConfigurationId)
            {
                _log.EnqueueingSafeToJoin(new MembershipServiceLogger.LoggableEndpoint(joinMessage.Sender), new MembershipServiceLogger.CurrentConfigId(_membershipView),
                    new MembershipServiceLogger.MembershipSize(_membershipView));

                var tcs = new TaskCompletionSource<RapidClusterResponse>();
                ref var channel = ref CollectionsMarshal.GetValueRefOrAddDefault(_joinersToRespondTo, joinMessage.Sender, out var _);
                channel ??= Channel.CreateUnbounded<TaskCompletionSource<RapidClusterResponse>>();
                channel.Writer.TryWrite(tcs);

                var alertMsg = new AlertMessage
                {
                    EdgeSrc = _myAddr,
                    EdgeDst = joinMessage.Sender,
                    EdgeStatus = EdgeStatus.Up,
                    ConfigurationId = currentConfiguration,
                    NodeId = joinMessage.NodeId,
                    Metadata = joinMessage.Metadata
                };
                alertMsg.RingNumber.AddRange(joinMessage.RingNumber);

                EnqueueAlertMessage(alertMsg);

                resultTask = tcs.Task;
            }
            else
            {
                // This handles the corner case where the configuration changed between phase 1 and phase 2
                // of the joining node's bootstrap. It should attempt to rejoin the network.
                var configuration = _membershipView.Configuration;
                _log.WrongConfiguration(new MembershipServiceLogger.LoggableEndpoint(joinMessage.Sender), joinMessage.ConfigurationId,
                    new MembershipServiceLogger.CurrentConfigId(_membershipView), new MembershipServiceLogger.MembershipSize(_membershipView));

                var responseBuilder = new JoinResponse
                {
                    Sender = _myAddr,
                    ConfigurationId = _membershipView.ConfigurationId,
                    MaxMonotonicId = _membershipView.MaxMonotonicId
                };

                if (_membershipView.IsHostPresent(joinMessage.Sender) &&
                    _membershipView.IsIdentifierPresent(joinMessage.NodeId))
                {
                    // Race condition where a observer already crossed H messages for the joiner and changed
                    // the configuration, but the JoinPhase2 messages show up at the observer
                    // after it has already added the joiner. In this case, we simply
                    // tell the sender that they're safe to join.
                    _log.JoinerAlreadyInRing();
                    responseBuilder.StatusCode = JoinStatusCode.SafeToJoin;
                    responseBuilder.Endpoints.AddRange(configuration.Endpoints);
                    responseBuilder.Identifiers.AddRange(configuration.NodeIds);
                    var allMetadata = _metadataManager.GetAllMetadata();
                    responseBuilder.MetadataKeys.AddRange(allMetadata.Keys);
                    responseBuilder.MetadataValues.AddRange(allMetadata.Values);
                }
                else
                {
                    responseBuilder.StatusCode = JoinStatusCode.ConfigChanged;
                }

                return responseBuilder.ToRapidClusterResponse();
            }
        }

        return await resultTask.WaitAsync(TimeSpan.FromSeconds(30), _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Creates a MembershipProposal from a list of endpoints to add/remove.
    /// The proposal contains the complete new membership state including all NodeIds,
    /// ensuring that nodes can correctly apply the view change even without receiving
    /// AlertMessages containing the joiner UUIDs.
    /// </summary>
    /// <param name="endpointsToChange">Endpoints being added or removed from the cluster.</param>
    /// <returns>A complete MembershipProposal with the new view state.</returns>
    private MembershipProposal CreateMembershipProposal(List<Endpoint> endpointsToChange)
    {
        var proposal = new MembershipProposal
        {
            ConfigurationId = _membershipView.ConfigurationId + 1
        };

        // Track the next monotonic ID to assign (starts from current max + 1)
        var nextMonotonicId = _membershipView.MaxMonotonicId;

        // Compute new member set: current members +/- changes
        var newMembers = new List<Endpoint>(_membershipView.Members);
        var joiningEndpoints = new HashSet<Endpoint>(EndpointAddressComparer.Instance);

        foreach (var endpoint in endpointsToChange)
        {
            if (_membershipView.IsHostPresent(endpoint))
            {
                // Use EndpointAddressComparer to find by hostname:port, ignoring MonotonicNodeId
                var index = newMembers.FindIndex(e => EndpointAddressComparer.Instance.Equals(e, endpoint));
                if (index >= 0)
                {
                    newMembers.RemoveAt(index);
                }
            }
            else
            {
                newMembers.Add(endpoint);      // Adding
                joiningEndpoints.Add(endpoint);
            }
        }

        // Build MemberInfo list with NodeIds for each member in the new view
        foreach (var endpoint in newMembers)
        {
            NodeId? nodeId = null;
            Endpoint endpointWithMonotonicId;

            // Check if this is an existing member (already has monotonic ID)
            if (_membershipView.IsHostPresent(endpoint))
            {
                // Existing member - get NodeId from tracking and keep existing monotonic ID
                if (_memberNodeIds.TryGetValue(endpoint, out var existingId))
                {
                    nodeId = existingId;
                }
                endpointWithMonotonicId = endpoint; // Already has MonotonicNodeId set
            }
            else
            {
                // New joiner - get NodeId from joiner tracking and assign new monotonic ID
                if (_joinerUuid.TryGetValue(endpoint, out var joinerId))
                {
                    nodeId = joinerId;
                }

                // Assign the next monotonic ID to this joiner
                nextMonotonicId++;
                endpointWithMonotonicId = new Endpoint
                {
                    Hostname = endpoint.Hostname,
                    Port = endpoint.Port,
                    MonotonicNodeId = nextMonotonicId
                };
            }

            if (nodeId == null)
            {
                // This should not happen - skip the node if we don't have its ID
                _log.DecidedNodeWithoutUuid(new MembershipServiceLogger.LoggableEndpoint(endpoint));
                continue;
            }

            proposal.Members.Add(new MemberInfo { Endpoint = endpointWithMonotonicId, NodeId = nodeId });

            // Add metadata for this member
            var metadata = _metadataManager.Get(endpoint) ?? _joinerMetadata.GetValueOrDefault(endpoint, new Metadata());
            proposal.MemberMetadata.Add(metadata);
        }

        // Include all NodeIds ever seen (for UUID uniqueness checking)
        proposal.AllNodeIds.AddRange(_membershipView.NodeIds);

        // Set the max monotonic ID (highest ever assigned, only increases)
        proposal.MaxMonotonicId = nextMonotonicId;

        return proposal;
    }

    /// <summary>
    /// This method receives edge update events and delivers them to
    /// the cut detector to check if it will return a valid
    /// proposal.
    ///
    /// Edge update messages that do not affect an ongoing proposal
    /// needs to be dropped.
    /// </summary>
    private RapidClusterResponse HandleBatchedAlertMessage(BatchedAlertMessage messageBatch, CancellationToken cancellationToken)
    {
        _log.HandleBatchedAlertMessage(messageBatch.Messages.Count, new MembershipServiceLogger.LoggableEndpoint(messageBatch.Sender));

        lock (_membershipUpdateLock)
        {
            if (!FilterAlertMessages(messageBatch, _membershipView.ConfigurationId))
            {
                _log.BatchedAlertFiltered(new MembershipServiceLogger.CurrentConfigId(_membershipView));
                return new ConsensusResponse().ToRapidClusterResponse();
            }

            // Use SortedSet for deduplication and consistent ordering across all nodes.
            // This ensures all nodes propose the same set in the same order.
            var proposals = new SortedSet<Endpoint>(EndpointComparer.Instance);

            // Process alerts by ring number to enable proper batching.
            // This ensures multiple nodes can accumulate in the preProposal set
            // before any of them reaches the H threshold, allowing them to be
            // batched into a single view change proposal.
            //
            // Without this interleaving, if a single observer sends alerts for
            // multiple nodes with all ring numbers, each node would go from
            // 0 -> L -> H reports atomically, triggering individual proposals.
            var maxRingNumber = _membershipView.RingCount;
            for (var ringNumber = 0; ringNumber < maxRingNumber; ringNumber++)
            {
                foreach (var msg in messageBatch.Messages)
                {
                    if (!msg.RingNumber.Contains(ringNumber))
                    {
                        continue;
                    }

                    _log.ProcessingAlert(new MembershipServiceLogger.LoggableEndpoint(msg.EdgeSrc), new MembershipServiceLogger.LoggableEndpoint(msg.EdgeDst), msg.EdgeStatus);
                    // For valid UP alerts, extract the joiner details (UUID and metadata) which is going to be needed
                    // when the node is added to the rings
                    var extractedMessage = ExtractJoinerUuidAndMetadata(msg);
                    var cutProposals = _cutDetection.AggregateForProposalSingleRing(extractedMessage, ringNumber);
                    _log.CutDetectionProposals(cutProposals.Count);
                    foreach (var proposal in cutProposals)
                    {
                        proposals.Add(proposal);
                    }
                }
            }

            // Lastly, we apply implicit detections
            var implicitProposals = _cutDetection.InvalidateFailingEdges();
            _log.ImplicitEdgeInvalidation(implicitProposals.Count);
            foreach (var proposal in implicitProposals)
            {
                proposals.Add(proposal);
            }

            // If we have a proposal for this stage, start an instance of consensus on it.
            lock (_membershipUpdateLock)
            {
                if (proposals.Count > 0 && !_announcedProposal)
                {
                    _announcedProposal = true;
                    var currentConfigurationId = _membershipView.ConfigurationId;
                    var proposalList = proposals.ToList();
                    _log.InitiatingConsensus(new MembershipServiceLogger.LoggableEndpoints(proposalList));

                    // Inform subscribers that a proposal has been announced.
                    var nodeStatusChanges = CreateNodeStatusChangeList(proposalList);
                    var currentMembership = _membershipView.Members;
                    var clusterStatusChange = new ClusterStatusChange(currentConfigurationId, [.. currentMembership], nodeStatusChanges);

                    PublishEvent(ClusterEvents.ViewChangeProposal, clusterStatusChange);

                    // Create full membership proposal with NodeIds for all members
                    var membershipProposal = CreateMembershipProposal(proposalList);
                    _consensusInstance.Propose(membershipProposal, cancellationToken);
                }
            }

            return new ConsensusResponse().ToRapidClusterResponse();
        }
    }

    /// <summary>
    /// Receives proposal for the one-step consensus (essentially phase 2 of Fast Paxos).
    ///
    /// XXX: Implement recovery for the extremely rare possibility of conflicting proposals.
    /// </summary>
    private RapidClusterResponse HandleConsensusMessages(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        _log.HandleConsensusMessages();

        // Extract configuration ID from the message
        var messageConfigId = GetConfigurationIdFromConsensusMessage(request);

        lock (_membershipUpdateLock)
        {
            var currentConfigId = _membershipView.ConfigurationId;

            if (messageConfigId > currentConfigId)
            {
                // Message is for a future configuration - buffer it for later processing
                _log.BufferingFutureConsensusMessage(request.ContentCase, messageConfigId, currentConfigId);

                if (!_pendingConsensusMessages.TryGetValue(messageConfigId, out var pendingList))
                {
                    pendingList = [];
                    _pendingConsensusMessages[messageConfigId] = pendingList;
                }
                pendingList.Add(request);

                return new ConsensusResponse().ToRapidClusterResponse();
            }

            // Message is for current or past configuration - process normally
            // (past config messages will be rejected by Paxos due to config mismatch)
            _consensusInstance.HandleMessages(request, cancellationToken);
        }

        return new ConsensusResponse().ToRapidClusterResponse();
    }

    /// <summary>
    /// Extracts the configuration ID from a consensus message.
    /// </summary>
    private static long GetConfigurationIdFromConsensusMessage(RapidClusterRequest request)
    {
        return request.ContentCase switch
        {
            RapidClusterRequest.ContentOneofCase.FastRoundPhase2BMessage => request.FastRoundPhase2BMessage.ConfigurationId,
            RapidClusterRequest.ContentOneofCase.Phase1AMessage => request.Phase1AMessage.ConfigurationId,
            RapidClusterRequest.ContentOneofCase.Phase1BMessage => request.Phase1BMessage.ConfigurationId,
            RapidClusterRequest.ContentOneofCase.Phase2AMessage => request.Phase2AMessage.ConfigurationId,
            RapidClusterRequest.ContentOneofCase.Phase2BMessage => request.Phase2BMessage.ConfigurationId,
            _ => throw new ArgumentException($"Unexpected consensus message type: {request.ContentCase}")
        };
    }

    /// <summary>
    /// Propagates the intent of a node to leave the group
    /// </summary>
    private RapidClusterResponse HandleLeaveMessage(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        var leaveMessage = request.LeaveMessage;
        _log.ReceivedLeaveMessage(new MembershipServiceLogger.LoggableEndpoint(leaveMessage.Sender), new MembershipServiceLogger.LoggableEndpoint(_myAddr));
        EdgeFailureNotification(leaveMessage.Sender, _membershipView.ConfigurationId);
        return new ConsensusResponse().ToRapidClusterResponse();
    }

    /// <summary>
    /// Invoked by observers of a node for failure detection.
    /// Also performs bidirectional stale view detection: if the prober has a higher
    /// configuration ID than us, we trigger the learner protocol to catch up.
    /// </summary>
    private RapidClusterResponse HandleProbeMessage(ProbeMessage probeMessage, CancellationToken cancellationToken)
    {
        _log.HandleProbeMessage();
        var senderInMembership = probeMessage.Sender != null && _membershipView.IsHostPresent(probeMessage.Sender);

        // Bidirectional stale view detection: if the prober has a higher config ID,
        // we need to catch up. This handles the case where we're being monitored by
        // nodes that have advanced past us (e.g., we missed a consensus round).
        var senderConfigId = probeMessage.ConfigurationId;
        var localConfigId = _membershipView.ConfigurationId;
        if (senderConfigId > localConfigId && probeMessage.Sender != null)
        {
            OnStaleViewDetected(probeMessage.Sender, senderConfigId, localConfigId);
        }

        return new ProbeResponse
        {
            ConfigurationId = _membershipView.ConfigurationId,
            SenderInMembership = senderInMembership
        }.ToRapidClusterResponse();
    }

    /// <summary>
    /// Handles a request from a node that has detected it has a stale view.
    /// This is the "learner" role in Paxos - allowing nodes that missed consensus
    /// decisions to catch up by requesting the current view from another node.
    /// </summary>
    private RapidClusterResponse HandleMembershipViewRequest(MembershipViewRequest request, CancellationToken cancellationToken)
    {
        _log.HandleMembershipViewRequest(
            new MembershipServiceLogger.LoggableEndpoint(request.Sender),
            request.CurrentConfigurationId,
            new MembershipServiceLogger.CurrentConfigId(_membershipView));

        // Build response with current membership view
        var response = new MembershipViewResponse
        {
            Sender = _myAddr,
            ConfigurationId = _membershipView.ConfigurationId,
            MaxMonotonicId = _membershipView.MaxMonotonicId
        };

        // Add all endpoints and their node IDs
        var members = _membershipView.Members;
        response.Endpoints.AddRange(members);
        response.Identifiers.AddRange(_membershipView.NodeIds);

        // Add metadata for all nodes
        foreach (var endpoint in members)
        {
            var metadata = _metadataManager.Get(endpoint) ?? new Metadata();
            response.MetadataKeys.Add(endpoint);
            response.MetadataValues.Add(metadata);
        }

        return response.ToRapidClusterResponse();
    }

    /// <summary>
    /// This is invoked by FastPaxos modules when they arrive at a decision.
    ///
    /// The MembershipProposal contains the complete new membership view state,
    /// including all members and their NodeIds. This eliminates the race condition
    /// where nodes might not have received AlertMessages with joiner UUIDs.
    /// </summary>
    /// <param name="proposal">The decided membership proposal containing the complete new view state.</param>
    /// <param name="decidingConfigurationId">The configuration ID when consensus was started.</param>
    private async Task DecideViewChange(MembershipProposal proposal, ConfigurationId decidingConfigurationId)
    {
        _log.DecideViewChange(proposal.Members.Count);

        ConsensusCoordinator? previousConsensusInstance;
        lock (_membershipUpdateLock)
        {
            // Check if this decision is for the current configuration.
            // A stale decision can arrive if:
            // 1. Node A crashed while joining (consensus to add A was in progress)
            // 2. Other nodes detected A's crash and started new consensus to remove A
            // 3. The "remove A" consensus completed first, advancing the configuration
            // 4. The old "add A" consensus completes later with a stale decision
            // In this case, we should ignore the stale decision. The stale consensus
            // instance has already been disposed when the view changed (via SetMembershipView).
            if (_membershipView.ConfigurationId != decidingConfigurationId)
            {
                _log.IgnoringStaleConsensusDecision(proposal.Members.Count);
                return;
            }

            _announcedProposal = false;

            // Track nodes that were added so we can notify their joiners after ALL nodes are processed
            var addedNodes = new List<Endpoint>();

            // Build status changes during the loop, capturing state BEFORE modifications
            // This ensures consistent semantics: Up = joining, Down = leaving/failing
            var nodeStatusChanges = new List<NodeStatusChange>(proposal.Members.Count);

            // Create a builder from the current view to make modifications
            // Use the protocol's ObserversPerSubject as the max ring count so the cluster
            // can grow beyond its current ring count when new nodes join
            var builder = _membershipView.ToBuilder(_options.ObserversPerSubject);

            // Build a lookup from the proposal for fast access to NodeIds and endpoints with MonotonicNodeId
            var proposalMemberLookup = proposal.Members.ToDictionary(
                m => m.Endpoint,
                m => (NodeId: m.NodeId, EndpointWithMonotonicId: m.Endpoint),
                EndpointAddressComparer.Instance);
            var proposalMetadataLookup = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
            for (var i = 0; i < proposal.Members.Count; i++)
            {
                if (i < proposal.MemberMetadata.Count)
                {
                    proposalMetadataLookup[proposal.Members[i].Endpoint] = proposal.MemberMetadata[i];
                }
            }

            // Update the builder's max monotonic ID from the proposal
            builder.SetMaxMonotonicId(proposal.MaxMonotonicId);

            // Determine which nodes are being added and which are being removed
            var currentMembers = new HashSet<Endpoint>(_membershipView.Members, EndpointAddressComparer.Instance);
            var proposedMembers = new HashSet<Endpoint>(proposal.Members.Select(m => m.Endpoint), EndpointAddressComparer.Instance);

            // Nodes to remove: in current but not in proposed
            foreach (var node in currentMembers.Except(proposedMembers, EndpointAddressComparer.Instance))
            {
                _log.RemovingNode(new MembershipServiceLogger.LoggableEndpoint(node));
                builder.RingDelete(node);
                nodeStatusChanges.Add(new NodeStatusChange(node, EdgeStatus.Down, _metadataManager.Get(node) ?? new Metadata()));

                // Remove from memberNodeIds tracking
                _memberNodeIds.Remove(node);
            }

            // Nodes to add: in proposed but not in current
            foreach (var node in proposedMembers.Except(currentMembers, EndpointAddressComparer.Instance))
            {
                if (!proposalMemberLookup.TryGetValue(node, out var memberInfo))
                {
                    // This should never happen - the proposal should always have the NodeId
                    _log.DecidedNodeWithoutUuid(new MembershipServiceLogger.LoggableEndpoint(node));
                    continue;
                }

                var nodeId = memberInfo.NodeId;
                var endpointWithMonotonicId = memberInfo.EndpointWithMonotonicId;
                var metadata = proposalMetadataLookup.GetValueOrDefault(node) ?? _joinerMetadata.GetValueOrDefault(node, new Metadata());

                _log.AddingNode(new MembershipServiceLogger.LoggableEndpoint(endpointWithMonotonicId));
                builder.RingAdd(endpointWithMonotonicId, nodeId);
                _metadataManager.Add(endpointWithMonotonicId, metadata);
                nodeStatusChanges.Add(new NodeStatusChange(endpointWithMonotonicId, EdgeStatus.Up, metadata));

                // Track NodeId for this member (use endpoint with monotonic ID)
                _memberNodeIds[endpointWithMonotonicId] = nodeId;

                // Clean up joiner data (use original node key without monotonic ID)
                _joinerUuid.Remove(node);
                _joinerMetadata.Remove(node);

                // Track this node for later notification (use endpoint with monotonic ID)
                addedNodes.Add(endpointWithMonotonicId);
            }

            // Build the new immutable view with incremented version
            var newView = builder.Build(_membershipView.ConfigurationId);

            _log.DecideViewChangeCleanup();

            // Use SetMembershipView to apply all changes - pass null for metadataMap to preserve existing
            previousConsensusInstance = SetMembershipView(newView, metadataMap: null, nodeStatusChanges, addedNodes);
        }

        if (previousConsensusInstance != null)
        {
            await previousConsensusInstance.DisposeAsync();
        }
    }

    /// <summary>
    /// Gets the async enumerable for subscribing to cluster events.
    /// Each subscriber receives all events published after they start iterating.
    /// </summary>
    public BroadcastChannelReader<ClusterEventNotification> Events => _eventChannel.Reader;

    /// <summary>
    /// Gets the list of endpoints currently in the membership view.
    /// </summary>
    /// <returns>list of endpoints in the membership view</returns>
    public List<Endpoint> GetMembershipView() => [.. _membershipView.Members];

    /// <summary>
    /// Gets the list of endpoints currently in the membership view.
    /// </summary>
    /// <returns>list of endpoints in the membership view</returns>
    public int GetMembershipSize() => _membershipView.Size;

    /// <summary>
    /// Gets the list of endpoints currently in the membership view.
    /// </summary>
    /// <returns>list of endpoints in the membership view</returns>
    public Dictionary<Endpoint, Metadata> GetMetadata() => new(_metadataManager.GetAllMetadata(), EndpointAddressComparer.Instance);

    /// <summary>
    /// Queues a AlertMessage to be broadcasted after potentially being batched.
    /// </summary>
    /// <param name="msg">the AlertMessage to be broadcasted</param>
    private void EnqueueAlertMessage(AlertMessage msg)
    {
        _log.EnqueueAlertMessage(new MembershipServiceLogger.LoggableEndpoint(msg.EdgeSrc), new MembershipServiceLogger.LoggableEndpoint(msg.EdgeDst), msg.EdgeStatus);
        _sendQueue.Writer.TryWrite(msg);
    }

    /// <summary>
    /// Batches outgoing AlertMessages into a single BatchAlertMessage.
    /// </summary>
    private async Task AlertBatcherAsync()
    {
        var buffer = new List<AlertMessage>();
        var stoppingToken = _stoppingCts.Token;

        while (!stoppingToken.IsCancellationRequested)
        {
            buffer.Clear();
            try
            {
                await Task.Delay(_options.BatchingWindow, _sharedResources.TimeProvider, stoppingToken).ConfigureAwait(true);
                await _sendQueue.Reader.WaitToReadAsync(stoppingToken);
                while (_sendQueue.Reader.TryRead(out var msg))
                {
                    buffer.Add(msg);
                }

                if (buffer.Count > 0)
                {
                    _log.AlertBatcherBroadcast(buffer.Count);

                    var batchedMessage = new BatchedAlertMessage
                    {
                        Sender = _myAddr
                    };
                    batchedMessage.Messages.AddRange(buffer);

                    var request = batchedMessage.ToRapidClusterRequest();
                    _broadcaster.Broadcast(request, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                _log.AlertBatcherExit();
                break;
            }
        }
    }

    /// <summary>
    /// A filter for removing invalid edge update messages. These include messages that were for a
    /// configuration that the current node is not a part of, and messages that violate the semantics
    /// of a node being a part of a configuration.
    /// </summary>
    private static bool FilterAlertMessages(BatchedAlertMessage batchedAlertMessage, long currentConfigurationId) => batchedAlertMessage.Messages.Any(m => m.ConfigurationId == currentConfigurationId);

    private AlertMessage ExtractJoinerUuidAndMetadata(AlertMessage alertMessage)
    {
        if (alertMessage.EdgeStatus == EdgeStatus.Up && alertMessage.NodeId != null)
        {
            // Both the UUID and Metadata are saved only after the node is done being added.
            _joinerUuid[alertMessage.EdgeDst] = alertMessage.NodeId;
            _joinerMetadata[alertMessage.EdgeDst] = alertMessage.Metadata;
            _log.ExtractJoinerUuidAndMetadata(new MembershipServiceLogger.LoggableEndpoint(alertMessage.EdgeDst));
        }
        return alertMessage;
    }

    /// <summary>
    /// Formats a proposal or view change for application subscriptions.
    /// Determines status based on current view state:
    /// - Nodes NOT in view → EdgeStatus.Up (joining)
    /// - Nodes IN view → EdgeStatus.Down (leaving/failing)
    ///
    /// For ViewChangeProposal: called BEFORE view update, so joining nodes aren't in view yet.
    /// For ViewChange: status is captured inline BEFORE each node is added/removed.
    /// Both cases produce consistent semantics: Up = joining, Down = leaving.
    /// </summary>
    private List<NodeStatusChange> CreateNodeStatusChangeList(IEnumerable<Endpoint> proposal)
    {
        var list = new List<NodeStatusChange>();
        foreach (var node in proposal)
        {
            var status = _membershipView.IsHostPresent(node) ? EdgeStatus.Down : EdgeStatus.Up;
            list.Add(new NodeStatusChange(node, status, _metadataManager.Get(node) ?? new Metadata()));
        }
        return list;
    }

    /// <summary>
    /// Prepares a view change notification for a node that has just become part of a cluster. This is invoked when the
    /// membership service is first initialized by a new node, which only happens on a Cluster.join() or Cluster.start().
    /// Therefore, all EdgeStatus values will be UP.
    /// </summary>
    private List<NodeStatusChange> GetInitialViewChange()
    {
        var list = new List<NodeStatusChange>();
        foreach (var node in _membershipView.Members)
        {
            list.Add(new NodeStatusChange(node, EdgeStatus.Up, _metadataManager.Get(node) ?? new Metadata()));
        }
        return list;
    }

    /// <summary>
    /// Publishes a cluster event to all subscribers via the channel.
    /// </summary>
    /// <param name="evt">The cluster event type.</param>
    /// <param name="statusChange">The cluster status change details.</param>
    private void PublishEvent(ClusterEvents evt, ClusterStatusChange statusChange)
    {
        var notification = new ClusterEventNotification(evt, statusChange);
        _eventChannel.Writer.TryPublish(notification);
    }

    /// <summary>
    /// Attempts to rejoin the cluster after being kicked.
    /// Cycles through known members to find a live seed and performs the join protocol.
    /// </summary>
    private async Task RejoinClusterAsync(CancellationToken cancellationToken)
    {
        if (_isRejoining || _disposed != 0)
        {
            _log.RejoinSkipped(new MembershipServiceLogger.LoggableEndpoint(_myAddr));
            return;
        }
        _isRejoining = true;

        try
        {
            _log.StartingRejoin(new MembershipServiceLogger.LoggableEndpoint(_myAddr));

            // Get known members from our current (stale) view to try as seeds
            var knownMembers = _membershipView.Members.Where(e => !e.Equals(_myAddr)).ToList();
            var metadata = _metadataManager.Get(_myAddr) ?? new Metadata();

            JoinResponse? successfulResponse = null;
            NodeId? successfulNodeId = null;

            foreach (var seed in knownMembers)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                try
                {
                    // Generate a new node ID for each attempt - this avoids UUID collision issues
                    // where our old UUID might still be in the cluster's identifiers-seen set
                    var nodeId = RapidClusterUtils.NodeIdFromUuid(_sharedResources.NewGuid());

                    successfulResponse = await TryRejoinThroughSeedAsync(seed, nodeId, metadata, cancellationToken).ConfigureAwait(true);
                    if (successfulResponse != null)
                    {
                        successfulNodeId = nodeId;
                        break;
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    _log.RejoinFailedThroughSeed(new MembershipServiceLogger.LoggableEndpoint(_myAddr), new MembershipServiceLogger.LoggableEndpoint(seed), ex.Message);
                    // Try next seed
                }
            }

            if (successfulResponse == null || successfulNodeId == null)
            {
                _log.RejoinFailedNoSeeds(new MembershipServiceLogger.LoggableEndpoint(_myAddr));
                return;
            }

            // Reset internal state with new membership
            ResetStateAfterRejoin(successfulResponse, successfulNodeId);

            _log.RejoinSuccessful(
                new MembershipServiceLogger.LoggableEndpoint(_myAddr),
                successfulResponse.Endpoints.Count,
                successfulResponse.ConfigurationId);
        }
        finally
        {
            _isRejoining = false;
        }
    }

    /// <summary>
    /// Attempts to rejoin the cluster through a specific seed node.
    /// </summary>
    private async Task<JoinResponse?> TryRejoinThroughSeedAsync(
        Endpoint seed,
        NodeId nodeId,
        Metadata metadata,
        CancellationToken cancellationToken)
    {
        _log.AttemptingRejoinThroughSeed(new MembershipServiceLogger.LoggableEndpoint(_myAddr), new MembershipServiceLogger.LoggableEndpoint(seed));

        // Phase 1: PreJoin to get observers
        var preJoinMessage = new PreJoinMessage
        {
            Sender = _myAddr,
            NodeId = nodeId
        };

        var preJoinResponse = await _messagingClient.SendMessageAsync(
            seed,
            preJoinMessage.ToRapidClusterRequest(),
            cancellationToken).ConfigureAwait(true);

        var joinResponse = preJoinResponse.JoinResponse;
        _log.RejoinPreJoinResponse(
            new MembershipServiceLogger.LoggableEndpoint(_myAddr),
            joinResponse.StatusCode,
            joinResponse.Endpoints.Count);

        if (joinResponse.StatusCode != JoinStatusCode.SafeToJoin &&
            joinResponse.StatusCode != JoinStatusCode.HostnameAlreadyInRing)
        {
            return null;
        }

        var observers = joinResponse.Endpoints.ToList();
        if (observers.Count == 0)
        {
            return null;
        }

        // Phase 2: Contact observers
        var ringNumbersPerObserver = new Dictionary<Endpoint, List<int>>(EndpointAddressComparer.Instance);
        for (var ringNumber = 0; ringNumber < observers.Count; ringNumber++)
        {
            var observer = observers[ringNumber];
            if (!ringNumbersPerObserver.TryGetValue(observer, out var value))
            {
                ringNumbersPerObserver[observer] = value = [];
            }
            value.Add(ringNumber);
        }

        var tasks = ringNumbersPerObserver.Select(async entry =>
        {
            var joinMessageForObserver = new JoinMessage
            {
                Sender = _myAddr,
                NodeId = nodeId,
                Metadata = metadata,
                ConfigurationId = joinResponse.ConfigurationId
            };
            joinMessageForObserver.RingNumber.AddRange(entry.Value);

            return await _messagingClient.SendMessageBestEffortAsync(
                entry.Key,
                joinMessageForObserver.ToRapidClusterRequest(),
                cancellationToken).ConfigureAwait(true);
        });

        var responses = await Task.WhenAll(tasks).ConfigureAwait(true);
        return responses.FirstOrDefault(r => r?.JoinResponse?.StatusCode == JoinStatusCode.SafeToJoin)?.JoinResponse;
    }

    /// <summary>
    /// Resets the internal state after a successful rejoin.
    /// </summary>
    private void ResetStateAfterRejoin(JoinResponse response, NodeId nodeId)
    {
        ConsensusCoordinator? oldConsensus;
        lock (_membershipUpdateLock)
        {
            // Clear pending data before setting new view
            _joinersToRespondTo.Clear();
            _joinerUuid.Clear();
            _joinerMetadata.Clear();
            _pendingConsensusMessages.Clear();

            // Build new membership view from response
            var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
            for (var i = 0; i < response.MetadataKeys.Count && i < response.MetadataValues.Count; i++)
            {
                metadataMap[response.MetadataKeys[i]] = response.MetadataValues[i];
            }

            // Pass maxMonotonicId to ensure it's preserved even if the node with that ID was removed
            var newView = new MembershipViewBuilder(
                _options.ObserversPerSubject,
                [.. response.Identifiers],
                [.. response.Endpoints],
                response.MaxMonotonicId).BuildWithConfigurationId(new ConfigurationId(response.ConfigurationId));

            // Update _memberNodeIds for all members in the new view
            _memberNodeIds.Clear();
            for (var i = 0; i < response.Endpoints.Count && i < response.Identifiers.Count; i++)
            {
                _memberNodeIds[response.Endpoints[i]] = response.Identifiers[i];
            }

            // Use SetMembershipView to apply all changes - for rejoin, all nodes are treated as Up
            oldConsensus = SetMembershipView(newView, metadataMap, nodeStatusChanges: null, addedNodes: null);
        }

        // Dispose old consensus - track it so we await it during shutdown
        if (oldConsensus != null)
        {
            TrackBackgroundTask(oldConsensus.DisposeAsync().AsTask());
        }
    }

    /// <summary>
    /// Called by the failure detector when a probe response indicates this node
    /// has a stale view (remote has higher config ID, but we're still in membership).
    /// This is the Paxos "learner" role - requesting missed consensus decisions.
    /// </summary>
    /// <param name="remoteEndpoint">The endpoint that reported the higher config ID.</param>
    /// <param name="remoteConfigId">The configuration ID from the probe response.</param>
    /// <param name="localConfigId">The local configuration ID when the stale view was detected.</param>
    private void OnStaleViewDetected(Endpoint remoteEndpoint, long remoteConfigId, long localConfigId)
    {
        _log.StaleViewDetected(new MembershipServiceLogger.LoggableEndpoint(remoteEndpoint), remoteConfigId, localConfigId);

        // Schedule refresh on the background task scheduler
        var refreshTask = Task.Factory.StartNew(
            () => RefreshMembershipViewAsync(remoteEndpoint, remoteConfigId, _stoppingCts.Token),
            _stoppingCts.Token,
            TaskCreationOptions.None,
            _sharedResources.TaskScheduler).Unwrap();
        TrackBackgroundTask(refreshTask);
    }

    /// <summary>
    /// Requests an updated membership view from a remote node.
    /// This implements the Paxos "learner" role - catching up on missed consensus decisions.
    /// </summary>
    private async Task RefreshMembershipViewAsync(Endpoint remoteEndpoint, long expectedConfigId, CancellationToken cancellationToken)
    {
        // Prevent concurrent refresh attempts
        if (_isRefreshingView || _disposed != 0)
        {
            _log.SkippingStaleViewRefresh(expectedConfigId, _membershipView.ConfigurationId);
            return;
        }

        // Rate limit refresh attempts
        var now = _sharedResources.TimeProvider.GetTimestamp();
        var lastRefresh = Interlocked.Read(ref _lastStaleViewRefreshTicks);
        var elapsed = _sharedResources.TimeProvider.GetElapsedTime(lastRefresh, now);
        if (elapsed < _options.StaleViewRefreshInterval)
        {
            _log.SkippingStaleViewRefresh(expectedConfigId, _membershipView.ConfigurationId);
            return;
        }

        // Double-check we still need to refresh (config may have been updated by another mechanism)
        if (expectedConfigId <= _membershipView.ConfigurationId)
        {
            _log.SkippingStaleViewRefresh(expectedConfigId, _membershipView.ConfigurationId);
            return;
        }

        _isRefreshingView = true;
        try
        {
            _log.RequestingMembershipView(new MembershipServiceLogger.LoggableEndpoint(remoteEndpoint));

            var request = new MembershipViewRequest
            {
                Sender = _myAddr,
                CurrentConfigurationId = _membershipView.ConfigurationId
            };

            var response = await _messagingClient.SendMessageAsync(
                remoteEndpoint,
                request.ToRapidClusterRequest(),
                cancellationToken).ConfigureAwait(true);

            var viewResponse = response.MembershipViewResponse;
            if (viewResponse == null)
            {
                _log.MembershipViewRefreshFailed(new MembershipServiceLogger.LoggableEndpoint(remoteEndpoint), "No MembershipViewResponse in reply");
                return;
            }

            // Only apply if the response is newer than our current view
            if (viewResponse.ConfigurationId <= _membershipView.ConfigurationId)
            {
                _log.SkippingStaleViewRefresh(viewResponse.ConfigurationId, _membershipView.ConfigurationId);
                return;
            }

            // Apply the learned view
            ApplyLearnedMembershipView(viewResponse);

            // Update last refresh timestamp on success
            Interlocked.Exchange(ref _lastStaleViewRefreshTicks, _sharedResources.TimeProvider.GetTimestamp());

            _log.MembershipViewRefreshed(
                new MembershipServiceLogger.LoggableEndpoint(remoteEndpoint),
                viewResponse.ConfigurationId,
                viewResponse.Endpoints.Count);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.MembershipViewRefreshFailed(new MembershipServiceLogger.LoggableEndpoint(remoteEndpoint), ex.Message);
        }
        finally
        {
            _isRefreshingView = false;
        }
    }

    /// <summary>
    /// Applies a learned membership view from a remote node.
    /// This is used by the Paxos learner mechanism to catch up on missed consensus decisions.
    /// If the local node is not in the new view, it triggers a kicked event and rejoin.
    /// </summary>
    private void ApplyLearnedMembershipView(MembershipViewResponse viewResponse)
    {
        ConsensusCoordinator? oldConsensus;
        bool wasKicked;
        lock (_membershipUpdateLock)
        {
            // Guard against config ID regression - never allow a stale view to replace a fresher one
            if (viewResponse.ConfigurationId <= _membershipView.ConfigurationId)
            {
                return;
            }

            // Build metadata map from response
            var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
            for (var i = 0; i < viewResponse.MetadataKeys.Count && i < viewResponse.MetadataValues.Count; i++)
            {
                metadataMap[viewResponse.MetadataKeys[i]] = viewResponse.MetadataValues[i];
            }

            // Build the new view from the response
            // Pass maxMonotonicId to ensure it's preserved even if the node with that ID was removed
            var newView = new MembershipViewBuilder(
                _options.ObserversPerSubject,
                [.. viewResponse.Identifiers],
                [.. viewResponse.Endpoints],
                viewResponse.MaxMonotonicId).BuildWithConfigurationId(new ConfigurationId(viewResponse.ConfigurationId));

            // Check if we were kicked (not in the new membership)
            // Use EndpointAddressComparer to ignore MonotonicNodeId when checking membership
            var newMembers = new HashSet<Endpoint>(newView.Members, EndpointAddressComparer.Instance);
            wasKicked = !newMembers.Contains(_myAddr);

            // If we're not initialized and not in the new view, don't apply it.
            // The ongoing join process will handle getting us into the cluster.
            // Applying a view where we're not a member would interfere with the join.
            if (!_initialized && wasKicked)
            {
                return;
            }

            // Get old members (Members property safely returns empty for empty views)
            var oldMembers = new HashSet<Endpoint>(_membershipView.Members, EndpointAddressComparer.Instance);

            if (wasKicked)
            {
                // If we had no previous membership (empty view), we weren't really "kicked" -
                // we just haven't joined yet. Skip the kicked event in this case.
                if (oldMembers.Count > 0)
                {
                    // We were kicked - publish kicked event but don't apply the view
                    // (we'll rejoin with a new identity)
                    _log.NodeKicked(
                        new MembershipServiceLogger.LoggableEndpoint(_myAddr),
                        viewResponse.ConfigurationId,
                        _membershipView.ConfigurationId.Version);

                    var currentMembership = _membershipView.Members;
                    var nodeStatusChange = new NodeStatusChange(_myAddr, EdgeStatus.Down, _metadataManager.Get(_myAddr) ?? new Metadata());
                    var clusterStatusChange = new ClusterStatusChange(_membershipView.ConfigurationId, [.. currentMembership], [nodeStatusChange]);

                    PublishEvent(ClusterEvents.Kicked, clusterStatusChange);
                }
                oldConsensus = null;
            }
            else
            {
                // We're still in membership - compute status changes and apply the view
                var nodeStatusChanges = new List<NodeStatusChange>();

                // Nodes that left (in old but not in new)
                foreach (var node in oldMembers)
                {
                    if (!newMembers.Contains(node))
                    {
                        nodeStatusChanges.Add(new NodeStatusChange(node, EdgeStatus.Down, _metadataManager.Get(node) ?? new Metadata()));
                    }
                }

                // Nodes that joined (in new but not in old)
                foreach (var node in newMembers)
                {
                    if (!oldMembers.Contains(node))
                    {
                        var metadata = metadataMap.GetValueOrDefault(node, new Metadata());
                        nodeStatusChanges.Add(new NodeStatusChange(node, EdgeStatus.Up, metadata));
                    }
                }

                // Update _memberNodeIds for all members in the new view
                _memberNodeIds.Clear();
                for (var i = 0; i < viewResponse.Endpoints.Count && i < viewResponse.Identifiers.Count; i++)
                {
                    _memberNodeIds[viewResponse.Endpoints[i]] = viewResponse.Identifiers[i];
                }

                // Apply the new view
                oldConsensus = SetMembershipView(newView, metadataMap, nodeStatusChanges, addedNodes: null);
            }
        }

        // Dispose old consensus - track it so we await it during shutdown
        if (oldConsensus != null)
        {
            TrackBackgroundTask(oldConsensus.DisposeAsync().AsTask());
        }

        // If we were kicked, schedule rejoin outside the lock.
        // Note: If !_initialized && wasKicked, we already returned early above,
        // so this only triggers for initialized nodes that were kicked.
        if (wasKicked)
        {
            var rejoinTask = Task.Factory.StartNew(
                () => RejoinClusterAsync(_stoppingCts.Token),
                _stoppingCts.Token,
                TaskCreationOptions.None,
                _sharedResources.TaskScheduler).Unwrap();
            TrackBackgroundTask(rejoinTask);
        }
    }

    /// <summary>
    /// Creates and schedules failure detector instances based on the fdFactory instance.
    /// </summary>
    private void CreateFailureDetectorsForCurrentConfiguration()
    {
        // Check if this node is still in the ring - it may have been removed during a view change
        if (!_membershipView.IsHostPresent(_myAddr))
        {
            _log.SkippingFailureDetectorsNotInRing();
            return;
        }

        var subjects = _membershipView.GetSubjectsOf(_myAddr);
        var configurationId = _membershipView.ConfigurationId;

        _log.CreateFailureDetectors(subjects.Length);
        _log.CreateFailureDetectorsSummary(new MembershipServiceLogger.LoggableEndpoints(subjects), configurationId);

        for (var i = 0; i < subjects.Length; i++)
        {
            var subject = subjects[i];
            var ringNumber = i;
            var fd = _fdFactory.CreateInstance(subject, () => EdgeFailureNotification(subject, configurationId));

            _log.CreatedFailureDetector(new MembershipServiceLogger.LoggableEndpoint(subject), ringNumber);

            fd.Start();
            _failureDetectors.Add(fd);
        }
    }

    /// <summary>
    /// This is a notification from a local edge failure detector at an observer. This changes
    /// the status of the edge between the observer and the subject to DOWN.
    /// </summary>
    /// <param name="subject">The subject that has failed.</param>
    /// <param name="configurationId">Configuration ID when the failure was detected</param>
    private void EdgeFailureNotification(Endpoint subject, long configurationId)
    {
        _log.EdgeFailureNotificationScheduled(new MembershipServiceLogger.LoggableEndpoint(subject), configurationId);

        try
        {
            if (configurationId != _membershipView.ConfigurationId)
            {
                _log.IgnoringOldConfigNotification(new MembershipServiceLogger.LoggableEndpoint(subject), new MembershipServiceLogger.CurrentConfigId(_membershipView), configurationId);
                return;
            }

            _log.AnnouncingEdgeFail(new MembershipServiceLogger.LoggableEndpoint(subject), new MembershipServiceLogger.LoggableEndpoint(_myAddr), configurationId, new MembershipServiceLogger.MembershipSize(_membershipView));

            var ringNumbers = _membershipView.GetRingNumbers(_myAddr, subject);
            _log.EdgeFailureNotificationEnqueued(new MembershipServiceLogger.LoggableEndpoint(subject), new MembershipServiceLogger.LoggableRingNumbers(ringNumbers));

            // Note: setUuid is deliberately missing here because it does not affect leaves.
            var msg = new AlertMessage
            {
                EdgeSrc = _myAddr,
                EdgeDst = subject,
                EdgeStatus = EdgeStatus.Down,
                ConfigurationId = configurationId
            };
            msg.RingNumber.AddRange(ringNumbers);

            EnqueueAlertMessage(msg);
        }
        catch (Exception ex)
        {
            _log.ErrorInEdgeFailureNotification(ex, new MembershipServiceLogger.LoggableEndpoint(subject));
            throw;
        }
    }

    /// <summary>
    /// Registers a continuation on ConsensusCoordinator.Decided that handles the result and checks for shutdown.
    /// The continuation is tracked as a background task to ensure proper cleanup during shutdown.
    /// </summary>
    private void RegisterConsensusDecidedContinuation(ConsensusCoordinator consensusInstance, ConfigurationId configurationId)
    {
        var continuationTask = consensusInstance.Decided.ContinueWith(async decision =>
        {
            if (decision.IsCanceled)
            {
                // Consensus was cancelled (e.g., during shutdown or view change) - nothing to do
                return;
            }

            if (decision.IsFaulted)
            {
                _log.ConsensusDecidedFaulted(decision.Exception!);
                // Consensus failed (e.g., exhausted all rounds during a partition).
                // Reset state to allow new proposals when alerts arrive.
                await ResetConsensusStateAfterFailure(consensusInstance);
                return;
            }

            await DecideViewChange(await decision, configurationId);
        }, CancellationToken.None, TaskContinuationOptions.None, _sharedResources.TaskScheduler);
        continuationTask.Unwrap().Ignore();
    }

    /// <summary>
    /// Resets consensus state after a failure to allow new proposals.
    /// Called when consensus exhausts all rounds without reaching a decision.
    /// </summary>
    private async Task ResetConsensusStateAfterFailure(ConsensusCoordinator failedInstance)
    {
        lock (_membershipUpdateLock)
        {
            // Only reset if this is still the current consensus instance
            if (!ReferenceEquals(_consensusInstance, failedInstance))
            {
                return;
            }

            // Reset the announced proposal flag so new alerts can trigger consensus
            _announcedProposal = false;

            // Create a fresh consensus coordinator for the same configuration
            _consensusInstance = _consensusCoordinatorFactory.Create(
                _myAddr,
                _membershipView.ConfigurationId,
                _membershipView.Size,
                _broadcaster);
            RegisterConsensusDecidedContinuation(_consensusInstance, _membershipView.ConfigurationId);
        }

        await failedInstance.DisposeAsync();
    }

    /// <summary>
    /// Replays buffered consensus messages for the given configuration.
    /// Called after a view change to process any messages that arrived before we transitioned.
    /// Also cleans up messages for old configurations.
    /// </summary>
    private void ReplayBufferedConsensusMessages(long currentConfigId, CancellationToken cancellationToken)
    {
        // Clean up messages for old configurations (they're no longer relevant)
        var keysToRemove = _pendingConsensusMessages.Keys.Where(k => k < currentConfigId).ToList();
        foreach (var key in keysToRemove)
        {
            _pendingConsensusMessages.Remove(key);
        }

        // Replay messages for the current configuration
        if (_pendingConsensusMessages.TryGetValue(currentConfigId, out var pendingMessages))
        {
            _pendingConsensusMessages.Remove(currentConfigId);

            if (pendingMessages.Count > 0)
            {
                _log.ReplayingBufferedMessages(pendingMessages.Count, currentConfigId);

                foreach (var message in pendingMessages)
                {
                    _consensusInstance.HandleMessages(message, cancellationToken);
                }
            }
        }
    }

    /// <summary>
    /// Tracks a background task to ensure it can be awaited during shutdown.
    /// </summary>
    private void TrackBackgroundTask(Task task)
    {
        lock (_backgroundTasksLock)
        {
            _backgroundTasks.Add(task);
        }
    }

    /// <summary>
    /// Stops the membership service gracefully by notifying observers and waiting for background tasks.
    /// Does not dispose resources - call <see cref="DisposeAsync"/> after this method.
    /// </summary>
    /// <remarks>
    /// For graceful shutdown: call <c>StopAsync()</c> then <c>DisposeAsync()</c>.
    /// For hard crash (no notification): call <c>DisposeAsync()</c> directly.
    /// </remarks>
    /// <param name="cancellationToken">Cancellation token to observe.</param>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        _log.Stopping();

        // Send leave messages to observers
        try
        {
            var leaveMessage = new LeaveMessage { Sender = _myAddr };
            var leave = leaveMessage.ToRapidClusterRequest();

            var observers = _membershipView.GetObserversOf(_myAddr);
            _log.LeavingWithObservers(new MembershipServiceLogger.LoggableEndpoint(_myAddr), observers.Length, new MembershipServiceLogger.LoggableEndpoints(observers));

            var leaveTasks = observers.Select(endpoint =>
                _messagingClient.SendMessageBestEffortAsync(endpoint, leave, cancellationToken));

            await Task.WhenAll(leaveTasks).WaitAsync(_options.LeaveMessageTimeout, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
        }
        catch (OperationCanceledException)
        {
            // Cancellation requested - continue with shutdown
        }
        catch (TimeoutException)
        {
            _log.TimeoutWhileLeaving();
        }
        catch (NodeNotInRingException)
        {
            // We may already have been removed, continue with shutdown
            _log.NodeAlreadyRemoved();
        }

        // Cancel background tasks
        _stoppingCts.SafeCancel(_log.Logger);

        // Wait for background tasks to complete
        Task[] backgroundTasks;
        lock (_backgroundTasksLock)
        {
            backgroundTasks = [.. _backgroundTasks];
        }

        if (backgroundTasks.Length > 0)
        {
            _log.WaitingForBackgroundTasks(backgroundTasks.Length);

            try
            {
                await Task.WhenAll(backgroundTasks).WaitAsync(cancellationToken).ConfigureAwait(true);
            }
            catch (OperationCanceledException)
            {
                // Expected during forced shutdown
            }
        }
    }

    /// <summary>
    /// Asynchronously disposes the membership service.
    /// Disposes event channels, failure detectors, and consensus instance.
    /// </summary>
    /// <remarks>
    /// For graceful shutdown: call <c>StopAsync()</c> before this method.
    /// For hard crash (no notification): call this method directly.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return; // Already disposed
        }

        _log.Dispose();

        // Cancel background tasks (in case StopAsync wasn't called)
        _stoppingCts.SafeCancel(_log.Logger);
        _stoppingCts.Dispose();

        // Dispose the event channel to signal completion to all subscribers
        _eventChannel.Dispose();

        // Dispose failure detectors
        foreach (var fd in _failureDetectors)
        {
            fd.Dispose();
        }
        _failureDetectors.Clear();

        // Dispose consensus instance (may be null if initialization failed)
        if (_consensusInstance is not null)
        {
            await _consensusInstance.DisposeAsync();
        }
    }

}
