using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

using Microsoft.Extensions.Options;
using RapidCluster.Discovery;
using RapidCluster.Exceptions;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Membership server class that implements the Rapid protocol.
/// </summary>
internal sealed class MembershipService : IMembershipServiceHandler, IAsyncDisposable
{
    private readonly MembershipServiceLogger _log;
    private readonly CutDetector _cutDetector;
    private readonly Endpoint _myAddr;
    private readonly IBroadcaster _broadcaster;

    /// <summary>
    /// Tracks all pending joiners - nodes that have sent JoinMessages and are waiting to be added.
    /// Key: joiner endpoint, Value: JoinerInfo containing response channel, metadata, and alert-sent flag.
    /// </summary>
    private readonly Dictionary<Endpoint, JoinerInfo> _pendingJoiners = new(EndpointAddressComparer.Instance);

    private readonly IMessagingClient _messagingClient;
    private readonly IConsensusCoordinatorFactory _consensusCoordinatorFactory;
    private MembershipView _membershipView = null!;

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
    private readonly SortedSet<Endpoint> _deferredProposals = new(ProtobufEndpointComparer.Instance); // Proposals deferred while consensus is running

    // Unstable mode timeout handling (cut detection)
    private readonly OneShotTimer _unstableModeTimer = new();
    private ConfigurationId _unstableModeTimerConfigId;
    private bool _unstableModeTimerArmed;

    // Initialization state
    private bool _initialized;

    // Configuration for join - stored from RapidClusterOptions
    private List<Endpoint> _seedAddresses;
    private readonly Metadata _nodeMetadata;
    private readonly ISeedProvider _seedProvider;

    // The established ClusterId. Initialized during bootstrap or learned during join.
    private ClusterId _clusterId = ClusterId.None;

    // Bootstrap configuration
    private readonly int _bootstrapExpect;

    // Tracks whether this node was at the first position in the seed list
    // (before filtering self). Used to determine the bootstrap coordinator.
    private bool _wasFirstSeed;

    // Buffer for consensus messages from future configurations
    // Key: configurationId, Value: list of messages waiting for that config
    private readonly Dictionary<ConfigurationId, List<RapidClusterRequest>> _pendingConsensusMessages = [];

    // View change accessor for publishing updates
    private readonly MembershipViewAccessor _viewAccessor;

    // Metrics
    private readonly RapidClusterMetrics _metrics;

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
        /// <summary>Join failed but should be retried (transient error like timeout, network issue).</summary>
        RetryNeeded,
        /// <summary>Configuration changed during join - retry immediately without consuming an attempt.</summary>
        ConfigChanged,
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
        public static JoinAttemptResult ConfigChanged() => new(JoinAttemptStatus.ConfigChanged, null, null);
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
        MembershipViewAccessor viewAccessor,
        SharedResources sharedResources,
        RapidClusterMetrics metrics,
        ISeedProvider seedProvider,
        ILogger<MembershipService> logger,
        ILogger<CutDetector> cutDetectorLogger)
    {
        var opts = RapidClusterOptions.Value;
        _myAddr = opts.ListenAddress.ToProtobuf();
        _nodeMetadata = opts.Metadata.ToProtobuf();
        _options = protocolOptions.Value;
        _seedProvider = seedProvider;
        _bootstrapExpect = opts.BootstrapExpect;

        // Seed addresses will be fetched asynchronously during InitializeAsync
        // For now, use the static list from options as a fallback
        var configuredSeeds = opts.SeedAddresses ?? [];
        var seen = new HashSet<Endpoint>(EndpointAddressComparer.Instance);
        _seedAddresses = [];
        foreach (var seed in configuredSeeds)
        {
            var pbSeed = seed.ToProtobuf();
            if (!EndpointAddressComparer.Instance.Equals(pbSeed, _myAddr) && seen.Add(pbSeed))
            {
                _seedAddresses.Add(pbSeed);
            }
        }

        _membershipView = MembershipView.Empty;

        // Create cut detector with configured thresholds - it will compute effective values via UpdateView()
        _cutDetector = new CutDetector(
            _options.ObserversPerSubject,
            _options.HighWatermark,
            _options.LowWatermark,
            cutDetectorLogger);

        _sharedResources = sharedResources;
        _messagingClient = messagingClient;
        _broadcaster = broadcasterFactory.Create();
        _fdFactory = edgeFailureDetector;
        _consensusCoordinatorFactory = consensusCoordinatorFactory;
        _viewAccessor = viewAccessor;
        _metrics = metrics;
        _log = new MembershipServiceLogger(logger);
        _sendQueue = Channel.CreateUnbounded<AlertMessage>();

        // Create linked CTS so background tasks stop on either StopAsync or SharedResources shutdown
        _stoppingCts = CancellationTokenSource.CreateLinkedTokenSource(sharedResources.ShuttingDownToken);

        // Configure the failure detector factory to detect stale views (learner role - missed consensus decisions)
        if (edgeFailureDetector is PingPongFailureDetectorFactory pingPongFactory)
        {
            pingPongFactory.OnStaleViewDetected = OnStaleViewDetected;
            pingPongFactory.ViewAccessor = viewAccessor;
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

        // Fetch seeds from the provider
        await RefreshSeedsAsync(cancellationToken).ConfigureAwait(true);

        if (_seedAddresses.Count == 0)
        {
            // No valid seed addresses (empty list or all were self) - start a new cluster
            StartNewCluster();
        }
        else if (ShouldBeBootstrapCoordinator())
        {
            // This node has the lowest address among all seeds (including self)
            // and BootstrapExpect is set, so this node becomes the bootstrap coordinator.
            // It starts as a single-node cluster and other nodes will join it.
            _log.StartingAsBootstrapCoordinator(_myAddr, _bootstrapExpect);
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
    /// Determines if this node should be the bootstrap coordinator.
    /// When BootstrapExpect > 0, the node that was first in the seed list becomes the coordinator.
    /// This ensures deterministic leader election during bootstrap - the first seed always wins.
    /// </summary>
    private bool ShouldBeBootstrapCoordinator()
    {
        // Only applies when BootstrapExpect is set
        if (_bootstrapExpect <= 0)
        {
            return false;
        }

        // The node that was first in the seed list becomes the coordinator
        return _wasFirstSeed;
    }

    /// <summary>
    /// Refreshes the seed addresses from the seed provider.
    /// Filters out self and duplicates while preserving order.
    /// Also tracks whether self was the first seed (for bootstrap coordinator election).
    /// </summary>
    private async Task RefreshSeedsAsync(CancellationToken cancellationToken)
    {
        var seeds = await _seedProvider.GetSeedsAsync(cancellationToken).ConfigureAwait(true);

        var seen = new HashSet<Endpoint>(EndpointAddressComparer.Instance);
        _seedAddresses = [];
        _wasFirstSeed = false;
        var isFirstSeed = true;
        Endpoint? firstSeed = null;

        foreach (var seed in seeds)
        {
            var pbSeed = seed.ToProtobuf();

            // Check if this is the first seed and if it matches self
            if (isFirstSeed)
            {
                isFirstSeed = false;
                firstSeed = pbSeed;

                // Compare by port only when bootstrapExpect > 0 and ports match
                // This handles dynamic environments (like Aspire) where the hostname
                // might differ between discovery (container IP) and local address (hostname)
                if (_bootstrapExpect > 0 && pbSeed.Port == _myAddr.Port)
                {
                    _wasFirstSeed = true;
                }
                else if (EndpointAddressComparer.Instance.Equals(pbSeed, _myAddr))
                {
                    _wasFirstSeed = true;
                }
            }

            if (!EndpointAddressComparer.Instance.Equals(pbSeed, _myAddr) && seen.Add(pbSeed))
            {
                _seedAddresses.Add(pbSeed);
            }
        }

        _log.SeedsRefreshed(_seedAddresses.Count);
        _log.BootstrapCoordinatorCheck(_myAddr, firstSeed, _wasFirstSeed, _bootstrapExpect);
    }

    private void StartNewCluster()
    {
        _log.StartingNewCluster(_myAddr);

        // Generate deterministic ClusterId from the seed list (which for bootstrap is just self)
        // In a multi-seed bootstrap, all seeds should arrive at the same ClusterId.
        var sortedSeeds = _seedAddresses.Concat([_myAddr])
            .OrderBy(e => e.Hostname.ToStringUtf8())
            .ThenBy(e => e.Port)
            .ToList();

        _clusterId = new ClusterId(ComputeClusterHash(sortedSeeds));

        // Create endpoint with node ID = 1 for the seed node
        var seedEndpoint = new Endpoint
        {
            Hostname = _myAddr.Hostname,
            Port = _myAddr.Port,
            NodeId = 1
        };

        // Create MemberInfo with the seed endpoint and its metadata
        var seedMemberInfo = new MemberInfo(seedEndpoint, _nodeMetadata);

        var configId = new ConfigurationId(_clusterId, 0);
        _membershipView = new MembershipViewBuilder(_options.ObserversPerSubject, [seedMemberInfo], maxNodeId: 1)
            .BuildWithConfigurationId(configId);

        FinalizeInitialization();
    }

    private static long ComputeClusterHash(List<Endpoint> seeds)
    {
        var hasher = new System.IO.Hashing.XxHash64();
        foreach (var seed in seeds)
        {
            hasher.Append(seed.Hostname.Span);
            hasher.Append(BitConverter.GetBytes(seed.Port));
        }
        return (long)hasher.GetCurrentHashAsUInt64();
    }

    /// <summary>
    /// Joins an existing cluster through the configured seed nodes.
    /// Cycles through seeds in round-robin fashion until join succeeds or max retries exhausted.
    /// </summary>
    private async Task JoinClusterAsync(CancellationToken cancellationToken)
    {
        _log.JoiningClusterWithSeeds(_seedAddresses.Count, _myAddr);

        var joinStopwatch = Stopwatch.StartNew();
        var maxRetries = _options.MaxJoinRetries;
        var retryDelay = _options.JoinRetryBaseDelay;
        JoinResponse? successfulResponse = null;
        string? lastFailureReason = null;
        var seedIndex = 0;
        var attempt = 0;

        do
        {
            // Check if we've already joined via the learner protocol (stale view detection).
            // This can happen when failure detection probes reveal we're behind and we learn
            // the current membership view from another node. If our address is already in the
            // membership, we're effectively joined and can skip the join protocol.
            if (_membershipView.IsHostPresent(_myAddr))
            {
                _log.JoinCompletedViaLearnerProtocol(_myAddr, _membershipView);
                _metrics.RecordJoinLatency(MetricNames.Results.Success, joinStopwatch);
                return;
            }

            // Select seed in round-robin fashion
            var currentSeed = _seedAddresses[seedIndex % _seedAddresses.Count];
            seedIndex++;

            _log.JoinAttemptWithSeed(attempt + 1, currentSeed);

            var result = await TryJoinClusterAsync(currentSeed, cancellationToken).ConfigureAwait(true);

            switch (result.Status)
            {
                case JoinAttemptStatus.Success:
                    successfulResponse = result.Response;
                    break;

                case JoinAttemptStatus.ConfigChanged:
                    // Configuration changed during join - retry immediately without consuming an attempt.
                    // This can happen indefinitely as the cluster membership changes.
                    _log.JoinRetryingAfterConfigChange(attempt + 1);
                    continue;

                case JoinAttemptStatus.RetryNeeded:
                    lastFailureReason = result.FailureReason;
                    attempt++;
                    if (attempt <= maxRetries)
                    {
                        _log.JoinRetry(attempt, result.FailureReason!, retryDelay.TotalMilliseconds);
                        await Task.Delay(retryDelay, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
                        retryDelay = TimeSpan.FromTicks((long)(retryDelay.Ticks * _options.JoinRetryBackoffMultiplier));
                        if (retryDelay > _options.JoinRetryMaxDelay)
                        {
                            retryDelay = _options.JoinRetryMaxDelay;
                        }
                        continue;
                    }
                    break;

                case JoinAttemptStatus.Failed:
                    // Permanent failure from this seed - continue to next seed (treat as retry)
                    lastFailureReason = result.FailureReason;
                    attempt++;
                    if (attempt <= maxRetries)
                    {
                        _log.JoinRetry(attempt, result.FailureReason!, retryDelay.TotalMilliseconds);
                        await Task.Delay(retryDelay, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
                        retryDelay = TimeSpan.FromTicks((long)(retryDelay.Ticks * _options.JoinRetryBackoffMultiplier));
                        if (retryDelay > _options.JoinRetryMaxDelay)
                        {
                            retryDelay = _options.JoinRetryMaxDelay;
                        }
                        continue;
                    }
                    break;
            }

            // Either succeeded or exhausted retries
            break;
        }
        while (true);

        if (successfulResponse == null)
        {
            _log.JoinFailed(attempt);
            _metrics.RecordJoinLatency(MetricNames.Results.Failed, joinStopwatch);
            throw new JoinException(lastFailureReason ?? $"Failed to join cluster after {attempt} attempts");
        }

        // Initialize membership from response - build MemberInfo with metadata
        var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
        for (var i = 0; i < successfulResponse.MetadataKeys.Count && i < successfulResponse.MetadataValues.Count; i++)
        {
            var endpoint = successfulResponse.MetadataKeys[i];
            var metadata = successfulResponse.MetadataValues[i];
            metadataMap[endpoint] = metadata;
        }

        // Build MemberInfo list from endpoints + metadata
        var memberInfos = successfulResponse.Endpoints
            .Select(e => new MemberInfo(e, metadataMap.GetValueOrDefault(e) ?? new Metadata()))
            .ToList();

        // Pass maxNodeId to ensure it's preserved even if the node with that ID was removed
        _membershipView = new MembershipViewBuilder(
            _options.ObserversPerSubject,
            memberInfos,
            successfulResponse.MaxNodeId).BuildWithConfigurationId(ConfigurationId.FromProtobuf(successfulResponse.ConfigurationId));

        FinalizeInitialization();
        _metrics.RecordJoinLatency(MetricNames.Results.Success, joinStopwatch);
    }

    /// <summary>
    /// Attempts a single join operation through the specified seed. Generates a new NodeId and handles UUID collisions internally.
    /// Returns a result indicating success, retry needed, or permanent failure.
    /// </summary>
    /// <param name="seedAddress">The seed node to contact for this join attempt.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    private async Task<JoinAttemptResult> TryJoinClusterAsync(Endpoint seedAddress, CancellationToken cancellationToken)
    {
        // Phase 1: Contact seed for observers
        var preJoinMessage = new PreJoinMessage
        {
            Sender = _myAddr
        };

        RapidClusterResponse preJoinResponse;
        try
        {
            preJoinResponse = await _messagingClient.SendMessageAsync(
                seedAddress,
                preJoinMessage.ToRapidClusterRequest(),
                cancellationToken).ConfigureAwait(true);
        }
        catch (TimeoutException)
        {
            return JoinAttemptResult.RetryNeeded($"Timeout contacting seed node {seedAddress.Hostname}:{seedAddress.Port}");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Network errors are typically transient
            return JoinAttemptResult.RetryNeeded($"Network error contacting seed {seedAddress.Hostname}:{seedAddress.Port}: {ex.Message}");
        }

        var joinResponse = preJoinResponse.JoinResponse;

        if (joinResponse.StatusCode != JoinStatusCode.SafeToJoin &&
            joinResponse.StatusCode != JoinStatusCode.HostnameAlreadyInRing)
        {
            // Most status codes indicate permanent failure for this seed
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
            // Check if we got a ConfigChanged response - configuration changed during join, retry immediately
            if (responses.Any(r => r?.JoinResponse?.StatusCode == JoinStatusCode.ConfigChanged))
            {
                return JoinAttemptResult.ConfigChanged();
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
            // SetMembershipView handles all the setup including notifying any waiting joiners
            SetMembershipView(_membershipView);
        }

        _log.MembershipServiceInitialized(_myAddr, _membershipView);
    }

    /// <summary>
    /// Centralized method for updating the membership view. All view changes flow through here.
    /// This method handles:
    /// - Updating the membership view
    /// - Updating the cut detector
    /// - Updating the broadcaster
    /// - Disposing old and creating new failure detectors  
    /// - Creating new consensus instance
    /// - Publishing the view to the accessor
    /// - Publishing VIEW_CHANGE event
    /// - Notifying waiting joiners for any nodes that were added
    /// </summary>
    /// <param name="newView">The new membership view to apply (metadata is embedded in MemberInfo).</param>
    /// <returns>The previous consensus instance that should be disposed by the caller.</returns>
    private ConsensusCoordinator? SetMembershipView(MembershipView newView)
    {
        // Must be called under _membershipUpdateLock
        // Always capture the previous consensus instance to ensure it gets disposed.
        // This handles the case where SetMembershipView is called multiple times
        // during initialization (e.g., via ApplyLearnedMembershipView during join).
        var previousConsensusInstance = _consensusInstance;

        // Capture old members BEFORE updating _membershipView so we can compute added nodes
        var oldMembers = _membershipView?.Members is { } members
            ? new HashSet<Endpoint>(members, EndpointAddressComparer.Instance)
            : [];

        // Update the view (metadata is embedded in MemberInfo within the view)
        _membershipView = newView;

        // Update cut detector for the new view.
        _cutDetector.UpdateView(_membershipView);

        // Reset unstable-mode timeout state (per-view)
        _unstableModeTimer.Dispose();
        _unstableModeTimerArmed = false;
        _unstableModeTimerConfigId = _membershipView.ConfigurationId;

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

        // Always notify waiting joiners for any nodes that were added.
        // This is computed internally to ensure joiners are never missed.
        NotifyWaitingJoiners(oldMembers);

        // Publish the new view to the accessor
        _viewAccessor.PublishView(_membershipView);

        // Record metrics for the view change
        _metrics.RecordMembershipViewChange();

        _log.PublishingViewChange(_membershipView);

        // Clear pending joiner data that's no longer needed
        _pendingConsensusMessages.Keys
            .Where(k => k < _membershipView.ConfigurationId)
            .ToList()
            .ForEach(k => _pendingConsensusMessages.Remove(k));

        return previousConsensusInstance;
    }

    /// <summary>
    /// Notifies joiners waiting for their join to complete.
    /// Computes added nodes by comparing old members with current membership.
    /// Also notifies joiners who weren't added (with ConfigChanged) so they can retry.
    /// </summary>
    /// <param name="oldMembers">The set of members before the view change.</param>
    private void NotifyWaitingJoiners(HashSet<Endpoint> oldMembers)
    {
        // Find nodes that were added (in new view but not in old)
        foreach (var node in _membershipView.Members)
        {
            if (oldMembers.Contains(node))
            {
                continue;
            }

            // Remove pending joiner state - the node has now successfully joined
            if (_pendingJoiners.Remove(node, out var joinerInfo))
            {
                // Prevent new attempts from writing to the channel
                joinerInfo.ResponseChannel.Writer.TryComplete();

                var waitingCount = 0;
                var config = _membershipView.Configuration;
                var response = new JoinResponse
                {
                    Sender = _myAddr,
                    StatusCode = JoinStatusCode.SafeToJoin,
                    ConfigurationId = _membershipView.ConfigurationId.ToProtobuf(),
                    MaxNodeId = _membershipView.MaxNodeId

                };
                response.Endpoints.AddRange(config.Endpoints);
                var allMetadata = _membershipView.GetAllMetadata();
                response.MetadataKeys.AddRange(allMetadata.Keys);
                response.MetadataValues.AddRange(allMetadata.Values);

                var rapidClusterResponse = response.ToRapidClusterResponse();

                // Send response to all waiting tasks
                while (joinerInfo.ResponseChannel.Reader.TryRead(out var tcs))
                {
                    waitingCount++;
                    tcs.TrySetResult(rapidClusterResponse);
                }

                _log.NotifyingJoiners(waitingCount, node);
            }
        }

        // Any remaining pending joiners were not added in this view change.
        // They need to be notified with ConfigChanged so they can retry with the new config.
        // This prevents them from hanging until timeout.
        if (_pendingJoiners.Count > 0)
        {
            var configChangedResponse = new JoinResponse
            {
                Sender = _myAddr,
                StatusCode = JoinStatusCode.ConfigChanged,
                ConfigurationId = _membershipView.ConfigurationId.ToProtobuf()
            }.ToRapidClusterResponse();

            foreach (var joinerInfo in _pendingJoiners.Values)
            {
                joinerInfo.ResponseChannel.Writer.TryComplete();

                while (joinerInfo.ResponseChannel.Reader.TryRead(out var tcs))
                {
                    tcs.TrySetResult(configChangedResponse);
                }
            }

            _log.NotifyingStaleJoiners(_pendingJoiners.Count);
            _pendingJoiners.Clear();
        }
    }

    /// <summary>
    /// Cancels all pending join requests by sending them a ConfigChanged response.
    /// Called during rejoin when the node was kicked and is adopting a new view.
    /// </summary>
    private void CancelAllPendingJoiners()
    {
        // Build a ConfigChanged response to tell joiners to retry
        var response = new JoinResponse
        {
            Sender = _myAddr,
            StatusCode = JoinStatusCode.ConfigChanged,
            ConfigurationId = _membershipView.ConfigurationId.ToProtobuf()
        };

        var configChangedResponse = response.ToRapidClusterResponse();

        foreach (var joinerInfo in _pendingJoiners.Values)
        {
            joinerInfo.ResponseChannel.Writer.TryComplete();

            while (joinerInfo.ResponseChannel.Reader.TryRead(out var tcs))
            {
                tcs.TrySetResult(configChangedResponse);
            }
        }
        _pendingJoiners.Clear();
    }

    /// <summary>
    /// Entry point for all messages.
    /// </summary>
    public async Task<RapidClusterResponse> HandleMessageAsync(RapidClusterRequest msg, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(msg);

        // Record incoming message metrics
        var messageType = GetMessageType(msg.ContentCase);
        _metrics.RecordMessageReceived(messageType);

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

        static string GetMessageType(RapidClusterRequest.ContentOneofCase contentCase) => contentCase switch
        {
            RapidClusterRequest.ContentOneofCase.PreJoinMessage => MetricNames.MessageTypes.PreJoinMessage,
            RapidClusterRequest.ContentOneofCase.JoinMessage => MetricNames.MessageTypes.JoinRequest,
            RapidClusterRequest.ContentOneofCase.BatchedAlertMessage => MetricNames.MessageTypes.BatchedAlert,
            RapidClusterRequest.ContentOneofCase.ProbeMessage => MetricNames.MessageTypes.ProbeRequest,
            RapidClusterRequest.ContentOneofCase.Phase1AMessage => MetricNames.MessageTypes.Phase1a,
            RapidClusterRequest.ContentOneofCase.Phase1BMessage => MetricNames.MessageTypes.Phase1b,
            RapidClusterRequest.ContentOneofCase.Phase2AMessage => MetricNames.MessageTypes.Phase2a,
            RapidClusterRequest.ContentOneofCase.Phase2BMessage => MetricNames.MessageTypes.Phase2b,
            RapidClusterRequest.ContentOneofCase.LeaveMessage => MetricNames.MessageTypes.LeaveMessage,
            RapidClusterRequest.ContentOneofCase.MembershipViewRequest => MetricNames.MessageTypes.MembershipViewRequest,
            _ => "unknown"
        };
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
            var statusCode = _membershipView.IsSafeToJoin(joiningEndpoint);
            var builder = new JoinResponse
            {
                Sender = _myAddr,
                ConfigurationId = _membershipView.ConfigurationId.ToProtobuf(),
                StatusCode = statusCode
            };

            _log.JoinAtSeed(_myAddr, msg.Sender, _membershipView);

            var observersCount = 0;
            if (statusCode == JoinStatusCode.SafeToJoin || statusCode == JoinStatusCode.HostnameAlreadyInRing)
            {
                var observers = _membershipView.GetExpectedObserversOf(joiningEndpoint);
                builder.Endpoints.AddRange(observers);
                observersCount = observers.Length;
            }

            _log.HandlePreJoinResult(joiningEndpoint, statusCode, observersCount);

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
        var incomingConfigId = joinMessage.ConfigurationId.ToConfigurationId();
        _log.HandleJoinMessage(joinMessage.Sender, incomingConfigId);
        _metrics.RecordJoinRequest(MetricNames.Results.Success);

        Task<RapidClusterResponse> resultTask;
        lock (_membershipUpdateLock)
        {
            var currentConfiguration = _membershipView.ConfigurationId;

            // If the joiner is already in the ring, respond immediately with SafeToJoin
            // This handles the case where:
            // 1. Joiner's first attempt triggered consensus and they were added
            // 2. But the joiner timed out waiting for the response
            // 3. Joiner retries with the new config (or old config if they haven't updated)
            if (_membershipView.IsHostPresent(joinMessage.Sender))
            {
                _log.JoinerAlreadyInRing();
                var configuration = _membershipView.Configuration;
                var responseBuilder = new JoinResponse
                {
                    Sender = _myAddr,
                    StatusCode = JoinStatusCode.SafeToJoin,
                    ConfigurationId = _membershipView.ConfigurationId.ToProtobuf(),
                    MaxNodeId = _membershipView.MaxNodeId
                };
                responseBuilder.Endpoints.AddRange(configuration.Endpoints);
                var allMetadata = _membershipView.GetAllMetadata();
                responseBuilder.MetadataKeys.AddRange(allMetadata.Keys);
                responseBuilder.MetadataValues.AddRange(allMetadata.Values);

                return responseBuilder.ToRapidClusterResponse();
            }

            if (currentConfiguration == incomingConfigId)
            {
                _log.EnqueueingSafeToJoin(joinMessage.Sender, _membershipView);

                // Get or create JoinerInfo for this endpoint
                ref var joinerInfo = ref CollectionsMarshal.GetValueRefOrAddDefault(_pendingJoiners, joinMessage.Sender, out var existed);
                joinerInfo ??= new JoinerInfo { Metadata = joinMessage.Metadata };

                // Enqueue TCS for this request
                var tcs = new TaskCompletionSource<RapidClusterResponse>();
                joinerInfo.ResponseChannel.Writer.TryWrite(tcs);

                // Only send alert if this is a new joiner (not already alerted)
                // This prevents duplicate alerts when a joiner retries while consensus is in progress
                if (!joinerInfo.AlertSent)
                {
                    joinerInfo.AlertSent = true;

                    var alertMsg = new AlertMessage
                    {
                        EdgeSrc = _myAddr,
                        EdgeDst = joinMessage.Sender,
                        EdgeStatus = EdgeStatus.Up,
                        ConfigurationId = currentConfiguration.ToProtobuf(),
                        Metadata = joinMessage.Metadata
                    };
                    alertMsg.RingNumber.AddRange(joinMessage.RingNumber);

                    EnqueueAlertMessage(alertMsg);
                }

                resultTask = tcs.Task;
            }
            else
            {
                // This handles the corner case where the configuration changed between phase 1 and phase 2
                // of the joining node's bootstrap. It should attempt to rejoin the network.
                // Note: The case where the joiner is already in the ring is handled above (before config check).
                _log.WrongConfiguration(joinMessage.Sender, incomingConfigId, _membershipView);

                var responseBuilder = new JoinResponse
                {
                    Sender = _myAddr,
                    StatusCode = JoinStatusCode.ConfigChanged,
                    ConfigurationId = _membershipView.ConfigurationId.ToProtobuf(),
                    MaxNodeId = _membershipView.MaxNodeId
                };

                return responseBuilder.ToRapidClusterResponse();
            }
        }

        return await resultTask.WaitAsync(TimeSpan.FromSeconds(30), _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
    }

    /// <summary>
    /// Creates a MembershipProposal from a list of endpoints to add/remove.
    /// The proposal contains the complete new membership state.
    /// </summary>
    /// <param name="endpointsToChange">Endpoints being added or removed from the cluster.</param>
    /// <returns>A complete MembershipProposal with the new view state.</returns>
    private MembershipProposal CreateMembershipProposal(List<Endpoint> endpointsToChange)
    {
        var proposal = new MembershipProposal
        {
            ConfigurationId = _membershipView.ConfigurationId.Next().ToProtobuf()
        };

        // Track the next node ID to assign (starts from current max + 1)
        var nextNodeId = _membershipView.MaxNodeId;

        // Compute new member set: current members +/- changes
        var newMembers = new List<Endpoint>(_membershipView.Members);
        var joiningEndpoints = new HashSet<Endpoint>(EndpointAddressComparer.Instance);

        foreach (var endpoint in endpointsToChange)
        {
            if (_membershipView.IsHostPresent(endpoint))
            {
                // Use EndpointAddressComparer to find by hostname:port, ignoring NodeId
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

        // Build member list for the proposal - endpoints already contain their node_id
        foreach (var endpoint in newMembers)
        {
            Endpoint endpointWithNodeId;

            // Check if this is an existing member (already has node ID in the endpoint)
            if (_membershipView.IsHostPresent(endpoint))
            {
                // Existing member - use endpoint as-is, it already has the node_id
                endpointWithNodeId = endpoint;
            }
            else
            {
                // New joiner - assign a new monotonic node ID
                nextNodeId++;
                endpointWithNodeId = new Endpoint
                {
                    Hostname = endpoint.Hostname,
                    Port = endpoint.Port,
                    NodeId = nextNodeId
                };
            }

            proposal.Members.Add(endpointWithNodeId);

            // Add metadata for this member - check view first (for existing members), then pending joiner metadata
            var metadata = _membershipView.GetMetadata(endpoint)
                ?? (_pendingJoiners.TryGetValue(endpoint, out var joinerInfo) ? joinerInfo.Metadata : new Metadata());
            proposal.MemberMetadata.Add(metadata);
        }

        // Set the max node ID (highest ever assigned, only increases)
        proposal.MaxNodeId = nextNodeId;

        return proposal;
    }

    /// <summary>
    /// Starts a consensus round for the given proposals.
    /// Must NOT be called while holding _membershipUpdateLock.
    /// </summary>
    /// <param name="proposals">The list of endpoints to propose for addition/removal.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    private void StartConsensusForProposals(List<Endpoint> proposals, CancellationToken cancellationToken)
    {
        lock (_membershipUpdateLock)
        {
            // Double-check that consensus is not already running
            // (another thread could have started it between our check and acquiring the lock)
            if (_announcedProposal)
            {
                // Re-defer these proposals
                foreach (var proposal in proposals)
                {
                    _deferredProposals.Add(proposal);
                }
                return;
            }

            _announcedProposal = true;
            DisarmUnstableModeTimeout();

            _log.InitiatingConsensus(proposals);

            // Create full membership proposal with NodeIds for all members
            var membershipProposal = CreateMembershipProposal(proposals);
            _consensusInstance.Propose(membershipProposal, cancellationToken);
        }
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
        _log.HandleBatchedAlertMessage(messageBatch.Messages.Count, messageBatch.Sender);

        lock (_membershipUpdateLock)
        {
            if (!FilterAlertMessages(messageBatch, _membershipView.ConfigurationId))
            {
                _log.BatchedAlertFiltered(_membershipView);
                return new ConsensusResponse().ToRapidClusterResponse();
            }

            // Use SortedSet for deduplication and consistent ordering across all nodes.
            // This ensures all nodes propose the same set in the same order.
            var proposals = new SortedSet<Endpoint>(ProtobufEndpointComparer.Instance);

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
                var currentConfigurationId = _membershipView.ConfigurationId;
                foreach (var msg in messageBatch.Messages)
                {
                    if (!msg.RingNumber.Contains(ringNumber))
                    {
                        continue;
                    }

                    // Filter out stale messages from old configurations.
                    // The batch-level filter only checks if ANY message matches, but we must
                    // also filter individual messages to prevent stale alerts from affecting
                    // the cut detector (e.g., old JOIN alerts causing spurious unstable mode).
                    var msgConfigId = msg.ConfigurationId.ToConfigurationId();
                    if (msgConfigId != currentConfigurationId)
                    {
                        _log.AlertFilteredByConfigurationId(msgConfigId, currentConfigurationId);
                        continue;
                    }

                    _log.ProcessingAlert(msg.EdgeSrc, msg.EdgeDst, msg.EdgeStatus);

                    // Record metrics for cut detector report received
                    var reportType = msg.EdgeStatus == EdgeStatus.Up
                        ? MetricNames.ReportTypes.Join
                        : MetricNames.ReportTypes.Fail;
                    _metrics.RecordCutDetectorReportReceived(reportType);

                    // For valid UP alerts, extract the joiner details (metadata) which is going to be needed
                    // when the node is added to the rings
                    var extractedMessage = ExtractJoinerMetadata(msg);
                    var cutProposals = _cutDetector.AggregateForProposalSingleRing(extractedMessage, ringNumber);
                    _log.CutDetectionProposals(cutProposals.Count);

                    // Record cut detected if there are proposals
                    if (cutProposals.Count > 0)
                    {
                        _metrics.RecordCutDetected();
                    }

                    foreach (var proposal in cutProposals)
                    {
                        proposals.Add(proposal);
                    }
                }
            }

            // Lastly, we apply implicit detections
            var implicitProposals = _cutDetector.InvalidateFailingEdges();
            _log.ImplicitEdgeInvalidation(implicitProposals.Count);

            // If there are nodes in unstable mode, arm a timeout so they don't block indefinitely.
            ArmUnstableModeTimeoutIfNeeded();

            // Record cut detected for implicit proposals
            if (implicitProposals.Count > 0)
            {
                _metrics.RecordCutDetected();
            }

            foreach (var proposal in implicitProposals)
            {
                proposals.Add(proposal);
            }

            // If we have a proposal for this stage, start an instance of consensus on it.
            lock (_membershipUpdateLock)
            {
                if (proposals.Count > 0)
                {
                    if (!_announcedProposal)
                    {
                        // Include any deferred proposals from previous rounds
                        foreach (var deferred in _deferredProposals)
                        {
                            proposals.Add(deferred);
                        }
                        _deferredProposals.Clear();

                        _announcedProposal = true;
                        DisarmUnstableModeTimeout();

                        var currentConfigurationId = _membershipView.ConfigurationId;
                        var proposalList = proposals.ToList();
                        _log.InitiatingConsensus(proposalList);

                        // Create full membership proposal with NodeIds for all members
                        var membershipProposal = CreateMembershipProposal(proposalList);
                        _consensusInstance.Propose(membershipProposal, cancellationToken);
                    }
                    else
                    {
                        // Consensus is already running - defer these proposals for the next round
                        foreach (var proposal in proposals)
                        {
                            _deferredProposals.Add(proposal);
                        }
                    }
                }
            }

            return new ConsensusResponse().ToRapidClusterResponse();
        }
    }

    /// <summary>
    /// Receives proposal for the one-step consensus (essentially phase 2 of Fast Paxos).
    /// </summary>
    private RapidClusterResponse HandleConsensusMessages(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        _log.HandleConsensusMessages();

        // Extract configuration ID from the message
        var messageConfigId = GetConfigurationIdFromConsensusMessage(request);

        lock (_membershipUpdateLock)
        {
            var currentConfigId = _membershipView.ConfigurationId;

            // Reject messages with mismatched ClusterIds (prevents cross-cluster crosstalk)
            if (messageConfigId.ClusterId != currentConfigId.ClusterId)
            {
                _log.ClusterIdMismatch(messageConfigId.ClusterId, currentConfigId.ClusterId);
                return new ConsensusResponse().ToRapidClusterResponse();
            }

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
    private static ConfigurationId GetConfigurationIdFromConsensusMessage(RapidClusterRequest request)
    {
        return request.ContentCase switch
        {
            RapidClusterRequest.ContentOneofCase.Phase1AMessage => ConfigurationId.FromProtobuf(request.Phase1AMessage.ConfigurationId),
            RapidClusterRequest.ContentOneofCase.Phase1BMessage => ConfigurationId.FromProtobuf(request.Phase1BMessage.ConfigurationId),
            RapidClusterRequest.ContentOneofCase.Phase2AMessage => ConfigurationId.FromProtobuf(request.Phase2AMessage.ConfigurationId),
            RapidClusterRequest.ContentOneofCase.Phase2BMessage => ConfigurationId.FromProtobuf(request.Phase2BMessage.ConfigurationId),
            _ => throw new ArgumentException($"Unexpected consensus message type: {request.ContentCase}")
        };
    }

    /// <summary>
    /// Propagates the intent of a node to leave the group
    /// </summary>
    private RapidClusterResponse HandleLeaveMessage(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        var leaveMessage = request.LeaveMessage;
        _log.ReceivedLeaveMessage(leaveMessage.Sender, _myAddr);
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
        var senderConfigId = probeMessage.ConfigurationId.ToConfigurationId();
        var localConfigId = _membershipView.ConfigurationId;
        // Only compare if ClusterIds match - a node with empty ClusterId hasn't joined yet
        if (senderConfigId.ClusterId == localConfigId.ClusterId
            && senderConfigId > localConfigId
            && probeMessage.Sender != null)
        {
            OnStaleViewDetected(probeMessage.Sender, senderConfigId, localConfigId);
        }

        return new ProbeResponse
        {
            ConfigurationId = _membershipView.ConfigurationId.ToProtobuf(),
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
            request.Sender,
            request.CurrentConfigurationId.ToConfigurationId(),
            _membershipView);

        // Build response with current membership view
        var response = new MembershipViewResponse
        {
            Sender = _myAddr,
            ConfigurationId = _membershipView.ConfigurationId.ToProtobuf(),
            MaxNodeId = _membershipView.MaxNodeId
        };


        // Add all endpoints (which already contain node_id)
        var members = _membershipView.Members;
        response.Endpoints.AddRange(members);

        // Add metadata for all nodes
        foreach (var endpoint in members)
        {
            var metadata = _membershipView.GetMetadata(endpoint) ?? new Metadata();
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
        List<Endpoint>? deferredProposalsToProcess = null;

        lock (_membershipUpdateLock)
        {
            // Check if this decision is for the current configuration.
            if (decidingConfigurationId != _membershipView.ConfigurationId)
            {
                _log.IgnoringStaleConsensusDecision(proposal.Members.Count);
                return;
            }


            _announcedProposal = false;

            // Create a builder from the current view to make modifications
            // Use the protocol's ObserversPerSubject as the max ring count so the cluster
            // can grow beyond its current ring count when new nodes join
            var builder = _membershipView.ToBuilder(_options.ObserversPerSubject);

            // Build a lookup from the proposal for fast access to endpoints (which contain NodeId)
            var proposalMemberLookup = proposal.Members.ToDictionary(
                m => m,
                m => m,
                EndpointAddressComparer.Instance);
            var proposalMetadataLookup = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
            for (var i = 0; i < proposal.Members.Count; i++)
            {
                if (i < proposal.MemberMetadata.Count)
                {
                    proposalMetadataLookup[proposal.Members[i]] = proposal.MemberMetadata[i];
                }
            }

            // Update the builder's max node ID from the proposal
            builder.SetMaxNodeId(proposal.MaxNodeId);

            // Determine which nodes are being added and which are being removed
            var currentMembers = new HashSet<Endpoint>(_membershipView.Members, EndpointAddressComparer.Instance);
            var proposedMembers = new HashSet<Endpoint>(proposal.Members, EndpointAddressComparer.Instance);

            // Track counts for metrics
            var removedCount = 0;
            var addedCount = 0;

            // Nodes to remove: in current but not in proposed
            foreach (var node in currentMembers.Except(proposedMembers, EndpointAddressComparer.Instance))
            {
                _log.RemovingNode(node);
                builder.RingDelete(node);
                removedCount++;
            }

            // Record metrics for removed nodes
            if (removedCount > 0)
            {
                _metrics.RecordNodesRemoved(removedCount, MetricNames.RemovalReasons.FailureDetected);
            }

            // Nodes to add: in proposed but not in current
            foreach (var node in proposedMembers.Except(currentMembers, EndpointAddressComparer.Instance))
            {
                if (!proposalMemberLookup.TryGetValue(node, out var endpointWithNodeId))
                {
                    // This should never happen - the proposal should always have the endpoint
                    _log.DecidedNodeWithoutUuid(node);
                    continue;
                }

                // Get metadata from proposal first, fall back to pending joiner metadata
                var metadata = proposalMetadataLookup.GetValueOrDefault(node)
                    ?? (_pendingJoiners.TryGetValue(node, out var joinerInfo) ? joinerInfo.Metadata : new Metadata());

                _log.AddingNode(endpointWithNodeId);
                builder.RingAdd(endpointWithNodeId, metadata);

                // Note: _pendingJoiners cleanup happens in NotifyWaitingJoiners called by SetMembershipView

                // Clear from deferred proposals - this node successfully joined
                _deferredProposals.Remove(node);

                addedCount++;
            }

            // Record metrics for added nodes
            if (addedCount > 0)
            {
                _metrics.RecordNodesAdded(addedCount);
            }

            // Build the new immutable view with incremented version
            var newView = builder.Build(_membershipView.ConfigurationId);

            _log.DecideViewChangeCleanup();

            // Use SetMembershipView to apply all changes (including notifying waiting joiners)
            previousConsensusInstance = SetMembershipView(newView);

            // Check if we have deferred proposals to process
            // These are proposals that arrived while consensus was running
            if (_deferredProposals.Count > 0)
            {
                deferredProposalsToProcess = [.. _deferredProposals];
                _deferredProposals.Clear();
            }
        }

        if (previousConsensusInstance != null)
        {
            await previousConsensusInstance.DisposeAsync();
        }

        // Process deferred proposals outside the lock
        // This starts a new consensus round for proposals that arrived during the previous round
        if (deferredProposalsToProcess != null)
        {
            _log.ProcessingDeferredProposals(deferredProposalsToProcess.Count);
            StartConsensusForProposals(deferredProposalsToProcess, CancellationToken.None);
        }
    }

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
    public Dictionary<Endpoint, Metadata> GetMetadata() => new(_membershipView.GetAllMetadata(), EndpointAddressComparer.Instance);

    /// <summary>
    /// Queues a AlertMessage to be broadcasted after potentially being batched.
    /// </summary>
    /// <param name="msg">the AlertMessage to be broadcasted</param>
    private void EnqueueAlertMessage(AlertMessage msg)
    {
        _log.EnqueueAlertMessage(msg.EdgeSrc, msg.EdgeDst, msg.EdgeStatus);
        _sendQueue.Writer.TryWrite(msg);
    }

    /// <summary>
    /// Batches outgoing AlertMessages into a single BatchAlertMessage.
    /// </summary>
    /// <remarks>
    /// The batching strategy is:
    /// 1. Wait for the first item to arrive (blocking wait)
    /// 2. Once an item arrives, wait for the batching window to collect more items
    /// 3. Read all available items and broadcast
    /// 
    /// This ensures low latency for the first item while still allowing batching.
    /// </remarks>
    private async Task AlertBatcherAsync()
    {
        var buffer = new List<AlertMessage>();
        var stoppingToken = _stoppingCts.Token;

        while (!stoppingToken.IsCancellationRequested)
        {
            buffer.Clear();
            try
            {
                // Wait for at least one item to be available (no timeout, just wait for work)
                await _sendQueue.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(true);

                // Read the first item that triggered the wait
                while (_sendQueue.Reader.TryRead(out var msg))
                {
                    buffer.Add(msg);
                }

                // If we have items, wait the batching window to allow more to accumulate
                if (buffer.Count > 0)
                {
                    await Task.Delay(_options.BatchingWindow, _sharedResources.TimeProvider, stoppingToken).ConfigureAwait(true);

                    // Read any additional items that arrived during the batching window
                    while (_sendQueue.Reader.TryRead(out var msg))
                    {
                        buffer.Add(msg);
                    }

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
    private bool FilterAlertMessages(BatchedAlertMessage batchedAlertMessage, ConfigurationId currentConfigurationId)
    {
        return batchedAlertMessage.Messages.Any(m =>
        {
            var msgConfig = m.ConfigurationId.ToConfigurationId();

            // Reject messages with mismatched ClusterIds (prevents cross-cluster crosstalk)
            if (msgConfig.ClusterId != currentConfigurationId.ClusterId)
            {
                _log.AlertFilteredByClusterId(msgConfig.ClusterId, currentConfigurationId.ClusterId);
                return false;
            }

            return msgConfig == currentConfigurationId;
        });
    }


    private AlertMessage ExtractJoinerMetadata(AlertMessage alertMessage)
    {
        if (alertMessage.EdgeStatus == EdgeStatus.Up)
        {
            // Update metadata for pending joiner, or create entry if not yet tracked
            // This can happen when alerts arrive from other observers
            ref var joinerInfo = ref CollectionsMarshal.GetValueRefOrAddDefault(_pendingJoiners, alertMessage.EdgeDst, out _);
            joinerInfo ??= new JoinerInfo();
            joinerInfo.Metadata = alertMessage.Metadata;
            _log.ExtractJoinerUuidAndMetadata(alertMessage.EdgeDst);
        }
        return alertMessage;
    }

    /// <summary>
    /// Attempts to rejoin the cluster after being kicked.
    /// Combines configured seed nodes with members from the last known view,
    /// trying configured seeds first, then last known members.
    /// If all attempts fail, refreshes seeds from the provider and retries.
    /// </summary>
    private async Task RejoinClusterAsync(CancellationToken cancellationToken)
    {
        if (_isRejoining || _disposed != 0)
        {
            _log.RejoinSkipped(_myAddr);
            return;
        }
        _isRejoining = true;

        try
        {
            _log.StartingRejoin(_myAddr);

            // Try rejoining with current seeds first
            var result = await TryRejoinWithCurrentSeedsAsync(cancellationToken).ConfigureAwait(true);
            if (result.HasValue)
            {
                return; // Successfully rejoined
            }

            // If all attempts failed, refresh seeds from the provider and retry
            _log.RefreshingSeedsForRejoin(_myAddr);
            await RefreshSeedsAsync(cancellationToken).ConfigureAwait(true);

            // Retry with fresh seeds
            result = await TryRejoinWithCurrentSeedsAsync(cancellationToken).ConfigureAwait(true);
            if (!result.HasValue)
            {
                _log.RejoinFailedNoSeeds(_myAddr);
            }
        }
        finally
        {
            _isRejoining = false;
        }
    }

    /// <summary>
    /// Attempts to rejoin the cluster using the current seed addresses and last known members.
    /// </summary>
    /// <returns>True if rejoin succeeded, false if all attempts failed.</returns>
    private async Task<bool?> TryRejoinWithCurrentSeedsAsync(CancellationToken cancellationToken)
    {
        // Build combined seed list: configured seeds first, then last known members
        // This ensures we try explicitly configured seeds before falling back to
        // potentially stale membership view members
        var candidateSeeds = new List<Endpoint>();
        var seen = new HashSet<Endpoint>(EndpointAddressComparer.Instance);

        // Add configured seeds first (excluding self)
        foreach (var seed in _seedAddresses)
        {
            if (!EndpointAddressComparer.Instance.Equals(seed, _myAddr) && seen.Add(seed))
            {
                candidateSeeds.Add(seed);
            }
        }

        // Then add members from last known view (excluding self and already-added seeds)
        foreach (var member in _membershipView.Members)
        {
            if (!EndpointAddressComparer.Instance.Equals(member, _myAddr) && seen.Add(member))
            {
                candidateSeeds.Add(member);
            }
        }

        if (candidateSeeds.Count == 0)
        {
            return null; // No seeds available
        }

        _log.RejoinWithCandidateSeeds(candidateSeeds.Count, _seedAddresses.Count);

        var metadata = _membershipView.GetMetadata(_myAddr) ?? new Metadata();

        JoinResponse? successfulResponse = null;

        foreach (var seed in candidateSeeds)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return null;
            }

            try
            {
                successfulResponse = await TryRejoinThroughSeedAsync(seed, metadata, cancellationToken).ConfigureAwait(true);
                if (successfulResponse != null)
                {
                    break;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _log.RejoinFailedThroughSeed(_myAddr, seed, ex.Message);
                // Try next seed
            }
        }

        if (successfulResponse == null)
        {
            return null; // All attempts failed
        }

        // Reset internal state with new membership
        ConsensusCoordinator? oldConsensus;
        lock (_membershipUpdateLock)
        {
            // Cancel pending join requests - joiners will retry with ConfigChanged response
            // CancelAllPendingJoiners also clears all pending joiner state
            CancelAllPendingJoiners();
            _pendingConsensusMessages.Clear();

            // Build new membership view from response - build MemberInfo with metadata
            var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
            for (var i = 0; i < successfulResponse.MetadataKeys.Count && i < successfulResponse.MetadataValues.Count; i++)
            {
                metadataMap[successfulResponse.MetadataKeys[i]] = successfulResponse.MetadataValues[i];
            }

            // Build MemberInfo list from endpoints + metadata
            var memberInfos = successfulResponse.Endpoints
                .Select(e => new MemberInfo(e, metadataMap.GetValueOrDefault(e) ?? new Metadata()))
                .ToList();

            // Build the new view - endpoints in the response already contain node_id
            var newView = new MembershipViewBuilder(
                _options.ObserversPerSubject,
                memberInfos,
                successfulResponse.MaxNodeId).BuildWithConfigurationId(ConfigurationId.FromProtobuf(successfulResponse.ConfigurationId));

            // Use SetMembershipView to apply all changes (including notifying waiting joiners)
            oldConsensus = SetMembershipView(newView);
        }

        // Dispose old consensus - track it so we await it during shutdown
        if (oldConsensus != null)
        {
            await oldConsensus.DisposeAsync().ConfigureAwait(true);
        }

        _log.RejoinSuccessful(
            _myAddr,
            successfulResponse.Endpoints.Count,
            successfulResponse.ConfigurationId.ToConfigurationId());

        return true;
    }

    /// <summary>
    /// Attempts to rejoin the cluster through a specific seed node.
    /// </summary>
    private async Task<JoinResponse?> TryRejoinThroughSeedAsync(
        Endpoint seed,
        Metadata metadata,
        CancellationToken cancellationToken)
    {
        _log.AttemptingRejoinThroughSeed(_myAddr, seed);

        // Phase 1: PreJoin to get observers
        var preJoinMessage = new PreJoinMessage
        {
            Sender = _myAddr
        };

        var preJoinResponse = await _messagingClient.SendMessageAsync(
            seed,
            preJoinMessage.ToRapidClusterRequest(),
            cancellationToken).ConfigureAwait(true);

        var joinResponse = preJoinResponse.JoinResponse;
        _log.RejoinPreJoinResponse(
            _myAddr,
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
    /// Called by the failure detector when a probe response indicates this node
    /// has a stale view (remote has higher config ID, but we're still in membership).
    /// This is the Paxos "learner" role - requesting missed consensus decisions.
    /// </summary>
    /// <param name="remoteEndpoint">The endpoint that reported the higher config ID.</param>
    /// <param name="remoteConfigId">The configuration ID from the probe response.</param>
    /// <param name="localConfigId">The local configuration ID when the stale view was detected.</param>
    private void OnStaleViewDetected(Endpoint remoteEndpoint, ConfigurationId remoteConfigId, ConfigurationId localConfigId)
    {
        _log.StaleViewDetected(remoteEndpoint, remoteConfigId, localConfigId);

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
    private async Task RefreshMembershipViewAsync(Endpoint remoteEndpoint, ConfigurationId expectedConfigId, CancellationToken cancellationToken)
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
            _log.RequestingMembershipView(remoteEndpoint);

            var request = new MembershipViewRequest
            {
                Sender = _myAddr,
                CurrentConfigurationId = _membershipView.ConfigurationId.ToProtobuf()
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

            var responseConfigId = viewResponse.ConfigurationId.ToConfigurationId();

            // Only apply if the response is newer than our current view
            if (responseConfigId <= _membershipView.ConfigurationId)
            {
                _log.SkippingStaleViewRefresh(responseConfigId, _membershipView.ConfigurationId);
                return;
            }

            // Apply the learned view
            ApplyLearnedMembershipView(viewResponse);

            // Update last refresh timestamp on success
            Interlocked.Exchange(ref _lastStaleViewRefreshTicks, _sharedResources.TimeProvider.GetTimestamp());

            _log.MembershipViewRefreshed(
                remoteEndpoint,
                responseConfigId,
                viewResponse.Endpoints.Count);

        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.MembershipViewRefreshFailed(remoteEndpoint, ex.Message);
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
            var responseConfigId = viewResponse.ConfigurationId.ToConfigurationId();

            // Guard against config ID regression - never allow a stale view to replace a fresher one
            if (responseConfigId <= _membershipView.ConfigurationId)
            {
                return;
            }

            // Build metadata map from response
            var metadataMap = new Dictionary<Endpoint, Metadata>(EndpointAddressComparer.Instance);
            for (var i = 0; i < viewResponse.MetadataKeys.Count && i < viewResponse.MetadataValues.Count; i++)
            {
                metadataMap[viewResponse.MetadataKeys[i]] = viewResponse.MetadataValues[i];
            }

            // Build MemberInfo list from endpoints + metadata
            var memberInfos = viewResponse.Endpoints
                .Select(e => new MemberInfo(e, metadataMap.GetValueOrDefault(e) ?? new Metadata()))
                .ToList();

            // Build the new view from the response - endpoints already contain node_id
            var newView = new MembershipViewBuilder(
                _options.ObserversPerSubject,
                memberInfos,
                viewResponse.MaxNodeId).BuildWithConfigurationId(responseConfigId);

            // Check if we were kicked (not in the new membership)
            // Use EndpointAddressComparer to ignore NodeId when checking membership
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
                        _myAddr,
                        responseConfigId,
                        _membershipView.ConfigurationId);
                }
                oldConsensus = null;
            }
            else
            {
                // We're still in membership - apply the new view
                // SetMembershipView will compute added nodes internally and notify waiting joiners
                oldConsensus = SetMembershipView(newView);
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
        _log.CreateFailureDetectorsSummary(subjects, configurationId);

        for (var i = 0; i < subjects.Length; i++)
        {
            var subject = subjects[i];
            var ringNumber = i;
            var fd = _fdFactory.CreateInstance(subject, () => EdgeFailureNotification(subject, configurationId));

            _log.CreatedFailureDetector(subject, ringNumber);

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
    private void EdgeFailureNotification(Endpoint subject, ConfigurationId configurationId)
    {
        _log.EdgeFailureNotificationScheduled(subject, configurationId);

        try
        {
            if (configurationId != _membershipView.ConfigurationId)
            {
                _log.IgnoringOldConfigNotification(subject, _membershipView, configurationId);
                return;
            }

            _log.AnnouncingEdgeFail(subject, _myAddr, configurationId, _membershipView);

            var ringNumbers = _membershipView.GetRingNumbers(_myAddr, subject);
            _log.EdgeFailureNotificationEnqueued(subject, ringNumbers);

            // Note: setUuid is deliberately missing here because it does not affect leaves.
            var msg = new AlertMessage
            {
                EdgeSrc = _myAddr,
                EdgeDst = subject,
                EdgeStatus = EdgeStatus.Down,
                ConfigurationId = configurationId.ToProtobuf()
            };
            msg.RingNumber.AddRange(ringNumbers);

            EnqueueAlertMessage(msg);
        }
        catch (Exception)
        {
            // TODO: Add logging for error in edge failure notification
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

            // Consensus decided, so any unstable-mode timeout is obsolete.
            DisarmUnstableModeTimeout();

            await DecideViewChange(await decision, configurationId);
        }, CancellationToken.None, TaskContinuationOptions.None, _sharedResources.TaskScheduler);
        continuationTask.Unwrap().Ignore();
    }

    private void ArmUnstableModeTimeoutIfNeeded()
    {
        if (_disposed != 0)
        {
            return;
        }

        if (_announcedProposal)
        {
            return;
        }

        if (!_cutDetector.HasNodesInUnstableMode())
        {
            DisarmUnstableModeTimeout();
            return;
        }

        if (_unstableModeTimerArmed && _unstableModeTimerConfigId == _membershipView.ConfigurationId)
        {
            return;
        }

        _unstableModeTimerArmed = true;
        _unstableModeTimerConfigId = _membershipView.ConfigurationId;

        _unstableModeTimer.Schedule(
            _sharedResources.TimeProvider,
            _options.UnstableModeTimeout,
            () => OnUnstableModeTimeout(_unstableModeTimerConfigId));
    }

    private void DisarmUnstableModeTimeout()
    {
        if (!_unstableModeTimerArmed)
        {
            return;
        }

        _unstableModeTimer.Dispose();
        _unstableModeTimerArmed = false;
    }

    private void OnUnstableModeTimeout(ConfigurationId expectedConfigId)
    {
        if (_disposed != 0)
        {
            return;
        }

        lock (_membershipUpdateLock)
        {
            if (expectedConfigId != _membershipView.ConfigurationId)
            {
                return;
            }

            if (_announcedProposal)
            {
                return;
            }

            if (!_cutDetector.HasNodesInUnstableMode())
            {
                DisarmUnstableModeTimeout();
                return;
            }

            var forcedProposals = _cutDetector.ForcePromoteUnstableNodes();
            if (forcedProposals.Count == 0)
            {
                DisarmUnstableModeTimeout();
                return;
            }

            _announcedProposal = true;
            DisarmUnstableModeTimeout();

            var membershipProposal = CreateMembershipProposal(forcedProposals);
            _consensusInstance.Propose(membershipProposal, _stoppingCts.Token);
        }
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
    private void ReplayBufferedConsensusMessages(ConfigurationId currentConfigId, CancellationToken cancellationToken)
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

        // Cancel any ongoing consensus immediately to avoid waiting for round timeouts
        _consensusInstance?.Cancel();

        // Stop all failure detectors immediately to prevent probe noise during shutdown
        foreach (var fd in _failureDetectors)
        {
            fd.Dispose();
        }
        _failureDetectors.Clear();

        // Send leave messages to observers
        try
        {
            var leaveMessage = new LeaveMessage { Sender = _myAddr };
            var leave = leaveMessage.ToRapidClusterRequest();

            var observers = _membershipView.GetObserversOf(_myAddr);
            _log.LeavingWithObservers(_myAddr, observers.Length, observers);

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

        // Dispose failure detectors (may already be disposed by StopAsync)
        foreach (var fd in _failureDetectors)
        {
            fd.Dispose();
        }
        _failureDetectors.Clear();

        _unstableModeTimer.Dispose();

        // Dispose consensus instance (may be null if initialization failed)
        if (_consensusInstance is not null)
        {
            await _consensusInstance.DisposeAsync();
        }
    }

}
