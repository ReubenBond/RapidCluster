using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RapidCluster.Logging;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster.Discovery;

/// <summary>
/// Controls the bootstrap process for forming a new cluster.
/// Manages state and logic for:
/// - Handling bootstrap-related messages (BOOTSTRAP_IN_PROGRESS)
/// - Determining if this node should be the bootstrap coordinator
/// - Signaling completion of the bootstrap phase
/// </summary>
internal sealed class BootstrapController
{
    private readonly Endpoint _myAddr;
    private readonly RapidClusterOptions _clusterOptions;
    private readonly Metadata _nodeMetadata;
    private readonly MembershipServiceLogger _log;
    private readonly Lock _lock = new();

    // Bootstrap state tracking
    // Nodes attempting to join this node during bootstrap
    private readonly HashSet<Endpoint> _knownNodes = new(EndpointAddressComparer.Instance);

    // Whether this node is still in the bootstrap phase (before cluster is formed)
    private bool _isBootstrapping;

    // Task completion source for signaling when bootstrap should complete
    private TaskCompletionSource<bool>? _bootstrapCompletionSource;

    /// <summary>
    /// Gets whether the node is currently in bootstrap mode.
    /// </summary>
    public bool IsBootstrapping
    {
        get
        {
            lock (_lock)
            {
                return _isBootstrapping;
            }
        }
    }

    public BootstrapController(
        Endpoint myAddr,
        RapidClusterOptions clusterOptions,
        Metadata nodeMetadata,
        MembershipServiceLogger log)
    {
        _myAddr = myAddr;
        _clusterOptions = clusterOptions;
        _nodeMetadata = nodeMetadata;
        _log = log;
    }

    /// <summary>
    /// Starts the bootstrap process if configured.
    /// </summary>
    public void Start()
    {
        if (_clusterOptions.BootstrapExpect > 0)
        {
            lock (_lock)
            {
                _isBootstrapping = true;
                _knownNodes.Add(_myAddr); // Always include self
                _bootstrapCompletionSource = new TaskCompletionSource<bool>();
            }
        }
    }

    /// <summary>
    /// Waits for bootstrap to complete when this node is the bootstrap coordinator.
    /// Other nodes will contact us via PreJoinMessage, and when we have enough,
    /// TriggerBootstrap will be called which sets the completion source.
    /// </summary>
    public async Task WaitForCompletionAsync(CancellationToken cancellationToken)
    {
        Task task;
        lock (_lock)
        {
            _log.BootstrapCoordinatorTriggered(_myAddr, _knownNodes.Count);

            if (_bootstrapCompletionSource == null)
            {
                return;
            }
            task = _bootstrapCompletionSource.Task;
        }

        using var registration = cancellationToken.Register(
            () =>
            {
                lock (_lock)
                {
                    _bootstrapCompletionSource?.TrySetCanceled(cancellationToken);
                }
            });

        await task.ConfigureAwait(true);
    }

    /// <summary>
    /// Handles a PreJoinMessage while this node is in bootstrap mode.
    /// Tracks the joining node and either triggers bootstrap or returns BOOTSTRAP_IN_PROGRESS.
    /// Returns a tuple containing the response and optionally a list of members if bootstrap was triggered.
    /// </summary>
    public (RapidClusterResponse Response, List<MemberInfo>? NewMembers) HandlePreJoin(Endpoint joiningEndpoint, ConfigurationId currentConfigId)
    {
        lock (_lock)
        {
            // Add the joiner to our known nodes
            _knownNodes.Add(joiningEndpoint);

            // Log using the logger passed in constructor - context is handled by caller logging JoinAtSeed
            // We'll let the caller handle the JoinAtSeed logging as it requires the full view which we don't have

            // Check if we should trigger bootstrap
            if (ShouldBeCoordinatorInternal())
            {
                // We have enough nodes and we're the smallest - trigger bootstrap!
                _log.BootstrapCoordinatorTriggered(_myAddr, _knownNodes.Count);

                var members = TriggerBootstrapInternal();

                // After bootstrap, the joiner is already in the ring (was included in bootstrap).
                // Respond with HOSTNAME_ALREADY_IN_RING with NO observers - this ensures the joiner
                // does NOT proceed to phase 2 of the join protocol.
                var response = new JoinResponse
                {
                    Sender = _myAddr,
                    ConfigurationId = currentConfigId.ToProtobuf(), // Caller will need to update this if members are returned
                    StatusCode = JoinStatusCode.HostnameAlreadyInRing
                };

                _log.HandlePreJoinResult(joiningEndpoint, JoinStatusCode.HostnameAlreadyInRing, 0);
                return (response.ToRapidClusterResponse(), members);
            }

            // Not ready to trigger bootstrap yet - respond with BOOTSTRAP_IN_PROGRESS
            _log.BootstrapRespondingInProgress(joiningEndpoint, _knownNodes.Count);

            var bootstrapResponse = new JoinResponse
            {
                Sender = _myAddr,
                ConfigurationId = currentConfigId.ToProtobuf(),
                StatusCode = JoinStatusCode.BootstrapInProgress
            };
            bootstrapResponse.Endpoints.AddRange(_knownNodes);

            _log.HandlePreJoinResult(joiningEndpoint, JoinStatusCode.BootstrapInProgress, _knownNodes.Count);
            return (bootstrapResponse.ToRapidClusterResponse(), null);
        }
    }

    /// <summary>
    /// Triggers cluster formation when bootstrap conditions are met.
    /// Internal helper called under lock.
    /// </summary>
    private List<MemberInfo> TriggerBootstrapInternal()
    {
        // Sort nodes deterministically by address (hostname:port)
        var sortedNodes = _knownNodes
            .OrderBy(e => $"{e.Hostname.ToStringUtf8()}:{e.Port}", StringComparer.Ordinal)
            .ToList();

        // Assign monotonically increasing node IDs starting from 1 and create MemberInfo with metadata
        var memberInfos = new List<MemberInfo>();
        for (var i = 0; i < sortedNodes.Count; i++)
        {
            var node = sortedNodes[i];

            // Create endpoint with node ID
            var endpointWithId = new Endpoint
            {
                Hostname = node.Hostname,
                Port = node.Port,
                NodeId = i + 1
            };

            // Use own metadata for self, empty metadata for other nodes (metadata arrives with JoinMessage)
            var metadata = EndpointAddressComparer.Instance.Equals(node, _myAddr) ? _nodeMetadata : new Metadata();
            memberInfos.Add(new MemberInfo(endpointWithId, metadata));
        }

        // Exit bootstrap mode
        _isBootstrapping = false;

        // Signal any waiting bootstrap completion source
        _bootstrapCompletionSource?.TrySetResult(true);

        return memberInfos;
    }

    /// <summary>
    /// Exits bootstrap mode without triggering cluster formation.
    /// Called when this node discovers that bootstrap has been completed by another node.
    /// </summary>
    public void ExitBootstrapMode()
    {
        lock (_lock)
        {
            if (!_isBootstrapping)
            {
                return; // Already exited
            }

            _isBootstrapping = false;

            // Signal any waiting bootstrap completion source
            _bootstrapCompletionSource?.TrySetResult(true);

            _log.BootstrapModeExited(_myAddr);
        }
    }

    /// <summary>
    /// Checks if this node should wait as the bootstrap coordinator rather than continue trying to join.
    /// This happens when:
    /// 1. We're in bootstrap mode
    /// 2. We're the smallest known node (no node precedes us lexicographically)
    /// 3. We've contacted at least one other seed (so we know about others)
    /// </summary>
    public bool ShouldWaitAsCoordinator()
    {
        lock (_lock)
        {
            // Only relevant during bootstrap
            if (!_isBootstrapping || _clusterOptions.BootstrapExpect <= 0)
            {
                return false;
            }

            // We need to know about at least one other node (contacted at least one seed)
            // Otherwise we don't know if we're the smallest yet
            if (_knownNodes.Count <= 1)
            {
                return false;
            }

            // Check if we're the smallest known node
            var smallestNode = GetSmallestKnownNodeInternal();
            return EndpointAddressComparer.Instance.Equals(smallestNode, _myAddr);
        }
    }

    /// <summary>
    /// Handles a BOOTSTRAP_IN_PROGRESS response from a seed node.
    /// Updates our known nodes and determines if we should redirect to a smaller node.
    /// </summary>
    /// <param name="response">The join response with BOOTSTRAP_IN_PROGRESS status.</param>
    /// <param name="contactedSeed">The seed we contacted.</param>
    /// <returns>True if we should redirect to a smaller node (caller should check seed list), False otherwise.</returns>
    public (bool ShouldRedirect, Endpoint? SmallestNode) HandleBootstrapInProgressResponse(JoinResponse response, Endpoint contactedSeed)
    {
        var knownNodes = response.Endpoints.ToList();
        Endpoint? smallestNode = null;
        bool shouldRedirect = false;

        lock (_lock)
        {
            // Learn about nodes from the response
            foreach (var node in knownNodes)
            {
                _knownNodes.Add(node);
            }
            // Also add the contacted seed itself
            _knownNodes.Add(contactedSeed);

            _log.BootstrapInProgressReceived(_myAddr, contactedSeed, knownNodes.Count, _knownNodes.Count);

            // Check if there's a smaller node we should redirect to
            smallestNode = GetSmallestKnownNodeInternal();
            if (!EndpointAddressComparer.Instance.Equals(smallestNode, contactedSeed))
            {
                shouldRedirect = true;
            }
        }

        return (shouldRedirect, smallestNode);
    }

    /// <summary>
    /// Gets the count of known nodes during bootstrap.
    /// </summary>
    public int KnownNodesCount
    {
        get
        {
            lock (_lock)
            {
                return _knownNodes.Count;
            }
        }
    }

    /// <summary>
    /// Checks if this node should be the bootstrap coordinator.
    /// Internal helper called under lock.
    /// </summary>
    private bool ShouldBeCoordinatorInternal()
    {
        if (_clusterOptions.BootstrapExpect <= 0)
        {
            return false;
        }

        // Check if we have enough nodes
        if (_knownNodes.Count < _clusterOptions.BootstrapExpect)
        {
            return false;
        }

        // Check if this node is the smallest (no node precedes it)
        var smallestNode = GetSmallestKnownNodeInternal();
        return EndpointAddressComparer.Instance.Equals(smallestNode, _myAddr);
    }

    /// <summary>
    /// Gets the lexicographically smallest known node.
    /// Internal helper called under lock.
    /// </summary>
    private Endpoint GetSmallestKnownNodeInternal()
    {
        return _knownNodes
            .OrderBy(e => $"{e.Hostname.ToStringUtf8()}:{e.Port}", StringComparer.Ordinal)
            .First();
    }
}
