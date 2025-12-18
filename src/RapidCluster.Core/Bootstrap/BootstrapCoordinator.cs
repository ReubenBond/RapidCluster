using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RapidCluster.Discovery;
using RapidCluster.Exceptions;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster.Bootstrap;

/// <summary>
/// Result of a successful bootstrap operation.
/// </summary>
internal sealed class BootstrapResult
{
    /// <summary>
    /// The membership view created during bootstrap.
    /// The view contains all bootstrap state including MaxNodeId and members.
    /// </summary>
    public required MembershipView View { get; init; }
}

/// <summary>
/// Coordinates the cluster bootstrap process.
/// Handles both static seed bootstrap (deterministic, fast) and dynamic seed bootstrap
/// (gossip-based agreement when seeds are discovered dynamically).
/// </summary>
/// <remarks>
/// <para>
/// The bootstrap process ensures all initial cluster members create identical membership views:
/// </para>
/// <para>
/// <b>Static Seeds (IsStatic = true):</b>
/// All nodes have identical seed lists from configuration. Bootstrap proceeds directly
/// by waiting for seeds to be reachable, then creating a deterministic membership view.
/// </para>
/// <para>
/// <b>Dynamic Seeds (IsStatic = false):</b>
/// Seeds are discovered dynamically (DNS, service discovery, etc.) and may differ across nodes.
/// A gossip protocol ensures all nodes agree on the same seed set before forming the cluster,
/// preventing split-brain scenarios.
/// </para>
/// </remarks>
internal sealed class BootstrapCoordinator
{
    private readonly Endpoint _myAddr;
    private readonly int _bootstrapExpect;
    private readonly int _observersPerSubject;
    private readonly IMessagingClient _messagingClient;
    private readonly SharedResources _sharedResources;
    private readonly BootstrapCoordinatorLogger _log;
    private readonly TimeSpan _gossipInterval;
    private readonly TimeSpan _gossipTimeout;
    private readonly TimeSpan _gossipRpcTimeout;
    private readonly TimeSpan _bootstrapTimeout;
    private readonly TimeSpan _probeInterval;
    private readonly TimeSpan _probeTimeout;

    // Mutable state during negotiation
    private readonly HashSet<Endpoint> _knownSeeds;
    private byte[] _currentSeedSetHash;

    /// <summary>
    /// Creates a new BootstrapCoordinator.
    /// </summary>
    /// <param name="myAddr">This node's address.</param>
    /// <param name="bootstrapExpect">Expected number of nodes to bootstrap.</param>
    /// <param name="observersPerSubject">Number of observers per subject for the membership view.</param>
    /// <param name="messagingClient">Client for sending messages to other nodes.</param>
    /// <param name="sharedResources">Shared resources (time provider, etc.).</param>
    /// <param name="logger">Logger for diagnostic output.</param>
    /// <param name="bootstrapTimeout">Total timeout for bootstrap (waiting for seeds). Default: 30 seconds.</param>
    /// <param name="gossipInterval">Interval between gossip rounds. Default: 1 second.</param>
    /// <param name="gossipTimeout">Total timeout for seed gossip negotiation. Default: 60 seconds.</param>
    /// <param name="gossipRpcTimeout">Timeout for individual gossip RPC calls. Default: 5 seconds.</param>
    /// <param name="probeInterval">Interval between seed probe rounds. Default: 1 second.</param>
    /// <param name="probeTimeout">Timeout for individual seed probes. Default: 2 seconds.</param>
    public BootstrapCoordinator(
        Endpoint myAddr,
        int bootstrapExpect,
        int observersPerSubject,
        IMessagingClient messagingClient,
        SharedResources sharedResources,
        ILogger logger,
        TimeSpan? bootstrapTimeout = null,
        TimeSpan? gossipInterval = null,
        TimeSpan? gossipTimeout = null,
        TimeSpan? gossipRpcTimeout = null,
        TimeSpan? probeInterval = null,
        TimeSpan? probeTimeout = null)
    {
        _myAddr = myAddr;
        _bootstrapExpect = bootstrapExpect;
        _observersPerSubject = observersPerSubject;
        _messagingClient = messagingClient;
        _sharedResources = sharedResources;
        _log = new BootstrapCoordinatorLogger(logger);
        _bootstrapTimeout = bootstrapTimeout ?? TimeSpan.FromSeconds(30);
        _gossipInterval = gossipInterval ?? TimeSpan.FromSeconds(1);
        _gossipTimeout = gossipTimeout ?? TimeSpan.FromSeconds(60);
        _gossipRpcTimeout = gossipRpcTimeout ?? TimeSpan.FromSeconds(5);
        _probeInterval = probeInterval ?? TimeSpan.FromSeconds(1);
        _probeTimeout = probeTimeout ?? TimeSpan.FromSeconds(2);

        // Initialize with just self
        _knownSeeds = new HashSet<Endpoint>(EndpointAddressComparer.Instance) { _myAddr };
        _currentSeedSetHash = ComputeSeedSetHash(_knownSeeds);
    }

    /// <summary>
    /// Attempts to bootstrap the cluster using the provided seed provider.
    /// This is the main entry point for bootstrap that handles the full flow:
    /// fetching seeds, normalizing them, and running the appropriate bootstrap protocol.
    /// </summary>
    /// <param name="seedProvider">The seed provider to fetch seeds from.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// The bootstrap result if bootstrap was performed, or <c>null</c> if bootstrap doesn't apply
    /// (no seeds available after filtering self).
    /// </returns>
    /// <exception cref="BootstrapAlreadyCompleteException">
    /// Thrown if an already-formed cluster is detected during dynamic seed bootstrap.
    /// The caller should fall back to joining the cluster.
    /// </exception>
    /// <exception cref="TimeoutException">
    /// Thrown if seeds are not reachable or agreement cannot be reached within timeout.
    /// </exception>
    /// <remarks>
    /// <para>
    /// Returns <c>null</c> in these cases (caller should start a new single-node cluster):
    /// <list type="bullet">
    /// <item>No seeds were discovered</item>
    /// <item>All discovered seeds are self (only this node in seed list)</item>
    /// </list>
    /// </para>
    /// <para>
    /// The method handles seed normalization internally:
    /// <list type="bullet">
    /// <item>Seeds are sorted deterministically by address</item>
    /// <item>If more seeds than <c>bootstrapExpect</c>, only the first N are used</item>
    /// <item>Self is filtered from the seed list for probing</item>
    /// </list>
    /// </para>
    /// </remarks>
    public async Task<BootstrapResult?> TryBootstrapAsync(
        ISeedProvider seedProvider,
        CancellationToken cancellationToken)
    {
        // Bootstrap only applies when BootstrapExpect > 0
        if (_bootstrapExpect <= 0)
        {
            _log.BootstrapNotApplicable(_myAddr, _bootstrapExpect);
            return null;
        }

        // Fetch seeds from the provider
        var seedResult = await seedProvider.GetSeedsAsync(cancellationToken).ConfigureAwait(true);

        // Normalize seeds: convert to protobuf, deduplicate, and sort deterministically
        var allSeeds = NormalizeSeeds(seedResult.Seeds);

        // Filter out self to get the list of seeds to contact
        var seedsExcludingSelf = allSeeds
            .Where(s => !EndpointAddressComparer.Instance.Equals(s, _myAddr))
            .ToList();

        _log.SeedsRefreshed(_myAddr, seedsExcludingSelf.Count, seedResult.IsStatic);

        // If no seeds remain after filtering self, bootstrap doesn't apply
        // (caller should start a new single-node cluster)
        if (seedsExcludingSelf.Count == 0)
        {
            _log.NoSeedsAvailable(_myAddr);
            return null;
        }

        // Execute the appropriate bootstrap protocol
        if (seedResult.IsStatic)
        {
            return await BootstrapWithStaticSeedsAsync(seedsExcludingSelf, cancellationToken).ConfigureAwait(true);
        }
        else
        {
            return await BootstrapWithDynamicSeedsAsync(seedsExcludingSelf, cancellationToken).ConfigureAwait(true);
        }
    }

    /// <summary>
    /// Normalizes a list of seed endpoints by converting to protobuf, deduplicating,
    /// sorting deterministically, and limiting to <c>bootstrapExpect</c> count.
    /// </summary>
    /// <param name="seeds">The raw seed endpoints from the provider.</param>
    /// <returns>Normalized list of seed endpoints as protobuf.</returns>
    private List<Endpoint> NormalizeSeeds(IReadOnlyList<System.Net.EndPoint> seeds)
    {
        // Convert to protobuf, deduplicate by address string, and sort deterministically
        var allSeeds = seeds
            .Select(s => s.ToProtobuf())
            .DistinctBy(s => $"{s.Hostname.ToStringUtf8()}:{s.Port}")
            .OrderBy(s => $"{s.Hostname.ToStringUtf8()}:{s.Port}", StringComparer.Ordinal)
            .ToList();

        // If we have more seeds than bootstrapExpect, take only the first N
        // This ensures all nodes agree on the same seed set
        if (allSeeds.Count > _bootstrapExpect)
        {
            _log.NormalizingSeedSet(_myAddr, allSeeds.Count, _bootstrapExpect);
            allSeeds = allSeeds.Take(_bootstrapExpect).ToList();
        }

        return allSeeds;
    }

    /// <summary>
    /// Bootstraps the cluster with static seeds.
    /// All nodes have identical seed lists, so they can create deterministic views directly.
    /// </summary>
    /// <param name="seedAddresses">The seed addresses (excluding self).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bootstrap result containing the membership view.</returns>
    /// <exception cref="TimeoutException">Thrown if seeds are not reachable within timeout.</exception>
    public async Task<BootstrapResult> BootstrapWithStaticSeedsAsync(
        IReadOnlyList<Endpoint> seedAddresses,
        CancellationToken cancellationToken)
    {
        _log.StartingStaticSeedBootstrap(_myAddr, seedAddresses.Count, _bootstrapExpect);

        // Initialize known seeds
        foreach (var seed in seedAddresses)
        {
            _knownSeeds.Add(seed);
        }

        // Wait until enough seeds are reachable
        await WaitForSeedsAsync(seedAddresses, cancellationToken).ConfigureAwait(true);

        // Create deterministic membership view
        var (view, _) = CreateMembershipView(seedAddresses);
        _log.BootstrapComplete(_myAddr, view.Size, view.ConfigurationId);

        return new BootstrapResult
        {
            View = view
        };
    }

    /// <summary>
    /// Bootstraps the cluster with dynamically discovered seeds.
    /// Uses a gossip protocol to agree on a common seed set before forming the cluster.
    /// </summary>
    /// <param name="initialSeeds">Initial seeds discovered by this node (excluding self).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The bootstrap result containing the membership view.</returns>
    /// <exception cref="BootstrapAlreadyCompleteException">
    /// Thrown if an already-formed cluster is detected. The caller should fall back to joining.
    /// </exception>
    /// <exception cref="TimeoutException">
    /// Thrown if agreement cannot be reached within the timeout period.
    /// </exception>
    public async Task<BootstrapResult> BootstrapWithDynamicSeedsAsync(
        IReadOnlyList<Endpoint> initialSeeds,
        CancellationToken cancellationToken)
    {
        _log.StartingDynamicSeedBootstrap(_myAddr, initialSeeds.Count, _bootstrapExpect);

        // Negotiate with other seeds to agree on a common seed set
        var agreedSeeds = await NegotiateSeedsAsync(initialSeeds, cancellationToken).ConfigureAwait(true);

        // Get seeds excluding self for the wait phase
        var seedsExcludingSelf = agreedSeeds
            .Where(s => !EndpointAddressComparer.Instance.Equals(s, _myAddr))
            .ToList();

        // Wait until enough seeds are reachable
        await WaitForSeedsAsync(seedsExcludingSelf, cancellationToken).ConfigureAwait(true);

        // Create deterministic membership view using agreed seeds
        var (view, _) = CreateMembershipView(seedsExcludingSelf);
        _log.BootstrapComplete(_myAddr, view.Size, view.ConfigurationId);

        return new BootstrapResult
        {
            View = view
        };
    }

    /// <summary>
    /// Handles an incoming seed gossip message from another node.
    /// </summary>
    /// <param name="gossip">The incoming gossip message.</param>
    /// <returns>Response indicating our status and known seeds.</returns>
    public BootstrapSeedGossipResponse HandleGossip(BootstrapSeedGossip gossip)
    {
        // Merge the sender's seeds into our known set
        var addedCount = 0;
        foreach (var seed in gossip.KnownSeeds)
        {
            if (_knownSeeds.Add(seed))
            {
                addedCount++;
            }
        }

        if (addedCount > 0)
        {
            _log.MergedSeeds(gossip.Sender, addedCount, _knownSeeds.Count);
            UpdateSeedSetHash();
        }

        // Build response with our current state
        var response = new BootstrapSeedGossipResponse
        {
            Sender = _myAddr,
            Status = BootstrapStatus.Negotiating,
            SeedSetHash = ByteString.CopyFrom(_currentSeedSetHash)
        };
        response.KnownSeeds.AddRange(_knownSeeds);

        return response;
    }

    /// <summary>
    /// Creates a response indicating this node is part of an already-formed cluster.
    /// </summary>
    /// <param name="configurationId">The configuration ID of the formed cluster.</param>
    /// <returns>Response with ALREADY_FORMED status.</returns>
    public BootstrapSeedGossipResponse CreateAlreadyFormedResponse(Pb.ConfigurationId configurationId)
    {
        return new BootstrapSeedGossipResponse
        {
            Sender = _myAddr,
            Status = BootstrapStatus.AlreadyFormed,
            ConfigurationId = configurationId
        };
    }

    /// <summary>
    /// Negotiates with other seeds to agree on a common seed set.
    /// </summary>
    private async Task<IReadOnlyList<Endpoint>> NegotiateSeedsAsync(
        IReadOnlyList<Endpoint> initialSeeds,
        CancellationToken cancellationToken)
    {
        _log.StartingSeedGossip(_myAddr, initialSeeds.Count, _bootstrapExpect);

        // Add initial seeds to our known set
        foreach (var seed in initialSeeds)
        {
            _knownSeeds.Add(seed);
        }
        UpdateSeedSetHash();

        var startTime = _sharedResources.TimeProvider.GetTimestamp();
        var round = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            round++;

            // Check if we've timed out
            var elapsed = _sharedResources.TimeProvider.GetElapsedTime(startTime);
            if (elapsed >= _gossipTimeout)
            {
                _log.GossipTimeout(_gossipTimeout, _knownSeeds.Count, _bootstrapExpect);
                throw new TimeoutException($"Seed gossip timed out after {_gossipTimeout}. " +
                    $"Only {_knownSeeds.Count}/{_bootstrapExpect} seeds known.");
            }

            // Get current seeds to gossip to (excluding self)
            var seedsToContact = GetSeedsToContact();
            _log.GossipRoundStarting(round, _myAddr, _knownSeeds);

            // Send gossip to all known seeds in parallel
            var gossipTasks = seedsToContact.Select(seed => GossipToSeedAsync(seed, cancellationToken)).ToList();
            var results = await Task.WhenAll(gossipTasks).ConfigureAwait(true);

            // Process results and check for agreement
            var agreementResult = ProcessGossipResults(results, seedsToContact);

            if (agreementResult.AlreadyFormedCluster != null)
            {
                // An already-formed cluster was detected
                throw agreementResult.AlreadyFormedCluster;
            }

            if (agreementResult.AgreementReached)
            {
                // All reachable seeds agree on the same hash
                var finalSeeds = GetNormalizedSeedList();
                _log.FinalAgreedSeeds(finalSeeds);
                return finalSeeds;
            }

            // Wait before next round
            _log.WaitingForNextRound(_gossipInterval);
            await Task.Delay(_gossipInterval, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
        }

        cancellationToken.ThrowIfCancellationRequested();
        throw new InvalidOperationException("Seed gossip cancelled");
    }

    /// <summary>
    /// Waits until BootstrapExpect seeds are reachable.
    /// </summary>
    private async Task WaitForSeedsAsync(IReadOnlyList<Endpoint> seedAddresses, CancellationToken cancellationToken)
    {
        var startTime = _sharedResources.TimeProvider.GetTimestamp();

        // Total expected count includes self
        var expectedCount = _bootstrapExpect;

        while (!cancellationToken.IsCancellationRequested)
        {
            var elapsed = _sharedResources.TimeProvider.GetElapsedTime(startTime);
            if (elapsed >= _bootstrapTimeout)
            {
                throw new TimeoutException(
                    $"Bootstrap timed out waiting for {expectedCount} seeds. " +
                    $"Only {seedAddresses.Count + 1} seeds configured (including self).");
            }

            // Count reachable seeds (self is always reachable)
            var reachableCount = 1; // self

            // Probe other seeds in parallel
            var probeTasks = seedAddresses.Select(seed => ProbeSeedAsync(seed, cancellationToken)).ToList();
            var results = await Task.WhenAll(probeTasks).ConfigureAwait(true);
            reachableCount += results.Count(r => r);

            _log.BootstrapProbeResult(_myAddr, reachableCount, expectedCount);

            if (reachableCount >= expectedCount)
            {
                _log.BootstrapSeedsReady(_myAddr, reachableCount);
                return;
            }

            // Wait before next probe round
            await Task.Delay(_probeInterval, _sharedResources.TimeProvider, cancellationToken).ConfigureAwait(true);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Probes a seed to check if it's reachable.
    /// </summary>
    private async Task<bool> ProbeSeedAsync(Endpoint seed, CancellationToken cancellationToken)
    {
#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            // Use a simple probe request - we're just checking connectivity
            var probeRequest = new RapidClusterRequest
            {
                ProbeMessage = new ProbeMessage { Sender = _myAddr }
            };

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_probeTimeout);

            await _messagingClient.SendMessageAsync(seed, probeRequest, cts.Token).ConfigureAwait(true);
            return true;
        }
        catch
        {
            // Seed not reachable
            return false;
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    /// <summary>
    /// Creates a deterministic MembershipView from the seed list.
    /// All nodes will create the same view since the seed list is sorted identically.
    /// </summary>
    private (MembershipView View, long MaxNodeId) CreateMembershipView(IReadOnlyList<Endpoint> seedAddresses)
    {
        // Build the member list: self + all seeds (seeds already excludes self)
        var allMembers = new List<Endpoint>(seedAddresses) { _myAddr };

        // Sort deterministically by address string to ensure all nodes create identical views
        allMembers.Sort((a, b) =>
        {
            var aStr = $"{a.Hostname.ToStringUtf8()}:{a.Port}";
            var bStr = $"{b.Hostname.ToStringUtf8()}:{b.Port}";
            return string.Compare(aStr, bStr, StringComparison.Ordinal);
        });

        // Assign NodeIds deterministically: 1, 2, 3, ... based on sorted order
        long nodeId = 0;
        var membersWithNodeIds = new List<Endpoint>();
        foreach (var member in allMembers)
        {
            nodeId++;
            var endpointWithNodeId = new Endpoint
            {
                Hostname = member.Hostname,
                Port = member.Port,
                NodeId = nodeId
            };
            membersWithNodeIds.Add(endpointWithNodeId);
        }

        var maxNodeId = nodeId;

        _log.CreatedBootstrapProposal(_myAddr, membersWithNodeIds.Count, maxNodeId);

        // Build the membership view with deterministic ConfigurationId
        var view = new MembershipViewBuilder(
            _observersPerSubject,
            membersWithNodeIds,
            maxNodeId).Build();

        return (view, maxNodeId);
    }

    private List<Endpoint> GetSeedsToContact()
    {
        return _knownSeeds
            .Where(s => !EndpointAddressComparer.Instance.Equals(s, _myAddr))
            .ToList();
    }

    private async Task<GossipResult> GossipToSeedAsync(Endpoint target, CancellationToken cancellationToken)
    {
        var hashString = Convert.ToHexString(_currentSeedSetHash)[..16];
        _log.SendingGossipTo(target, hashString);

        var gossipMessage = new BootstrapSeedGossip
        {
            Sender = _myAddr,
            SeedSetHash = ByteString.CopyFrom(_currentSeedSetHash),
            BootstrapExpect = _bootstrapExpect
        };
        gossipMessage.KnownSeeds.AddRange(_knownSeeds);

        var request = new RapidClusterRequest
        {
            BootstrapSeedGossip = gossipMessage
        };

        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            cts.CancelAfter(_gossipRpcTimeout);

            var response = await _messagingClient.SendMessageAsync(target, request, cts.Token).ConfigureAwait(true);

            if (response.BootstrapSeedGossipResponse != null)
            {
                var gossipResponse = response.BootstrapSeedGossipResponse;
                var respHashString = gossipResponse.SeedSetHash.IsEmpty
                    ? "empty"
                    : Convert.ToHexString(gossipResponse.SeedSetHash.ToByteArray())[..Math.Min(16, gossipResponse.SeedSetHash.Length * 2)];

                _log.ReceivedGossipResponse(
                    gossipResponse.Sender,
                    gossipResponse.Status,
                    gossipResponse.KnownSeeds.Count,
                    respHashString);

                return new GossipResult
                {
                    Target = target,
                    Success = true,
                    Response = gossipResponse
                };
            }

            return new GossipResult { Target = target, Success = false };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.GossipFailed(target, ex.Message);
            return new GossipResult { Target = target, Success = false };
        }
    }

    private GossipAgreementResult ProcessGossipResults(GossipResult[] results, List<Endpoint> contactedSeeds)
    {
        var successfulResponses = results.Where(r => r.Success && r.Response != null).ToList();

        // Check for already-formed cluster or AGREED status from formed cluster
        foreach (var result in successfulResponses)
        {
            var response = result.Response!;

            if (response.Status == BootstrapStatus.AlreadyFormed)
            {
                // Cluster already formed and we're NOT in it - need to join
                _log.AlreadyFormedClusterDetected(response.Sender, response.ConfigurationId);
                return new GossipAgreementResult
                {
                    AlreadyFormedCluster = new BootstrapAlreadyCompleteException(
                        $"Cluster already formed by {response.Sender.GetNetworkAddressString()}",
                        ConfigurationId.FromProtobuf(response.ConfigurationId))
                };
            }

            // If we receive AGREED with a ConfigurationId set, it means the responder has
            // already formed the cluster and we're part of it. We should complete bootstrap
            // using the membership they sent us.
            if (response.Status == BootstrapStatus.Agreed &&
                response.ConfigurationId.Version > 0)
            {
                _log.AgreedResponseFromFormedCluster(response.Sender, response.ConfigurationId);
                // Update our known seeds to match the formed cluster's membership
                _knownSeeds.Clear();
                foreach (var seed in response.KnownSeeds)
                {
                    _knownSeeds.Add(seed);
                }
                UpdateSeedSetHash();
                return new GossipAgreementResult { AgreementReached = true };
            }
        }

        // Merge seeds from all responses
        var seedsAdded = false;
        foreach (var result in successfulResponses)
        {
            foreach (var seed in result.Response!.KnownSeeds)
            {
                if (_knownSeeds.Add(seed))
                {
                    seedsAdded = true;
                }
            }
        }

        if (seedsAdded)
        {
            UpdateSeedSetHash();
        }

        // Check if we have enough seeds and all responding seeds agree
        if (_knownSeeds.Count < _bootstrapExpect)
        {
            return new GossipAgreementResult { AgreementReached = false };
        }

        // Count how many seeds agree with our current hash
        var agreedCount = 1; // Self always agrees with itself
        foreach (var result in successfulResponses)
        {
            if (result.Response!.SeedSetHash.Span.SequenceEqual(_currentSeedSetHash))
            {
                agreedCount++;
            }
        }

        // Agreement requires all expected seeds to agree
        // We need _bootstrapExpect agreeing nodes
        if (agreedCount >= _bootstrapExpect)
        {
            var hashString = Convert.ToHexString(_currentSeedSetHash)[..16];
            _log.AgreementReached(_myAddr, agreedCount, _bootstrapExpect, hashString);
            return new GossipAgreementResult { AgreementReached = true };
        }

        return new GossipAgreementResult { AgreementReached = false };
    }

    private void UpdateSeedSetHash()
    {
        var oldHash = _currentSeedSetHash;
        _currentSeedSetHash = ComputeSeedSetHash(_knownSeeds);

        if (!oldHash.SequenceEqual(_currentSeedSetHash))
        {
            var oldHashString = Convert.ToHexString(oldHash)[..16];
            var newHashString = Convert.ToHexString(_currentSeedSetHash)[..16];
            _log.SeedSetHashChanged(oldHashString, newHashString);
        }
    }

    private byte[] ComputeSeedSetHash(IEnumerable<Endpoint> seeds)
    {
        // Normalize: sort seeds deterministically and take first BootstrapExpect
        var normalizedSeeds = seeds
            .OrderBy(s => $"{s.Hostname.ToStringUtf8()}:{s.Port}", StringComparer.Ordinal)
            .Take(_bootstrapExpect)
            .ToList();

        _log.ComputingSeedSetHash(normalizedSeeds.Count);

        // Build a deterministic string representation of the seed set
        var sb = new StringBuilder();
        foreach (var seed in normalizedSeeds)
        {
            sb.Append(seed.Hostname.ToStringUtf8());
            sb.Append(':');
            sb.Append(seed.Port);
            sb.Append(';');
        }

        // Compute SHA256 hash
        var bytes = Encoding.UTF8.GetBytes(sb.ToString());
        return SHA256.HashData(bytes);
    }

    private List<Endpoint> GetNormalizedSeedList()
    {
        return _knownSeeds
            .OrderBy(s => $"{s.Hostname.ToStringUtf8()}:{s.Port}", StringComparer.Ordinal)
            .Take(_bootstrapExpect)
            .ToList();
    }

    private struct GossipResult
    {
        public Endpoint Target;
        public bool Success;
        public BootstrapSeedGossipResponse? Response;
    }

    private struct GossipAgreementResult
    {
        public bool AgreementReached;
        public BootstrapAlreadyCompleteException? AlreadyFormedCluster;
    }
}
