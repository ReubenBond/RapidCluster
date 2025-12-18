using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using RapidCluster.Exceptions;
using RapidCluster.Messaging;
using RapidCluster.Pb;

namespace RapidCluster.Bootstrap;

/// <summary>
/// Coordinates the seed gossip protocol for dynamic seed discovery.
/// When seeds are discovered dynamically (DNS, service discovery, etc.),
/// different nodes may see different seed lists. This coordinator ensures
/// all bootstrap nodes agree on the same seed set before forming a cluster.
/// </summary>
/// <remarks>
/// <para>
/// The gossip protocol works as follows:
/// 1. Each node starts with its initial set of discovered seeds
/// 2. Nodes gossip their known seeds to other seeds they know about
/// 3. When receiving gossip, nodes merge the seed sets (union)
/// 4. Nodes sort seeds deterministically and take the first BootstrapExpect
/// 5. Once all reachable seeds report the same hash, agreement is reached
/// 6. If any seed reports an already-formed cluster, abort and join instead
/// </para>
/// </remarks>
internal sealed class BootstrapCoordinator
{
    private readonly Endpoint _myAddr;
    private readonly int _bootstrapExpect;
    private readonly IMessagingClient _messagingClient;
    private readonly SharedResources _sharedResources;
    private readonly BootstrapCoordinatorLogger _log;
    private readonly TimeSpan _gossipInterval;
    private readonly TimeSpan _gossipTimeout;
    private readonly TimeSpan _gossipRpcTimeout;

    // Mutable state during negotiation
    private readonly HashSet<Endpoint> _knownSeeds;
    private byte[] _currentSeedSetHash;

    /// <summary>
    /// Creates a new BootstrapCoordinator.
    /// </summary>
    /// <param name="myAddr">This node's address.</param>
    /// <param name="bootstrapExpect">Expected number of nodes to bootstrap.</param>
    /// <param name="messagingClient">Client for sending messages to other nodes.</param>
    /// <param name="sharedResources">Shared resources (time provider, etc.).</param>
    /// <param name="logger">Logger for diagnostic output.</param>
    /// <param name="gossipInterval">Interval between gossip rounds.</param>
    /// <param name="gossipTimeout">Total timeout for seed gossip negotiation.</param>
    /// <param name="gossipRpcTimeout">Timeout for individual gossip RPC calls.</param>
    public BootstrapCoordinator(
        Endpoint myAddr,
        int bootstrapExpect,
        IMessagingClient messagingClient,
        SharedResources sharedResources,
        ILogger logger,
        TimeSpan? gossipInterval = null,
        TimeSpan? gossipTimeout = null,
        TimeSpan? gossipRpcTimeout = null)
    {
        _myAddr = myAddr;
        _bootstrapExpect = bootstrapExpect;
        _messagingClient = messagingClient;
        _sharedResources = sharedResources;
        _log = new BootstrapCoordinatorLogger(logger);
        _gossipInterval = gossipInterval ?? TimeSpan.FromSeconds(1);
        _gossipTimeout = gossipTimeout ?? TimeSpan.FromSeconds(60);
        _gossipRpcTimeout = gossipRpcTimeout ?? TimeSpan.FromSeconds(5);

        // Initialize with just self
        _knownSeeds = new HashSet<Endpoint>(EndpointAddressComparer.Instance) { _myAddr };
        _currentSeedSetHash = ComputeSeedSetHash(_knownSeeds);
    }

    /// <summary>
    /// Negotiates with other seeds to agree on a common seed set.
    /// </summary>
    /// <param name="initialSeeds">Initial seeds discovered by this node (excluding self).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The agreed-upon seed set.</returns>
    /// <exception cref="BootstrapAlreadyCompleteException">
    /// Thrown if an already-formed cluster is detected. The node should fall back to joining.
    /// </exception>
    /// <exception cref="TimeoutException">
    /// Thrown if agreement cannot be reached within the timeout period.
    /// </exception>
    public async Task<IReadOnlyList<Endpoint>> NegotiateSeedsAsync(
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
