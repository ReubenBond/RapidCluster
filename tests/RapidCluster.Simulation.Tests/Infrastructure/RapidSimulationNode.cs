using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics.Metrics;
using System.Globalization;
using Clockwork;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RapidCluster.Discovery;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;
using RapidCluster.Simulation.Tests.Infrastructure.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Represents a simulated node in a Rapid cluster.
/// Uses in-memory transport instead of gRPC and does not require a WebApplication.
/// Lifetime is managed by the SimulationHarness via Destroy() - do not implement IDisposable.
/// </summary>
[SuppressMessage("Reliability", "CA1001:Types that own disposable fields should be disposable",
    Justification = "Lifetime is managed by SimulationHarness.Destroy() to avoid CA2000 warnings in tests")]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed class RapidSimulationNode : SimulationNode
{
    private readonly RapidClusterProtocolOptions _protocolOptions;
    private readonly PingPongFailureDetectorOptions _failureDetectorOptions;
    private readonly ILoggerFactory _loggerFactory;
    private readonly RapidSimulationNodeLogger _log;
    private readonly MembershipService _membershipService;
    private readonly SharedResources _sharedResources;
    private readonly PingPongFailureDetectorFactory _failureDetectorFactory;
    private readonly IConsensusCoordinatorFactory _consensusCoordinatorFactory;
    private readonly ILogger<CutDetector> _cutDetectorLogger;
    private readonly MembershipViewAccessor _viewAccessor;
    private readonly ILogger<MembershipService> _membershipServiceLogger;
    private readonly CancellationTokenSource _disposeCts = new();
    private readonly TestMeterFactory _meterFactory;
    private readonly RapidClusterMetrics _metrics;
    private bool _disposed;
    private bool _isInitialized;

    /// <summary>
    /// Gets the endpoint address of this node.
    /// </summary>
    public Endpoint Address { get; }

    /// <summary>
    /// Gets the network address of this node as a string for routing.
    /// </summary>
    public override string NetworkAddress => RapidClusterUtils.Loggable(Address);

    /// <summary>
    /// Gets the simulation context for this node.
    /// </summary>
    public override SimulationNodeContext Context { get; }

    /// <summary>
    /// Gets the current membership view of this node.
    /// </summary>
    public MembershipView CurrentView => _viewAccessor.CurrentView;

    /// <summary>
    /// Gets the membership view accessor for subscribing to view changes.
    /// </summary>
    public IMembershipViewAccessor ViewAccessor => _viewAccessor;

    /// <summary>
    /// Gets a value indicating whether gets whether this node is initialized and part of a cluster.
    /// </summary>
    public override bool IsInitialized => _isInitialized;

    /// <summary>
    /// Gets the membership size of this node's view.
    /// </summary>
    public int MembershipSize => _viewAccessor.CurrentView.Size;

    /// <summary>
    /// Gets the messaging client for testing purposes.
    /// </summary>
    internal InMemoryMessagingClient MessagingClient { get; }

    /// <summary>
    /// Gets the cancellation token that is triggered when this node is being torn down.
    /// </summary>
    internal CancellationToken TeardownCancellationToken => _disposeCts.Token;

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with a single seed address.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        Endpoint address,
        Endpoint? seedAddress,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
        : this(harness, address, seedAddress != null ? [seedAddress] : null, metadata, protocolOptions, failureDetectorOptions, loggerFactory)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidSimulationNode"/> class.
    /// Creates a new simulation node with multiple seed addresses for testing seed failover.
    /// </summary>
    internal RapidSimulationNode(
        RapidSimulationCluster harness,
        Endpoint address,
        IList<Endpoint>? seedAddresses,
        Metadata? metadata,
        RapidClusterProtocolOptions? protocolOptions,
        PingPongFailureDetectorOptions? failureDetectorOptions,
        ILoggerFactory? loggerFactory)
    {
        Address = address;

        // Wrap the logger factory to prepend the node name to all log messages
        var baseLoggerFactory = loggerFactory ?? harness.LoggerFactory;
        var nodeName = string.Create(CultureInfo.InvariantCulture, $"{address.Hostname.ToStringUtf8()}:{address.Port}");
        _loggerFactory = baseLoggerFactory != null
            ? new NodePrefixedLoggerFactory(baseLoggerFactory, nodeName)
            : NullLoggerFactory.Instance;

        // Create logger for the context (suspend/resume logging happens there)
        var contextLogger = _loggerFactory.CreateLogger<SimulationNodeContext>();
        Context = new SimulationNodeContext(harness.Clock, harness.Guard, harness.CreateDerivedRandom(), harness.TaskQueue, contextLogger);

        _log = new RapidSimulationNodeLogger(_loggerFactory.CreateLogger<RapidSimulationNode>());
        _membershipServiceLogger = _loggerFactory.CreateLogger<MembershipService>();

        // Create protocol options
        _protocolOptions = protocolOptions ?? new RapidClusterProtocolOptions();
        _failureDetectorOptions = failureDetectorOptions ?? new PingPongFailureDetectorOptions();

        // Create shared resources with the node's time provider, task scheduler, and harness teardown token
        _sharedResources = new SharedResources(
            Context.TimeProvider,
            Context.TaskScheduler,
            Context.Random,
            Context.Random.NextGuid,
            harness.TeardownCancellationToken);

        // Create in-memory messaging client using timeouts from options.
        // For tests with suspended nodes requiring Classic Paxos fallback,
        // a longer timeout (e.g., 30 seconds) may be needed to allow
        // for the random jitter delay before Classic Paxos starts.
        MessagingClient = new InMemoryMessagingClient(harness, this, address, _protocolOptions, _failureDetectorOptions);

        // Create view accessor
        _viewAccessor = new MembershipViewAccessor();

        // Create metrics for observability
        _meterFactory = new TestMeterFactory();
        _metrics = new RapidClusterMetrics(_meterFactory, cluster: null);

        // Create failure detector factory
        var listenAddressProvider = new StaticListenAddressProvider(address.ToEndPointPreferIP());
        var failureDetectorLogger = _loggerFactory.CreateLogger<PingPongFailureDetector>();
        _failureDetectorFactory = new PingPongFailureDetectorFactory(
            listenAddressProvider,
            MessagingClient,
            _sharedResources,
            Options.Create(_failureDetectorOptions),
            _metrics,
            failureDetectorLogger);

        // Create consensus coordinator factory
        var consensusCoordinatorLogger = _loggerFactory.CreateLogger<ConsensusCoordinator>();
        _consensusCoordinatorFactory = new ConsensusCoordinatorFactory(
            MessagingClient,
            _viewAccessor,
            Options.Create(_protocolOptions),
            _sharedResources,
            _metrics,
            consensusCoordinatorLogger);

        // Create cut detector logger
        _cutDetectorLogger = _loggerFactory.CreateLogger<CutDetector>();

        // Create the MembershipService (but don't initialize it yet)
        var rapidClusterOptions = new RapidClusterOptions
        {
            ListenAddress = address.ToEndPointPreferIP(),
            SeedAddresses = seedAddresses?.Select(s => s.ToEndPointPreferIP()).ToList(),
            Metadata = metadata?.Metadata_.ToDictionary(kvp => kvp.Key, kvp => kvp.Value.ToByteArray(), StringComparer.Ordinal) ?? [],
        };
        var broadcasterFactory = new UnicastToAllBroadcasterFactory(MessagingClient);
        var seedProvider = new ConfigurationSeedProvider(new TestOptionsMonitor<RapidClusterOptions>(rapidClusterOptions));
        _membershipService = new MembershipService(
            Options.Create(rapidClusterOptions),
            Options.Create(_protocolOptions),
            listenAddressProvider,
            MessagingClient,
            broadcasterFactory,
            _failureDetectorFactory,
            _consensusCoordinatorFactory,
            _viewAccessor,
            _sharedResources,
            _metrics,
            seedProvider,
            _membershipServiceLogger,
            _cutDetectorLogger);
    }

    /// <summary>
    /// Initializes the membership service (completes the join or cluster start).
    /// </summary>
    internal async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        await _membershipService.InitializeAsync(cancellationToken);
        _isInitialized = true;

        _log.NodeInitialized(RapidClusterUtils.Loggable(Address), CurrentView.Size, CurrentView.ConfigurationId.Version);
    }

    /// <summary>
    /// Handles an incoming request from another node.
    /// </summary>
    internal async Task<RapidClusterResponse> HandleRequestAsync(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        _log.HandlingRequest(RapidClusterUtils.Loggable(Address), request.ContentCase);

        // Link the caller's cancellation token with our disposal token so that
        // in-flight requests complete when this node is disposed
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        return await _membershipService.HandleMessageAsync(request, linkedCts.Token);
    }

    /// <summary>
    /// Gracefully stops the node by notifying observers and waiting for background tasks.
    /// The node can still receive messages after this method returns.
    /// Call <see cref="DisposeAsync"/> after unregistering from the network to release resources.
    /// </summary>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (_disposed) return;

        _log.NodeLeaving(RapidClusterUtils.Loggable(Address));

        // Graceful stop: notify observers and wait for background tasks
        // Keep MessagingClient alive so we can still participate in consensus
        await _membershipService.StopAsync(cancellationToken);

        _log.NodeLeftGracefully(RapidClusterUtils.Loggable(Address));
    }

    /// <summary>
    /// Disposes the node's resources. Should be called after unregistering from the network.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Cancel any in-flight requests first so they complete promptly
        _disposeCts.SafeCancel(_log.Logger);

        await _membershipService.DisposeAsync();
        await MessagingClient.DisposeAsync();

        _meterFactory.Dispose();
        _disposeCts.Dispose();
    }

    private string DebuggerDisplay => string.Create(CultureInfo.InvariantCulture, $"{RapidClusterUtils.Loggable(Address)} Size={MembershipSize} {(IsSuspended ? "Suspended" : "Running")}");

    /// <summary>
    /// Simple broadcaster factory that creates UnicastToAllBroadcaster instances.
    /// </summary>
    private sealed class UnicastToAllBroadcasterFactory(IMessagingClient messagingClient) : IBroadcasterFactory
    {
        public IBroadcaster Create() => new UnicastToAllBroadcaster(messagingClient);
    }

    /// <summary>
    /// Simple meter factory for test metrics collection.
    /// </summary>
    private sealed class TestMeterFactory : IMeterFactory
    {
        private readonly List<Meter> _meters = [];

        public Meter Create(MeterOptions options)
        {
            var meter = new Meter(options);
            _meters.Add(meter);
            return meter;
        }

        public void Dispose()
        {
            foreach (var meter in _meters)
            {
                meter.Dispose();
            }

            _meters.Clear();
        }
    }

    /// <summary>
    /// Simple options monitor for tests that returns a fixed value.
    /// </summary>
    private sealed class TestOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue => value;

        public T Get(string? name) => value;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
