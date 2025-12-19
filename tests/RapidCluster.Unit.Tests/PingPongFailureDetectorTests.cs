using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using RapidCluster;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for PingPongFailureDetector.
/// </summary>
public sealed class PingPongFailureDetectorTests
{
    private static readonly IMeterFactory MeterFactory = new TestMeterFactory();
    private static RapidClusterMetrics CreateMetrics() => new(MeterFactory);

    #region Constructor Tests

    [Fact]
    public void Constructor_WithDefaults_CreatesInstance()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { });

        Assert.NotNull(detector);
    }

    [Fact]
    public void Constructor_WithCustomThreshold_CreatesInstance()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            consecutiveFailuresThreshold: 5);

        Assert.NotNull(detector);
    }

    #endregion

    #region Consecutive Failures Tests

    [Fact]
    public async Task ConsecutiveFailures_AtThreshold_NotifiesFailure()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var failureNotified = false;

        // All probes fail
        client.ResponseGenerator = _ => throw new Exception("Probe failed");

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            consecutiveFailuresThreshold: 3,
            probeInterval: TimeSpan.FromSeconds(1),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // First two failures (not yet at threshold)
        for (var i = 0; i < 2; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(50, TestContext.Current.CancellationToken);
            Assert.False(failureNotified);
        }

        // Third failure (at threshold)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.True(failureNotified);
    }

    [Fact]
    public async Task ConsecutiveFailures_BelowThreshold_DoesNotNotify()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var failureNotified = false;

        // All probes fail
        client.ResponseGenerator = _ => throw new Exception("Probe failed");

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            consecutiveFailuresThreshold: 5,
            probeInterval: TimeSpan.FromSeconds(1),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // Only 4 failures (below threshold of 5)
        for (var i = 0; i < 4; i++)
        {
            timeProvider.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(50, TestContext.Current.CancellationToken);
        }

        Assert.False(failureNotified);
    }

    #endregion

    #region Probe Success Tests

    [Fact]
    public async Task ProbeSuccess_ResetsConsecutiveFailures()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var failureNotified = false;

        // First probe fails, second succeeds, third fails
        var responseIndex = 0;
        client.ResponseGenerator = _ =>
        {
            var idx = responseIndex++;
            if (idx == 1)
            {
                // Second probe succeeds
                return new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 10).ToProtobuf() }.ToRapidClusterResponse();
            }
            throw new Exception("Probe failed");
        };

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            consecutiveFailuresThreshold: 2,
            probeInterval: TimeSpan.FromSeconds(1),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // First probe (failure)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Second probe (success - resets count)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Third probe (failure again - count is now 1, not 2)
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        // Should NOT have notified because success reset the counter
        Assert.False(failureNotified);
    }

    #endregion

    #region Dispose Tests

    [Fact]
    public void Dispose_CanBeCalledMultipleTimes()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        // Multiple disposes should not throw
        detector.Dispose();
        detector.Dispose();
        detector.Dispose();
    }

    [Fact]
    public void Start_AfterDispose_Throws()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();

        var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Dispose();

        Assert.Throws<ObjectDisposedException>(() => detector.Start());
    }

    #endregion

    #region Stale View Detection Tests

    [Fact]
    public async Task StaleViewDetected_WhenRemoteHasHigherConfigId()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var staleViewDetected = false;
        ConfigurationId detectedRemoteConfigId = ConfigurationId.Empty;
        ConfigurationId detectedLocalConfigId = ConfigurationId.Empty;

        client.ResponseGenerator = _ => new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 10).ToProtobuf() }.ToRapidClusterResponse();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            onStaleViewDetected: (remote, remoteConfig, localConfig) =>
            {
                staleViewDetected = true;
                detectedRemoteConfigId = remoteConfig;
                detectedLocalConfigId = localConfig;
            },
            viewAccessor: new TestMembershipViewAccessor(Utils.CreateMembershipView(numNodes: 5, configVersion: 5)),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.True(staleViewDetected);
        Assert.Equal(10, detectedRemoteConfigId.Version);
        Assert.Equal(5, detectedLocalConfigId.Version);
    }

    [Fact]
    public async Task StaleViewNotDetected_WhenLocalConfigIsHigher()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var staleViewDetected = false;

        client.ResponseGenerator = _ => new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 5).ToProtobuf() }.ToRapidClusterResponse();

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => { },
            onStaleViewDetected: (_, _, _) => staleViewDetected = true,
            viewAccessor: new TestMembershipViewAccessor(Utils.CreateMembershipView(numNodes: 5, configVersion: 10)),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.False(staleViewDetected);
    }

    #endregion

    #region Exception Handling Tests

    [Fact]
    public async Task TimeoutException_CountsAsFailure()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);
        var observer = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        using var client = new TestMessagingClient();
        var failureNotified = false;

        client.ResponseGenerator = _ => throw new TimeoutException("Request timed out");

        using var detector = new PingPongFailureDetector(
            subject,
            observer,
            client,
            sharedResources,
            () => failureNotified = true,
            consecutiveFailuresThreshold: 2,
            probeInterval: TimeSpan.FromSeconds(1),
            metrics: CreateMetrics(),
            logger: NullLogger<PingPongFailureDetector>.Instance);

        detector.Start();

        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);
        timeProvider.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.True(failureNotified);
    }

    #endregion

    #region Factory Tests

    [Fact]
    public void Factory_CreatesDetectorWithCorrectConfiguration()
    {
        var timeProvider = new FakeTimeProvider();
        var sharedResources = new SharedResources(timeProvider: timeProvider);

        using var client = new TestMessagingClient();
        var localEndpoint = Utils.HostFromParts("127.0.0.1", 1000);
        var subject = Utils.HostFromParts("127.0.0.2", 2000);
        var protocolOptions = Microsoft.Extensions.Options.Options.Create(new RapidClusterProtocolOptions
        {
            FailureDetectorConsecutiveFailures = 5,
            FailureDetectorInterval = TimeSpan.FromSeconds(2)
        });

        var factory = new PingPongFailureDetectorFactory(
            localEndpoint,
            client,
            sharedResources,
            protocolOptions,
            CreateMetrics(),
            NullLogger<PingPongFailureDetector>.Instance);

        using var detector = factory.CreateInstance(subject, () => { });

        Assert.NotNull(detector);
        Assert.IsType<PingPongFailureDetector>(detector);
    }

    #endregion

    #region Test Doubles

    /// <summary>
    /// Test messaging client that allows customizing responses.
    /// </summary>

    private sealed class TestMembershipViewAccessor(MembershipView view) : IMembershipViewAccessor
    {
        public MembershipView CurrentView => view;
        public BroadcastChannelReader<MembershipView> Updates => throw new NotImplementedException();
    }
    private sealed class TestMessagingClient : IMessagingClient, IDisposable
    {
        public Func<RapidClusterRequest, RapidClusterResponse>? ResponseGenerator { get; set; }

        public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
        {
        }

        public Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
        {
            if (ResponseGenerator != null)
            {
                return Task.FromResult(ResponseGenerator(request));
            }

            return Task.FromResult(new ProbeResponse { ConfigurationId = new ConfigurationId(new ClusterId(888), 10).ToProtobuf() }.ToRapidClusterResponse());
        }

        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
        }
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

    #endregion
}
