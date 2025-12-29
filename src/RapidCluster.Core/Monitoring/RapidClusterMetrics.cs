using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace RapidCluster.Monitoring;

/// <summary>
/// Centralized metrics instrumentation for RapidCluster using System.Diagnostics.Metrics.
/// Uses IMeterFactory for DI-aware meter creation following Microsoft best practices.
/// </summary>
public sealed class RapidClusterMetrics
{
    private readonly Meter _meter;

    // ========================================
    // Counters (Event Metrics)
    // ========================================
    private readonly Counter<long> _messagesSent;
    private readonly Counter<long> _messagesReceived;
    private readonly Counter<long> _messagesDropped;
    private readonly Counter<long> _messagesErrors;

    private readonly Counter<long> _consensusProposals;
    private readonly Counter<long> _consensusRoundsStarted;
    private readonly Counter<long> _consensusRoundsCompleted;
    private readonly Counter<long> _consensusConflicts;
    private readonly Counter<long> _consensusVotesSent;
    private readonly Counter<long> _consensusVotesReceived;

    private readonly Counter<long> _membershipViewChanges;
    private readonly Counter<long> _membershipNodesAdded;
    private readonly Counter<long> _membershipNodesRemoved;
    private readonly Counter<long> _membershipJoinRequests;

    private readonly Counter<long> _failureDetectionProbesSent;
    private readonly Counter<long> _failureDetectionProbeSuccesses;
    private readonly Counter<long> _failureDetectionProbeFailures;
    private readonly Counter<long> _failureDetectionAlertsRaised;

    private readonly Counter<long> _cutDetectorReportsReceived;
    private readonly Counter<long> _cutDetectorCutsDetected;

    private readonly Counter<long> _grpcCallsStarted;
    private readonly Counter<long> _grpcCallsCompleted;
    private readonly Counter<long> _grpcConnectionErrors;

    // ========================================
    // Histograms (Latency/Duration Metrics)
    // ========================================
    private readonly Histogram<double> _consensusLatency;
    private readonly Histogram<double> _joinLatency;
    private readonly Histogram<double> _probeLatency;
    private readonly Histogram<double> _grpcCallDuration;

    // ========================================
    // State for Observable Gauges
    // ========================================
    private readonly IRapidCluster? _cluster;

    /// <summary>
    /// Histogram bucket boundaries for fast operations (sub-millisecond to tens of milliseconds).
    /// Used for: probe.latency, message.send_duration, message.processing_time.
    /// </summary>
    private static readonly double[] FastOperationBuckets =
        [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1];

    /// <summary>
    /// Histogram bucket boundaries for medium operations (milliseconds to seconds).
    /// Used for: consensus.latency, consensus.round_duration, message.roundtrip_time.
    /// </summary>
    private static readonly double[] MediumOperationBuckets =
        [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10];

    /// <summary>
    /// Histogram bucket boundaries for slow operations (seconds to tens of seconds).
    /// Used for: join.latency, leave.latency, view_change.duration.
    /// </summary>
    private static readonly double[] SlowOperationBuckets =
        [0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120];

    /// <summary>
    /// Initializes a new instance of the <see cref="RapidClusterMetrics"/> class.
    /// Creates a new RapidClusterMetrics instance using an IMeterFactory.
    /// </summary>
    /// <param name="meterFactory">The meter factory for DI-aware meter creation.</param>
    /// <param name="cluster">Optional cluster interface for observable gauges.</param>
    public RapidClusterMetrics(IMeterFactory meterFactory, IRapidCluster? cluster = null)
    {
        ArgumentNullException.ThrowIfNull(meterFactory);

        _cluster = cluster;
        _meter = meterFactory.Create(MetricNames.MeterName);

        // ========================================
        // Initialize Counters
        // ========================================
        _messagesSent = _meter.CreateCounter<long>(
            name: MetricNames.MessagesSent,
            unit: "{messages}",
            description: "Total messages sent by type");

        _messagesReceived = _meter.CreateCounter<long>(
            name: MetricNames.MessagesReceived,
            unit: "{messages}",
            description: "Total messages received by type");

        _messagesDropped = _meter.CreateCounter<long>(
            name: MetricNames.MessagesDropped,
            unit: "{messages}",
            description: "Messages dropped (queue full, invalid, etc.)");

        _messagesErrors = _meter.CreateCounter<long>(
            name: MetricNames.MessagesErrors,
            unit: "{errors}",
            description: "Message send/receive errors");

        _consensusProposals = _meter.CreateCounter<long>(
            name: MetricNames.ConsensusProposals,
            unit: "{proposals}",
            description: "Consensus proposals initiated");

        _consensusRoundsStarted = _meter.CreateCounter<long>(
            name: MetricNames.ConsensusRoundsStarted,
            unit: "{rounds}",
            description: "Consensus rounds started");

        _consensusRoundsCompleted = _meter.CreateCounter<long>(
            name: MetricNames.ConsensusRoundsCompleted,
            unit: "{rounds}",
            description: "Consensus rounds completed");

        _consensusConflicts = _meter.CreateCounter<long>(
            name: MetricNames.ConsensusConflicts,
            unit: "{conflicts}",
            description: "Fast path conflicts requiring classic Paxos fallback");

        _consensusVotesSent = _meter.CreateCounter<long>(
            name: MetricNames.ConsensusVotesSent,
            unit: "{votes}",
            description: "Votes sent in consensus rounds");

        _consensusVotesReceived = _meter.CreateCounter<long>(
            name: MetricNames.ConsensusVotesReceived,
            unit: "{votes}",
            description: "Votes received in consensus rounds");

        _membershipViewChanges = _meter.CreateCounter<long>(
            name: MetricNames.MembershipViewChanges,
            unit: "{changes}",
            description: "Total membership view changes");

        _membershipNodesAdded = _meter.CreateCounter<long>(
            name: MetricNames.MembershipNodesAdded,
            unit: "{nodes}",
            description: "Nodes added to the cluster");

        _membershipNodesRemoved = _meter.CreateCounter<long>(
            name: MetricNames.MembershipNodesRemoved,
            unit: "{nodes}",
            description: "Nodes removed from the cluster");

        _membershipJoinRequests = _meter.CreateCounter<long>(
            name: MetricNames.MembershipJoinRequests,
            unit: "{requests}",
            description: "Join requests processed");

        _failureDetectionProbesSent = _meter.CreateCounter<long>(
            name: MetricNames.FailureDetectionProbesSent,
            unit: "{probes}",
            description: "Failure detection probes sent");

        _failureDetectionProbeSuccesses = _meter.CreateCounter<long>(
            name: MetricNames.FailureDetectionProbeSuccesses,
            unit: "{probes}",
            description: "Successful probe responses");

        _failureDetectionProbeFailures = _meter.CreateCounter<long>(
            name: MetricNames.FailureDetectionProbeFailures,
            unit: "{probes}",
            description: "Failed probes");

        _failureDetectionAlertsRaised = _meter.CreateCounter<long>(
            name: MetricNames.FailureDetectionAlertsRaised,
            unit: "{alerts}",
            description: "Failure alerts raised");

        _cutDetectorReportsReceived = _meter.CreateCounter<long>(
            name: MetricNames.CutDetectorReportsReceived,
            unit: "{reports}",
            description: "Reports received by cut detector");

        _cutDetectorCutsDetected = _meter.CreateCounter<long>(
            name: MetricNames.CutDetectorCutsDetected,
            unit: "{cuts}",
            description: "Stable cuts detected");

        _grpcCallsStarted = _meter.CreateCounter<long>(
            name: MetricNames.GrpcCallsStarted,
            unit: "{calls}",
            description: "gRPC calls started");

        _grpcCallsCompleted = _meter.CreateCounter<long>(
            name: MetricNames.GrpcCallsCompleted,
            unit: "{calls}",
            description: "gRPC calls completed");

        _grpcConnectionErrors = _meter.CreateCounter<long>(
            name: MetricNames.GrpcConnectionErrors,
            unit: "{errors}",
            description: "gRPC connection errors");

        // ========================================
        // Initialize Histograms
        // ========================================
        _consensusLatency = _meter.CreateHistogram(
            name: MetricNames.ConsensusLatency,
            unit: "s",
            description: "Time to reach consensus",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = MediumOperationBuckets,
            });

        _joinLatency = _meter.CreateHistogram(
            name: MetricNames.JoinLatency,
            unit: "s",
            description: "Time for a node to complete join",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = SlowOperationBuckets,
            });

        _probeLatency = _meter.CreateHistogram(
            name: MetricNames.ProbeLatency,
            unit: "s",
            description: "Failure detection probe round-trip time",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = FastOperationBuckets,
            });

        _grpcCallDuration = _meter.CreateHistogram(
            name: MetricNames.GrpcCallDuration,
            unit: "s",
            description: "gRPC call duration",
            advice: new InstrumentAdvice<double>
            {
                HistogramBucketBoundaries = MediumOperationBuckets,
            });

        // ========================================
        // Initialize Observable Gauges
        // ========================================
        if (_cluster != null)
        {
            _meter.CreateObservableGauge(
                name: MetricNames.ClusterSize,
                observeValue: () => _cluster.CurrentView.Members.Length,
                unit: "{nodes}",
                description: "Current number of nodes in the cluster");

            _meter.CreateObservableGauge(
                name: MetricNames.ClusterConfigurationId,
                observeValue: () => _cluster.CurrentView.ConfigurationId.Version,
                unit: "{id}",
                description: "Current configuration ID");
        }
    }

    // ========================================
    // Messaging Recording Methods
    // ========================================

    /// <summary>
    /// Records a message sent.
    /// </summary>
    public void RecordMessageSent(string messageType) => _messagesSent.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.MessageType, messageType));

    /// <summary>
    /// Records a message received.
    /// </summary>
    public void RecordMessageReceived(string messageType) => _messagesReceived.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.MessageType, messageType));

    /// <summary>
    /// Records a dropped message.
    /// </summary>
    public void RecordMessageDropped(string messageType, string reason)
    {
        _messagesDropped.Add(1,
            new KeyValuePair<string, object?>(MetricNames.Tags.MessageType, messageType),
            new KeyValuePair<string, object?>(MetricNames.Tags.Reason, reason));
    }

    /// <summary>
    /// Records a message error.
    /// </summary>
    public void RecordMessageError(string messageType, string errorType)
    {
        _messagesErrors.Add(1,
            new KeyValuePair<string, object?>(MetricNames.Tags.MessageType, messageType),
            new KeyValuePair<string, object?>(MetricNames.Tags.ErrorType, errorType));
    }

    // ========================================
    // Consensus Recording Methods
    // ========================================

    /// <summary>
    /// Records a consensus proposal initiated.
    /// </summary>
    public void RecordConsensusProposal(string protocol) => _consensusProposals.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.Protocol, protocol));

    /// <summary>
    /// Records a consensus round started.
    /// </summary>
    public void RecordConsensusRoundStarted(string protocol) => _consensusRoundsStarted.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.Protocol, protocol));

    /// <summary>
    /// Records a consensus round completed.
    /// </summary>
    public void RecordConsensusRoundCompleted(string protocol, string result)
    {
        _consensusRoundsCompleted.Add(1,
            new KeyValuePair<string, object?>(MetricNames.Tags.Protocol, protocol),
            new KeyValuePair<string, object?>(MetricNames.Tags.Result, result));
    }

    /// <summary>
    /// Records a fast path conflict requiring classic Paxos fallback.
    /// </summary>
    public void RecordConsensusConflict() => _consensusConflicts.Add(1);

    /// <summary>
    /// Records a vote sent in a consensus round.
    /// </summary>
    public void RecordConsensusVoteSent(string voteType) => _consensusVotesSent.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.VoteType, voteType));

    /// <summary>
    /// Records a vote received in a consensus round.
    /// </summary>
    public void RecordConsensusVoteReceived(string voteType) => _consensusVotesReceived.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.VoteType, voteType));

    /// <summary>
    /// Records consensus latency.
    /// </summary>
    public void RecordConsensusLatency(string protocol, string result, double latencySeconds)
    {
        _consensusLatency.Record(latencySeconds,
            new KeyValuePair<string, object?>(MetricNames.Tags.Protocol, protocol),
            new KeyValuePair<string, object?>(MetricNames.Tags.Result, result));
    }

    /// <summary>
    /// Records consensus latency using a Stopwatch.
    /// </summary>
    public void RecordConsensusLatency(string protocol, string result, Stopwatch stopwatch)
    {
        ArgumentNullException.ThrowIfNull(stopwatch);
        RecordConsensusLatency(protocol, result, stopwatch.Elapsed.TotalSeconds);
    }

    // ========================================
    // Membership Recording Methods
    // ========================================

    /// <summary>
    /// Records a membership view change.
    /// </summary>
    public void RecordMembershipViewChange() => _membershipViewChanges.Add(1);

    /// <summary>
    /// Records nodes added to the cluster.
    /// </summary>
    public void RecordNodesAdded(int count = 1) => _membershipNodesAdded.Add(count);

    /// <summary>
    /// Records nodes removed from the cluster.
    /// </summary>
    public void RecordNodesRemoved(int count, string reason) => _membershipNodesRemoved.Add(count, new KeyValuePair<string, object?>(MetricNames.Tags.Reason, reason));

    /// <summary>
    /// Records a join request.
    /// </summary>
    public void RecordJoinRequest(string result) => _membershipJoinRequests.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.Result, result));

    /// <summary>
    /// Records join latency.
    /// </summary>
    public void RecordJoinLatency(string result, double latencySeconds) => _joinLatency.Record(latencySeconds, new KeyValuePair<string, object?>(MetricNames.Tags.Result, result));

    /// <summary>
    /// Records join latency using a Stopwatch.
    /// </summary>
    public void RecordJoinLatency(string result, Stopwatch stopwatch)
    {
        ArgumentNullException.ThrowIfNull(stopwatch);
        RecordJoinLatency(result, stopwatch.Elapsed.TotalSeconds);
    }

    // ========================================
    // Failure Detection Recording Methods
    // ========================================

    /// <summary>
    /// Records a failure detection probe sent.
    /// </summary>
    public void RecordProbeSent() => _failureDetectionProbesSent.Add(1);

    /// <summary>
    /// Records a successful probe response.
    /// </summary>
    public void RecordProbeSuccess() => _failureDetectionProbeSuccesses.Add(1);

    /// <summary>
    /// Records a failed probe.
    /// </summary>
    public void RecordProbeFailure(string reason) => _failureDetectionProbeFailures.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.Reason, reason));

    /// <summary>
    /// Records a failure alert raised.
    /// </summary>
    public void RecordFailureAlertRaised(string alertType) => _failureDetectionAlertsRaised.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.AlertType, alertType));

    /// <summary>
    /// Records probe latency.
    /// </summary>
    public void RecordProbeLatency(string result, double latencySeconds) => _probeLatency.Record(latencySeconds, new KeyValuePair<string, object?>(MetricNames.Tags.Result, result));

    /// <summary>
    /// Records probe latency using a Stopwatch.
    /// </summary>
    public void RecordProbeLatency(string result, Stopwatch stopwatch)
    {
        ArgumentNullException.ThrowIfNull(stopwatch);
        RecordProbeLatency(result, stopwatch.Elapsed.TotalSeconds);
    }

    // ========================================
    // Cut Detector Recording Methods
    // ========================================

    /// <summary>
    /// Records a report received by the cut detector.
    /// </summary>
    public void RecordCutDetectorReportReceived(string reportType) => _cutDetectorReportsReceived.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.ReportType, reportType));

    /// <summary>
    /// Records a stable cut detected.
    /// </summary>
    public void RecordCutDetected() => _cutDetectorCutsDetected.Add(1);

    // ========================================
    // gRPC Recording Methods
    // ========================================

    /// <summary>
    /// Records a gRPC call started.
    /// </summary>
    public void RecordGrpcCallStarted(string method) => _grpcCallsStarted.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.Method, method));

    /// <summary>
    /// Records a gRPC call completed.
    /// </summary>
    public void RecordGrpcCallCompleted(string method, string status)
    {
        _grpcCallsCompleted.Add(1,
            new KeyValuePair<string, object?>(MetricNames.Tags.Method, method),
            new KeyValuePair<string, object?>(MetricNames.Tags.Status, status));
    }

    /// <summary>
    /// Records a gRPC connection error.
    /// </summary>
    public void RecordGrpcConnectionError(string errorType) => _grpcConnectionErrors.Add(1, new KeyValuePair<string, object?>(MetricNames.Tags.ErrorType, errorType));

    /// <summary>
    /// Records gRPC call duration.
    /// </summary>
    public void RecordGrpcCallDuration(string method, string status, double durationSeconds)
    {
        _grpcCallDuration.Record(durationSeconds,
            new KeyValuePair<string, object?>(MetricNames.Tags.Method, method),
            new KeyValuePair<string, object?>(MetricNames.Tags.Status, status));
    }

    /// <summary>
    /// Records gRPC call duration using a Stopwatch.
    /// </summary>
    public void RecordGrpcCallDuration(string method, string status, Stopwatch stopwatch)
    {
        ArgumentNullException.ThrowIfNull(stopwatch);
        RecordGrpcCallDuration(method, status, stopwatch.Elapsed.TotalSeconds);
    }
}
