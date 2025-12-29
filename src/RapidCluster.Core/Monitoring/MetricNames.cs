namespace RapidCluster.Monitoring;

/// <summary>
/// Constants for metric names, tag names, and tag values used throughout RapidCluster.
/// Following OpenTelemetry naming conventions.
/// </summary>
public static class MetricNames
{
    /// <summary>
    /// The meter name for all RapidCluster metrics.
    /// </summary>
    public const string MeterName = "RapidCluster";

    // ========================================
    // Cluster State Metrics
    // ========================================

    /// <summary>
    /// Current number of nodes in the cluster.
    /// </summary>
    public const string ClusterSize = "rapidcluster.cluster.size";

    /// <summary>
    /// Current configuration ID (monotonically increasing).
    /// </summary>
    public const string ClusterConfigurationId = "rapidcluster.cluster.configuration_id";

    // ========================================
    // Messaging Metrics
    // ========================================

    /// <summary>
    /// Total messages sent by type.
    /// </summary>
    public const string MessagesSent = "rapidcluster.messages.sent";

    /// <summary>
    /// Total messages received by type.
    /// </summary>
    public const string MessagesReceived = "rapidcluster.messages.received";

    /// <summary>
    /// Messages dropped (queue full, invalid, etc.).
    /// </summary>
    public const string MessagesDropped = "rapidcluster.messages.dropped";

    /// <summary>
    /// Message send errors.
    /// </summary>
    public const string MessagesErrors = "rapidcluster.messages.errors";

    // ========================================
    // Consensus Metrics
    // ========================================

    /// <summary>
    /// Consensus proposals initiated.
    /// </summary>
    public const string ConsensusProposals = "rapidcluster.consensus.proposals";

    /// <summary>
    /// Consensus rounds started.
    /// </summary>
    public const string ConsensusRoundsStarted = "rapidcluster.consensus.rounds_started";

    /// <summary>
    /// Consensus rounds completed.
    /// </summary>
    public const string ConsensusRoundsCompleted = "rapidcluster.consensus.rounds_completed";

    /// <summary>
    /// Fast path conflicts requiring classic Paxos fallback.
    /// </summary>
    public const string ConsensusConflicts = "rapidcluster.consensus.conflicts";

    /// <summary>
    /// Votes sent in consensus rounds.
    /// </summary>
    public const string ConsensusVotesSent = "rapidcluster.consensus.votes_sent";

    /// <summary>
    /// Votes received in consensus rounds.
    /// </summary>
    public const string ConsensusVotesReceived = "rapidcluster.consensus.votes_received";

    /// <summary>
    /// Time to reach consensus.
    /// </summary>
    public const string ConsensusLatency = "rapidcluster.consensus.latency";

    // ========================================
    // Membership Metrics
    // ========================================

    /// <summary>
    /// Total membership view changes.
    /// </summary>
    public const string MembershipViewChanges = "rapidcluster.membership.view_changes";

    /// <summary>
    /// Nodes added to the cluster.
    /// </summary>
    public const string MembershipNodesAdded = "rapidcluster.membership.nodes_added";

    /// <summary>
    /// Nodes removed from the cluster.
    /// </summary>
    public const string MembershipNodesRemoved = "rapidcluster.membership.nodes_removed";

    /// <summary>
    /// Join requests processed.
    /// </summary>
    public const string MembershipJoinRequests = "rapidcluster.membership.join_requests";

    /// <summary>
    /// Time for a node to complete join.
    /// </summary>
    public const string JoinLatency = "rapidcluster.join.latency";

    // ========================================
    // Failure Detection Metrics
    // ========================================

    /// <summary>
    /// Failure detection probes sent.
    /// </summary>
    public const string FailureDetectionProbesSent = "rapidcluster.failure_detection.probes_sent";

    /// <summary>
    /// Successful probe responses.
    /// </summary>
    public const string FailureDetectionProbeSuccesses = "rapidcluster.failure_detection.probe_successes";

    /// <summary>
    /// Failed probes.
    /// </summary>
    public const string FailureDetectionProbeFailures = "rapidcluster.failure_detection.probe_failures";

    /// <summary>
    /// Failure alerts raised.
    /// </summary>
    public const string FailureDetectionAlertsRaised = "rapidcluster.failure_detection.alerts_raised";

    /// <summary>
    /// Failure detection probe round-trip time.
    /// </summary>
    public const string ProbeLatency = "rapidcluster.probe.latency";

    // ========================================
    // Cut Detector Metrics
    // ========================================

    /// <summary>
    /// Reports received by cut detector.
    /// </summary>
    public const string CutDetectorReportsReceived = "rapidcluster.cut_detector.reports_received";

    /// <summary>
    /// Stable cuts detected.
    /// </summary>
    public const string CutDetectorCutsDetected = "rapidcluster.cut_detector.cuts_detected";

    /// <summary>
    /// Number of nodes with partial failure reports (unstable).
    /// </summary>
    public const string CutDetectorUnstableNodes = "rapidcluster.cut_detector.unstable_nodes";

    // ========================================
    // gRPC/Network Metrics
    // ========================================

    /// <summary>
    /// gRPC calls started.
    /// </summary>
    public const string GrpcCallsStarted = "rapidcluster.grpc.calls_started";

    /// <summary>
    /// gRPC calls completed.
    /// </summary>
    public const string GrpcCallsCompleted = "rapidcluster.grpc.calls_completed";

    /// <summary>
    /// gRPC connection errors.
    /// </summary>
    public const string GrpcConnectionErrors = "rapidcluster.grpc.connection_errors";

    /// <summary>
    /// gRPC call duration.
    /// </summary>
    public const string GrpcCallDuration = "rapidcluster.grpc.call_duration";

    // ========================================
    // Tag Names
    // ========================================
    internal static class Tags
    {
        public const string MessageType = "message.type";
        public const string ErrorType = "error.type";
        public const string Reason = "reason";
        public const string Result = "result";
        public const string VoteType = "vote.type";
        public const string Protocol = "protocol";
        public const string Phase = "phase";
        public const string AlertType = "alert.type";
        public const string ReportType = "report.type";
        public const string Method = "method";
        public const string Status = "status";
    }

    // ========================================
    // Tag Values
    // ========================================
    internal static class MessageTypes
    {
        public const string JoinRequest = "join_request";
        public const string JoinResponse = "join_response";
        public const string PreJoinMessage = "pre_join";
        public const string ProbeRequest = "probe_request";
        public const string ProbeResponse = "probe_response";
        public const string Alert = "alert";
        public const string BatchedAlert = "batched_alert";
        public const string ConsensusProposal = "consensus_proposal";
        public const string FastRoundPhase2b = "fast_round_phase2b";
        public const string Phase1a = "phase1a";
        public const string Phase1b = "phase1b";
        public const string Phase2a = "phase2a";
        public const string Phase2b = "phase2b";
        public const string LeaveMessage = "leave";
        public const string MembershipViewRequest = "membership_view_request";
        public const string MembershipViewResponse = "membership_view_response";
    }

    internal static class Protocols
    {
        public const string Consensus = "consensus";
        public const string FastPaxos = "fast_paxos";
        public const string ClassicPaxos = "classic_paxos";
    }

    internal static class Results
    {
        public const string Success = "success";
        public const string Timeout = "timeout";
        public const string Conflict = "conflict";
        public const string Superseded = "superseded";
        public const string Aborted = "aborted";
        public const string Rejected = "rejected";
        public const string Failed = "failed";
    }

    internal static class VoteTypes
    {
        public const string FastVote = "fast_vote";
        public const string Phase1b = "phase1b";
        public const string Phase2b = "phase2b";
    }

    internal static class Phases
    {
        public const string FastRound = "fast_round";
        public const string Phase1a = "phase1a";
        public const string Phase1b = "phase1b";
        public const string Phase2a = "phase2a";
        public const string Phase2b = "phase2b";
    }

    internal static class ReportTypes
    {
        public const string Join = "join";
        public const string Fail = "fail";
        public const string Leave = "leave";
    }

    internal static class ErrorTypes
    {
        public const string Serialization = "serialization";
        public const string Network = "network";
        public const string Timeout = "timeout";
        public const string Rejected = "rejected";
        public const string Unknown = "unknown";
    }

    internal static class RemovalReasons
    {
        public const string GracefulLeave = "graceful_leave";
        public const string FailureDetected = "failure_detected";
        public const string Kicked = "kicked";
        public const string Timeout = "timeout";
    }
}
