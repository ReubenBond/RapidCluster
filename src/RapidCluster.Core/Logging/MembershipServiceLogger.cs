using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class MembershipServiceLogger(ILogger<MembershipService> logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger => _logger;

    // Logging helpers
    internal readonly struct LoggableEndpoint(Endpoint endpoint)
    {
        private readonly Endpoint _endpoint = endpoint;
        public override readonly string ToString() => RapidClusterUtils.Loggable(_endpoint);
    }

    internal readonly struct LoggableEndpoints(IEnumerable<Endpoint> endpoints)
    {
        private readonly IEnumerable<Endpoint> _endpoints = endpoints;
        public override readonly string ToString() => RapidClusterUtils.Loggable(_endpoints);
    }

    internal readonly struct CurrentConfigId(MembershipView view)
    {
        private readonly MembershipView _view = view;
        public override readonly string ToString() => _view.ConfigurationId.ToString();
    }

    internal readonly struct MembershipSize(MembershipView view)
    {
        private readonly MembershipView _view = view;
        public override readonly string ToString() => _view.Size.ToString();
    }

    internal readonly struct LoggableRingNumbers(IEnumerable<int> ringNumbers)
    {
        private readonly IEnumerable<int> _ringNumbers = ringNumbers;
        public override readonly string ToString() => string.Join(",", _ringNumbers);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Initiating consensus for {Proposal}")]
    public partial void InitiatingConsensus(LoggableEndpoints proposal);

    [LoggerMessage(Level = LogLevel.Information, Message = "Received leave message from {Sender} at {MyAddr}")]
    public partial void ReceivedLeaveMessage(LoggableEndpoint sender, LoggableEndpoint myAddr);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Removing node {Node}")]
    public partial void RemovingNode(LoggableEndpoint node);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Decided on a node without UUID: {Node}")]
    public partial void DecidedNodeWithoutUuid(LoggableEndpoint node);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Adding node {Node}")]
    public partial void AddingNode(LoggableEndpoint node);

    [LoggerMessage(Level = LogLevel.Information, Message = "Leaving: {MyAddr} has {Count} observers: {Observers}")]
    public partial void LeavingWithObservers(LoggableEndpoint myAddr, int count, LoggableEndpoints observers);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Timeout while leaving")]
    public partial void TimeoutWhileLeaving();

    [LoggerMessage(Level = LogLevel.Trace, Message = "Exception while leaving")]
    public partial void ExceptionWhileLeaving(Exception ex);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Node was already removed prior to leaving")]
    public partial void NodeAlreadyRemoved();

    [LoggerMessage(Level = LogLevel.Information, Message = "Ignoring failure notification from old configuration {Subject}, config: {CurrentConfig}, oldConfiguration: {OldConfig}")]
    public partial void IgnoringOldConfigNotification(LoggableEndpoint subject, CurrentConfigId currentConfig, long oldConfig);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Announcing EdgeFail event {Subject}, observer: {MyAddr}, config: {Config}, size: {Size}")]
    public partial void AnnouncingEdgeFail(LoggableEndpoint subject, LoggableEndpoint myAddr, long config, MembershipSize size);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in EdgeFailureNotification for {Subject}")]
    public partial void ErrorInEdgeFailureNotification(Exception ex, LoggableEndpoint subject);

    [LoggerMessage(Level = LogLevel.Information, Message = "Join at seed for {{seed:{Seed}, sender:{Sender}, config:{Config}, size:{Size}}}")]
    public partial void JoinAtSeed(LoggableEndpoint seed, LoggableEndpoint sender, CurrentConfigId config, MembershipSize size);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Enqueuing SAFE_TO_JOIN for {{sender:{Sender}, config:{Config}, size:{Size}}}")]
    public partial void EnqueueingSafeToJoin(LoggableEndpoint sender, CurrentConfigId config, MembershipSize size);

    [LoggerMessage(Level = LogLevel.Information, Message = "Wrong configuration for {{sender:{Sender}, config:{Config}, myConfig:{MyConfig}, size:{Size}}}")]
    public partial void WrongConfiguration(LoggableEndpoint sender, long config, CurrentConfigId myConfig, MembershipSize size);

    [LoggerMessage(Level = LogLevel.Debug, Message = "MembershipService initialized: myAddr={MyAddr}, configId={ConfigId}, membershipSize={MembershipSize}")]
    public partial void MembershipServiceInitialized(LoggableEndpoint myAddr, CurrentConfigId configId, MembershipSize membershipSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleMessageAsync: received {MessageType} from request")]
    public partial void HandleMessageReceivedDebug(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(Level = LogLevel.Trace, Message = "HandleMessageAsync: received {MessageType} from request")]
    public partial void HandleMessageReceivedTrace(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandlePreJoinMessage: joiner={Joiner}, statusCode={StatusCode}, observers count={ObserversCount}")]
    public partial void HandlePreJoinResult(LoggableEndpoint joiner, JoinStatusCode statusCode, int observersCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleJoinMessageAsync: processing join from {Sender}, configId={ConfigId}")]
    public partial void HandleJoinMessage(LoggableEndpoint sender, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleJoinMessageAsync: joiner already in ring, responding SAFE_TO_JOIN")]
    public partial void JoinerAlreadyInRing();

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: received batch with {Count} messages from {Sender}")]
    public partial void HandleBatchedAlertMessage(int count, LoggableEndpoint sender);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: filtered out messages not matching current config {ConfigId}")]
    public partial void BatchedAlertFiltered(CurrentConfigId configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: processing alert edgeSrc={EdgeSrc}, edgeDst={EdgeDst}, status={Status}")]
    public partial void ProcessingAlert(LoggableEndpoint edgeSrc, LoggableEndpoint edgeDst, EdgeStatus status);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: cut detection returned {Count} proposals")]
    public partial void CutDetectionProposals(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: implicit edge invalidation returned {Count} proposals")]
    public partial void ImplicitEdgeInvalidation(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleConsensusMessages: forwarding to ConsensusCoordinator instance")]
    public partial void HandleConsensusMessages();

    [LoggerMessage(Level = LogLevel.Trace, Message = "HandleProbeMessage: responding to probe")]
    public partial void HandleProbeMessage();

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleMembershipViewRequest: sender={Sender} requested view (their config={TheirConfig}, our config={OurConfig})")]
    public partial void HandleMembershipViewRequest(LoggableEndpoint sender, long theirConfig, CurrentConfigId ourConfig);

    [LoggerMessage(Level = LogLevel.Debug, Message = "DecideViewChange: processing {Count} nodes in proposal")]
    public partial void DecideViewChange(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "DecideViewChange: ignoring stale consensus decision with {Count} nodes (consensus instance is no longer current)")]
    public partial void IgnoringStaleConsensusDecision(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "DecideViewChange: notifying {Count} joiners waiting through us for node {Node}")]
    public partial void NotifyingJoiners(int count, LoggableEndpoint node);

    [LoggerMessage(Level = LogLevel.Debug, Message = "DecideViewChange: recreated cut detector, updated broadcaster, recreated failure detectors")]
    public partial void DecideViewChangeCleanup();

    [LoggerMessage(Level = LogLevel.Debug, Message = "DecideViewChange: publishing VIEW_CHANGE event, configId={ConfigId}, membershipSize={MembershipSize}")]
    public partial void PublishingViewChange(CurrentConfigId configId, MembershipSize membershipSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Shutdown: cancelling background tasks and disposing failure detectors")]
    public partial void Shutdown();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stopping: sending leave messages and waiting for background tasks")]
    public partial void Stopping();

    [LoggerMessage(Level = LogLevel.Debug, Message = "EnqueueAlertMessage: queued alert edgeSrc={EdgeSrc}, edgeDst={EdgeDst}, status={Status}")]
    public partial void EnqueueAlertMessage(LoggableEndpoint edgeSrc, LoggableEndpoint edgeDst, EdgeStatus status);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AlertBatcherAsync: broadcasting batch with {Count} messages")]
    public partial void AlertBatcherBroadcast(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AlertBatcherAsync: exiting due to cancellation")]
    public partial void AlertBatcherExit();

    [LoggerMessage(Level = LogLevel.Debug, Message = "ExtractJoinerUuidAndMetadata: saved UUID and metadata for joiner {Joiner}")]
    public partial void ExtractJoinerUuidAndMetadata(LoggableEndpoint joiner);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: creating {Count} failure detectors for subjects")]
    public partial void CreateFailureDetectors(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: monitoring subjects={Subjects}, configId={ConfigId}")]
    public partial void CreateFailureDetectorsSummary(LoggableEndpoints subjects, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: created detector for subject {Subject}, ringNumber={RingNumber}")]
    public partial void CreatedFailureDetector(LoggableEndpoint subject, int ringNumber);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: skipping, this node is no longer in the ring")]
    public partial void SkippingFailureDetectorsNotInRing();

    [LoggerMessage(Level = LogLevel.Warning, Message = "ConsensusCoordinator Decided task faulted, resetting consensus state to allow future proposals")]
    public partial void ConsensusDecidedFaulted(Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "EdgeFailureNotification: scheduling callback for subject {Subject}, configId={ConfigId}")]
    public partial void EdgeFailureNotificationScheduled(LoggableEndpoint subject, long configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "EdgeFailureNotification: enqueueing DOWN alert for subject {Subject}, ringNumbers={RingNumbers}")]
    public partial void EdgeFailureNotificationEnqueued(LoggableEndpoint subject, LoggableRingNumbers ringNumbers);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dispose: disposing MembershipService resources")]
    public partial void Dispose();

    [LoggerMessage(Level = LogLevel.Debug, Message = "ConsensusCoordinator decided continuation skipped due to shutdown")]
    public partial void ConsensusDecidedSkippedShutdown();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Buffering consensus message {MessageType} for future config {FutureConfigId}, current config is {CurrentConfigId}")]
    public partial void BufferingFutureConsensusMessage(RapidClusterRequest.ContentOneofCase messageType, long futureConfigId, long currentConfigId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Replaying {Count} buffered consensus messages for config {ConfigId}")]
    public partial void ReplayingBufferedMessages(int count, long configId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {MyAddr} detected it has been kicked from the cluster. Remote config version {RemoteConfigVersion} > local {LocalConfigVersion}")]
    public partial void NodeKicked(LoggableEndpoint myAddr, long remoteConfigVersion, long localConfigVersion);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {MyAddr} starting rejoin process")]
    public partial void StartingRejoin(LoggableEndpoint myAddr);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {MyAddr} attempting rejoin through seed {Seed}")]
    public partial void AttemptingRejoinThroughSeed(LoggableEndpoint myAddr, LoggableEndpoint seed);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {MyAddr} rejoin PreJoin response: {StatusCode}, observers: {ObserverCount}")]
    public partial void RejoinPreJoinResponse(LoggableEndpoint myAddr, JoinStatusCode statusCode, int observerCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {MyAddr} successfully rejoined cluster with {MemberCount} members, configId={ConfigId}")]
    public partial void RejoinSuccessful(LoggableEndpoint myAddr, int memberCount, long configId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Node {MyAddr} failed to rejoin through seed {Seed}: {Message}")]
    public partial void RejoinFailedThroughSeed(LoggableEndpoint myAddr, LoggableEndpoint seed, string message);

    [LoggerMessage(Level = LogLevel.Error, Message = "Node {MyAddr} failed to rejoin cluster - no live seeds found")]
    public partial void RejoinFailedNoSeeds(LoggableEndpoint myAddr);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {MyAddr} rejoin skipped (already rejoining or disposed)")]
    public partial void RejoinSkipped(LoggableEndpoint myAddr);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {MyAddr} waiting {WaitTime} before rejoin attempt (backoff attempt {AttemptCount})")]
    public partial void RejoinBackoffWaiting(LoggableEndpoint myAddr, TimeSpan waitTime, int attemptCount);

    [LoggerMessage(Level = LogLevel.Information, Message = "Starting new cluster at {MyAddr}")]
    public partial void StartingNewCluster(LoggableEndpoint myAddr);

    [LoggerMessage(Level = LogLevel.Information, Message = "Joining cluster through seed {Seed} at {MyAddr}")]
    public partial void JoiningCluster(LoggableEndpoint seed, LoggableEndpoint myAddr);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Join attempt {Attempt} failed: {Message}. Retrying in {DelayMs}ms")]
    public partial void JoinRetry(int attempt, string message, double delayMs);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to join cluster after {Attempts} attempts")]
    public partial void JoinFailed(int attempts);

    [LoggerMessage(Level = LogLevel.Information, Message = "Join completed via learner protocol for {MyAddr}, already in membership at config {ConfigId}")]
    public partial void JoinCompletedViaLearnerProtocol(LoggableEndpoint myAddr, CurrentConfigId configId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Stale view detected from {RemoteEndpoint}: remote config {RemoteConfigId} > local config {LocalConfigId}. Requesting updated view.")]
    public partial void StaleViewDetected(LoggableEndpoint remoteEndpoint, long remoteConfigId, long localConfigId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Requesting membership view from {RemoteEndpoint}")]
    public partial void RequestingMembershipView(LoggableEndpoint remoteEndpoint);

    [LoggerMessage(Level = LogLevel.Information, Message = "Successfully refreshed membership view from {RemoteEndpoint}: new config {NewConfigId}, {MemberCount} members")]
    public partial void MembershipViewRefreshed(LoggableEndpoint remoteEndpoint, long newConfigId, int memberCount);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to refresh membership view from {RemoteEndpoint}: {Message}")]
    public partial void MembershipViewRefreshFailed(LoggableEndpoint remoteEndpoint, string message);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping stale view refresh - already in progress or received config {ReceivedConfigId} not newer than local {LocalConfigId}")]
    public partial void SkippingStaleViewRefresh(long receivedConfigId, long localConfigId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Waiting for {Count} background tasks to complete")]
    public partial void WaitingForBackgroundTasks(int count);
}
