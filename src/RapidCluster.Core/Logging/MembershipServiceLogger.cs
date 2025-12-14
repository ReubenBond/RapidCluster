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

    [LoggerMessage(EventName = nameof(InitiatingConsensus), Level = LogLevel.Debug, Message = "Initiating consensus for {Proposal}")]
    private partial void InitiatingConsensusCore(LoggableEndpoints proposal);
    public void InitiatingConsensus(IEnumerable<Endpoint> proposal) => InitiatingConsensusCore(new(proposal));

    [LoggerMessage(EventName = nameof(ReceivedLeaveMessage), Level = LogLevel.Information, Message = "Received leave message from {Sender} at {MyAddr}")]
    private partial void ReceivedLeaveMessageCore(LoggableEndpoint sender, LoggableEndpoint myAddr);
    public void ReceivedLeaveMessage(Endpoint sender, Endpoint myAddr) => ReceivedLeaveMessageCore(new(sender), new(myAddr));

    [LoggerMessage(EventName = nameof(RemovingNode), Level = LogLevel.Debug, Message = "Removing node {Node}")]
    private partial void RemovingNodeCore(LoggableEndpoint node);
    public void RemovingNode(Endpoint node) => RemovingNodeCore(new(node));

    [LoggerMessage(EventName = nameof(DecidedNodeWithoutUuid), Level = LogLevel.Warning, Message = "Decided on a node without UUID: {Node}")]
    private partial void DecidedNodeWithoutUuidCore(LoggableEndpoint node);
    public void DecidedNodeWithoutUuid(Endpoint node) => DecidedNodeWithoutUuidCore(new(node));

    [LoggerMessage(EventName = nameof(AddingNode), Level = LogLevel.Debug, Message = "Adding node {Node}")]
    private partial void AddingNodeCore(LoggableEndpoint node);
    public void AddingNode(Endpoint node) => AddingNodeCore(new(node));

    [LoggerMessage(EventName = nameof(LeavingWithObservers), Level = LogLevel.Information, Message = "Leaving: {MyAddr} has {Count} observers: {Observers}")]
    private partial void LeavingWithObserversCore(LoggableEndpoint myAddr, int count, LoggableEndpoints observers);
    public void LeavingWithObservers(Endpoint myAddr, int count, IEnumerable<Endpoint> observers) => LeavingWithObserversCore(new(myAddr), count, new(observers));

    [LoggerMessage(Level = LogLevel.Trace, Message = "Timeout while leaving")]
    public partial void TimeoutWhileLeaving();

    [LoggerMessage(Level = LogLevel.Trace, Message = "Exception while leaving")]
    public partial void ExceptionWhileLeaving(Exception ex);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Node was already removed prior to leaving")]
    public partial void NodeAlreadyRemoved();

    [LoggerMessage(EventName = nameof(IgnoringOldConfigNotification), Level = LogLevel.Information, Message = "Ignoring failure notification from old configuration {Subject}, config: {CurrentConfig}, oldConfiguration: {OldConfig}")]
    private partial void IgnoringOldConfigNotificationCore(LoggableEndpoint subject, LoggableConfigurationId currentConfig, long oldConfig);
    public void IgnoringOldConfigNotification(Endpoint subject, MembershipView currentView, long oldConfig) => IgnoringOldConfigNotificationCore(new(subject), new(currentView), oldConfig);

    [LoggerMessage(EventName = nameof(AnnouncingEdgeFail), Level = LogLevel.Debug, Message = "Announcing EdgeFail event {Subject}, observer: {MyAddr}, config: {Config}, size: {Size}")]
    private partial void AnnouncingEdgeFailCore(LoggableEndpoint subject, LoggableEndpoint myAddr, long config, LoggableMembershipSize size);
    public void AnnouncingEdgeFail(Endpoint subject, Endpoint myAddr, long config, MembershipView view) => AnnouncingEdgeFailCore(new(subject), new(myAddr), config, new(view));

    [LoggerMessage(EventName = nameof(ErrorInEdgeFailureNotification), Level = LogLevel.Error, Message = "Error in EdgeFailureNotification for {Subject}")]
    private partial void ErrorInEdgeFailureNotificationCore(Exception ex, LoggableEndpoint subject);
    public void ErrorInEdgeFailureNotification(Exception ex, Endpoint subject) => ErrorInEdgeFailureNotificationCore(ex, new(subject));

    [LoggerMessage(EventName = nameof(JoinAtSeed), Level = LogLevel.Information, Message = "Join at seed for {{seed:{Seed}, sender:{Sender}, config:{Config}, size:{Size}}}")]
    private partial void JoinAtSeedCore(LoggableEndpoint seed, LoggableEndpoint sender, LoggableConfigurationId config, LoggableMembershipSize size);
    public void JoinAtSeed(Endpoint seed, Endpoint sender, MembershipView view) => JoinAtSeedCore(new(seed), new(sender), new(view), new(view));

    [LoggerMessage(EventName = nameof(EnqueueingSafeToJoin), Level = LogLevel.Trace, Message = "Enqueuing SAFE_TO_JOIN for {{sender:{Sender}, config:{Config}, size:{Size}}}")]
    private partial void EnqueueingSafeToJoinCore(LoggableEndpoint sender, LoggableConfigurationId config, LoggableMembershipSize size);
    public void EnqueueingSafeToJoin(Endpoint sender, MembershipView view) => EnqueueingSafeToJoinCore(new(sender), new(view), new(view));

    [LoggerMessage(EventName = nameof(WrongConfiguration), Level = LogLevel.Information, Message = "Wrong configuration for {{sender:{Sender}, config:{Config}, myConfig:{MyConfig}, size:{Size}}}")]
    private partial void WrongConfigurationCore(LoggableEndpoint sender, long config, LoggableConfigurationId myConfig, LoggableMembershipSize size);
    public void WrongConfiguration(Endpoint sender, long config, MembershipView view) => WrongConfigurationCore(new(sender), config, new(view), new(view));

    [LoggerMessage(EventName = nameof(MembershipServiceInitialized), Level = LogLevel.Debug, Message = "MembershipService initialized: myAddr={MyAddr}, configId={ConfigId}, membershipSize={MembershipSize}")]
    private partial void MembershipServiceInitializedCore(LoggableEndpoint myAddr, LoggableConfigurationId configId, LoggableMembershipSize membershipSize);
    public void MembershipServiceInitialized(Endpoint myAddr, MembershipView view) => MembershipServiceInitializedCore(new(myAddr), new(view), new(view));

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleMessageAsync: received {MessageType} from request")]
    public partial void HandleMessageReceivedDebug(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(Level = LogLevel.Trace, Message = "HandleMessageAsync: received {MessageType} from request")]
    public partial void HandleMessageReceivedTrace(RapidClusterRequest.ContentOneofCase messageType);

    [LoggerMessage(EventName = nameof(HandlePreJoinResult), Level = LogLevel.Debug, Message = "HandlePreJoinMessage: joiner={Joiner}, statusCode={StatusCode}, observers count={ObserversCount}")]
    private partial void HandlePreJoinResultCore(LoggableEndpoint joiner, JoinStatusCode statusCode, int observersCount);
    public void HandlePreJoinResult(Endpoint joiner, JoinStatusCode statusCode, int observersCount) => HandlePreJoinResultCore(new(joiner), statusCode, observersCount);

    [LoggerMessage(EventName = nameof(HandleJoinMessage), Level = LogLevel.Debug, Message = "HandleJoinMessageAsync: processing join from {Sender}, configId={ConfigId}")]
    private partial void HandleJoinMessageCore(LoggableEndpoint sender, long configId);
    public void HandleJoinMessage(Endpoint sender, long configId) => HandleJoinMessageCore(new(sender), configId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleJoinMessageAsync: joiner already in ring, responding SAFE_TO_JOIN")]
    public partial void JoinerAlreadyInRing();

    [LoggerMessage(EventName = nameof(HandleBatchedAlertMessage), Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: received batch with {Count} messages from {Sender}")]
    private partial void HandleBatchedAlertMessageCore(int count, LoggableEndpoint sender);
    public void HandleBatchedAlertMessage(int count, Endpoint sender) => HandleBatchedAlertMessageCore(count, new(sender));

    [LoggerMessage(EventName = nameof(BatchedAlertFiltered), Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: filtered out messages not matching current config {ConfigId}")]
    private partial void BatchedAlertFilteredCore(LoggableConfigurationId configId);
    public void BatchedAlertFiltered(MembershipView view) => BatchedAlertFilteredCore(new(view));

    [LoggerMessage(EventName = nameof(ProcessingAlert), Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: processing alert edgeSrc={EdgeSrc}, edgeDst={EdgeDst}, status={Status}")]
    private partial void ProcessingAlertCore(LoggableEndpoint edgeSrc, LoggableEndpoint edgeDst, EdgeStatus status);
    public void ProcessingAlert(Endpoint edgeSrc, Endpoint edgeDst, EdgeStatus status) => ProcessingAlertCore(new(edgeSrc), new(edgeDst), status);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: cut detection returned {Count} proposals")]
    public partial void CutDetectionProposals(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleBatchedAlertMessage: implicit edge invalidation returned {Count} proposals")]
    public partial void ImplicitEdgeInvalidation(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "HandleConsensusMessages: forwarding to ConsensusCoordinator instance")]
    public partial void HandleConsensusMessages();

    [LoggerMessage(Level = LogLevel.Trace, Message = "HandleProbeMessage: responding to probe")]
    public partial void HandleProbeMessage();

    [LoggerMessage(EventName = nameof(HandleMembershipViewRequest), Level = LogLevel.Debug, Message = "HandleMembershipViewRequest: sender={Sender} requested view (their config={TheirConfig}, our config={OurConfig})")]
    private partial void HandleMembershipViewRequestCore(LoggableEndpoint sender, long theirConfig, LoggableConfigurationId ourConfig);
    public void HandleMembershipViewRequest(Endpoint sender, long theirConfig, MembershipView view) => HandleMembershipViewRequestCore(new(sender), theirConfig, new(view));

    [LoggerMessage(Level = LogLevel.Debug, Message = "DecideViewChange: processing {Count} nodes in proposal")]
    public partial void DecideViewChange(int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "DecideViewChange: ignoring stale consensus decision with {Count} nodes (consensus instance is no longer current)")]
    public partial void IgnoringStaleConsensusDecision(int count);

    [LoggerMessage(EventName = nameof(NotifyingJoiners), Level = LogLevel.Debug, Message = "DecideViewChange: notifying {Count} joiners waiting through us for node {Node}")]
    private partial void NotifyingJoinersCore(int count, LoggableEndpoint node);
    public void NotifyingJoiners(int count, Endpoint node) => NotifyingJoinersCore(count, new(node));

    [LoggerMessage(Level = LogLevel.Debug, Message = "DecideViewChange: recreated cut detector, updated broadcaster, recreated failure detectors")]
    public partial void DecideViewChangeCleanup();

    [LoggerMessage(EventName = nameof(PublishingViewChange), Level = LogLevel.Debug, Message = "DecideViewChange: publishing VIEW_CHANGE event, configId={ConfigId}, membershipSize={MembershipSize}")]
    private partial void PublishingViewChangeCore(LoggableConfigurationId configId, LoggableMembershipSize membershipSize);
    public void PublishingViewChange(MembershipView view) => PublishingViewChangeCore(new(view), new(view));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Shutdown: cancelling background tasks and disposing failure detectors")]
    public partial void Shutdown();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stopping: sending leave messages and waiting for background tasks")]
    public partial void Stopping();

    [LoggerMessage(EventName = nameof(EnqueueAlertMessage), Level = LogLevel.Debug, Message = "EnqueueAlertMessage: queued alert edgeSrc={EdgeSrc}, edgeDst={EdgeDst}, status={Status}")]
    private partial void EnqueueAlertMessageCore(LoggableEndpoint edgeSrc, LoggableEndpoint edgeDst, EdgeStatus status);
    public void EnqueueAlertMessage(Endpoint edgeSrc, Endpoint edgeDst, EdgeStatus status) => EnqueueAlertMessageCore(new(edgeSrc), new(edgeDst), status);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AlertBatcherAsync: broadcasting batch with {Count} messages")]
    public partial void AlertBatcherBroadcast(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AlertBatcherAsync: exiting due to cancellation")]
    public partial void AlertBatcherExit();

    [LoggerMessage(EventName = nameof(ExtractJoinerUuidAndMetadata), Level = LogLevel.Debug, Message = "ExtractJoinerUuidAndMetadata: saved UUID and metadata for joiner {Joiner}")]
    private partial void ExtractJoinerUuidAndMetadataCore(LoggableEndpoint joiner);
    public void ExtractJoinerUuidAndMetadata(Endpoint joiner) => ExtractJoinerUuidAndMetadataCore(new(joiner));

    [LoggerMessage(Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: creating {Count} failure detectors for subjects")]
    public partial void CreateFailureDetectors(int count);

    [LoggerMessage(EventName = nameof(CreateFailureDetectorsSummary), Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: monitoring subjects={Subjects}, configId={ConfigId}")]
    private partial void CreateFailureDetectorsSummaryCore(LoggableEndpoints subjects, long configId);
    public void CreateFailureDetectorsSummary(IEnumerable<Endpoint> subjects, long configId) => CreateFailureDetectorsSummaryCore(new(subjects), configId);

    [LoggerMessage(EventName = nameof(CreatedFailureDetector), Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: created detector for subject {Subject}, ringNumber={RingNumber}")]
    private partial void CreatedFailureDetectorCore(LoggableEndpoint subject, int ringNumber);
    public void CreatedFailureDetector(Endpoint subject, int ringNumber) => CreatedFailureDetectorCore(new(subject), ringNumber);

    [LoggerMessage(Level = LogLevel.Debug, Message = "CreateFailureDetectorsForCurrentConfiguration: skipping, this node is no longer in the ring")]
    public partial void SkippingFailureDetectorsNotInRing();

    [LoggerMessage(Level = LogLevel.Warning, Message = "ConsensusCoordinator Decided task faulted, resetting consensus state to allow future proposals")]
    public partial void ConsensusDecidedFaulted(Exception ex);

    [LoggerMessage(EventName = nameof(EdgeFailureNotificationScheduled), Level = LogLevel.Debug, Message = "EdgeFailureNotification: scheduling callback for subject {Subject}, configId={ConfigId}")]
    private partial void EdgeFailureNotificationScheduledCore(LoggableEndpoint subject, long configId);
    public void EdgeFailureNotificationScheduled(Endpoint subject, long configId) => EdgeFailureNotificationScheduledCore(new(subject), configId);

    [LoggerMessage(EventName = nameof(EdgeFailureNotificationEnqueued), Level = LogLevel.Debug, Message = "EdgeFailureNotification: enqueueing DOWN alert for subject {Subject}, ringNumbers={RingNumbers}")]
    private partial void EdgeFailureNotificationEnqueuedCore(LoggableEndpoint subject, LoggableRingNumbers ringNumbers);
    public void EdgeFailureNotificationEnqueued(Endpoint subject, IEnumerable<int> ringNumbers) => EdgeFailureNotificationEnqueuedCore(new(subject), new(ringNumbers));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Dispose: disposing MembershipService resources")]
    public partial void Dispose();

    [LoggerMessage(Level = LogLevel.Debug, Message = "ConsensusCoordinator decided continuation skipped due to shutdown")]
    public partial void ConsensusDecidedSkippedShutdown();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Buffering consensus message {MessageType} for future config {FutureConfigId}, current config is {CurrentConfigId}")]
    public partial void BufferingFutureConsensusMessage(RapidClusterRequest.ContentOneofCase messageType, long futureConfigId, long currentConfigId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Replaying {Count} buffered consensus messages for config {ConfigId}")]
    public partial void ReplayingBufferedMessages(int count, long configId);

    [LoggerMessage(EventName = nameof(NodeKicked), Level = LogLevel.Warning, Message = "Node {MyAddr} detected it has been kicked from the cluster. Remote config version {RemoteConfigVersion} > local {LocalConfigVersion}")]
    private partial void NodeKickedCore(LoggableEndpoint myAddr, long remoteConfigVersion, long localConfigVersion);
    public void NodeKicked(Endpoint myAddr, long remoteConfigVersion, long localConfigVersion) => NodeKickedCore(new(myAddr), remoteConfigVersion, localConfigVersion);

    [LoggerMessage(EventName = nameof(StartingRejoin), Level = LogLevel.Information, Message = "Node {MyAddr} starting rejoin process")]
    private partial void StartingRejoinCore(LoggableEndpoint myAddr);
    public void StartingRejoin(Endpoint myAddr) => StartingRejoinCore(new(myAddr));

    [LoggerMessage(EventName = nameof(AttemptingRejoinThroughSeed), Level = LogLevel.Information, Message = "Node {MyAddr} attempting rejoin through seed {Seed}")]
    private partial void AttemptingRejoinThroughSeedCore(LoggableEndpoint myAddr, LoggableEndpoint seed);
    public void AttemptingRejoinThroughSeed(Endpoint myAddr, Endpoint seed) => AttemptingRejoinThroughSeedCore(new(myAddr), new(seed));

    [LoggerMessage(EventName = nameof(RejoinPreJoinResponse), Level = LogLevel.Debug, Message = "Node {MyAddr} rejoin PreJoin response: {StatusCode}, observers: {ObserverCount}")]
    private partial void RejoinPreJoinResponseCore(LoggableEndpoint myAddr, JoinStatusCode statusCode, int observerCount);
    public void RejoinPreJoinResponse(Endpoint myAddr, JoinStatusCode statusCode, int observerCount) => RejoinPreJoinResponseCore(new(myAddr), statusCode, observerCount);

    [LoggerMessage(EventName = nameof(RejoinSuccessful), Level = LogLevel.Information, Message = "Node {MyAddr} successfully rejoined cluster with {MemberCount} members, configId={ConfigId}")]
    private partial void RejoinSuccessfulCore(LoggableEndpoint myAddr, int memberCount, long configId);
    public void RejoinSuccessful(Endpoint myAddr, int memberCount, long configId) => RejoinSuccessfulCore(new(myAddr), memberCount, configId);

    [LoggerMessage(EventName = nameof(RejoinFailedThroughSeed), Level = LogLevel.Warning, Message = "Node {MyAddr} failed to rejoin through seed {Seed}: {Message}")]
    private partial void RejoinFailedThroughSeedCore(LoggableEndpoint myAddr, LoggableEndpoint seed, string message);
    public void RejoinFailedThroughSeed(Endpoint myAddr, Endpoint seed, string message) => RejoinFailedThroughSeedCore(new(myAddr), new(seed), message);

    [LoggerMessage(EventName = nameof(RejoinFailedNoSeeds), Level = LogLevel.Error, Message = "Node {MyAddr} failed to rejoin cluster - no live seeds found")]
    private partial void RejoinFailedNoSeedsCore(LoggableEndpoint myAddr);
    public void RejoinFailedNoSeeds(Endpoint myAddr) => RejoinFailedNoSeedsCore(new(myAddr));

    [LoggerMessage(EventName = nameof(RejoinSkipped), Level = LogLevel.Debug, Message = "Node {MyAddr} rejoin skipped (already rejoining or disposed)")]
    private partial void RejoinSkippedCore(LoggableEndpoint myAddr);
    public void RejoinSkipped(Endpoint myAddr) => RejoinSkippedCore(new(myAddr));

    [LoggerMessage(EventName = nameof(RejoinBackoffWaiting), Level = LogLevel.Debug, Message = "Node {MyAddr} waiting {WaitTime} before rejoin attempt (backoff attempt {AttemptCount})")]
    private partial void RejoinBackoffWaitingCore(LoggableEndpoint myAddr, TimeSpan waitTime, int attemptCount);
    public void RejoinBackoffWaiting(Endpoint myAddr, TimeSpan waitTime, int attemptCount) => RejoinBackoffWaitingCore(new(myAddr), waitTime, attemptCount);

    [LoggerMessage(EventName = nameof(StartingNewCluster), Level = LogLevel.Information, Message = "Starting new cluster at {MyAddr}")]
    private partial void StartingNewClusterCore(LoggableEndpoint myAddr);
    public void StartingNewCluster(Endpoint myAddr) => StartingNewClusterCore(new(myAddr));

    [LoggerMessage(EventName = nameof(JoiningCluster), Level = LogLevel.Information, Message = "Joining cluster through seed {Seed} at {MyAddr}")]
    private partial void JoiningClusterCore(LoggableEndpoint seed, LoggableEndpoint myAddr);
    public void JoiningCluster(Endpoint seed, Endpoint myAddr) => JoiningClusterCore(new(seed), new(myAddr));

    [LoggerMessage(EventName = nameof(JoiningClusterWithSeeds), Level = LogLevel.Information, Message = "Joining cluster with {SeedCount} configured seed(s) at {MyAddr}")]
    private partial void JoiningClusterWithSeedsCore(int seedCount, LoggableEndpoint myAddr);
    public void JoiningClusterWithSeeds(int seedCount, Endpoint myAddr) => JoiningClusterWithSeedsCore(seedCount, new(myAddr));

    [LoggerMessage(EventName = nameof(JoinAttemptWithSeed), Level = LogLevel.Debug, Message = "Join attempt {Attempt} trying seed {Seed}")]
    private partial void JoinAttemptWithSeedCore(int attempt, LoggableEndpoint seed);
    public void JoinAttemptWithSeed(int attempt, Endpoint seed) => JoinAttemptWithSeedCore(attempt, new(seed));

    [LoggerMessage(Level = LogLevel.Warning, Message = "Join attempt {Attempt} failed: {Message}. Retrying in {DelayMs}ms")]
    public partial void JoinRetry(int attempt, string message, double delayMs);

    [LoggerMessage(Level = LogLevel.Error, Message = "Failed to join cluster after {Attempts} attempts")]
    public partial void JoinFailed(int attempts);

    [LoggerMessage(EventName = nameof(JoinCompletedViaLearnerProtocol), Level = LogLevel.Information, Message = "Join completed via learner protocol for {MyAddr}, already in membership at config {ConfigId}")]
    private partial void JoinCompletedViaLearnerProtocolCore(LoggableEndpoint myAddr, LoggableConfigurationId configId);
    public void JoinCompletedViaLearnerProtocol(Endpoint myAddr, MembershipView view) => JoinCompletedViaLearnerProtocolCore(new(myAddr), new(view));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Rejoin using {TotalCandidates} candidate seeds ({ConfiguredCount} from config, rest from last known view)")]
    public partial void RejoinWithCandidateSeeds(int totalCandidates, int configuredCount);

    [LoggerMessage(EventName = nameof(StaleViewDetected), Level = LogLevel.Information, Message = "Stale view detected from {RemoteEndpoint}: remote config {RemoteConfigId} > local config {LocalConfigId}. Requesting updated view.")]
    private partial void StaleViewDetectedCore(LoggableEndpoint remoteEndpoint, long remoteConfigId, long localConfigId);
    public void StaleViewDetected(Endpoint remoteEndpoint, long remoteConfigId, long localConfigId) => StaleViewDetectedCore(new(remoteEndpoint), remoteConfigId, localConfigId);

    [LoggerMessage(EventName = nameof(RequestingMembershipView), Level = LogLevel.Debug, Message = "Requesting membership view from {RemoteEndpoint}")]
    private partial void RequestingMembershipViewCore(LoggableEndpoint remoteEndpoint);
    public void RequestingMembershipView(Endpoint remoteEndpoint) => RequestingMembershipViewCore(new(remoteEndpoint));

    [LoggerMessage(EventName = nameof(MembershipViewRefreshed), Level = LogLevel.Information, Message = "Successfully refreshed membership view from {RemoteEndpoint}: new config {NewConfigId}, {MemberCount} members")]
    private partial void MembershipViewRefreshedCore(LoggableEndpoint remoteEndpoint, long newConfigId, int memberCount);
    public void MembershipViewRefreshed(Endpoint remoteEndpoint, long newConfigId, int memberCount) => MembershipViewRefreshedCore(new(remoteEndpoint), newConfigId, memberCount);

    [LoggerMessage(EventName = nameof(MembershipViewRefreshFailed), Level = LogLevel.Warning, Message = "Failed to refresh membership view from {RemoteEndpoint}: {Message}")]
    private partial void MembershipViewRefreshFailedCore(LoggableEndpoint remoteEndpoint, string message);
    public void MembershipViewRefreshFailed(Endpoint remoteEndpoint, string message) => MembershipViewRefreshFailedCore(new(remoteEndpoint), message);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Skipping stale view refresh - already in progress or received config {ReceivedConfigId} not newer than local {LocalConfigId}")]
    public partial void SkippingStaleViewRefresh(long receivedConfigId, long localConfigId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Waiting for {Count} background tasks to complete")]
    public partial void WaitingForBackgroundTasks(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Refreshed {Count} seeds from seed provider")]
    public partial void SeedsRefreshed(int count);

    [LoggerMessage(EventName = nameof(RefreshingSeedsForRejoin), Level = LogLevel.Information, Message = "Node {MyAddr} refreshing seeds from provider before retry")]
    private partial void RefreshingSeedsForRejoinCore(LoggableEndpoint myAddr);
    public void RefreshingSeedsForRejoin(Endpoint myAddr) => RefreshingSeedsForRejoinCore(new(myAddr));
}
