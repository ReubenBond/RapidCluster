using Microsoft.Extensions.Logging;
using RapidCluster.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Bootstrap;

internal sealed partial class BootstrapCoordinatorLogger(ILogger logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger => _logger;

    [LoggerMessage(EventName = nameof(StartingSeedGossip), Level = LogLevel.Information, Message = "Starting seed gossip at {MyAddr} with {InitialSeedCount} initial seeds, bootstrapExpect={BootstrapExpect}")]
    private partial void StartingSeedGossipCore(LoggableEndpoint myAddr, int initialSeedCount, int bootstrapExpect);
    public void StartingSeedGossip(Endpoint myAddr, int initialSeedCount, int bootstrapExpect) => StartingSeedGossipCore(new(myAddr), initialSeedCount, bootstrapExpect);

    [LoggerMessage(EventName = nameof(GossipRoundStarting), Level = LogLevel.Debug, Message = "Gossip round {Round} starting at {MyAddr}, known seeds: {KnownSeeds}")]
    private partial void GossipRoundStartingCore(int round, LoggableEndpoint myAddr, LoggableEndpoints knownSeeds);
    public void GossipRoundStarting(int round, Endpoint myAddr, IEnumerable<Endpoint> knownSeeds) => GossipRoundStartingCore(round, new(myAddr), new(knownSeeds));

    [LoggerMessage(EventName = nameof(SendingGossipTo), Level = LogLevel.Debug, Message = "Sending seed gossip to {Target}, seedSetHash={SeedSetHash}")]
    private partial void SendingGossipToCore(LoggableEndpoint target, string seedSetHash);
    public void SendingGossipTo(Endpoint target, string seedSetHash) => SendingGossipToCore(new(target), seedSetHash);

    [LoggerMessage(EventName = nameof(ReceivedGossipResponse), Level = LogLevel.Debug, Message = "Received gossip response from {Sender}: status={Status}, seedCount={SeedCount}, hash={SeedSetHash}")]
    private partial void ReceivedGossipResponseCore(LoggableEndpoint sender, BootstrapStatus status, int seedCount, string seedSetHash);
    public void ReceivedGossipResponse(Endpoint sender, BootstrapStatus status, int seedCount, string seedSetHash) => ReceivedGossipResponseCore(new(sender), status, seedCount, seedSetHash);

    [LoggerMessage(EventName = nameof(MergedSeeds), Level = LogLevel.Debug, Message = "Merged seeds from {Sender}: added {AddedCount} new seeds, now have {TotalCount} seeds")]
    private partial void MergedSeedsCore(LoggableEndpoint sender, int addedCount, int totalCount);
    public void MergedSeeds(Endpoint sender, int addedCount, int totalCount) => MergedSeedsCore(new(sender), addedCount, totalCount);

    [LoggerMessage(EventName = nameof(SeedSetHashChanged), Level = LogLevel.Debug, Message = "Seed set hash changed from {OldHash} to {NewHash}")]
    public partial void SeedSetHashChanged(string oldHash, string newHash);

    [LoggerMessage(EventName = nameof(AgreementReached), Level = LogLevel.Information, Message = "Agreement reached at {MyAddr}: {AgreedCount}/{ExpectedCount} seeds agree on hash {SeedSetHash}")]
    private partial void AgreementReachedCore(LoggableEndpoint myAddr, int agreedCount, int expectedCount, string seedSetHash);
    public void AgreementReached(Endpoint myAddr, int agreedCount, int expectedCount, string seedSetHash) => AgreementReachedCore(new(myAddr), agreedCount, expectedCount, seedSetHash);

    [LoggerMessage(EventName = nameof(AlreadyFormedClusterDetected), Level = LogLevel.Information, Message = "Already-formed cluster detected from {Sender}, configurationId={ConfigId}")]
    private partial void AlreadyFormedClusterDetectedCore(LoggableEndpoint sender, LoggableConfigurationId configId);
    public void AlreadyFormedClusterDetected(Endpoint sender, Pb.ConfigurationId configId) => AlreadyFormedClusterDetectedCore(new(sender), new(configId));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Gossip to {Target} failed: {ErrorMessage}")]
    private partial void GossipFailedCore(LoggableEndpoint target, string errorMessage);
    public void GossipFailed(Endpoint target, string errorMessage) => GossipFailedCore(new(target), errorMessage);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Seed gossip timed out after {Timeout}. Only {ReachableCount}/{ExpectedCount} seeds reachable")]
    public partial void GossipTimeout(TimeSpan timeout, int reachableCount, int expectedCount);

    [LoggerMessage(EventName = nameof(FinalAgreedSeeds), Level = LogLevel.Information, Message = "Final agreed seed set: {Seeds}")]
    private partial void FinalAgreedSeedsCore(LoggableEndpoints seeds);
    public void FinalAgreedSeeds(IEnumerable<Endpoint> seeds) => FinalAgreedSeedsCore(new(seeds));

    [LoggerMessage(Level = LogLevel.Debug, Message = "Computing seed set hash for {SeedCount} seeds")]
    public partial void ComputingSeedSetHash(int seedCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Waiting {Delay} before next gossip round")]
    public partial void WaitingForNextRound(TimeSpan delay);

[LoggerMessage(EventName = nameof(AgreedResponseFromFormedCluster), Level = LogLevel.Information, Message = "Received AGREED response from already-formed cluster member {Sender}, configurationId={ConfigId} - completing bootstrap")]
    private partial void AgreedResponseFromFormedClusterCore(LoggableEndpoint sender, LoggableConfigurationId configId);
    public void AgreedResponseFromFormedCluster(Endpoint sender, Pb.ConfigurationId configId) => AgreedResponseFromFormedClusterCore(new(sender), new(configId));

    // New log methods for expanded BootstrapCoordinator functionality

    [LoggerMessage(EventName = nameof(StartingStaticSeedBootstrap), Level = LogLevel.Information, Message = "Starting static seed bootstrap at {MyAddr} with {SeedCount} seeds, bootstrapExpect={BootstrapExpect}")]
    private partial void StartingStaticSeedBootstrapCore(LoggableEndpoint myAddr, int seedCount, int bootstrapExpect);
    public void StartingStaticSeedBootstrap(Endpoint myAddr, int seedCount, int bootstrapExpect) => StartingStaticSeedBootstrapCore(new(myAddr), seedCount, bootstrapExpect);

    [LoggerMessage(EventName = nameof(StartingDynamicSeedBootstrap), Level = LogLevel.Information, Message = "Starting dynamic seed bootstrap at {MyAddr} with {SeedCount} initial seeds, bootstrapExpect={BootstrapExpect}")]
    private partial void StartingDynamicSeedBootstrapCore(LoggableEndpoint myAddr, int seedCount, int bootstrapExpect);
    public void StartingDynamicSeedBootstrap(Endpoint myAddr, int seedCount, int bootstrapExpect) => StartingDynamicSeedBootstrapCore(new(myAddr), seedCount, bootstrapExpect);

    [LoggerMessage(EventName = nameof(BootstrapComplete), Level = LogLevel.Information, Message = "Bootstrap complete at {MyAddr}: {MemberCount} members, configurationId={ConfigId}")]
    private partial void BootstrapCompleteCore(LoggableEndpoint myAddr, int memberCount, LoggableConfigurationId configId);
    public void BootstrapComplete(Endpoint myAddr, int memberCount, ConfigurationId configId) => BootstrapCompleteCore(new(myAddr), memberCount, new(configId.ToProtobuf()));

    [LoggerMessage(EventName = nameof(BootstrapProbeResult), Level = LogLevel.Debug, Message = "Bootstrap probe at {MyAddr}: {ReachableCount}/{ExpectedCount} seeds reachable")]
    private partial void BootstrapProbeResultCore(LoggableEndpoint myAddr, int reachableCount, int expectedCount);
    public void BootstrapProbeResult(Endpoint myAddr, int reachableCount, int expectedCount) => BootstrapProbeResultCore(new(myAddr), reachableCount, expectedCount);

    [LoggerMessage(EventName = nameof(BootstrapSeedsReady), Level = LogLevel.Information, Message = "Bootstrap seeds ready at {MyAddr}: {ReachableCount} seeds reachable")]
    private partial void BootstrapSeedsReadyCore(LoggableEndpoint myAddr, int reachableCount);
    public void BootstrapSeedsReady(Endpoint myAddr, int reachableCount) => BootstrapSeedsReadyCore(new(myAddr), reachableCount);

    [LoggerMessage(EventName = nameof(CreatedBootstrapProposal), Level = LogLevel.Debug, Message = "Created bootstrap proposal at {MyAddr}: {MemberCount} members, maxNodeId={MaxNodeId}")]
    private partial void CreatedBootstrapProposalCore(LoggableEndpoint myAddr, int memberCount, long maxNodeId);
    public void CreatedBootstrapProposal(Endpoint myAddr, int memberCount, long maxNodeId) => CreatedBootstrapProposalCore(new(myAddr), memberCount, maxNodeId);
}
