using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RapidCluster.Messaging;
using RapidCluster.Monitoring;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Factory for creating ConsensusCoordinator instances.
/// This exists because ConsensusCoordinator instances are created per consensus round and need runtime configuration.
/// </summary>
internal interface IConsensusCoordinatorFactory
{
    /// <summary>
    /// Creates a new ConsensusCoordinator instance for a consensus round.
    /// </summary>
    /// <param name="myAddr">The local endpoint.</param>
    /// <param name="configurationId">The current configuration ID.</param>
    /// <param name="membershipSize">The current membership size.</param>
    /// <param name="broadcaster">The broadcaster to use for message distribution.</param>
    /// <returns>A new ConsensusCoordinator instance.</returns>
    ConsensusCoordinator Create(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IBroadcaster broadcaster);
}

/// <summary>
/// Default implementation of IConsensusCoordinatorFactory.
/// </summary>
internal sealed class ConsensusCoordinatorFactory(
    IMessagingClient messagingClient,
    IMembershipViewAccessor membershipViewAccessor,
    IOptions<RapidClusterProtocolOptions> protocolOptions,
    SharedResources sharedResources,
    RapidClusterMetrics metrics,
    ILogger<ConsensusCoordinator> coordinatorLogger) : IConsensusCoordinatorFactory
{
    public ConsensusCoordinator Create(
        Endpoint myAddr,
        ConfigurationId configurationId,
        int membershipSize,
        IBroadcaster broadcaster)
    {
        return new ConsensusCoordinator(
            myAddr,
            configurationId,
            membershipSize,
            messagingClient,
            broadcaster,
            membershipViewAccessor,
            protocolOptions,
            sharedResources,
            metrics,
            coordinatorLogger);
    }
}
