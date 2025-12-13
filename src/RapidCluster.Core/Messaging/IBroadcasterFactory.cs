namespace RapidCluster.Messaging;

/// <summary>
/// Factory for creating IBroadcaster instances.
/// This exists to support DI patterns and avoid direct instantiation of broadcasters.
/// </summary>
public interface IBroadcasterFactory
{
    /// <summary>
    /// Creates a new broadcaster instance.
    /// </summary>
    /// <returns>A new IBroadcaster instance.</returns>
    IBroadcaster Create();
}

/// <summary>
/// Default factory implementation that creates UnicastToAllBroadcaster instances.
/// </summary>
internal sealed class UnicastToAllBroadcasterFactory(IMessagingClient messagingClient) : IBroadcasterFactory
{
    public IBroadcaster Create() => new UnicastToAllBroadcaster(messagingClient);
}
