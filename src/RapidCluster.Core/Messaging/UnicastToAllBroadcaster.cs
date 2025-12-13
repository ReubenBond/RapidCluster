using RapidCluster.Pb;

namespace RapidCluster.Messaging;

public sealed class UnicastToAllBroadcaster(IMessagingClient client) : IBroadcaster
{
    private readonly IMessagingClient _client = client;
    private IReadOnlyList<Endpoint> _membership = [];

    public void SetMembership(IReadOnlyList<Endpoint> membership) => _membership = membership;

    public void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken)
    {
        foreach (var member in _membership)
        {
            _client.SendOneWayMessage(member, request, cancellationToken);
        }
    }

    public void Broadcast(RapidClusterRequest request, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        foreach (var member in _membership)
        {
            _client.SendOneWayMessage(member, request, onDeliveryFailure != null ? ep => onDeliveryFailure(ep) : null, cancellationToken);
        }
    }
}
