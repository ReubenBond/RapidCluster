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

    public void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        foreach (var member in _membership)
        {
            _client.SendOneWayMessage(
                member,
                request,
                rank,
                onDeliveryFailure != null ? (ep, _) => onDeliveryFailure(ep, rank ?? throw new InvalidOperationException("Rank required when onDeliveryFailure is provided.")) : null,
                cancellationToken);
        }
    }
}
