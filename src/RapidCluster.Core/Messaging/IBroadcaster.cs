using RapidCluster.Pb;

namespace RapidCluster.Messaging;

/// <summary>
/// Delegate invoked when a broadcast message fails to be delivered to a recipient.
/// </summary>
/// <param name="failedEndpoint">The endpoint that failed to receive the message.</param>
/// <param name="rank">The rank associated with the broadcast.</param>
public delegate void BroadcastFailureCallback(Endpoint failedEndpoint, Rank rank);

/// <summary>
/// Interface for broadcasting messages to multiple nodes in the cluster.
/// </summary>
public interface IBroadcaster
{
    /// <summary>
    /// Updates the membership list for broadcast operations.
    /// </summary>
    /// <param name="membership">The current cluster membership.</param>
    void SetMembership(IReadOnlyList<Endpoint> membership);

    /// <summary>
    /// Broadcasts a message to all nodes in the membership (fire-and-forget).
    /// </summary>
    /// <param name="request">The request message to broadcast.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    void Broadcast(RapidClusterRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Broadcasts a message to all nodes in the membership with failure notification.
    /// The callback is invoked for each recipient where delivery fails.
    /// </summary>
    /// <param name="request">The request message to broadcast.</param>
    /// <param name="rank">Rank associated with the broadcast. Required when <paramref name="onDeliveryFailure"/> is non-null.</param>
    /// <param name="onDeliveryFailure">Callback invoked when delivery to a recipient fails. May be called from any thread.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    void Broadcast(RapidClusterRequest request, Rank? rank, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken);

    void Broadcast(RapidClusterRequest request, BroadcastFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) =>
        Broadcast(request, rank: null, onDeliveryFailure, cancellationToken);
}
