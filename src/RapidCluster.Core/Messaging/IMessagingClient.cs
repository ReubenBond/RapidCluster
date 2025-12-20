using RapidCluster.Pb;

namespace RapidCluster.Messaging;

/// <summary>
/// Delegate invoked when a one-way message fails to be delivered.
/// </summary>
/// <param name="remote">The endpoint that failed to receive the message.</param>
/// <param name="rank">The rank associated with the send attempt.</param>
public delegate void DeliveryFailureCallback(Endpoint remote, Rank rank);

/// <summary>
/// Interface for sending messages to remote nodes in the cluster.
/// </summary>
public interface IMessagingClient : IAsyncDisposable
{
    /// <summary>
    /// Sends a message to a remote node without waiting for a response.
    /// May retry on failures based on implementation.
    /// </summary>
    /// <param name="remote">The remote endpoint to send to.</param>
    /// <param name="request">The request message.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
    void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken) =>
        SendOneWayMessage(remote, request, rank: null, onDeliveryFailure: null, cancellationToken);

    /// <summary>
    /// Sends a message to a remote node without waiting for a response, with failure notification.
    /// </summary>
    /// <param name="remote">The remote endpoint to send to.</param>
    /// <param name="request">The request message.</param>
    /// <param name="rank">Rank associated with the send attempt. Required when <paramref name="onDeliveryFailure"/> is non-null.</param>
    /// <param name="onDeliveryFailure">Callback invoked if delivery fails. May be called from any thread.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, Rank? rank, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken);

    void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken) =>
        SendOneWayMessage(remote, request, rank: null, onDeliveryFailure, cancellationToken);

    /// <summary>
    /// Sends a message to a remote node and waits for a response.
    /// May retry on failures based on implementation.
    /// </summary>
    /// <param name="remote">The remote endpoint to send to.</param>
    /// <param name="request">The request message.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>The response from the remote node.</returns>
    /// <exception cref="OperationCanceledException">Thrown when the operation is cancelled.</exception>
    Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken);

    /// <summary>
    /// Sends a message to a remote node with best-effort delivery.
    /// Does not retry on failures.
    /// </summary>
    /// <param name="remote">The remote endpoint to send to.</param>
    /// <param name="request">The request message.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>The response from the remote node, or an error response.</returns>
    async Task<RapidClusterResponse> SendMessageBestEffortAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
    {
#pragma warning disable CA1031
        try
        {
            return await SendMessageAsync(remote, request, cancellationToken).ConfigureAwait(true);
        }
        catch
        {
            return RapidClusterResponse.Parser.ParseFrom([]);
        }
#pragma warning restore CA1031
    }
}
