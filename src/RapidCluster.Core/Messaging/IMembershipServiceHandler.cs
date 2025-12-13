using RapidCluster.Pb;

namespace RapidCluster.Messaging;

/// <summary>
/// Interface for handling incoming membership protocol messages.
/// </summary>
public interface IMembershipServiceHandler
{
    /// <summary>
    /// Handles an incoming Rapid protocol message and returns a response.
    /// </summary>
    /// <param name="request">The incoming request message.</param>
    /// <param name="cancellationToken">Token to cancel the operation.</param>
    /// <returns>The response message.</returns>
    Task<RapidClusterResponse> HandleMessageAsync(RapidClusterRequest request, CancellationToken cancellationToken);
}
