using Grpc.Core;
using RapidCluster.Pb;

namespace RapidCluster.Messaging;

/// <summary>
/// gRPC service implementation for Rapid membership protocol.
/// </summary>
internal sealed class MembershipServiceImpl(IMembershipServiceHandler handler) : Pb.MembershipService.MembershipServiceBase
{
    public override async Task<RapidClusterResponse> SendRequest(RapidClusterRequest request, ServerCallContext context) => await handler.HandleMessageAsync(request, context.CancellationToken).ConfigureAwait(true);
}
