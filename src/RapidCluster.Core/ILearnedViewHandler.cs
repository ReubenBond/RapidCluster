using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Interface for applying learned membership views.
/// This is used by the <see cref="MembershipRefreshCoordinator"/> to notify
/// the <see cref="MembershipService"/> when a new view has been learned from a remote node.
/// </summary>
internal interface ILearnedViewHandler
{
    /// <summary>
    /// Applies a learned membership view from a remote node.
    /// This is used by the Paxos learner mechanism to catch up on missed consensus decisions.
    /// </summary>
    /// <param name="viewResponse">The membership view response received from a remote node.</param>
    /// <returns>A task that completes when the view has been applied.</returns>
    ValueTask ApplyLearnedMembershipViewAsync(MembershipViewResponse viewResponse);
}
