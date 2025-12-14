using System.Diagnostics;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Represents a cluster membership status change.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class ClusterStatusChange(
    long configurationId,
    IReadOnlyList<Endpoint> membership,
    IReadOnlyList<NodeStatusChange> delta)
{
    public long ConfigurationId { get; } = configurationId;
    public IReadOnlyList<Endpoint> Membership { get; } = membership;
    public IReadOnlyList<NodeStatusChange> Delta { get; } = delta;

    public override string ToString()
    {
        return $"ClusterStatusChange{{configurationId={ConfigurationId}, " +
               $"membership={string.Join(",", Membership)}, delta={string.Join(",", Delta)}}}";
    }

    private string DebuggerDisplay => $"ClusterStatusChange(Config={ConfigurationId}, Members={Membership.Count}, Delta={Delta.Count})";
}
