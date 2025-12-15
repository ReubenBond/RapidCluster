using System.Diagnostics;
using System.Net;

namespace RapidCluster;

/// <summary>
/// Represents a member of the cluster with its endpoint, unique identifier, and metadata.
/// </summary>
/// <param name="EndPoint">The network endpoint of the member.</param>
/// <param name="Id">The unique monotonically increasing identifier assigned during join.</param>
/// <param name="Metadata">The metadata associated with this member.</param>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed record ClusterMember(EndPoint EndPoint, long Id, ClusterNodeMetadata Metadata)
{
    private string DebuggerDisplay => $"{EndPoint} (Id={Id})";
}
