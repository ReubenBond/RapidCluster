namespace RapidCluster;

/// <summary>
/// Represents the status of a node in the cluster.
/// </summary>
public enum NodeStatus
{
    /// <summary>
    /// The node has joined and is available in the cluster.
    /// </summary>
    Available,

    /// <summary>
    /// The node has left or been removed from the cluster.
    /// </summary>
    Unavailable,
}
