namespace RapidCluster;

/// <summary>
/// Event types to subscribe from the cluster.
/// </summary>
public enum ClusterEvents
{
    /// <summary>When a node announces a proposal using the multi node cut detector.</summary>
    ViewChangeProposal,

    /// <summary>When a fast-paxos quorum of identical proposals were received.</summary>
    ViewChange,

    /// <summary>When a fast-paxos quorum of identical proposals is unavailable.</summary>
    ViewChangeOneStepFailed,

    /// <summary>When a node detects that it has been removed from the network.</summary>
    Kicked
}

/// <summary>
/// Represents a cluster event notification combining the event type and the status change details.
/// </summary>
/// <param name="Event">The type of cluster event that occurred.</param>
/// <param name="Change">The status change details associated with this event.</param>
public readonly record struct ClusterEventNotification(ClusterEvents Event, ClusterStatusChange Change);
