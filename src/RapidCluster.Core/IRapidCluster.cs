namespace RapidCluster;

/// <summary>
/// Provides access to the Rapid cluster membership information.
/// Inject this interface to interact with the cluster.
/// </summary>
public interface IRapidCluster
{
    /// <summary>
    /// Gets the current membership view.
    /// </summary>
    ClusterMembershipView CurrentView { get; }

    /// <summary>
    /// Gets a reader for subscribing to view changes.
    /// Yields a new ClusterMembershipView each time consensus is reached.
    /// </summary>
    BroadcastChannelReader<ClusterMembershipView> ViewUpdates { get; }
}
