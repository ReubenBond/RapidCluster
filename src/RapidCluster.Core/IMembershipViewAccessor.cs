namespace RapidCluster;

/// <summary>
/// Provides read-only access to the current membership view and view change notifications.
/// This interface is used internally - MembershipService publishes view changes to the implementation of this interface.
/// </summary>
internal interface IMembershipViewAccessor
{
    /// <summary>
    /// Gets the current membership view.
    /// </summary>
    MembershipView CurrentView { get; }

    /// <summary>
    /// Gets subscribes to view changes, returning an async enumerable of subsequently decided views.
    /// The enumerable will yield a new MembershipView each time consensus is reached on a view change.
    /// </summary>
    /// <returns>An async enumerable of MembershipView instances.</returns>
    BroadcastChannelReader<MembershipView> Updates { get; }
}
