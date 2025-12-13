using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Provides access to the Rapid cluster membership information.
/// Inject this interface to interact with the cluster.
/// </summary>
public interface IRapidCluster
{
    /// <summary>
    /// Returns the list of endpoints currently in the membership set.
    /// </summary>
    IReadOnlyList<Endpoint> GetMemberlist();

    /// <summary>
    /// Returns the number of endpoints currently in the membership set.
    /// </summary>
    int GetMembershipSize();

    /// <summary>
    /// Returns cluster metadata.
    /// </summary>
    Dictionary<Endpoint, Metadata> GetClusterMetadata();

    /// <summary>
    /// Gets the async enumerable for subscribing to cluster events.
    /// Each subscriber receives all events published after they start iterating.
    /// Uses the Orleans-style TaskCompletionSource chaining pattern for efficient broadcast.
    /// </summary>
    IAsyncEnumerable<ClusterEventNotification> EventStream { get; }

    /// <summary>
    /// Gets the observable for subscribing to cluster events.
    /// Multiple subscribers receive the same events through multicast.
    /// This is equivalent to using <see cref="EventStream"/> with Rx.NET's Publish() operator,
    /// but built-in to the channel implementation for convenience.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike <see cref="EventStream"/>, observable subscribers receive notifications
    /// synchronously when events are published (via OnNext), making it suitable for
    /// reactive event processing patterns.
    /// </para>
    /// <para>
    /// The observable inherently supports multiple observers without needing <c>Publish()</c>
    /// from Rx.NET because the underlying <see cref="BroadcastChannel{T}"/> implementation
    /// multicasts to all subscribers.
    /// </para>
    /// </remarks>
    IObservable<ClusterEventNotification> Events { get; }

    /// <summary>
    /// Gracefully leaves the cluster.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to cancel the operation.</param>
    Task LeaveGracefullyAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets the accessor for membership view information.
    /// This provides access to the current view and view change notifications.
    /// </summary>
    IMembershipViewAccessor ViewAccessor { get; }
}

/// <summary>
/// Implementation of IRapidCluster that delegates to the membership service and view accessor.
/// </summary>
internal sealed class RapidCluster(MembershipService membershipService, IMembershipViewAccessor viewAccessor) : IRapidCluster
{
    public IReadOnlyList<Endpoint> GetMemberlist() => viewAccessor.CurrentView.Members;

    public int GetMembershipSize() => viewAccessor.CurrentView.Size;

    public Dictionary<Endpoint, Metadata> GetClusterMetadata() => membershipService.GetMetadata();

    public IAsyncEnumerable<ClusterEventNotification> EventStream => membershipService.Events;

    public IObservable<ClusterEventNotification> Events => membershipService.Events;

    public async Task LeaveGracefullyAsync(CancellationToken cancellationToken = default)
    {
        await membershipService.StopAsync(cancellationToken).ConfigureAwait(true);
    }

    public IMembershipViewAccessor ViewAccessor => viewAccessor;
}
