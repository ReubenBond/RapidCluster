namespace RapidCluster;

/// <summary>
/// Default implementation of <see cref="IMembershipViewAccessor"/>.
/// Receives view updates from MembershipService and provides them to consumers.
/// </summary>
internal sealed class MembershipViewAccessor : IMembershipViewAccessor, IDisposable
{
    private readonly BroadcastChannel<MembershipView> _viewChangeChannel = new();

    public MembershipViewAccessor()
    {
        _viewChangeChannel.Publish(MembershipView.Empty);
    }

    /// <inheritdoc/>
    public MembershipView CurrentView => _viewChangeChannel.Current.Value;

    /// <inheritdoc/>
    public BroadcastChannelReader<MembershipView> Updates => _viewChangeChannel.Reader;

    /// <summary>
    /// Publishes a new view to all listeners. Called by MembershipService when consensus is reached.
    /// </summary>
    /// <param name="view">The new membership view.</param>
    internal void PublishView(MembershipView view)
    {
        ArgumentNullException.ThrowIfNull(view);
        _viewChangeChannel.Publish(view);
    }

    public void Dispose()
    {
        _viewChangeChannel.Dispose();
    }
}
