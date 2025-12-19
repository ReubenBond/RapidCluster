namespace RapidCluster;

/// <summary>
/// Implementation of IRapidCluster that provides access to the cluster membership view.
/// This class bridges between the internal MembershipView and the public ClusterMembershipView types.
/// </summary>
internal sealed class RapidClusterImpl : IRapidCluster, IDisposable
{
    private readonly MembershipViewAccessor _viewAccessor;
    private readonly BroadcastChannel<ClusterMembershipView> _publicViewChannel = new();
    private readonly CancellationTokenSource _cts = new();

    public RapidClusterImpl(MembershipViewAccessor viewAccessor)
    {
        _viewAccessor = viewAccessor;

        // Publish initial empty view
        _publicViewChannel.Publish(ClusterMembershipView.Empty);

        // Subscribe to internal view changes and transform to public views
        _ = SubscribeToViewChangesAsync(_cts.Token);
    }

    /// <inheritdoc/>
    public ClusterMembershipView CurrentView => _publicViewChannel.Current.Value;

    /// <inheritdoc/>
    public BroadcastChannelReader<ClusterMembershipView> ViewUpdates => _publicViewChannel.Reader;

    private async Task SubscribeToViewChangesAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var internalView in _viewAccessor.Updates.WithCancellation(cancellationToken))
            {
                var publicView = internalView.ToClusterMembershipView();
                _publicViewChannel.Publish(publicView);
            }
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
    }

    public void Dispose()
    {
        _cts.Cancel();
        _cts.Dispose();
        _publicViewChannel.Dispose();
    }
}
