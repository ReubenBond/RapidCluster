using System.Threading.Channels;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Tracks state for a node that is attempting to join the cluster.
/// Consolidates response channels, metadata, and alert-sent tracking.
/// </summary>
internal sealed class JoinerInfo
{
    /// <summary>
    /// Gets channel of TaskCompletionSource instances for pending join requests from this joiner.
    /// Multiple requests can arrive if the joiner retries while waiting for consensus.
    /// </summary>
    public Channel<TaskCompletionSource<RapidClusterResponse>> ResponseChannel { get; } =
        Channel.CreateUnbounded<TaskCompletionSource<RapidClusterResponse>>();

    /// <summary>
    /// Gets or sets the joiner's metadata. Initially set from the JoinMessage, may be updated from AlertMessages.
    /// </summary>
    public Metadata Metadata { get; set; } = new();

    /// <summary>
    /// Gets or sets a value indicating whether whether an alert has been sent for this joiner. Used to prevent duplicate alerts
    /// when a joiner retries while consensus is in progress.
    /// </summary>
    public bool AlertSent { get; set; }
}
