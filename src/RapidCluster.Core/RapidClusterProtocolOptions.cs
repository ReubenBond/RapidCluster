namespace RapidCluster;

/// <summary>
/// Configuration options for the Rapid protocol.
/// </summary>
/// <remarks>
/// <para>
/// The Rapid protocol uses a K-ring topology where each node monitors K subjects and is monitored
/// by K observers. This creates an expander graph with strong connectivity properties that ensures
/// healthy processes detect failures with high probability.
/// </para>
/// <para>
/// Multi-node cut detection uses two thresholds (H and L) to achieve almost-everywhere agreement:
/// <list type="bullet">
///   <item><description>H (HighWatermark): A subject is in "stable" mode when it has ≥H observer reports. This indicates high-fidelity failure detection.</description></item>
///   <item><description>L (LowWatermark): A subject enters "unstable" mode when it has between L and H reports. The system waits until all unstable subjects become stable before proposing a view change.</description></item>
/// </list>
/// </para>
/// <para>
/// The constraints from the Rapid paper are: K ≥ 3, K > H ≥ L > 0. The gap (H - L) affects
/// the probability of achieving almost-everywhere agreement. Typical values: K=10, H=9, L=3.
/// </para>
/// <para>
/// When the cluster size is smaller than K, effective values are computed automatically:
/// <list type="bullet">
///   <item><description>Effective K = min(K, clusterSize - 1) since a node cannot monitor itself</description></item>
///   <item><description>Effective H and L are scaled proportionally to maintain protocol properties</description></item>
/// </list>
/// </para>
/// </remarks>
public sealed class RapidClusterProtocolOptions
{
    /// <summary>
    /// The minimum number of observers per subject required for the protocol.
    /// Below this threshold, the multi-node cut detection cannot function properly.
    /// </summary>
    public const int MinObserversPerSubject = 3;

    /// <summary>
    /// Gets or sets window for batching alert messages before broadcasting. Default: 100 milliseconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A longer batching window allows more JOIN/REMOVE alerts to accumulate before
    /// triggering consensus, resulting in fewer configuration changes. This is especially
    /// beneficial for large cluster bootstrapping.
    /// </para>
    /// <para>
    /// The Rapid paper achieved 2000-node bootstrap with only 8 configuration changes
    /// by batching multiple joins into single view changes.
    /// </para>
    /// <para>
    /// Recommended values:
    /// <list type="bullet">
    ///   <item><description>Small clusters (&lt;50 nodes): 100ms (default)</description></item>
    ///   <item><description>Medium clusters (50-200 nodes): 200-300ms</description></item>
    ///   <item><description>Large clusters (200+ nodes): 300-500ms</description></item>
    /// </list>
    /// </para>
    /// </remarks>
    public TimeSpan BatchingWindow { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets base delay for consensus fallback timeout. Default: 500 milliseconds.
    /// This is the minimum time to wait before falling back to Classic Paxos
    /// if Fast Paxos hasn't completed. Actual delay includes random jitter.
    /// </summary>
    public TimeSpan ConsensusFallbackTimeoutBaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Gets or sets timeout for leave messages. Default: 1.5 seconds.
    /// </summary>
    public TimeSpan LeaveMessageTimeout { get; set; } = TimeSpan.FromMilliseconds(1500);

    /// <summary>
    /// Gets or sets number of observers per subject (K in the Rapid paper). Also equals the number of
    /// subjects each node monitors. This determines the number of virtual rings in the
    /// K-ring monitoring topology. Default: 10.
    /// </summary>
    /// <remarks>
    /// <para>
    /// In a cluster of size N, each node will monitor min(K, N-1) subjects and be monitored
    /// by the same number of observers. The effective value is computed automatically.
    /// </para>
    /// <para>
    /// Higher values increase monitoring overhead but improve failure detection reliability
    /// and the probability of achieving almost-everywhere agreement.
    /// </para>
    /// </remarks>
    public int ObserversPerSubject { get; set; } = 10;

    /// <summary>
    /// Gets or sets high watermark threshold for multi-node cut detection (H in the Rapid paper).
    /// A subject enters "stable" report mode when it has received at least H observer reports,
    /// indicating high-fidelity failure detection that is safe to act upon. Default: 9.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Must satisfy: K > H > L > 0 where K is <see cref="ObserversPerSubject"/>.
    /// </para>
    /// <para>
    /// Higher values (closer to K) require more agreement from observers before acting,
    /// reducing false positives but potentially delaying detection.
    /// </para>
    /// <para>
    /// The effective H is scaled proportionally when cluster size is smaller than K.
    /// </para>
    /// </remarks>
    public int HighWatermark { get; set; } = 9;

    /// <summary>
    /// Gets or sets low watermark threshold for multi-node cut detection (L in the Rapid paper).
    /// A subject enters "unstable" report mode when it has between L and H reports.
    /// The system delays proposing a view change until all subjects in unstable mode
    /// transition to stable mode. Default: 3.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Must satisfy: K > H > L > 0 where K is <see cref="ObserversPerSubject"/>.
    /// </para>
    /// <para>
    /// Lower values (further from H) provide more filtering of noise but increase
    /// the gap (H - L) which reduces conflict probability in almost-everywhere agreement.
    /// The paper recommends H - L ≥ 5 for low conflict rates with K=10.
    /// </para>
    /// <para>
    /// The effective L is scaled proportionally when cluster size is smaller than K.
    /// </para>
    /// </remarks>
    public int LowWatermark { get; set; } = 3;

    /// <summary>
    /// Gets or sets maximum number of times to retry join attempts.
    /// </summary>
    public int MaxJoinRetries { get; set; } = int.MaxValue;

    /// <summary>
    /// Gets or sets base delay between join retry attempts. Default: 100 milliseconds.
    /// </summary>
    public TimeSpan JoinRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Gets or sets maximum delay between join retry attempts. Default: 5 seconds.
    /// </summary>
    public TimeSpan JoinRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets multiplier for exponential backoff between join retries. Default: 2.0.
    /// </summary>
    public double JoinRetryBackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Gets or sets minimum delay before attempting to rejoin after being kicked. Default: 1 second.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This backoff prevents rapid kick-rejoin cycles that can destabilize the cluster.
    /// When a node is repeatedly kicked (e.g., due to network partitions), the backoff
    /// increases exponentially up to <see cref="RejoinBackoffMax"/>.
    /// </para>
    /// <para>
    /// The backoff resets to this base value after a successful rejoin that lasts longer
    /// than the current backoff period.
    /// </para>
    /// </remarks>
    public TimeSpan RejoinBackoffBase { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets maximum delay before attempting to rejoin after being kicked. Default: 30 seconds.
    /// </summary>
    public TimeSpan RejoinBackoffMax { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Gets or sets timeout for unstable mode in the cut detector. Default: 5 seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When a node detects it has a stale view (via probe responses with higher config IDs),
    /// it requests the current membership from a remote node. This rate limit prevents
    /// excessive refresh requests during rapid view changes or network instability.
    /// </para>
    /// </remarks>
    public TimeSpan StaleViewRefreshInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets timeout for unstable mode in the cut detector. Default: 5 seconds.
    /// </summary>
    /// <remarks>
    /// <para>
    /// When nodes have between L and H reports (unstable mode), the cut detector waits for
    /// them to reach the H threshold before proposing a view change. This timeout ensures
    /// that stuck nodes in unstable mode don't block proposals indefinitely.
    /// </para>
    /// <para>
    /// After this timeout expires, nodes in unstable mode are moved to the proposal set
    /// even without reaching the H threshold, allowing the view change to proceed.
    /// </para>
    /// </remarks>
    public TimeSpan UnstableModeTimeout { get; set; } = TimeSpan.FromSeconds(5);
}
