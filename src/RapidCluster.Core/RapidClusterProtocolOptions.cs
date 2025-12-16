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
    /// Whether to use in-process transport for testing. Default: false
    /// </summary>
    public bool UseInProcessTransport { get; set; }

    /// <summary>
    /// Whether to use HTTPS for gRPC communication. Default: false (use HTTP)
    /// </summary>
    /// <remarks>
    /// <para>
    /// When enabled, gRPC clients will connect using https:// scheme instead of http://.
    /// This is required when the server uses TLS and Http1AndHttp2 protocol mode,
    /// which allows both HTTP/1.1 (for health checks) and HTTP/2 (for gRPC) on the same port.
    /// </para>
    /// <para>
    /// In development environments with self-signed certificates, you may need to
    /// configure the HttpClient to trust the development certificate.
    /// </para>
    /// </remarks>
    public bool UseHttps { get; set; }

    /// <summary>
    /// gRPC request timeout. Default: 10 seconds
    /// </summary>
    public TimeSpan GrpcTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Number of retries for failed gRPC requests. Default: 5
    /// </summary>
    public int GrpcDefaultRetries { get; set; } = 5;

    /// <summary>
    /// Timeout for join operations. Default: 5 seconds
    /// </summary>
    public TimeSpan GrpcJoinTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Timeout for failure detector probe messages. Default: 500 milliseconds
    /// </summary>
    public TimeSpan GrpcProbeTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Interval between failure detector probes. Default: 1 second
    /// </summary>
    public TimeSpan FailureDetectorInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Window for batching alert messages before broadcasting. Default: 100 milliseconds
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
    /// Base delay for consensus fallback timeout. Default: 500 milliseconds.
    /// This is the minimum time to wait before falling back to Classic Paxos
    /// if Fast Paxos hasn't completed. Actual delay includes random jitter.
    /// </summary>
    public TimeSpan ConsensusFallbackTimeoutBaseDelay { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Maximum number of consensus rounds to attempt before giving up. Default: unlimited.
    /// Each round uses exponential backoff with jitter. In practice, consensus is bounded
    /// by higher-level timeouts (join retries, failure detection) rather than round count.
    /// </summary>
    public int MaxConsensusRounds { get; set; } = int.MaxValue;

    /// <summary>
    /// Timeout for leave messages. Default: 1.5 seconds
    /// </summary>
    public TimeSpan LeaveMessageTimeout { get; set; } = TimeSpan.FromMilliseconds(1500);

    /// <summary>
    /// Number of observers per subject (K in the Rapid paper). Also equals the number of
    /// subjects each node monitors. This determines the number of virtual rings in the
    /// K-ring monitoring topology. Default: 10
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
    /// High watermark threshold for multi-node cut detection (H in the Rapid paper). 
    /// A subject enters "stable" report mode when it has received at least H observer reports,
    /// indicating high-fidelity failure detection that is safe to act upon. Default: 9
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
    /// Low watermark threshold for multi-node cut detection (L in the Rapid paper).
    /// A subject enters "unstable" report mode when it has between L and H reports.
    /// The system delays proposing a view change until all subjects in unstable mode
    /// transition to stable mode. Default: 3
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
    /// Number of consecutive probe failures required before declaring a node down. Default: 3
    /// </summary>
    public int FailureDetectorConsecutiveFailures { get; set; } = 3;

    /// <summary>
    /// Maximum number of times to retry join attempts
    /// </summary>
    public int MaxJoinRetries { get; set; } = int.MaxValue;

    /// <summary>
    /// Base delay between join retry attempts. Default: 100 milliseconds
    /// </summary>
    public TimeSpan JoinRetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Maximum delay between join retry attempts. Default: 5 seconds
    /// </summary>
    public TimeSpan JoinRetryMaxDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Multiplier for exponential backoff between join retries. Default: 2.0
    /// </summary>
    public double JoinRetryBackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Minimum delay before attempting to rejoin after being kicked. Default: 1 second
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
    /// Maximum delay before attempting to rejoin after being kicked. Default: 30 seconds
    /// </summary>
    public TimeSpan RejoinBackoffMax { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Minimum interval between stale view refresh attempts from the same remote endpoint. Default: 1 second.
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
    /// Timeout for unstable mode in the cut detector. Default: 5 seconds.
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

    /// <summary>
    /// Computes effective protocol parameters based on the actual cluster size.
    /// When the cluster is smaller than the configured ObserversPerSubject (K),
    /// the effective values are scaled down proportionally.
    /// </summary>
    /// <param name="clusterSize">The current number of nodes in the cluster.</param>
    /// <returns>The effective ObserversPerSubject, HighWatermark, and LowWatermark values for the given cluster size.</returns>
    /// <remarks>
    /// <para>
    /// Effective ObserversPerSubject = min(K, clusterSize - 1) because a node cannot monitor itself.
    /// </para>
    /// <para>
    /// When effective K is less than configured K, H and L are scaled proportionally:
    /// <list type="bullet">
    ///   <item><description>Effective HighWatermark = max(1, ceil(effectiveK * H / K))</description></item>
    ///   <item><description>Effective LowWatermark = max(1, floor(effectiveK * L / K))</description></item>
    /// </list>
    /// The scaling ensures K > H >= L >= 1 to satisfy MultiNodeCutDetector constraints.
    /// </para>
    /// </remarks>
    public (int ObserversPerSubject, int HighWatermark, int LowWatermark) GetEffectiveParameters(int clusterSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(clusterSize);

        // For a single node cluster, monitoring is meaningless
        if (clusterSize == 1)
        {
            return (0, 0, 0);
        }

        // Effective K cannot exceed (clusterSize - 1) since a node doesn't monitor itself
        var observersPerSubject = Math.Min(ObserversPerSubject, clusterSize - 1);

        // If effective K equals configured K, use configured values directly
        if (observersPerSubject == ObserversPerSubject)
        {
            return (observersPerSubject, HighWatermark, LowWatermark);
        }

        // For small clusters, use simple scaling that maintains K > H >= L >= 1
        // H should be close to K to require high agreement, but must be strictly less than K
        // L should be at least 1
        var highWatermark = Math.Max(1, (int)Math.Ceiling((double)observersPerSubject * HighWatermark / ObserversPerSubject));

        // Ensure K > H (strict inequality required by MultiNodeCutDetector)
        if (highWatermark >= observersPerSubject)
        {
            highWatermark = observersPerSubject - 1;
        }

        var lowWatermark = Math.Max(1, (int)Math.Floor((double)observersPerSubject * LowWatermark / ObserversPerSubject));
        lowWatermark = Math.Min(lowWatermark, highWatermark); // L cannot exceed H

        return (observersPerSubject, highWatermark, lowWatermark);
    }
}
