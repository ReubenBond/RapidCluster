namespace RapidCluster.Monitoring;

/// <summary>
/// Configuration options for the ping-pong failure detector.
/// </summary>
public sealed class PingPongFailureDetectorOptions
{
    /// <summary>
    /// Gets or sets the interval between failure detector probes. Default: 1 second.
    /// </summary>
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the timeout for individual probe messages. Default: 500 milliseconds.
    /// </summary>
    /// <remarks>
    /// This timeout should be less than <see cref="Interval"/> to ensure
    /// probes complete before the next probe is scheduled.
    /// </remarks>
    public TimeSpan ProbeTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Gets or sets the number of consecutive probe failures required before declaring a node down. Default: 3.
    /// </summary>
    public int ConsecutiveFailuresThreshold { get; set; } = 3;
}
