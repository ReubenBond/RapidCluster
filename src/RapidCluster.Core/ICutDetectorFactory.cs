using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace RapidCluster;

/// <summary>
/// Factory for creating cut detectors based on cluster size.
/// This exists because the appropriate cut detector varies based on cluster parameters.
/// </summary>
internal interface ICutDetectorFactory
{
    /// <summary>
    /// Creates the appropriate cut detector for the given membership view.
    /// </summary>
    /// <param name="membershipView">The current membership view.</param>
    /// <returns>
    /// A cut detector appropriate for the cluster size:
    /// - For small clusters (RingCount &lt; 3): SimpleCutDetector
    /// - For larger clusters (RingCount >= 3): MultiNodeCutDetector with H/L watermarks
    /// </returns>
    ICutDetector Create(MembershipView membershipView);
}

/// <summary>
/// Default implementation of ICutDetectorFactory.
/// Uses the membership view's RingCount as the authoritative source for ObserversPerSubject.
/// </summary>
internal sealed class CutDetectorFactory(
    IOptions<RapidClusterProtocolOptions> protocolOptions,
    ILogger<SimpleCutDetector> simpleCutDetectorLogger,
    ILogger<MultiNodeCutDetector> multiNodeCutDetectorLogger) : ICutDetectorFactory
{
    private readonly RapidClusterProtocolOptions _options = protocolOptions.Value;

    /// <inheritdoc/>
    public ICutDetector Create(MembershipView membershipView)
    {
        ArgumentNullException.ThrowIfNull(membershipView);

        var (observersPerSubject, highWatermark, lowWatermark) = _options.GetEffectiveParameters(membershipView.Size);

        // MultiNodeCutDetector requires K >= 3 and K > H >= L >= 1.
        // If these constraints cannot be satisfied, use SimpleCutDetector.
        if (observersPerSubject < RapidClusterProtocolOptions.MinObserversPerSubject ||
            highWatermark < 1 ||
            lowWatermark < 1 ||
            highWatermark >= observersPerSubject ||
            lowWatermark > highWatermark)
        {
            return new SimpleCutDetector(membershipView, simpleCutDetectorLogger);
        }

        // For clusters with valid parameters, use the full multi-node cut detection.
        return new MultiNodeCutDetector(highWatermark, lowWatermark, membershipView, multiNodeCutDetectorLogger);
    }
}
