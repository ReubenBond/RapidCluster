using System.Diagnostics;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Logging;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// <para>
/// Cut detector that aggregates failure/join reports and determines when there is
/// sufficient agreement to propose a view change.
/// </para>
/// <para>
/// This unified implementation handles all cluster sizes:
/// - For K &lt; 3: Simple threshold mode (require K votes from K observers)
/// - For K >= 3: Watermark mode with H/L thresholds for batching.
/// </para>
/// <para>
/// The detector supports updating the view while preserving valid pending state,
/// which prevents unnecessary delays when nodes catch up to the current view.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// In watermark mode (K >= 3):
/// - H (high watermark): Reports needed to consider a node "stable" for proposal
/// - L (low watermark): Reports needed to enter "unstable mode" (blocks other proposals)
/// - A proposal is output only when all nodes with L+ reports have reached H.
/// </para>
/// <para>
/// In simple mode (K &lt; 3):
/// - No unstable region exists (H = L = K)
/// - Proposals trigger immediately when a node reaches the threshold.
/// </para>
/// <para>
/// The detector is initialized with an empty membership view. Call <see cref="UpdateView"/>
/// to set the initial view and begin operation.
/// </para>
/// </remarks>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed partial class CutDetector
{
    private readonly int _configuredObserversPerSubject;
    private readonly int _configuredHighWatermark;
    private readonly int _configuredLowWatermark;
    private readonly ILogger<CutDetector> _logger;
    private readonly Lock _lock = new();
    private readonly Dictionary<Endpoint, Dictionary<int, Endpoint>> _reportsPerHost = new(EndpointAddressComparer.Instance);
    private readonly SortedSet<Endpoint> _proposal = new(ProtobufEndpointComparer.Instance);
    private readonly SortedSet<Endpoint> _preProposal = new(ProtobufEndpointComparer.Instance);
    private MembershipView _membershipView = MembershipView.Empty;
    private int _highWaterMark;
    private int _lowWaterMark;
    private int _proposalCount;
    private int _updatesInProgress;
    private bool _seenLinkDownEvents;

    /// <summary>
    /// Gets a value indicating whether whether the detector is in simple mode (K &lt; 3) or watermark mode (K >= 3).
    /// </summary>
    private bool IsSimpleMode => _membershipView.RingCount < 3;

    /// <summary>
    /// Gets number of observers per subject, derived from the membership view's ring count.
    /// </summary>
    private int ObserversPerSubject => _membershipView.RingCount;

    [LoggerMessage(Level = LogLevel.Debug, Message = "CutDetector created: K={K}, H={H}, L={L}, membershipSize={MembershipSize}, mode={Mode}")]
    private partial void LogCreated(int K, int H, int L, int MembershipSize, string Mode);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: src={Src}, dst={Dst}, status={Status}, ringNumber={RingNumber}")]
    private partial void LogAggregate(LoggableEndpoint Src, LoggableEndpoint Dst, EdgeStatus Status, int RingNumber);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: duplicate report for dst={Dst}, ringNumber={RingNumber}, ignoring")]
    private partial void LogDuplicateReport(LoggableEndpoint Dst, int RingNumber);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} now has {NumReports} reports (L={L}, H={H})")]
    private partial void LogReportCount(LoggableEndpoint Dst, int NumReports, int L, int H);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} reached L threshold, updatesInProgress={UpdatesInProgress}")]
    private partial void LogReachedLowWatermark(LoggableEndpoint Dst, int UpdatesInProgress);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} reached H threshold, updatesInProgress={UpdatesInProgress}")]
    private partial void LogReachedHighWatermark(LoggableEndpoint Dst, int UpdatesInProgress);

    [LoggerMessage(Level = LogLevel.Information, Message = "AggregateForProposal: proposing view change for {Count} nodes")]
    private partial void LogProposal(int Count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: checking {PreProposalCount} preProposal nodes, {ProposalCount} proposal nodes, seenLinkDownEvents={SeenDown}")]
    private partial void LogInvalidateStart(int PreProposalCount, int ProposalCount, bool SeenDown);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: implicit edge between observer={Observer} and nodeInFlux={NodeInFlux}, status={Status}")]
    private partial void LogImplicitEdge(LoggableEndpoint Observer, LoggableEndpoint NodeInFlux, EdgeStatus Status);

    [LoggerMessage(Level = LogLevel.Debug, Message = "ForcePromoteUnstableNodes: promoting {Count} nodes from unstable to proposal set")]
    private partial void LogForcePromote(int Count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "UpdateView: updating from config {OldConfig} to {NewConfig}, K: {OldK} -> {NewK}, preserving {PreservedCount} pending proposals")]
    private partial void LogUpdateView(long OldConfig, long NewConfig, int OldK, int NewK, int PreservedCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "UpdateView: removing reports for node {Node} (reason: {Reason})")]
    private partial void LogRemoveReports(LoggableEndpoint Node, string Reason);

    /// <summary>
    /// Initializes a new instance of the <see cref="CutDetector"/> class.
    /// Creates a CutDetector with specified configuration options.
    /// </summary>
    /// <param name="observersPerSubject">The configured number of observers per subject (K).</param>
    /// <param name="highWatermark">The configured high watermark (H).</param>
    /// <param name="lowWatermark">The configured low watermark (L).</param>
    /// <param name="logger">Optional logger for diagnostic output.</param>
    /// <remarks>
    /// The detector is initialized with an empty membership view and zero thresholds.
    /// Call <see cref="UpdateView"/> with an actual membership view before use.
    /// The effective H/L values are computed during UpdateView based on the actual cluster size.
    /// </remarks>
    public CutDetector(int observersPerSubject, int highWatermark, int lowWatermark, ILogger<CutDetector>? logger = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(observersPerSubject);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(highWatermark);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(lowWatermark);

        _configuredObserversPerSubject = observersPerSubject;
        _configuredHighWatermark = highWatermark;
        _configuredLowWatermark = lowWatermark;
        _logger = logger ?? NullLogger<CutDetector>.Instance;

        // Initialize with empty/zero values - UpdateView will set actual values
        _highWaterMark = 0;
        _lowWaterMark = 0;
    }

    /// <summary>
    /// Gets the number of proposals that have been generated by this detector.
    /// </summary>
    public int GetNumProposals()
    {
        lock (_lock)
        {
            return _proposalCount;
        }
    }

    /// <summary>
    /// Apply an AlertMessage against the cut detector. When sufficient reports
    /// have been received about a node, the method returns a view change proposal.
    /// </summary>
    /// <param name="msg">An AlertMessage to apply against the detector.</param>
    /// <returns>A list of endpoints about which a view change has been recorded. Empty if no proposal.</returns>
    public List<Endpoint> AggregateForProposal(AlertMessage msg)
    {
        ArgumentNullException.ThrowIfNull(msg);

        var proposals = new List<Endpoint>();
        foreach (var ringNumber in msg.RingNumber)
        {
            proposals.AddRange(AggregateForProposalSingleRing(msg, ringNumber));
        }

        return proposals;
    }

    /// <summary>
    /// Apply a single ring's report from an AlertMessage against the cut detector.
    /// </summary>
    public List<Endpoint> AggregateForProposalSingleRing(AlertMessage msg, int ringNumber)
    {
        ArgumentNullException.ThrowIfNull(msg);
        return AggregateForProposalCore(msg.EdgeSrc, msg.EdgeDst, msg.EdgeStatus, ringNumber);
    }

    private List<Endpoint> AggregateForProposalCore(Endpoint linkSrc, Endpoint linkDst,
                                                    EdgeStatus edgeStatus, int ringNumber)
    {
        if (ringNumber < 0 || ringNumber >= ObserversPerSubject)
        {
            throw new ArgumentException(
                string.Create(CultureInfo.InvariantCulture, $"ringNumber ({ringNumber}) must be in range [0, {ObserversPerSubject})"),
                nameof(ringNumber));
        }

        LogAggregate(new LoggableEndpoint(linkSrc), new LoggableEndpoint(linkDst), edgeStatus, ringNumber);

        lock (_lock)
        {
            if (edgeStatus == EdgeStatus.Down)
            {
                _seenLinkDownEvents = true;
            }

            if (!_reportsPerHost.TryGetValue(linkDst, out var reportsForHost))
            {
                reportsForHost = new Dictionary<int, Endpoint>(ObserversPerSubject);
                _reportsPerHost[linkDst] = reportsForHost;
            }

            if (!reportsForHost.TryAdd(ringNumber, linkSrc))
            {
                LogDuplicateReport(new LoggableEndpoint(linkDst), ringNumber);
                return []; // duplicate announcement, ignore.
            }

            var numReportsForHost = reportsForHost.Count;
            LogReportCount(new LoggableEndpoint(linkDst), numReportsForHost, _lowWaterMark, _highWaterMark);

            if (IsSimpleMode)
            {
                return AggregateSimpleMode(linkDst, numReportsForHost);
            }

            return AggregateWatermarkMode(linkDst, numReportsForHost);
        }
    }

    /// <summary>
    /// Simple mode aggregation (K &lt; 3): Proposal triggers when threshold is reached.
    /// </summary>
    private List<Endpoint> AggregateSimpleMode(Endpoint linkDst, int numReportsForHost)
    {
        // Track in preProposal for edge invalidation (when K > 1)
        if (numReportsForHost == 1 && _highWaterMark > 1)
        {
            _preProposal.Add(linkDst);
        }

        if (numReportsForHost >= _highWaterMark)
        {
            _preProposal.Remove(linkDst);
            _proposalCount++;
            LogProposal(1);
            return [linkDst];
        }

        return [];
    }

    /// <summary>
    /// Watermark mode aggregation (K >= 3): Uses H/L thresholds for batching.
    /// </summary>
    private List<Endpoint> AggregateWatermarkMode(Endpoint linkDst, int numReportsForHost)
    {
        if (numReportsForHost == _lowWaterMark)
        {
            _updatesInProgress++;
            _preProposal.Add(linkDst);
            LogReachedLowWatermark(new LoggableEndpoint(linkDst), _updatesInProgress);
        }

        if (numReportsForHost == _highWaterMark)
        {
            _preProposal.Remove(linkDst);
            _proposal.Add(linkDst);
            _updatesInProgress--;
            LogReachedHighWatermark(new LoggableEndpoint(linkDst), _updatesInProgress);

            if (_updatesInProgress == 0)
            {
                // No outstanding updates, so all nodes that have crossed H are part of a single proposal
                _proposalCount++;
                var ret = new List<Endpoint>(_proposal);
                _proposal.Clear();
                LogProposal(ret.Count);
                return ret;
            }
        }

        return [];
    }

    /// <summary>
    /// Invalidates edges between nodes that are failing or have failed.
    /// This step may be skipped safely when there are no failing nodes.
    /// </summary>
    public List<Endpoint> InvalidateFailingEdges()
    {
        lock (_lock)
        {
            LogInvalidateStart(_preProposal.Count, _proposal.Count, _seenLinkDownEvents);

            // Link invalidation is only required when we have failing nodes
            if (!_seenLinkDownEvents)
            {
                return [];
            }

            var proposalsToReturn = new List<Endpoint>();
            var preProposalCopy = new List<Endpoint>(_preProposal);

            foreach (var nodeInFlux in preProposalCopy)
            {
                var observers = _membershipView.IsHostPresent(nodeInFlux)
                    ? _membershipView.GetObserversOf(nodeInFlux) // For failing nodes
                    : _membershipView.GetExpectedObserversOf(nodeInFlux); // For joining nodes

                var ringNumber = 0;
                foreach (var observer in observers)
                {
                    // Check if the observer itself has reports (meaning it may be failing)
                    var shouldInvalidate = IsSimpleMode
                        ? _reportsPerHost.ContainsKey(observer)
                        : _proposal.Contains(observer) || _preProposal.Contains(observer);

                    if (shouldInvalidate)
                    {
                        var edgeStatus = _membershipView.IsHostPresent(nodeInFlux) ? EdgeStatus.Down : EdgeStatus.Up;
                        LogImplicitEdge(new LoggableEndpoint(observer), new LoggableEndpoint(nodeInFlux), edgeStatus);
                        proposalsToReturn.AddRange(AggregateForProposalCore(observer, nodeInFlux, edgeStatus, ringNumber));
                    }

                    ringNumber++;
                }
            }

            return proposalsToReturn;
        }
    }

    /// <summary>
    /// Gets whether there are nodes in "unstable mode" (between L and H reports).
    /// </summary>
    public bool HasNodesInUnstableMode()
    {
        lock (_lock)
        {
            return IsSimpleMode ? _preProposal.Count > 0 : _updatesInProgress > 0;
        }
    }

    /// <summary>
    /// Forces nodes in unstable mode to be promoted to the proposal set.
    /// This is called when the unstable mode timeout expires.
    /// </summary>
    public List<Endpoint> ForcePromoteUnstableNodes()
    {
        lock (_lock)
        {
            if (IsSimpleMode)
            {
                return ForcePromoteSimpleMode();
            }

            return ForcePromoteWatermarkMode();
        }
    }

    private List<Endpoint> ForcePromoteSimpleMode()
    {
        if (_preProposal.Count == 0)
        {
            return [];
        }

        LogForcePromote(_preProposal.Count);
        _proposalCount++;

        var promoted = new List<Endpoint>(_preProposal);
        _preProposal.Clear();
        return promoted;
    }

    private List<Endpoint> ForcePromoteWatermarkMode()
    {
        if (_updatesInProgress == 0 || _preProposal.Count == 0)
        {
            return [];
        }

        LogForcePromote(_preProposal.Count);

        // Move all nodes from preProposal to proposal
        foreach (var node in _preProposal)
        {
            _proposal.Add(node);
        }

        _preProposal.Clear();
        _updatesInProgress = 0;

        // Return all proposed nodes
        if (_proposal.Count > 0)
        {
            _proposalCount++;
            var ret = new List<Endpoint>(_proposal);
            _proposal.Clear();
            return ret;
        }

        return [];
    }

    /// <summary>
    /// Updates the cut detector for a new membership view.
    /// Preserves pending join proposals for nodes not yet in the new membership.
    /// </summary>
    /// <param name="newView">The new membership view.</param>
    /// <remarks>
    /// The effective H/L thresholds are computed from the configured values based on the
    /// actual cluster size. This ensures the watermark constraints (K > H >= L >= 1) are
    /// always satisfied regardless of cluster size.
    /// </remarks>
    public void UpdateView(MembershipView newView)
    {
        ArgumentNullException.ThrowIfNull(newView);

        lock (_lock)
        {
            var oldConfig = _membershipView.ConfigurationId.Version;
            var newConfig = newView.ConfigurationId.Version;
            var oldK = _membershipView.RingCount;
            var newK = newView.RingCount;

            // Identify nodes to remove from tracking
            var nodesToRemove = new List<Endpoint>();
            foreach (var (node, _) in _reportsPerHost)
            {
                // Remove if node is now in membership (join completed)
                if (newView.IsHostPresent(node))
                {
                    nodesToRemove.Add(node);
                    LogRemoveReports(new LoggableEndpoint(node), "now in membership");
                }

                // Note: We keep reports for nodes not in membership (still joining)
                // DOWN reports are kept as they may still be relevant for failure detection
            }

            foreach (var node in nodesToRemove)
            {
                _reportsPerHost.Remove(node);
                _preProposal.Remove(node);
                _proposal.Remove(node);
            }

            // Update view and compute effective thresholds
            _membershipView = newView;
            ComputeEffectiveThresholds(newView.Size);

            LogCreated(ObserversPerSubject, _highWaterMark, _lowWaterMark, newView.Size,
                IsSimpleMode ? "simple" : "watermark");

            // If K changed, the ring assignments have changed.
            // - When K decreases: Reports for ring numbers >= newK are invalid
            // - When K increases: ALL existing reports are invalid because the observer
            //   for each ring number may have changed (different node assignment per ring)
            if (newK != oldK)
            {
                // Clear all reports since ring assignments have changed
                _reportsPerHost.Clear();
                LogUpdateView(oldConfig, newConfig, oldK, newK, 0);
                return;
            }

            // Recalculate updatesInProgress based on new thresholds
            _updatesInProgress = 0;
            _preProposal.Clear();
            _proposal.Clear();

            foreach (var (node, reports) in _reportsPerHost)
            {
                var count = reports.Count;
                if (IsSimpleMode)
                {
                    if (count > 0 && count < _highWaterMark)
                    {
                        _preProposal.Add(node);
                    }
                }
                else
                {
                    if (count >= _lowWaterMark && count < _highWaterMark)
                    {
                        _preProposal.Add(node);
                        _updatesInProgress++;
                    }
                    else if (count >= _highWaterMark)
                    {
                        _proposal.Add(node);
                    }
                }
            }

            var preservedCount = _reportsPerHost.Count;
            LogUpdateView(oldConfig, newConfig, oldK, newK, preservedCount);
        }
    }

    /// <summary>
    /// Computes effective H/L thresholds based on the configured values and actual cluster size.
    /// </summary>
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
    /// The scaling ensures K > H >= L >= 1 to satisfy watermark constraints.
    /// </para>
    /// <para>
    /// For simple mode (K &lt; 3), H and L are both set to K (require all observers).
    /// </para>
    /// </remarks>
    private void ComputeEffectiveThresholds(int clusterSize)
    {
        // For a single node cluster, monitoring is meaningless
        if (clusterSize <= 1)
        {
            _highWaterMark = 0;
            _lowWaterMark = 0;
            return;
        }

        // Effective K cannot exceed (clusterSize - 1) since a node doesn't monitor itself
        var effectiveK = Math.Min(_configuredObserversPerSubject, clusterSize - 1);

        // For simple mode (K < 3), require all observers to agree
        if (effectiveK < 3)
        {
            _highWaterMark = effectiveK;
            _lowWaterMark = effectiveK;
            return;
        }

        // Watermark mode: compute effective H/L from configured values
        int effectiveH, effectiveL;

        if (effectiveK == _configuredObserversPerSubject)
        {
            // Use configured values directly
            effectiveH = _configuredHighWatermark;
            effectiveL = _configuredLowWatermark;
        }
        else
        {
            // Scale configured values proportionally
            effectiveH = Math.Max(1, (int)Math.Ceiling((double)effectiveK * _configuredHighWatermark / _configuredObserversPerSubject));
            effectiveL = Math.Max(1, (int)Math.Floor((double)effectiveK * _configuredLowWatermark / _configuredObserversPerSubject));
        }

        // Ensure K > H (strict inequality required)
        if (effectiveH >= effectiveK)
        {
            effectiveH = effectiveK - 1;
        }

        // Ensure L <= H
        effectiveL = Math.Min(effectiveL, effectiveH);

        // Validate constraints: K > H >= L >= 1
        if (effectiveH < 1 || effectiveL < 1 || effectiveH >= effectiveK || effectiveL > effectiveH)
        {
            throw new InvalidOperationException(string.Create(CultureInfo.InvariantCulture,
                $"Watermark constraints not satisfied: K > H >= L >= 1 required, got K={effectiveK}, H={effectiveH}, L={effectiveL}"));
        }

        _highWaterMark = effectiveH;
        _lowWaterMark = effectiveL;
    }

    private string DebuggerDisplay =>
        IsSimpleMode
            ? string.Create(CultureInfo.InvariantCulture, $"CutDetector(K={ObserversPerSubject}, Simple, Pending={_preProposal.Count}, Proposals={_proposalCount})")
            : string.Create(CultureInfo.InvariantCulture, $"CutDetector(K={ObserversPerSubject}, H={_highWaterMark}, L={_lowWaterMark}, Unstable={_updatesInProgress}, Proposals={_proposalCount})");
}
