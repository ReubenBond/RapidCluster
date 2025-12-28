using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Logging;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Cut detector that aggregates failure/join reports and determines when there is
/// sufficient agreement to propose a view change.
/// 
/// This unified implementation handles all cluster sizes:
/// - For K &lt; 3: Simple threshold mode (require K votes from K observers)
/// - For K >= 3: Watermark mode with H/L thresholds for batching
/// 
/// The detector supports updating the view while preserving valid pending state,
/// which prevents unnecessary delays when nodes catch up to the current view.
/// </summary>
/// <remarks>
/// In watermark mode (K >= 3):
/// - H (high watermark): Reports needed to consider a node "stable" for proposal
/// - L (low watermark): Reports needed to enter "unstable mode" (blocks other proposals)
/// - A proposal is output only when all nodes with L+ reports have reached H
/// 
/// In simple mode (K &lt; 3):
/// - No unstable region exists (H = L = K)
/// - Proposals trigger immediately when a node reaches the threshold
/// </remarks>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed partial class CutDetector
{
    private MembershipView _membershipView;
    private int _highWaterMark;
    private int _lowWaterMark;
    private readonly ILogger<CutDetector> _logger;
    private readonly Lock _lock = new();
    private int _proposalCount;
    private int _updatesInProgress;
    private readonly Dictionary<Endpoint, Dictionary<int, Endpoint>> _reportsPerHost = new(EndpointAddressComparer.Instance);
    private readonly SortedSet<Endpoint> _proposal = new(ProtobufEndpointComparer.Instance);
    private readonly SortedSet<Endpoint> _preProposal = new(ProtobufEndpointComparer.Instance);
    private readonly HashSet<Endpoint> _alreadyProposed = new(EndpointAddressComparer.Instance);
    private bool _seenLinkDownEvents;

    /// <summary>
    /// Whether the detector is in simple mode (K &lt; 3) or watermark mode (K >= 3).
    /// </summary>
    private bool IsSimpleMode => _membershipView.RingCount < 3;

    /// <summary>
    /// Number of observers per subject, derived from the membership view's ring count.
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

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} already proposed, skipping")]
    private partial void LogAlreadyProposed(LoggableEndpoint Dst);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: checking {PreProposalCount} preProposal nodes, {ProposalCount} proposal nodes, seenLinkDownEvents={SeenDown}")]
    private partial void LogInvalidateStart(int PreProposalCount, int ProposalCount, bool SeenDown);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: implicit edge between observer={Observer} and nodeInFlux={NodeInFlux}, status={Status}")]
    private partial void LogImplicitEdge(LoggableEndpoint Observer, LoggableEndpoint NodeInFlux, EdgeStatus Status);

    [LoggerMessage(Level = LogLevel.Information, Message = "ForcePromoteUnstableNodes: promoting {Count} nodes from unstable to proposal set")]
    private partial void LogForcePromote(int Count);

    [LoggerMessage(Level = LogLevel.Information, Message = "UpdateView: updating from config {OldConfig} to {NewConfig}, K: {OldK} -> {NewK}, preserving {PreservedCount} pending proposals")]
    private partial void LogUpdateView(long OldConfig, long NewConfig, int OldK, int NewK, int PreservedCount);

    [LoggerMessage(Level = LogLevel.Debug, Message = "UpdateView: removing reports for node {Node} (reason: {Reason})")]
    private partial void LogRemoveReports(LoggableEndpoint Node, string Reason);

    /// <summary>
    /// Creates a CutDetector for the given membership view.
    /// </summary>
    /// <param name="membershipView">The current membership view</param>
    /// <param name="logger">Optional logger for diagnostic output</param>
    /// <remarks>
    /// The detector is initialized with default thresholds based on the membership view.
    /// Use <see cref="UpdateView"/> to set specific H/L thresholds after construction.
    /// </remarks>
    public CutDetector(MembershipView membershipView, ILogger<CutDetector>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(membershipView);

        _membershipView = membershipView;
        _logger = logger ?? NullLogger<CutDetector>.Instance;

        InitializeDefaultThresholds();

        LogCreated(ObserversPerSubject, _highWaterMark, _lowWaterMark, membershipView.Size,
            IsSimpleMode ? "simple" : "watermark");
    }

    /// <summary>
    /// Initializes the H/L thresholds with default values based on the current membership view.
    /// Called during construction.
    /// </summary>
    private void InitializeDefaultThresholds()
    {
        var k = _membershipView.RingCount;

        if (k < 3)
        {
            // Simple mode: require all K observers to agree
            _highWaterMark = k;
            _lowWaterMark = k;
        }
        else
        {
            // Watermark mode: use defaults
            // Default: H = K-1, L = max(1, H/2)
            _highWaterMark = k - 1;
            _lowWaterMark = Math.Max(1, _highWaterMark / 2);
        }
    }

    /// <summary>
    /// Sets the H/L thresholds to specific values.
    /// Called during UpdateView with explicit thresholds.
    /// </summary>
    private void SetThresholds(int highWaterMark, int lowWaterMark)
    {
        var k = _membershipView.RingCount;

        if (k < 3)
        {
            // Simple mode: require all K observers to agree (ignore provided values)
            _highWaterMark = k;
            _lowWaterMark = k;
        }
        else
        {
            // Watermark mode: use provided values
            _highWaterMark = highWaterMark;
            _lowWaterMark = lowWaterMark;

            // Validate constraints: K > H >= L >= 1
            if (_highWaterMark < 1 || _lowWaterMark < 1 ||
                _highWaterMark >= k || _lowWaterMark > _highWaterMark)
            {
                throw new ArgumentException(
                    $"Watermark constraints not satisfied: K > H >= L >= 1 required, got K={k}, H={_highWaterMark}, L={_lowWaterMark}");
            }
        }
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
    /// <param name="msg">An AlertMessage to apply against the detector</param>
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
                $"ringNumber ({ringNumber}) must be in range [0, {ObserversPerSubject})",
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
            else
            {
                return AggregateWatermarkMode(linkDst, numReportsForHost);
            }
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
            if (_alreadyProposed.Add(linkDst))
            {
                _proposalCount++;
                LogProposal(1);
                return [linkDst];
            }
            else
            {
                LogAlreadyProposed(new LoggableEndpoint(linkDst));
            }
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
                foreach (var node in ret)
                {
                    _alreadyProposed.Add(node);
                }
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
                    ? _membershipView.GetObserversOf(nodeInFlux)          // For failing nodes
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
            else
            {
                return ForcePromoteWatermarkMode();
            }
        }
    }

    private List<Endpoint> ForcePromoteSimpleMode()
    {
        if (_preProposal.Count == 0)
        {
            return [];
        }

        LogForcePromote(_preProposal.Count);

        var promoted = new List<Endpoint>();
        foreach (var node in _preProposal)
        {
            if (_alreadyProposed.Add(node))
            {
                _proposalCount++;
                promoted.Add(node);
            }
        }
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
            foreach (var node in ret)
            {
                _alreadyProposed.Add(node);
            }
            _proposal.Clear();
            return ret;
        }

        return [];
    }

    /// <summary>
    /// Updates the cut detector for a new membership view.
    /// Preserves pending join proposals for nodes not yet in the new membership.
    /// </summary>
    /// <param name="newView">The new membership view</param>
    /// <param name="newHighWaterMark">The new high watermark (H)</param>
    /// <param name="newLowWaterMark">The new low watermark (L)</param>
    public void UpdateView(MembershipView newView, int newHighWaterMark, int newLowWaterMark)
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
                _alreadyProposed.Remove(node);
            }

            // Update view and thresholds
            _membershipView = newView;
            var oldH = _highWaterMark;
            var oldL = _lowWaterMark;
            SetThresholds(newHighWaterMark, newLowWaterMark);

            // If K changed, the ring assignments have changed.
            // - When K decreases: Reports for ring numbers >= newK are invalid
            // - When K increases: ALL existing reports are invalid because the observer
            //   for each ring number may have changed (different node assignment per ring)
            if (newK != oldK)
            {
                // Clear all reports since ring assignments have changed
                _reportsPerHost.Clear();
                _alreadyProposed.Clear();
                LogUpdateView(oldConfig, newConfig, oldK, newK, 0);
                return;
            }

            // Recalculate updatesInProgress based on new thresholds
            _updatesInProgress = 0;
            _preProposal.Clear();
            _proposal.Clear();

            foreach (var (node, reports) in _reportsPerHost)
            {
                if (_alreadyProposed.Contains(node))
                {
                    continue; // Already proposed, skip
                }

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

    private string DebuggerDisplay =>
        IsSimpleMode
            ? $"CutDetector(K={ObserversPerSubject}, Simple, Pending={_preProposal.Count}, Proposals={_proposalCount})"
            : $"CutDetector(K={ObserversPerSubject}, H={_highWaterMark}, L={_lowWaterMark}, Unstable={_updatesInProgress}, Proposals={_proposalCount})";
}
