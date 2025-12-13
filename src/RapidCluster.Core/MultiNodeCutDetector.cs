using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// A filter that outputs a view change proposal about a node only if:
/// - there are H reports about a node.
/// - there is no other node about which there are more than L but less than H reports.
/// 
/// The output of this filter gives us almost-everywhere agreement.
/// 
/// This detector requires K >= 3 observers per subject to function correctly.
/// For smaller clusters, use <see cref="SimpleCutDetector"/> instead.
/// 
/// A new instance is created for each view, so no Clear() method is needed.
/// </summary>
/// <remarks>
/// TODO: Currently, if a node is reported as DOWN but then reported as UP again (e.g., a transient
/// failure that recovers before reaching the H threshold), the original DOWN reports remain in the
/// system and are not cleared. The subsequent UP report is simply ignored as a duplicate for that
/// ring number. This could potentially lead to stale DOWN reports contributing to an incorrect
/// proposal if the node experiences another failure later. Consider adding support for clearing
/// stale DOWN reports when a subsequent UP report is received for the same (node, ringNumber) pair.
/// </remarks>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
internal sealed partial class MultiNodeCutDetector : ICutDetector
{
    private readonly int _highWaterMark; // High watermark
    private readonly int _lowWaterMark; // Low watermark
    private readonly MembershipView _membershipView;
    private readonly ILogger<MultiNodeCutDetector> _logger;
    private readonly Lock _lock = new();
    private int _proposalCount;
    private int _updatesInProgress;
    private readonly Dictionary<Endpoint, Dictionary<int, Endpoint>> _reportsPerHost = new(EndpointAddressComparer.Instance);
    private readonly SortedSet<Endpoint> _proposal = new(EndpointComparer.Instance);
    private readonly SortedSet<Endpoint> _preProposal = new(EndpointComparer.Instance);
    private bool _seenLinkDownEvents;

    /// <summary>
    /// Number of observers per subject, derived from the membership view's ring count.
    /// </summary>
    private int ObserversPerSubject => _membershipView.RingCount;

    private readonly struct LoggableEndpoint(Endpoint endpoint)
    {
        private readonly Endpoint _endpoint = endpoint;
        public override readonly string ToString() => RapidClusterUtils.Loggable(_endpoint);
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "MultiNodeCutDetector created: K={K}, H={H}, L={L}, membershipSize={MembershipSize}")]
    private partial void LogCreated(int K, int H, int L, int MembershipSize);

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

    [LoggerMessage(Level = LogLevel.Information, Message = "AggregateForProposal: proposing view change for {Count} nodes (updatesInProgress=0)")]
    private partial void LogProposal(int Count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: checking {PreProposalCount} preProposal nodes, {ProposalCount} proposal nodes, seenLinkDownEvents={SeenDown}")]
    private partial void LogInvalidateStart(int PreProposalCount, int ProposalCount, bool SeenDown);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: implicit edge between observer={Observer} and nodeInFlux={NodeInFlux}, status={Status}")]
    private partial void LogImplicitEdge(LoggableEndpoint Observer, LoggableEndpoint NodeInFlux, EdgeStatus Status);

    [LoggerMessage(Level = LogLevel.Information, Message = "ForcePromoteUnstableNodes: promoting {Count} nodes from unstable to proposal set")]
    private partial void LogForcePromote(int Count);

    /// <summary>
    /// Creates a MultiNodeCutDetector for larger clusters.
    /// </summary>
    /// <param name="highWaterMark">High watermark threshold (H)</param>
    /// <param name="lowWaterMark">Low watermark threshold (L)</param>
    /// <param name="membershipView">The current membership view for observer lookups (K is derived from RingCount)</param>
    /// <param name="logger">Optional logger for diagnostic output</param>
    /// <exception cref="ArgumentException">If constraints K > H >= L >= 1 are not satisfied, or K less than 3</exception>
    public MultiNodeCutDetector(int highWaterMark, int lowWaterMark, MembershipView membershipView, ILogger<MultiNodeCutDetector>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(membershipView);

        var k = membershipView.RingCount;

        // Multi-node cut detection requires K >= 3 for proper H/L watermark behavior
        // For K < 3, use SimpleCutDetector instead
        if (k < 3)
        {
            throw new ArgumentException(
                $"MultiNodeCutDetector requires at least 3 observers per subject, got {k}. Use SimpleCutDetector for smaller clusters.",
                nameof(membershipView));
        }

        // Constraints: K > H >= L >= 1
        if (highWaterMark < 1 || lowWaterMark < 1 ||
            highWaterMark >= k || lowWaterMark > highWaterMark)
        {
            throw new ArgumentException($"Arguments do not satisfy K > H >= L >= 1: (K: {k}, H: {highWaterMark}, L: {lowWaterMark})");
        }

        _highWaterMark = highWaterMark;
        _lowWaterMark = lowWaterMark;
        _membershipView = membershipView;
        _logger = logger ?? NullLogger<MultiNodeCutDetector>.Instance;

        LogCreated(k, _highWaterMark, _lowWaterMark, membershipView.Size);
    }

    public int GetNumProposals()
    {
        lock (_lock)
        {
            return _proposalCount;
        }
    }

    /// <summary>
    /// Apply a AlertMessage against the cut detector. When an update moves a host
    /// past the H threshold of reports, and no other host has between H and L reports, the
    /// method returns a view change proposal.
    /// </summary>
    /// <param name="msg">A AlertMessage to apply against the filter</param>
    /// <returns>A list of endpoints about which a view change has been recorded. Empty list if there is no proposal.</returns>
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

            if (numReportsForHost == _lowWaterMark)
            {
                _updatesInProgress++;
                _preProposal.Add(linkDst);
                LogReachedLowWatermark(new LoggableEndpoint(linkDst), _updatesInProgress);
            }

            if (numReportsForHost == _highWaterMark)
            {
                // Enough reports about linkDst have been received that it is safe to act upon,
                // provided there are no other nodes with L < #reports < H.
                _preProposal.Remove(linkDst);
                _proposal.Add(linkDst);
                _updatesInProgress--;
                LogReachedHighWatermark(new LoggableEndpoint(linkDst), _updatesInProgress);

                if (_updatesInProgress == 0)
                {
                    // No outstanding updates, so all nodes that have crossed the H threshold of reports are
                    // now part of a single proposal.
                    _proposalCount++;
                    var ret = new List<Endpoint>(_proposal);
                    _proposal.Clear();
                    LogProposal(ret.Count);
                    return ret;
                }
            }

            return [];
        }
    }

    /// <summary>
    /// Invalidates edges between nodes that are failing or have failed. This step may be skipped safely
    /// when there are no failing nodes.
    /// </summary>
    /// <returns>A list of endpoints representing a view change proposal.</returns>
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

                // Account for all edges between nodes that are past the L threshold
                var ringNumber = 0;
                foreach (var observer in observers)
                {
                    if (_proposal.Contains(observer) || _preProposal.Contains(observer))
                    {
                        // Implicit detection of edges between observer and nodeInFlux
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
            return _updatesInProgress > 0;
        }
    }

    /// <summary>
    /// Forces nodes in unstable mode (between L and H reports) to be promoted to the proposal set.
    /// This is called when the unstable mode timeout expires to prevent indefinite blocking.
    /// </summary>
    public List<Endpoint> ForcePromoteUnstableNodes()
    {
        lock (_lock)
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
            var promoted = _preProposal.ToList();
            _preProposal.Clear();
            _updatesInProgress = 0;

            // Now that updatesInProgress is 0, return all proposed nodes
            if (_proposal.Count > 0)
            {
                _proposalCount++;
                var ret = new List<Endpoint>(_proposal);
                _proposal.Clear();
                return ret;
            }

            return promoted;
        }
    }

    private string DebuggerDisplay => $"MultiNodeCutDetector(K={ObserversPerSubject}, H={_highWaterMark}, L={_lowWaterMark}, Unstable={_updatesInProgress}, Proposals={_proposalCount})";
}
