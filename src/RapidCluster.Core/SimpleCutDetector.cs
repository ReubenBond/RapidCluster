using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RapidCluster.Logging;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// A simple cut detector for small clusters where the full multi-node cut detection
/// algorithm (with H/L watermarks) is not applicable.
/// 
/// This detector uses a simple voting threshold requiring all observers to agree:
/// - For K observers per subject, all K votes are required
/// 
/// Unlike <see cref="MultiNodeCutDetector"/>, this does not implement the
/// "unstable mode" waiting behavior - it simply triggers a proposal once
/// the required vote threshold is reached for a subject.
/// 
/// A new instance is created for each view, so no Clear() method is needed.
/// </summary>
internal sealed partial class SimpleCutDetector : ICutDetector
{
    private readonly MembershipView _membershipView;
    private readonly ILogger<SimpleCutDetector> _logger;
    private readonly Lock _lock = new();
    private int _proposalCount;
    private readonly Dictionary<Endpoint, Dictionary<int, Endpoint>> _reportsPerHost = new(EndpointAddressComparer.Instance);
    private readonly SortedSet<Endpoint> _pendingProposals = new(EndpointComparer.Instance);
    private readonly HashSet<Endpoint> _alreadyProposed = new(EndpointAddressComparer.Instance);
    private bool _seenLinkDownEvents;

    /// <summary>
    /// Number of observers per subject, derived from the membership view's ring count.
    /// </summary>
    private int ObserversPerSubject => _membershipView.RingCount;

    /// <summary>
    /// Number of votes required to trigger a proposal.
    /// For ObserversPerSubject=1: require 1 vote (the only observer)
    /// For ObserversPerSubject=2: require 2 votes (both observers must agree)
    /// </summary>
    private int RequiredVotes => ObserversPerSubject;

    [LoggerMessage(Level = LogLevel.Debug, Message = "SimpleCutDetector created: requiredVotes={RequiredVotes}, membershipSize={MembershipSize}")]
    private partial void LogCreated(int RequiredVotes, int MembershipSize);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: src={Src}, dst={Dst}, status={Status}, ringNumber={RingNumber}")]
    private partial void LogAggregate(LoggableEndpoint Src, LoggableEndpoint Dst, EdgeStatus Status, int RingNumber);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: duplicate report for dst={Dst}, ringNumber={RingNumber}, ignoring")]
    private partial void LogDuplicateReport(LoggableEndpoint Dst, int RingNumber);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} now has {NumReports}/{RequiredVotes} reports")]
    private partial void LogReportCount(LoggableEndpoint Dst, int NumReports, int RequiredVotes);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} reached threshold, adding to pending proposals")]
    private partial void LogAddedToPending(LoggableEndpoint Dst);

    [LoggerMessage(Level = LogLevel.Information, Message = "AggregateForProposal: proposing view change for dst={Dst} (reports={NumReports})")]
    private partial void LogProposal(LoggableEndpoint Dst, int NumReports);

    [LoggerMessage(Level = LogLevel.Debug, Message = "AggregateForProposal: dst={Dst} already proposed, skipping")]
    private partial void LogAlreadyProposed(LoggableEndpoint Dst);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: checking {Count} pending proposals, seenLinkDownEvents={SeenDown}")]
    private partial void LogInvalidateStart(int Count, bool SeenDown);

    [LoggerMessage(Level = LogLevel.Debug, Message = "InvalidateFailingEdges: implicit edge between observer={Observer} and nodeInFlux={NodeInFlux}")]
    private partial void LogImplicitEdge(LoggableEndpoint Observer, LoggableEndpoint NodeInFlux);

    /// <summary>
    /// Creates a SimpleCutDetector for clusters where MultiNodeCutDetector constraints cannot be satisfied.
    /// </summary>
    /// <param name="membershipView">The current membership view for observer lookups (K is derived from RingCount)</param>
    /// <param name="logger">Optional logger for diagnostic output</param>
    /// <exception cref="ArgumentException">If membershipView.RingCount is less than 1</exception>
    public SimpleCutDetector(MembershipView membershipView, ILogger<SimpleCutDetector>? logger = null)
    {
        ArgumentNullException.ThrowIfNull(membershipView);

        var k = membershipView.RingCount;
        if (k < 1)
        {
            throw new ArgumentException(
                $"SimpleCutDetector requires at least 1 observer per subject, got {k}",
                nameof(membershipView));
        }

        _membershipView = membershipView;
        _logger = logger ?? NullLogger<SimpleCutDetector>.Instance;

        LogCreated(RequiredVotes, membershipView.Size);
    }

    public int GetNumProposals()
    {
        lock (_lock)
        {
            return _proposalCount;
        }
    }

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
                reportsForHost = [];
                _reportsPerHost[linkDst] = reportsForHost;
            }

            if (!reportsForHost.TryAdd(ringNumber, linkSrc))
            {
                LogDuplicateReport(new LoggableEndpoint(linkDst), ringNumber);
                return []; // duplicate announcement, ignore.
            }

            var numReportsForHost = reportsForHost.Count;
            LogReportCount(new LoggableEndpoint(linkDst), numReportsForHost, RequiredVotes);

            // Track nodes that have at least one report (for edge invalidation)
            if (numReportsForHost == 1 && RequiredVotes > 1)
            {
                _pendingProposals.Add(linkDst);
                LogAddedToPending(new LoggableEndpoint(linkDst));
            }

            if (numReportsForHost >= RequiredVotes)
            {
                // Threshold reached - propose this node for view change (but only once)
                _pendingProposals.Remove(linkDst);
                if (_alreadyProposed.Add(linkDst))
                {
                    _proposalCount++;
                    LogProposal(new LoggableEndpoint(linkDst), numReportsForHost);
                    return [linkDst];
                }
                else
                {
                    LogAlreadyProposed(new LoggableEndpoint(linkDst));
                }
            }

            return [];
        }
    }

    public List<Endpoint> InvalidateFailingEdges()
    {
        lock (_lock)
        {
            LogInvalidateStart(_pendingProposals.Count, _seenLinkDownEvents);

            // Link invalidation is only required when we have failing nodes
            if (!_seenLinkDownEvents)
            {
                return [];
            }

            var proposalsToReturn = new List<Endpoint>();
            var pendingCopy = new List<Endpoint>(_pendingProposals);

            foreach (var nodeInFlux in pendingCopy)
            {
                var observers = _membershipView.IsHostPresent(nodeInFlux)
                    ? _membershipView.GetObserversOf(nodeInFlux)          // For failing nodes
                    : _membershipView.GetExpectedObserversOf(nodeInFlux); // For joining nodes

                // Account for all edges between nodes that have pending reports
                var ringNumber = 0;
                foreach (var observer in observers)
                {
                    // Check if the observer itself has reports (meaning it may be failing)
                    if (_reportsPerHost.ContainsKey(observer))
                    {
                        LogImplicitEdge(new LoggableEndpoint(observer), new LoggableEndpoint(nodeInFlux));
                        // Implicit detection of edges between observer and nodeInFlux
                        var edgeStatus = _membershipView.IsHostPresent(nodeInFlux) ? EdgeStatus.Down : EdgeStatus.Up;
                        proposalsToReturn.AddRange(AggregateForProposalCore(observer, nodeInFlux, edgeStatus, ringNumber));
                    }
                    ringNumber++;
                }
            }

            return proposalsToReturn;
        }
    }

    /// <summary>
    /// Gets whether there are nodes in "unstable mode" (have some reports but not enough).
    /// For SimpleCutDetector, this is when there are pending proposals.
    /// </summary>
    public bool HasNodesInUnstableMode()
    {
        lock (_lock)
        {
            return _pendingProposals.Count > 0;
        }
    }

    /// <summary>
    /// Forces pending nodes to be proposed immediately.
    /// This is called when the unstable mode timeout expires.
    /// </summary>
    public List<Endpoint> ForcePromoteUnstableNodes()
    {
        lock (_lock)
        {
            if (_pendingProposals.Count == 0)
            {
                return [];
            }

            var promoted = new List<Endpoint>();
            foreach (var node in _pendingProposals)
            {
                if (_alreadyProposed.Add(node))
                {
                    _proposalCount++;
                    promoted.Add(node);
                }
            }
            _pendingProposals.Clear();

            return promoted;
        }
    }
}
