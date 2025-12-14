using RapidCluster.Pb;

namespace RapidCluster.Logging;

/// <summary>
/// Wrapper for logging a single Endpoint.
/// </summary>
internal readonly struct LoggableEndpoint(Endpoint endpoint)
{
    private readonly Endpoint _endpoint = endpoint;
    public override readonly string ToString() => _endpoint.GetNetworkAddressString();
}

/// <summary>
/// Wrapper for logging multiple Endpoints.
/// </summary>
internal readonly struct LoggableEndpoints(IEnumerable<Endpoint> endpoints)
{
    private readonly IEnumerable<Endpoint> _endpoints = endpoints;
    public override readonly string ToString() =>
        $"[{string.Join(", ", _endpoints.Select(e => e.GetNetworkAddressString()))}]";
}

/// <summary>
/// Wrapper for logging a MembershipProposal. Extracts endpoints automatically in ToString().
/// </summary>
internal readonly struct LoggableMembershipProposal(MembershipProposal? proposal)
{
    private readonly MembershipProposal? _proposal = proposal;
    public override readonly string ToString() =>
        _proposal is null
            ? "[]"
            : $"[{string.Join(", ", _proposal.Members.Select(m => m.Endpoint.GetNetworkAddressString()))}]";
}

/// <summary>
/// Wrapper for logging a MembershipView's configuration ID.
/// </summary>
internal readonly struct LoggableConfigurationId(MembershipView view)
{
    private readonly MembershipView _view = view;
    public override readonly string ToString() => _view.ConfigurationId.ToString();
}

/// <summary>
/// Wrapper for logging a MembershipView's size.
/// </summary>
internal readonly struct LoggableMembershipSize(MembershipView view)
{
    private readonly MembershipView _view = view;
    public override readonly string ToString() => _view.Size.ToString();
}

/// <summary>
/// Wrapper for logging ring numbers.
/// </summary>
internal readonly struct LoggableRingNumbers(IEnumerable<int> ringNumbers)
{
    private readonly IEnumerable<int> _ringNumbers = ringNumbers;
    public override readonly string ToString() => string.Join(",", _ringNumbers);
}
