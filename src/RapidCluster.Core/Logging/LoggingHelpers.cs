using System.Net;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

/// <summary>
/// Wrapper for logging a single Endpoint.
/// </summary>
internal readonly struct LoggableEndpoint
{
    private readonly string _display;

    public LoggableEndpoint(Endpoint endpoint)
    {
        _display = endpoint.GetNetworkAddressString();
    }

    public LoggableEndpoint(EndPoint endpoint)
    {
        _display = endpoint switch
        {
            IPEndPoint ip => $"{ip.Address}:{ip.Port}",
            DnsEndPoint dns => $"{dns.Host}:{dns.Port}",
            _ => endpoint.ToString() ?? "unknown"
        };
    }

    public override readonly string ToString() => _display;
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
            : $"[{string.Join(", ", _proposal.Members.Select(m => m.GetNetworkAddressString()))}]";
}

/// <summary>
/// Wrapper for logging a ConfigurationId.
/// </summary>
internal readonly struct LoggableConfigurationId(ConfigurationId configId)
{
    private readonly ConfigurationId _configId = configId;
    public override readonly string ToString() => _configId.ToString();
}

/// <summary>
/// Wrapper for logging a MembershipView's configuration ID.
/// </summary>
internal readonly struct LoggableMembershipViewConfigurationId(MembershipView view)
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

/// <summary>
/// Wrapper for logging a ClusterId.
/// </summary>
internal readonly struct LoggableClusterId(ClusterId clusterId)
{
    private readonly ClusterId _clusterId = clusterId;
    public override readonly string ToString() => _clusterId.ToString();
}
