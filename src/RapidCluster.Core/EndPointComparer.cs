using System.Net;

namespace RapidCluster;

/// <summary>
/// Provides equality comparison for <see cref="EndPoint"/> instances.
/// Compares by address/host and port, handling both <see cref="IPEndPoint"/> and <see cref="DnsEndPoint"/>.
/// </summary>
public sealed class EndPointComparer : IEqualityComparer<EndPoint>
{
    /// <summary>
    /// Gets the singleton instance of <see cref="EndPointComparer"/>.
    /// </summary>
    public static EndPointComparer Instance { get; } = new();

    private EndPointComparer() { }

    /// <inheritdoc/>
    public bool Equals(EndPoint? x, EndPoint? y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null || y is null)
        {
            return false;
        }

        return (x, y) switch
        {
            (IPEndPoint ipX, IPEndPoint ipY) => ipX.Address.Equals(ipY.Address) && ipX.Port == ipY.Port,
            (DnsEndPoint dnsX, DnsEndPoint dnsY) => string.Equals(dnsX.Host, dnsY.Host, StringComparison.OrdinalIgnoreCase) && dnsX.Port == dnsY.Port,

            // Cross-type comparison: try to match IP strings
            (IPEndPoint ip, DnsEndPoint dns) => string.Equals(ip.Address.ToString(), dns.Host, StringComparison.OrdinalIgnoreCase) && ip.Port == dns.Port,
            (DnsEndPoint dns, IPEndPoint ip) => string.Equals(dns.Host, ip.Address.ToString(), StringComparison.OrdinalIgnoreCase) && dns.Port == ip.Port,
            _ => x.Equals(y),
        };
    }

    /// <inheritdoc/>
    public int GetHashCode(EndPoint obj)
    {
        ArgumentNullException.ThrowIfNull(obj);

        return obj switch
        {
            IPEndPoint ip => HashCode.Combine(ip.Address.ToString().ToUpperInvariant(), ip.Port),
            DnsEndPoint dns => HashCode.Combine(dns.Host.ToUpperInvariant(), dns.Port),
            _ => obj.GetHashCode(),
        };
    }
}
