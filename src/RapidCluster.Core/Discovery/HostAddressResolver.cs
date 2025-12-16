using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

namespace RapidCluster.Discovery;

/// <summary>
/// Utility class for resolving host IP addresses from network interfaces.
/// This is useful for determining the node's own address when listening on wildcard addresses (0.0.0.0 or ::).
/// </summary>
public static class HostAddressResolver
{
    /// <summary>
    /// Resolves the best non-loopback IPv4 address from available network interfaces.
    /// </summary>
    /// <returns>The resolved IP address, or null if no suitable address is found.</returns>
    public static IPAddress? ResolveIPAddressOrDefault()
        => ResolveIPAddressOrDefault(subnet: null, AddressFamily.InterNetwork);

    /// <summary>
    /// Resolves the best non-loopback IP address from available network interfaces,
    /// optionally filtering by subnet and address family.
    /// </summary>
    /// <param name="subnet">Optional subnet prefix to filter addresses (e.g., [10, 0] for 10.0.x.x).</param>
    /// <param name="family">The address family to filter by (InterNetwork for IPv4, InterNetworkV6 for IPv6).</param>
    /// <returns>The resolved IP address, or null if no suitable address is found.</returns>
    public static IPAddress? ResolveIPAddressOrDefault(byte[]? subnet, AddressFamily family)
    {
        IList<IPAddress> nodeIps = NetworkInterface.GetAllNetworkInterfaces()
            .Where(iface => iface.OperationalStatus == OperationalStatus.Up)
            .SelectMany(iface => iface.GetIPProperties().UnicastAddresses)
            .Select(addr => addr.Address)
            .Where(addr => addr.AddressFamily == family && !IPAddress.IsLoopback(addr))
            .ToList();

        return PickIPAddress(nodeIps, subnet, family);
    }

    /// <summary>
    /// Resolves an IP address from a hostname or address string.
    /// If the input is empty, resolves from network interfaces.
    /// </summary>
    /// <param name="addrOrHost">The address or hostname to resolve, or empty/null to enumerate interfaces.</param>
    /// <param name="subnet">Optional subnet prefix to filter addresses.</param>
    /// <param name="family">The address family to filter by.</param>
    /// <returns>The resolved IP address, or null if no suitable address is found.</returns>
    public static IPAddress? ResolveIPAddressOrDefault(string? addrOrHost, byte[]? subnet, AddressFamily family)
    {
        var loopback = family == AddressFamily.InterNetwork ? IPAddress.Loopback : IPAddress.IPv6Loopback;

        // If the address is empty, enumerate all IP addresses on this node
        if (string.IsNullOrEmpty(addrOrHost))
        {
            return ResolveIPAddressOrDefault(subnet, family);
        }

        if (addrOrHost.Equals("loopback", StringComparison.OrdinalIgnoreCase))
        {
            return loopback;
        }

        // Check if addrOrHost is a valid IP address including loopback and any addresses
        if (IPAddress.TryParse(addrOrHost, out var address))
        {
            return address;
        }

        // Get IP address from DNS
        var nodeIps = Dns.GetHostAddresses(addrOrHost);
        return PickIPAddress(nodeIps, subnet, family);
    }

    /// <summary>
    /// Determines whether the given address represents a wildcard or localhost address
    /// that should be replaced with an actual network address.
    /// </summary>
    /// <param name="host">The host string to check.</param>
    /// <returns>True if the address is a wildcard, any, or localhost address.</returns>
    public static bool IsWildcardOrLocalhost(string? host)
    {
        if (string.IsNullOrEmpty(host))
        {
            return true;
        }

        if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (IPAddress.TryParse(host, out var ip))
        {
            // 0.0.0.0 or ::
            if (IPAddress.Any.Equals(ip) || IPAddress.IPv6Any.Equals(ip))
            {
                return true;
            }

            // Loopback addresses (127.x.x.x or ::1)
            if (IPAddress.IsLoopback(ip))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Gets the best local IP address from network interfaces, preferring non-loopback addresses.
    /// </summary>
    /// <param name="family">The address family to look for.</param>
    /// <param name="interfaceName">Optional interface name prefix to filter by.</param>
    /// <returns>The resolved IP address.</returns>
    /// <exception cref="InvalidOperationException">Thrown when no suitable address is found.</exception>
    public static IPAddress GetLocalIPAddress(AddressFamily family = AddressFamily.InterNetwork, string? interfaceName = null)
    {
        var loopback = family == AddressFamily.InterNetwork ? IPAddress.Loopback : IPAddress.IPv6Loopback;
        NetworkInterface[] netInterfaces = NetworkInterface.GetAllNetworkInterfaces();

        var candidates = new List<IPAddress>();

        foreach (var netInterface in netInterfaces)
        {
            if (netInterface.OperationalStatus != OperationalStatus.Up)
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(interfaceName) &&
                !netInterface.Name.StartsWith(interfaceName, StringComparison.Ordinal))
            {
                continue;
            }

            bool isLoopbackInterface = netInterface.NetworkInterfaceType == NetworkInterfaceType.Loopback;

            foreach (UnicastIPAddressInformation ip in netInterface.GetIPProperties().UnicastAddresses)
            {
                if (ip.Address.AddressFamily == family)
                {
                    // Don't pick loopback address unless we were asked for a loopback interface
                    if (!(isLoopbackInterface && ip.Address.Equals(loopback)))
                    {
                        candidates.Add(ip.Address);
                    }
                }
            }
        }

        if (candidates.Count > 0)
        {
            return PickIPAddress(candidates) ?? throw new InvalidOperationException("Failed to get a local IP address.");
        }

        throw new InvalidOperationException("Failed to get a local IP address.");
    }

    private static IPAddress? PickIPAddress(IList<IPAddress> nodeIps, byte[]? subnet, AddressFamily family)
    {
        var candidates = new List<IPAddress>();
        foreach (var nodeIp in nodeIps.Where(x => x.AddressFamily == family))
        {
            // If subnet is not specified, pick smallest address deterministically
            if (subnet == null)
            {
                candidates.Add(nodeIp);
            }
            else
            {
                // Check if the address matches the subnet prefix
                var ipBytes = nodeIp.GetAddressBytes();
                bool matches = true;
                for (int i = 0; i < subnet.Length && i < ipBytes.Length; i++)
                {
                    if (ipBytes[i] != subnet[i])
                    {
                        matches = false;
                        break;
                    }
                }

                if (matches)
                {
                    candidates.Add(nodeIp);
                }
            }
        }

        return candidates.Count > 0 ? PickIPAddress(candidates) : null;
    }

    private static IPAddress? PickIPAddress(IReadOnlyList<IPAddress> candidates)
    {
        IPAddress? chosen = null;
        foreach (IPAddress addr in candidates)
        {
            if (chosen == null)
            {
                chosen = addr;
            }
            else
            {
                // Pick smallest address deterministically
                if (CompareIPAddresses(addr, chosen))
                {
                    chosen = addr;
                }
            }
        }

        return chosen;
    }

    /// <summary>
    /// Compares two IP addresses, returning true if lhs is "less than" rhs.
    /// Used for deterministic address selection.
    /// </summary>
    private static bool CompareIPAddresses(IPAddress lhs, IPAddress rhs)
    {
        byte[] lbytes = lhs.GetAddressBytes();
        byte[] rbytes = rhs.GetAddressBytes();

        if (lbytes.Length != rbytes.Length)
        {
            return lbytes.Length < rbytes.Length;
        }

        // Compare starting from most significant octet
        // e.g., 10.68.20.21 < 10.98.05.04
        for (int i = 0; i < lbytes.Length; i++)
        {
            if (lbytes[i] != rbytes[i])
            {
                return lbytes[i] < rbytes[i];
            }
        }

        // They're equal
        return false;
    }
}
