using System.Net;

namespace RapidCluster;

/// <summary>
/// A listen address provider that returns a statically configured address.
/// </summary>
/// <remarks>
/// Use this provider when the listen address is known at configuration time.
/// For dynamic address scenarios (e.g., ASP.NET Core with dynamic ports),
/// use a server-aware provider instead.
/// </remarks>
public sealed class StaticListenAddressProvider : IListenAddressProvider
{
    /// <summary>
    /// Initializes a new instance of the <see cref="StaticListenAddressProvider"/> class.
    /// </summary>
    /// <param name="listenAddress">The listen address.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="listenAddress"/> is null.</exception>
    public StaticListenAddressProvider(EndPoint listenAddress)
    {
        ArgumentNullException.ThrowIfNull(listenAddress);
        ListenAddress = listenAddress;
    }

    /// <inheritdoc />
    public EndPoint ListenAddress { get; }
}
