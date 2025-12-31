using System.Net;

namespace RapidCluster;

/// <summary>
/// Internal wrapper around <see cref="IListenAddressProvider"/> that guarantees non-null results.
/// </summary>
/// <remarks>
/// This wrapper provides a defensive layer that throws a clear exception if the underlying
/// provider returns null, rather than allowing a NullReferenceException to propagate from
/// consuming code.
/// </remarks>
internal sealed class ListenAddressProvider(IListenAddressProvider inner)
{
    private readonly IListenAddressProvider _inner = inner ?? throw new ArgumentNullException(nameof(inner));

    /// <summary>
    /// Gets the listen address, throwing if the provider returns null.
    /// </summary>
    public EndPoint ListenAddress
    {
        get
        {
            var address = _inner.ListenAddress;
            if (address is null)
            {
                throw new InvalidOperationException(
                    $"The listen address provider ({_inner.GetType().Name}) returned null. " +
                    "Ensure the provider is configured correctly and the server has started before accessing the listen address.");
            }

            return address;
        }
    }
}
