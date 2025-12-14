using Microsoft.Extensions.Options;
using RapidCluster.Pb;

namespace RapidCluster.Discovery;

/// <summary>
/// A seed provider that returns a static list of seed addresses.
/// This is the default provider and maintains backward compatibility
/// with existing configurations using <see cref="RapidClusterOptions.SeedAddresses"/>.
/// </summary>
public sealed class StaticSeedProvider : ISeedProvider
{
    private readonly IReadOnlyList<Endpoint> _seeds;

    /// <summary>
    /// Creates a new StaticSeedProvider with the specified list of seeds.
    /// </summary>
    /// <param name="seeds">The list of seed endpoints.</param>
    public StaticSeedProvider(IReadOnlyList<Endpoint> seeds)
    {
        ArgumentNullException.ThrowIfNull(seeds);
        _seeds = seeds;
    }

    /// <summary>
    /// Creates a new StaticSeedProvider from RapidClusterOptions.
    /// </summary>
    /// <param name="options">The RapidCluster options containing seed addresses.</param>
    public StaticSeedProvider(IOptions<RapidClusterOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _seeds = options.Value.SeedAddresses ?? [];
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        return ValueTask.FromResult(_seeds);
    }
}
