using Microsoft.Extensions.Options;
using RapidCluster.Pb;

namespace RapidCluster.Discovery;

/// <summary>
/// A seed provider that reads seed addresses from <see cref="RapidClusterOptions.SeedAddresses"/>.
/// Seeds are re-read on each call via <see cref="IOptionsMonitor{TOptions}"/>, allowing for
/// runtime configuration changes when using reloadable configuration sources.
/// </summary>
/// <remarks>
/// This is the default <see cref="ISeedProvider"/> implementation. It reads from the
/// <see cref="RapidClusterOptions.SeedAddresses"/> property which can be configured
/// via dependency injection options or bound to configuration sections.
/// </remarks>
public sealed class ConfigurationSeedProvider : ISeedProvider
{
    private readonly IOptionsMonitor<RapidClusterOptions> _optionsMonitor;

    /// <summary>
    /// Creates a new ConfigurationSeedProvider.
    /// </summary>
    /// <param name="optionsMonitor">The options monitor to read seed addresses from.</param>
    public ConfigurationSeedProvider(IOptionsMonitor<RapidClusterOptions> optionsMonitor)
    {
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        _optionsMonitor = optionsMonitor;
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var seeds = _optionsMonitor.CurrentValue.SeedAddresses ?? [];
        return ValueTask.FromResult(seeds);
    }
}
