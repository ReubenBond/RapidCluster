using Microsoft.Extensions.Options;

namespace RapidCluster.Discovery;

/// <summary>
/// A seed provider that reads seed addresses from <see cref="RapidClusterSeedOptions"/>.
/// </summary>
/// <remarks>
/// <para>
/// This provider reads seed addresses and the <see cref="RapidClusterSeedOptions.IsStatic"/> flag
/// from configuration. By default, <see cref="RapidClusterSeedOptions.IsStatic"/> is <c>false</c>,
/// which triggers seed gossip during bootstrap to prevent split-brain scenarios.
/// </para>
/// <para>
/// For static seeds that are guaranteed identical across all nodes,
/// set <see cref="RapidClusterSeedOptions.IsStatic"/> to <c>true</c>.
/// </para>
/// </remarks>
public sealed class ConfigurationSeedProvider : ISeedProvider
{
    private readonly IOptionsMonitor<RapidClusterSeedOptions> _optionsMonitor;

    /// <summary>
    /// Creates a new ConfigurationSeedProvider.
    /// </summary>
    /// <param name="optionsMonitor">The options monitor to read seed options from.</param>
    public ConfigurationSeedProvider(IOptionsMonitor<RapidClusterSeedOptions> optionsMonitor)
    {
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        _optionsMonitor = optionsMonitor;
    }

    /// <inheritdoc/>
    public ValueTask<SeedDiscoveryResult> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var options = _optionsMonitor.CurrentValue;
        var seeds = options.SeedAddresses ?? [];
        var result = new SeedDiscoveryResult
        {
            Seeds = seeds,
            IsStatic = options.IsStatic
        };
        return ValueTask.FromResult(result);
    }
}
