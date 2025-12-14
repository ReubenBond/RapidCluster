using Google.Protobuf;
using Microsoft.Extensions.Configuration;
using RapidCluster.Pb;

namespace RapidCluster.Discovery;

/// <summary>
/// A seed provider that reads seed addresses from <see cref="IConfiguration"/>.
/// Seeds are re-read on each call, allowing for runtime configuration changes.
/// </summary>
/// <remarks>
/// The configuration section should contain an array of endpoint strings in the format "hostname:port".
/// <example>
/// Example appsettings.json:
/// <code>
/// {
///   "RapidCluster": {
///     "Seeds": [
///       "192.168.1.10:5000",
///       "192.168.1.11:5000",
///       "192.168.1.12:5000"
///     ]
///   }
/// }
/// </code>
/// </example>
/// </remarks>
public sealed class ConfigurationSeedProvider : ISeedProvider
{
    private readonly IConfiguration _configuration;
    private readonly string _sectionName;

    /// <summary>
    /// Creates a new ConfigurationSeedProvider.
    /// </summary>
    /// <param name="configuration">The configuration to read seeds from.</param>
    /// <param name="sectionName">The configuration section name containing the seed list. Default is "RapidCluster:Seeds".</param>
    public ConfigurationSeedProvider(IConfiguration configuration, string sectionName = "RapidCluster:Seeds")
    {
        ArgumentNullException.ThrowIfNull(configuration);
        ArgumentException.ThrowIfNullOrWhiteSpace(sectionName);
        _configuration = configuration;
        _sectionName = sectionName;
    }

    /// <inheritdoc/>
    public ValueTask<IReadOnlyList<Endpoint>> GetSeedsAsync(CancellationToken cancellationToken = default)
    {
        var section = _configuration.GetSection(_sectionName);
        var seedStrings = section.Get<List<string>>() ?? [];

        var seeds = new List<Endpoint>(seedStrings.Count);
        foreach (var seedString in seedStrings)
        {
            if (string.IsNullOrWhiteSpace(seedString))
            {
                continue;
            }

            var endpoint = ParseEndpoint(seedString);
            if (endpoint != null)
            {
                seeds.Add(endpoint);
            }
        }

        return ValueTask.FromResult<IReadOnlyList<Endpoint>>(seeds);
    }

    /// <summary>
    /// Parses an endpoint string in the format "hostname:port".
    /// </summary>
    private static Endpoint? ParseEndpoint(string endpointString)
    {
        var trimmed = endpointString.Trim();
        var lastColon = trimmed.LastIndexOf(':');

        if (lastColon <= 0 || lastColon >= trimmed.Length - 1)
        {
            return null;
        }

        var hostname = trimmed[..lastColon];
        var portString = trimmed[(lastColon + 1)..];

        if (!int.TryParse(portString, out var port) || port <= 0 || port > 65535)
        {
            return null;
        }

        return new Endpoint { Hostname = ByteString.CopyFromUtf8(hostname), Port = port };
    }
}
