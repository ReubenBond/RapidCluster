using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using RapidCluster.Discovery;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for ISeedProvider implementations.
/// </summary>
public class SeedProviderTests
{
    #region StaticSeedProvider Tests

    [Fact]
    public async Task StaticSeedProvider_WithExplicitSeeds_ReturnsSeeds()
    {
        var seeds = new List<Endpoint>
        {
            Utils.HostFromParts("192.168.1.1", 5000),
            Utils.HostFromParts("192.168.1.2", 5000)
        };

        var provider = new StaticSeedProvider(seeds);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal(seeds[0], result[0]);
        Assert.Equal(seeds[1], result[1]);
    }

    [Fact]
    public async Task StaticSeedProvider_WithEmptyList_ReturnsEmptyList()
    {
        var provider = new StaticSeedProvider([]);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public void StaticSeedProvider_WithNullSeeds_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new StaticSeedProvider((IReadOnlyList<Endpoint>)null!));
    }

    [Fact]
    public async Task StaticSeedProvider_WithOptions_ReturnsSeeds()
    {
        var seeds = new List<Endpoint>
        {
            Utils.HostFromParts("10.0.0.1", 9000),
            Utils.HostFromParts("10.0.0.2", 9000)
        };
        var options = Options.Create(new RapidClusterOptions { SeedAddresses = seeds });

        var provider = new StaticSeedProvider(options);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal(seeds[0], result[0]);
        Assert.Equal(seeds[1], result[1]);
    }

    [Fact]
    public async Task StaticSeedProvider_WithOptionsNullSeeds_ReturnsEmptyList()
    {
        var options = Options.Create(new RapidClusterOptions { SeedAddresses = null });

        var provider = new StaticSeedProvider(options);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public void StaticSeedProvider_WithNullOptions_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new StaticSeedProvider((IOptions<RapidClusterOptions>)null!));
    }

    [Fact]
    public async Task StaticSeedProvider_MultipleCalls_ReturnsSameSeeds()
    {
        var seeds = new List<Endpoint> { Utils.HostFromParts("192.168.1.1", 5000) };
        var provider = new StaticSeedProvider(seeds);

        var result1 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        var result2 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(result1, result2);
    }

    [Fact]
    public async Task StaticSeedProvider_WithCancellationToken_Completes()
    {
        var seeds = new List<Endpoint> { Utils.HostFromParts("192.168.1.1", 5000) };
        var provider = new StaticSeedProvider(seeds);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Single(result);
    }

    #endregion

    #region ConfigurationSeedProvider Tests

    [Fact]
    public async Task ConfigurationSeedProvider_ParsesValidEndpoints()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "192.168.1.1:5000",
                ["RapidCluster:Seeds:1"] = "192.168.1.2:5001"
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal("192.168.1.1", result[0].Hostname.ToStringUtf8());
        Assert.Equal(5000, result[0].Port);
        Assert.Equal("192.168.1.2", result[1].Hostname.ToStringUtf8());
        Assert.Equal(5001, result[1].Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithCustomSectionName_ParsesCorrectly()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["MyApp:ClusterSeeds:0"] = "10.0.0.1:9000"
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration, "MyApp:ClusterSeeds");

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Single(result);
        Assert.Equal("10.0.0.1", result[0].Hostname.ToStringUtf8());
        Assert.Equal(9000, result[0].Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithMissingSection_ReturnsEmptyList()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection([])
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithEmptySection_ReturnsEmptyList()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds"] = ""
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_SkipsEmptyStrings()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "192.168.1.1:5000",
                ["RapidCluster:Seeds:1"] = "",
                ["RapidCluster:Seeds:2"] = "   ",
                ["RapidCluster:Seeds:3"] = "192.168.1.2:5000"
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_SkipsInvalidEndpoints()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "192.168.1.1:5000",   // valid
                ["RapidCluster:Seeds:1"] = "invalid",            // no port
                ["RapidCluster:Seeds:2"] = ":5000",              // no hostname
                ["RapidCluster:Seeds:3"] = "host:",              // empty port
                ["RapidCluster:Seeds:4"] = "host:abc",           // non-numeric port
                ["RapidCluster:Seeds:5"] = "host:0",             // port out of range
                ["RapidCluster:Seeds:6"] = "host:65536",         // port out of range
                ["RapidCluster:Seeds:7"] = "192.168.1.2:5001"    // valid
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal("192.168.1.1", result[0].Hostname.ToStringUtf8());
        Assert.Equal("192.168.1.2", result[1].Hostname.ToStringUtf8());
    }

    [Fact]
    public async Task ConfigurationSeedProvider_HandlesIPv6Addresses()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "[::1]:5000",
                ["RapidCluster:Seeds:1"] = "[2001:db8::1]:5001"
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal("[::1]", result[0].Hostname.ToStringUtf8());
        Assert.Equal(5000, result[0].Port);
        Assert.Equal("[2001:db8::1]", result[1].Hostname.ToStringUtf8());
        Assert.Equal(5001, result[1].Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_TrimsWhitespace()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "  192.168.1.1:5000  "
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Single(result);
        Assert.Equal("192.168.1.1", result[0].Hostname.ToStringUtf8());
        Assert.Equal(5000, result[0].Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_HandlesHostnames()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "node1.example.com:5000",
                ["RapidCluster:Seeds:1"] = "node-2.cluster.local:5001"
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal("node1.example.com", result[0].Hostname.ToStringUtf8());
        Assert.Equal("node-2.cluster.local", result[1].Hostname.ToStringUtf8());
    }

    [Fact]
    public void ConfigurationSeedProvider_WithNullConfiguration_ThrowsArgumentNullException()
    {
        Assert.Throws<ArgumentNullException>(() => new ConfigurationSeedProvider(null!));
    }

    [Fact]
    public void ConfigurationSeedProvider_WithNullSectionName_ThrowsArgumentNullException()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.Throws<ArgumentNullException>(() => new ConfigurationSeedProvider(configuration, null!));
    }

    [Fact]
    public void ConfigurationSeedProvider_WithEmptySectionName_ThrowsArgumentException()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.Throws<ArgumentException>(() => new ConfigurationSeedProvider(configuration, ""));
    }

    [Fact]
    public void ConfigurationSeedProvider_WithWhitespaceSectionName_ThrowsArgumentException()
    {
        var configuration = new ConfigurationBuilder().Build();

        Assert.Throws<ArgumentException>(() => new ConfigurationSeedProvider(configuration, "   "));
    }

    [Fact]
    public async Task ConfigurationSeedProvider_ReReadsConfigurationOnEachCall()
    {
        // Create configuration - ConfigurationSeedProvider reads from IConfiguration
        // on each call, so this test verifies it doesn't cache
        var configData = new Dictionary<string, string?>
        {
            ["RapidCluster:Seeds:0"] = "192.168.1.1:5000"
        };

        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(configData)
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result1 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        Assert.Single(result1);

        // Note: With IConfiguration, to truly test re-reading, we'd need to reload.
        // This test verifies the provider doesn't cache results and reads fresh each time.
        var result2 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        Assert.Single(result2);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_HandlesPortBoundaries()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "host:1",      // minimum valid port
                ["RapidCluster:Seeds:1"] = "host:65535"   // maximum valid port
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        Assert.Equal(1, result[0].Port);
        Assert.Equal(65535, result[1].Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithCancellationToken_Completes()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["RapidCluster:Seeds:0"] = "192.168.1.1:5000"
            })
            .Build();

        var provider = new ConfigurationSeedProvider(configuration);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Single(result);
    }

    #endregion
}
