using System.Net;
using Microsoft.Extensions.Options;
using RapidCluster.Discovery;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for ISeedProvider implementations.
/// </summary>
public class SeedProviderTests
{
    [Fact]
    public async Task ConfigurationSeedProvider_WithSeeds_ReturnsSeeds()
    {
        var seeds = new List<EndPoint>
        {
            new DnsEndPoint("192.168.1.1", 5000),
            new DnsEndPoint("192.168.1.2", 5001),
        };
        var options = new RapidClusterOptions { SeedAddresses = seeds };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        var seed0 = Assert.IsType<DnsEndPoint>(result[0]);
        Assert.Equal("192.168.1.1", seed0.Host);
        Assert.Equal(5000, seed0.Port);
        var seed1 = Assert.IsType<DnsEndPoint>(result[1]);
        Assert.Equal("192.168.1.2", seed1.Host);
        Assert.Equal(5001, seed1.Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithNullSeeds_ReturnsEmptyList()
    {
        var options = new RapidClusterOptions { SeedAddresses = null };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithEmptySeeds_ReturnsEmptyList()
    {
        var options = new RapidClusterOptions { SeedAddresses = [] };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Empty(result);
    }

    [Fact]
    public void ConfigurationSeedProvider_WithNullOptionsMonitor_ThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(() => new ConfigurationSeedProvider(null!));

    [Fact]
    public async Task ConfigurationSeedProvider_MultipleCalls_ReturnsSameSeeds()
    {
        var seeds = new List<EndPoint> { new DnsEndPoint("192.168.1.1", 5000) };
        var options = new RapidClusterOptions { SeedAddresses = seeds };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result1 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        var result2 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(result1, result2);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_ReReadsOptionsOnEachCall()
    {
        var seeds1 = new List<EndPoint> { new DnsEndPoint("192.168.1.1", 5000) };
        var seeds2 = new List<EndPoint>
        {
            new DnsEndPoint("192.168.1.1", 5000),
            new DnsEndPoint("192.168.1.2", 5000),
        };

        var options = new RapidClusterOptions { SeedAddresses = seeds1 };
        var optionsMonitor = new MutableTestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result1 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        Assert.Single(result1);

        // Update the options
        optionsMonitor.CurrentValue = new RapidClusterOptions { SeedAddresses = seeds2 };

        var result2 = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);
        Assert.Equal(2, result2.Count);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithCancellationToken_Completes()
    {
        var seeds = new List<EndPoint> { new DnsEndPoint("192.168.1.1", 5000) };
        var options = new RapidClusterOptions { SeedAddresses = seeds };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Single(result);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithIPv6Addresses_ReturnsCorrectly()
    {
        var seeds = new List<EndPoint>
        {
            new IPEndPoint(IPAddress.IPv6Loopback, 5000),
            new IPEndPoint(IPAddress.Parse("2001:db8::1"), 5001),
        };
        var options = new RapidClusterOptions { SeedAddresses = seeds };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        var ip0 = Assert.IsType<IPEndPoint>(result[0]);
        Assert.Equal(IPAddress.IPv6Loopback, ip0.Address);
        Assert.Equal(5000, ip0.Port);
        var ip1 = Assert.IsType<IPEndPoint>(result[1]);
        Assert.Equal(IPAddress.Parse("2001:db8::1"), ip1.Address);
        Assert.Equal(5001, ip1.Port);
    }

    [Fact]
    public async Task ConfigurationSeedProvider_WithHostnames_ReturnsCorrectly()
    {
        var seeds = new List<EndPoint>
        {
            new DnsEndPoint("node1.example.com", 5000),
            new DnsEndPoint("node-2.cluster.local", 5001),
        };
        var options = new RapidClusterOptions { SeedAddresses = seeds };
        var optionsMonitor = new TestOptionsMonitor<RapidClusterOptions>(options);

        var provider = new ConfigurationSeedProvider(optionsMonitor);

        var result = await provider.GetSeedsAsync(TestContext.Current.CancellationToken);

        Assert.Equal(2, result.Count);
        var dns0 = Assert.IsType<DnsEndPoint>(result[0]);
        Assert.Equal("node1.example.com", dns0.Host);
        var dns1 = Assert.IsType<DnsEndPoint>(result[1]);
        Assert.Equal("node-2.cluster.local", dns1.Host);
    }

    /// <summary>
    /// Simple options monitor for tests that returns a fixed value.
    /// </summary>
    private sealed class TestOptionsMonitor<T>(T value) : IOptionsMonitor<T>
    {
        public T CurrentValue => value;

        public T Get(string? name) => value;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }

    /// <summary>
    /// Mutable options monitor for tests that can change the current value.
    /// </summary>
    private sealed class MutableTestOptionsMonitor<T>(T initialValue) : IOptionsMonitor<T>
    {
        public T CurrentValue { get; set; } = initialValue;

        public T Get(string? name) => CurrentValue;

        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
