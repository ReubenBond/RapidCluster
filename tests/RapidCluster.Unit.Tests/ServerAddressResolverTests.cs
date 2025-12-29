using System.Net;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http.Features;
using RapidCluster.Grpc;

namespace RapidCluster.Unit.Tests;

public class ServerAddressResolverTests
{
    [Fact]
    public void GetAdvertisedEndpoint_WithHttpAddress_ReturnsEndpoint()
    {
        using var server = CreateMockServer("http://0.0.0.0:5000");

        // Use preferredHost since 0.0.0.0 would otherwise trigger network interface lookup
        var endpoint = ServerAddressResolver.GetAdvertisedEndpoint(server, preferredHost: "192.168.1.100");

        var ipEndpoint = Assert.IsType<IPEndPoint>(endpoint);
        Assert.Equal(IPAddress.Parse("192.168.1.100"), ipEndpoint.Address);
        Assert.Equal(5000, ipEndpoint.Port);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithHttpAndHttps_WhenPreferHttpsFalse_ReturnsHttpAddress()
    {
        using var server = CreateMockServer("http://0.0.0.0:5000", "https://0.0.0.0:5001");

        var endpoint = ServerAddressResolver.GetAdvertisedEndpoint(server, preferredHost: "myhost", preferHttps: false);

        var dnsEndpoint = Assert.IsType<DnsEndPoint>(endpoint);
        Assert.Equal("myhost", dnsEndpoint.Host);
        Assert.Equal(5000, dnsEndpoint.Port);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithHttpAndHttps_WhenPreferHttpsTrue_ReturnsHttpsAddress()
    {
        using var server = CreateMockServer("http://0.0.0.0:5000", "https://0.0.0.0:5001");

        var endpoint = ServerAddressResolver.GetAdvertisedEndpoint(server, preferredHost: "myhost", preferHttps: true);

        var dnsEndpoint = Assert.IsType<DnsEndPoint>(endpoint);
        Assert.Equal("myhost", dnsEndpoint.Host);
        Assert.Equal(5001, dnsEndpoint.Port);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithSpecificIPAddress_ReturnsAsIs()
    {
        using var server = CreateMockServer("http://192.168.1.50:5000");

        var endpoint = ServerAddressResolver.GetAdvertisedEndpoint(server);

        var ipEndpoint = Assert.IsType<IPEndPoint>(endpoint);
        Assert.Equal(IPAddress.Parse("192.168.1.50"), ipEndpoint.Address);
        Assert.Equal(5000, ipEndpoint.Port);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithPreferredHost_OverridesBoundHost()
    {
        using var server = CreateMockServer("http://192.168.1.50:5000");

        var endpoint = ServerAddressResolver.GetAdvertisedEndpoint(server, preferredHost: "custom.host.local");

        var dnsEndpoint = Assert.IsType<DnsEndPoint>(endpoint);
        Assert.Equal("custom.host.local", dnsEndpoint.Host);
        Assert.Equal(5000, dnsEndpoint.Port);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithIPv6Address_AndPreferredHost_ReturnsIPv6Endpoint()
    {
        using var server = CreateMockServer("http://[::1]:5000");

        // ::1 is loopback, so we provide a preferred host to avoid interface resolution
        var endpoint = ServerAddressResolver.GetAdvertisedEndpoint(server, preferredHost: "2001:db8::1");

        var ipEndpoint = Assert.IsType<IPEndPoint>(endpoint);
        Assert.Equal(IPAddress.Parse("2001:db8::1"), ipEndpoint.Address);
        Assert.Equal(5000, ipEndpoint.Port);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithNoAddresses_ThrowsInvalidOperationException()
    {
        using var server = CreateMockServer();

        var ex = Assert.Throws<InvalidOperationException>(() => ServerAddressResolver.GetAdvertisedEndpoint(server));

        Assert.Contains("Server addresses are not available", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void GetAdvertisedEndpoint_WithNullServer_ThrowsArgumentNullException() => Assert.Throws<ArgumentNullException>(() => ServerAddressResolver.GetAdvertisedEndpoint(null!));

    [Fact]
    public void GetAdvertisedEndpoint_WithInvalidUri_ThrowsInvalidOperationException()
    {
        using var server = CreateMockServer("not-a-valid-uri");

        Assert.Throws<InvalidOperationException>(() => ServerAddressResolver.GetAdvertisedEndpoint(server));
    }

    [Fact]
    public void GetAdvertisedUri_WithHttpAddress_ReturnsUriWithPreferredHost()
    {
        using var server = CreateMockServer("http://0.0.0.0:5000");

        var uri = ServerAddressResolver.GetAdvertisedUri(server, preferredHost: "myhost");

        Assert.Equal("http", uri.Scheme);
        Assert.Equal("myhost", uri.Host);
        Assert.Equal(5000, uri.Port);
    }

    [Fact]
    public void GetAdvertisedUri_WithHttpAndHttps_WhenPreferHttpsTrue_ReturnsHttpsUri()
    {
        using var server = CreateMockServer("http://0.0.0.0:5000", "https://0.0.0.0:5001");

        var uri = ServerAddressResolver.GetAdvertisedUri(server, preferredHost: "myhost", preferHttps: true);

        Assert.Equal("https", uri.Scheme);
        Assert.Equal("myhost", uri.Host);
        Assert.Equal(5001, uri.Port);
    }

    [Fact]
    public void TryGetFirstAddress_WithAddresses_ReturnsFirst()
    {
        using var server = CreateMockServer("http://localhost:5000", "https://localhost:5001");

        var result = ServerAddressResolver.TryGetFirstAddress(server);

        Assert.NotNull(result);
        Assert.Equal(5000, result.Port);
    }

    [Fact]
    public void TryGetFirstAddress_WithNoAddresses_ReturnsNull()
    {
        using var server = CreateMockServer();

        var result = ServerAddressResolver.TryGetFirstAddress(server);

        Assert.Null(result);
    }

    [Fact]
    public void HasAddresses_WithAddresses_ReturnsTrue()
    {
        using var server = CreateMockServer("http://localhost:5000");

        Assert.True(ServerAddressResolver.HasAddresses(server));
    }

    [Fact]
    public void HasAddresses_WithNoAddresses_ReturnsFalse()
    {
        using var server = CreateMockServer();

        Assert.False(ServerAddressResolver.HasAddresses(server));
    }

    private static MockServer CreateMockServer(params string[] addresses) => new(addresses);

    private sealed class MockServer : IServer
    {
        private readonly FeatureCollection _features;

        public MockServer(string[] addresses)
        {
            _features = new FeatureCollection();
            var addressesFeature = new MockServerAddressesFeature(addresses);
            _features.Set<IServerAddressesFeature>(addressesFeature);
        }

        public IFeatureCollection Features => _features;

        public Task StartAsync<TContext>(IHttpApplication<TContext> application, CancellationToken cancellationToken) where TContext : notnull => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose()
        {
        }
    }

    private sealed class MockServerAddressesFeature(string[] addresses) : IServerAddressesFeature
    {
        public ICollection<string> Addresses { get; } = [.. addresses];

        public bool PreferHostingUrls { get; set; }
    }
}
