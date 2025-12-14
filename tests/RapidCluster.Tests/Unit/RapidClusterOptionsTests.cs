using System.Net;
using System.Text;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for RapidClusterOptions configuration functionality.
/// </summary>
public class RapidClusterOptionsTests
{

    [Fact]
    public void ListenAddressDefaultIsNull()
    {
        var options = new RapidClusterOptions();
        Assert.Null(options.ListenAddress);
    }

    [Fact]
    public void ListenAddressCanBeSet()
    {
        var options = new RapidClusterOptions
        {
            ListenAddress = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1234)
        };

        Assert.NotNull(options.ListenAddress);
        var ipEndPoint = Assert.IsType<IPEndPoint>(options.ListenAddress);
        Assert.Equal("127.0.0.1", ipEndPoint.Address.ToString());
        Assert.Equal(1234, ipEndPoint.Port);
    }

    [Fact]
    public void SeedAddressesDefaultIsNull()
    {
        var options = new RapidClusterOptions();
        Assert.Null(options.SeedAddresses);
    }

    [Fact]
    public void SeedAddressesCanBeSet()
    {
        var options = new RapidClusterOptions
        {
            SeedAddresses = [new IPEndPoint(IPAddress.Parse("192.168.1.1"), 9000)]
        };

        Assert.NotNull(options.SeedAddresses);
        Assert.Single(options.SeedAddresses);
        var ipEndPoint = Assert.IsType<IPEndPoint>(options.SeedAddresses[0]);
        Assert.Equal("192.168.1.1", ipEndPoint.Address.ToString());
        Assert.Equal(9000, ipEndPoint.Port);
    }

    [Fact]
    public void SeedAddressesCanContainListenAddressForSeedNode()
    {
        var address = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 1234);
        var options = new RapidClusterOptions
        {
            ListenAddress = address,
            SeedAddresses = [address]
        };

        Assert.Equal(options.ListenAddress, options.SeedAddresses![0]);
    }

    [Fact]
    public void MetadataDefaultIsEmptyMetadata()
    {
        var options = new RapidClusterOptions();

        Assert.NotNull(options.Metadata);
        Assert.Empty(options.Metadata);
    }

    [Fact]
    public void MetadataCanBeSetDirectly()
    {
        var metadata = new Dictionary<string, byte[]>
        {
            ["key"] = Encoding.UTF8.GetBytes("value")
        };

        var options = new RapidClusterOptions { Metadata = metadata };

        Assert.Single(options.Metadata);
        Assert.Equal("value", Encoding.UTF8.GetString(options.Metadata["key"]));
    }

    [Fact]
    public void MetadataCanBeSetFromDictionary()
    {
        var options = new RapidClusterOptions
        {
            Metadata = new Dictionary<string, byte[]>
            {
                ["role"] = Encoding.UTF8.GetBytes("leader"),
                ["datacenter"] = Encoding.UTF8.GetBytes("us-west")
            }
        };

        Assert.Equal(2, options.Metadata.Count);
        Assert.Equal("leader", Encoding.UTF8.GetString(options.Metadata["role"]));
        Assert.Equal("us-west", Encoding.UTF8.GetString(options.Metadata["datacenter"]));
    }

    [Fact]
    public void MetadataCanBeCleared()
    {
        var options = new RapidClusterOptions
        {
            Metadata = new Dictionary<string, byte[]>
            {
                ["initial"] = Encoding.UTF8.GetBytes("value")
            }
        };

        options.Metadata.Clear();

        Assert.Empty(options.Metadata);
    }

    [Fact]
    public void MetadataCanBeReplaced()
    {
        var options = new RapidClusterOptions
        {
            Metadata = new Dictionary<string, byte[]>
            {
                ["old"] = Encoding.UTF8.GetBytes("value")
            }
        };

        options.Metadata = new Dictionary<string, byte[]>
        {
            ["new"] = Encoding.UTF8.GetBytes("value")
        };

        Assert.Single(options.Metadata);
        Assert.False(options.Metadata.ContainsKey("old"));
        Assert.True(options.Metadata.ContainsKey("new"));
    }

    [Fact]
    public void MetadataCanBeSetToNull()
    {
        var options = new RapidClusterOptions
        {
            Metadata = null!
        };

        Assert.Null(options.Metadata);
    }

    [Fact]
    public void MetadataMultipleEntriesAllStored()
    {
        var options = new RapidClusterOptions
        {
            Metadata = new Dictionary<string, byte[]>
            {
                ["a"] = Encoding.UTF8.GetBytes("1"),
                ["b"] = Encoding.UTF8.GetBytes("2"),
                ["c"] = Encoding.UTF8.GetBytes("3"),
                ["d"] = Encoding.UTF8.GetBytes("4"),
                ["e"] = Encoding.UTF8.GetBytes("5")
            }
        };

        Assert.Equal(5, options.Metadata.Count);
    }

    [Fact]
    public void MetadataBinaryDataStoredCorrectly()
    {
        var binaryData = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0xFE };
        var options = new RapidClusterOptions
        {
            Metadata = new Dictionary<string, byte[]>
            {
                ["binary"] = binaryData
            }
        };

        var stored = options.Metadata["binary"];
        Assert.Equal(binaryData, stored);
    }

    [Fact]
    public void FullConfigurationAllPropertiesSet()
    {
        var listenAddr = new IPEndPoint(IPAddress.Parse("10.0.0.1"), 5000);
        var seedAddr = new IPEndPoint(IPAddress.Parse("10.0.0.2"), 5000);

        var options = new RapidClusterOptions
        {
            ListenAddress = listenAddr,
            SeedAddresses = [seedAddr],
            Metadata = new Dictionary<string, byte[]>
            {
                ["role"] = Encoding.UTF8.GetBytes("worker"),
                ["version"] = Encoding.UTF8.GetBytes("1.0.0")
            }
        };

        Assert.Equal(listenAddr, options.ListenAddress);
        Assert.Equal(seedAddr, options.SeedAddresses![0]);
        Assert.Equal(2, options.Metadata.Count);
    }

    #region BootstrapExpect Tests

    [Fact]
    public void BootstrapExpectDefaultIsZero()
    {
        var options = new RapidClusterOptions();

        Assert.Equal(0, options.BootstrapExpect);
    }

    [Fact]
    public void BootstrapExpectCanBeSet()
    {
        var options = new RapidClusterOptions { BootstrapExpect = 3 };

        Assert.Equal(3, options.BootstrapExpect);
    }

    [Fact]
    public void BootstrapExpectCanBeSetToLargeValue()
    {
        var options = new RapidClusterOptions { BootstrapExpect = 100 };

        Assert.Equal(100, options.BootstrapExpect);
    }

    [Fact]
    public void BootstrapExpectCanBeSetToOne()
    {
        var options = new RapidClusterOptions { BootstrapExpect = 1 };

        Assert.Equal(1, options.BootstrapExpect);
    }

    #endregion

    #region BootstrapTimeout Tests

    [Fact]
    public void BootstrapTimeoutDefaultIsFiveMinutes()
    {
        var options = new RapidClusterOptions();

        Assert.Equal(TimeSpan.FromMinutes(5), options.BootstrapTimeout);
    }

    [Fact]
    public void BootstrapTimeoutCanBeSet()
    {
        var options = new RapidClusterOptions { BootstrapTimeout = TimeSpan.FromSeconds(30) };

        Assert.Equal(TimeSpan.FromSeconds(30), options.BootstrapTimeout);
    }

    [Fact]
    public void BootstrapTimeoutCanBeSetToZero()
    {
        var options = new RapidClusterOptions { BootstrapTimeout = TimeSpan.Zero };

        Assert.Equal(TimeSpan.Zero, options.BootstrapTimeout);
    }

    [Fact]
    public void BootstrapTimeoutCanBeSetToLargeValue()
    {
        var options = new RapidClusterOptions { BootstrapTimeout = TimeSpan.FromHours(1) };

        Assert.Equal(TimeSpan.FromHours(1), options.BootstrapTimeout);
    }

    [Fact]
    public void BootstrapTimeoutCanBeSetToInfinite()
    {
        var options = new RapidClusterOptions { BootstrapTimeout = Timeout.InfiniteTimeSpan };

        Assert.Equal(Timeout.InfiniteTimeSpan, options.BootstrapTimeout);
    }

    #endregion

}
