using Google.Protobuf;
using RapidCluster.Pb;

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
            ListenAddress = Utils.HostFromParts("127.0.0.1", 1234)
        };

        Assert.NotNull(options.ListenAddress);
        Assert.Equal("127.0.0.1", options.ListenAddress.Hostname.ToStringUtf8());
        Assert.Equal(1234, options.ListenAddress.Port);
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
            SeedAddresses = [Utils.HostFromParts("192.168.1.1", 9000)]
        };

        Assert.NotNull(options.SeedAddresses);
        Assert.Single(options.SeedAddresses);
        Assert.Equal("192.168.1.1", options.SeedAddresses[0].Hostname.ToStringUtf8());
        Assert.Equal(9000, options.SeedAddresses[0].Port);
    }

    [Fact]
    public void SeedAddressesCanContainListenAddressForSeedNode()
    {
        var address = Utils.HostFromParts("127.0.0.1", 1234);
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
        Assert.Empty(options.Metadata.Metadata_);
    }

    [Fact]
    public void MetadataCanBeSetDirectly()
    {
        var metadata = new Metadata();
        metadata.Metadata_.Add("key", ByteString.CopyFromUtf8("value"));

        var options = new RapidClusterOptions { Metadata = metadata };

        Assert.Single(options.Metadata.Metadata_);
        Assert.Equal("value", options.Metadata.Metadata_["key"].ToStringUtf8());
    }

    [Fact]
    public void SetMetadataSetsMetadataFromDictionary()
    {
        var options = new RapidClusterOptions();
        var dict = new Dictionary<string, ByteString>
        {
            ["role"] = ByteString.CopyFromUtf8("leader"),
            ["datacenter"] = ByteString.CopyFromUtf8("us-west")
        };

        options.SetMetadata(dict);

        Assert.Equal(2, options.Metadata.Metadata_.Count);
        Assert.Equal("leader", options.Metadata.Metadata_["role"].ToStringUtf8());
        Assert.Equal("us-west", options.Metadata.Metadata_["datacenter"].ToStringUtf8());
    }

    [Fact]
    public void SetMetadataEmptyDictionaryClearsMetadata()
    {
        var options = new RapidClusterOptions();
        options.SetMetadata(new Dictionary<string, ByteString>
        {
            ["initial"] = ByteString.CopyFromUtf8("value")
        });

        options.SetMetadata([]);

        Assert.Empty(options.Metadata.Metadata_);
    }

    [Fact]
    public void SetMetadataOverwritesPreviousMetadata()
    {
        var options = new RapidClusterOptions();
        options.SetMetadata(new Dictionary<string, ByteString>
        {
            ["old"] = ByteString.CopyFromUtf8("value")
        });

        options.SetMetadata(new Dictionary<string, ByteString>
        {
            ["new"] = ByteString.CopyFromUtf8("value")
        });

        Assert.Single(options.Metadata.Metadata_);
        Assert.False(options.Metadata.Metadata_.ContainsKey("old"));
        Assert.True(options.Metadata.Metadata_.ContainsKey("new"));
    }

    [Fact]
    public void SetMetadataNullDictionaryThrowsArgumentNullException()
    {
        var options = new RapidClusterOptions();

        Assert.Throws<ArgumentNullException>(() => options.SetMetadata(null!));
    }

    [Fact]
    public void SetMetadataMultipleEntriesAllStored()
    {
        var options = new RapidClusterOptions();
        var dict = new Dictionary<string, ByteString>
        {
            ["a"] = ByteString.CopyFromUtf8("1"),
            ["b"] = ByteString.CopyFromUtf8("2"),
            ["c"] = ByteString.CopyFromUtf8("3"),
            ["d"] = ByteString.CopyFromUtf8("4"),
            ["e"] = ByteString.CopyFromUtf8("5")
        };

        options.SetMetadata(dict);

        Assert.Equal(5, options.Metadata.Metadata_.Count);
    }

    [Fact]
    public void SetMetadataBinaryDataStoredCorrectly()
    {
        var options = new RapidClusterOptions();
        var binaryData = new byte[] { 0x00, 0x01, 0x02, 0xFF, 0xFE };
        var dict = new Dictionary<string, ByteString>
        {
            ["binary"] = ByteString.CopyFrom(binaryData)
        };

        options.SetMetadata(dict);

        var stored = options.Metadata.Metadata_["binary"].ToByteArray();
        Assert.Equal(binaryData, stored);
    }

    [Fact]
    public void FullConfigurationAllPropertiesSet()
    {
        var listenAddr = Utils.HostFromParts("10.0.0.1", 5000);
        var seedAddr = Utils.HostFromParts("10.0.0.2", 5000);

        var options = new RapidClusterOptions
        {
            ListenAddress = listenAddr,
            SeedAddresses = [seedAddr]
        };

        options.SetMetadata(new Dictionary<string, ByteString>
        {
            ["role"] = ByteString.CopyFromUtf8("worker"),
            ["version"] = ByteString.CopyFromUtf8("1.0.0")
        });

        Assert.Equal(listenAddr, options.ListenAddress);
        Assert.Equal(seedAddr, options.SeedAddresses![0]);
        Assert.Equal(2, options.Metadata.Metadata_.Count);
    }

}
