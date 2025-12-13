using CsCheck;
using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for MetadataManager functionality.
/// </summary>
public class MetadataManagerTests
{

    [Fact]
    public void AddSingleEndpointStoresMetadata()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        var metadata = CreateMetadata("role", "leader");

        manager.Add(endpoint, metadata);

        var result = manager.Get(endpoint);
        Assert.NotNull(result);
        Assert.Equal("leader", result.Metadata_["role"].ToStringUtf8());
    }

    [Fact]
    public void AddMultipleEndpointsStoresAll()
    {
        var manager = new MetadataManager();
        var endpoint1 = Utils.HostFromParts("127.0.0.1", 1234);
        var endpoint2 = Utils.HostFromParts("127.0.0.2", 1235);

        manager.Add(endpoint1, CreateMetadata("role", "leader"));
        manager.Add(endpoint2, CreateMetadata("role", "follower"));

        Assert.Equal("leader", manager.Get(endpoint1)!.Metadata_["role"].ToStringUtf8());
        Assert.Equal("follower", manager.Get(endpoint2)!.Metadata_["role"].ToStringUtf8());
    }

    [Fact]
    public void AddSameEndpointTwiceOverwrites()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);

        manager.Add(endpoint, CreateMetadata("role", "leader"));
        manager.Add(endpoint, CreateMetadata("role", "follower"));

        var result = manager.Get(endpoint);
        Assert.NotNull(result);
        Assert.Equal("follower", result.Metadata_["role"].ToStringUtf8());
    }

    [Fact]
    public void AddEmptyMetadataStores()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        var metadata = new Metadata();

        manager.Add(endpoint, metadata);

        var result = manager.Get(endpoint);
        Assert.NotNull(result);
        Assert.Empty(result.Metadata_);
    }

    [Fact]
    public void AddMultipleMetadataFieldsStoresAll()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        var metadata = new Metadata();
        metadata.Metadata_.Add("role", ByteString.CopyFromUtf8("leader"));
        metadata.Metadata_.Add("datacenter", ByteString.CopyFromUtf8("us-west"));
        metadata.Metadata_.Add("version", ByteString.CopyFromUtf8("1.0.0"));

        manager.Add(endpoint, metadata);

        var result = manager.Get(endpoint);
        Assert.NotNull(result);
        Assert.Equal(3, result.Metadata_.Count);
        Assert.Equal("leader", result.Metadata_["role"].ToStringUtf8());
        Assert.Equal("us-west", result.Metadata_["datacenter"].ToStringUtf8());
    }



    [Fact]
    public void AddMetadataBulkAddStoresAll()
    {
        var manager = new MetadataManager();
        var endpoint1 = Utils.HostFromParts("127.0.0.1", 1234);
        var endpoint2 = Utils.HostFromParts("127.0.0.2", 1235);

        var bulk = new Dictionary<Endpoint, Metadata>
        {
            { endpoint1, CreateMetadata("role", "leader") },
            { endpoint2, CreateMetadata("role", "follower") }
        };

        manager.AddMetadata(bulk);

        Assert.Equal("leader", manager.Get(endpoint1)!.Metadata_["role"].ToStringUtf8());
        Assert.Equal("follower", manager.Get(endpoint2)!.Metadata_["role"].ToStringUtf8());
    }

    [Fact]
    public void AddMetadataEmptyDictionaryDoesNotFail()
    {
        var manager = new MetadataManager();
        var empty = new Dictionary<Endpoint, Metadata>();

        manager.AddMetadata(empty);

        var all = manager.GetAllMetadata();
        Assert.Empty(all);
    }

    [Fact]
    public void AddMetadataOverwritesExisting()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);

        manager.Add(endpoint, CreateMetadata("role", "leader"));

        var bulk = new Dictionary<Endpoint, Metadata>
        {
            { endpoint, CreateMetadata("role", "follower") }
        };
        manager.AddMetadata(bulk);

        Assert.Equal("follower", manager.Get(endpoint)!.Metadata_["role"].ToStringUtf8());
    }



    [Fact]
    public void GetExistingEndpointReturnsMetadata()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        manager.Add(endpoint, CreateMetadata("role", "leader"));

        var result = manager.Get(endpoint);

        Assert.NotNull(result);
        Assert.Equal("leader", result.Metadata_["role"].ToStringUtf8());
    }

    [Fact]
    public void GetNonExistingEndpointReturnsNull()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);

        var result = manager.Get(endpoint);

        Assert.Null(result);
    }

    [Fact]
    public void GetAfterRemovalReturnsNull()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        manager.Add(endpoint, CreateMetadata("role", "leader"));
        manager.RemoveNode(endpoint);

        var result = manager.Get(endpoint);

        Assert.Null(result);
    }



    [Fact]
    public void RemoveNodeExistingEndpointRemoves()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        manager.Add(endpoint, CreateMetadata("role", "leader"));

        manager.RemoveNode(endpoint);

        Assert.Null(manager.Get(endpoint));
    }

    [Fact]
    public void RemoveNodeNonExistingEndpointDoesNotThrow()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);

        manager.RemoveNode(endpoint);

        Assert.Null(manager.Get(endpoint));
    }

    [Fact]
    public void RemoveNodeDoesNotAffectOtherEndpoints()
    {
        var manager = new MetadataManager();
        var endpoint1 = Utils.HostFromParts("127.0.0.1", 1234);
        var endpoint2 = Utils.HostFromParts("127.0.0.2", 1235);
        manager.Add(endpoint1, CreateMetadata("role", "leader"));
        manager.Add(endpoint2, CreateMetadata("role", "follower"));

        manager.RemoveNode(endpoint1);

        Assert.Null(manager.Get(endpoint1));
        Assert.NotNull(manager.Get(endpoint2));
        Assert.Equal("follower", manager.Get(endpoint2)!.Metadata_["role"].ToStringUtf8());
    }

    [Fact]
    public void RemoveNodeSameEndpointTwiceDoesNotThrow()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        manager.Add(endpoint, CreateMetadata("role", "leader"));

        manager.RemoveNode(endpoint);
        manager.RemoveNode(endpoint);

        Assert.Null(manager.Get(endpoint));
    }



    [Fact]
    public void GetAllMetadataEmptyReturnsEmptyDictionary()
    {
        var manager = new MetadataManager();

        var result = manager.GetAllMetadata();

        Assert.Empty(result);
    }

    [Fact]
    public void GetAllMetadataSingleEndpointReturnsSingleEntry()
    {
        var manager = new MetadataManager();
        var endpoint = Utils.HostFromParts("127.0.0.1", 1234);
        manager.Add(endpoint, CreateMetadata("role", "leader"));

        var result = manager.GetAllMetadata();

        Assert.Single(result);
        Assert.True(result.ContainsKey(endpoint));
    }

    [Fact]
    public void GetAllMetadataMultipleEndpointsReturnsAll()
    {
        var manager = new MetadataManager();
        var endpoint1 = Utils.HostFromParts("127.0.0.1", 1234);
        var endpoint2 = Utils.HostFromParts("127.0.0.2", 1235);
        var endpoint3 = Utils.HostFromParts("127.0.0.3", 1236);
        manager.Add(endpoint1, CreateMetadata("role", "leader"));
        manager.Add(endpoint2, CreateMetadata("role", "follower1"));
        manager.Add(endpoint3, CreateMetadata("role", "follower2"));

        var result = manager.GetAllMetadata();

        Assert.Equal(3, result.Count);
        Assert.True(result.ContainsKey(endpoint1));
        Assert.True(result.ContainsKey(endpoint2));
        Assert.True(result.ContainsKey(endpoint3));
    }

    [Fact]
    public void GetAllMetadataAfterRemovalExcludesRemovedEndpoint()
    {
        var manager = new MetadataManager();
        var endpoint1 = Utils.HostFromParts("127.0.0.1", 1234);
        var endpoint2 = Utils.HostFromParts("127.0.0.2", 1235);
        manager.Add(endpoint1, CreateMetadata("role", "leader"));
        manager.Add(endpoint2, CreateMetadata("role", "follower"));
        manager.RemoveNode(endpoint1);

        var result = manager.GetAllMetadata();

        Assert.Single(result);
        Assert.False(result.ContainsKey(endpoint1));
        Assert.True(result.ContainsKey(endpoint2));
    }


    private static Metadata CreateMetadata(string key, string value)
    {
        var metadata = new Metadata();
        metadata.Metadata_.Add(key, ByteString.CopyFromUtf8(value));
        return metadata;
    }

    #region Property-Based Tests

    /// <summary>
    /// Generator for valid endpoints.
    /// </summary>
    private static readonly Gen<Endpoint> GenEndpoint =
        Gen.Select(
            Gen.Int[1, 255],  // IP last octet
            Gen.Int[1000, 65535]  // Port
        ).Select((octet, port) => new Endpoint
        {
            Hostname = ByteString.CopyFromUtf8($"127.0.0.{octet}"),
            Port = port
        });

    [Fact]
    public void Property_Get_Returns_Set_Value()
    {
        Gen.Select(GenEndpoint, Gen.String[1, 100])
            .Sample((endpoint, metadataValue) =>
            {
                var manager = new MetadataManager();
                var metadata = new Metadata();
                metadata.Metadata_.Add("key", ByteString.CopyFromUtf8(metadataValue));

                manager.Add(endpoint, metadata);
                var retrieved = manager.Get(endpoint);

                return retrieved != null &&
                       retrieved.Metadata_.ContainsKey("key") &&
                       retrieved.Metadata_["key"].ToStringUtf8() == metadataValue;
            });
    }

    [Fact]
    public void Property_Get_Returns_Null_For_Unknown_Endpoint()
    {
        Gen.Select(GenEndpoint, GenEndpoint)
            .Where(t => !t.Item1.Equals(t.Item2))
            .Sample((stored, queried) =>
            {
                var manager = new MetadataManager();
                var metadata = new Metadata();
                metadata.Metadata_.Add("key", ByteString.CopyFromUtf8("value"));

                manager.Add(stored, metadata);
                var retrieved = manager.Get(queried);

                return retrieved == null;
            });
    }

    [Fact]
    public void Property_RemoveNode_Removes_Metadata()
    {
        Gen.Select(GenEndpoint, Gen.String[1, 100])
            .Sample((endpoint, metadataValue) =>
            {
                var manager = new MetadataManager();
                var metadata = new Metadata();
                metadata.Metadata_.Add("key", ByteString.CopyFromUtf8(metadataValue));

                manager.Add(endpoint, metadata);
                manager.RemoveNode(endpoint);
                var retrieved = manager.Get(endpoint);

                return retrieved == null;
            });
    }

    #endregion
}
