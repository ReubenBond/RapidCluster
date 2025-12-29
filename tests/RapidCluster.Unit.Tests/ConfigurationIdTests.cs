using CsCheck;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for ConfigurationId functionality.
/// </summary>
public class ConfigurationIdTests
{
    [Fact]
    public void ClusterId_Empty_Has_Value_Zero() => Assert.Equal(0, ClusterId.None.Value);

    [Fact]
    public void ClusterId_Constructor_Sets_Value()
    {
        var clusterId = new ClusterId(12345);
        Assert.Equal(12345, clusterId.Value);
    }

    [Fact]
    public void ClusterId_Equality_Same_Value_Returns_True()
    {
        var id1 = new ClusterId(42);
        var id2 = new ClusterId(42);
        Assert.True(id1 == id2);
        Assert.False(id1 != id2);
        Assert.True(id1.Equals(id2));
    }

    [Fact]
    public void ClusterId_Equality_Different_Value_Returns_False()
    {
        var id1 = new ClusterId(42);
        var id2 = new ClusterId(43);
        Assert.False(id1 == id2);
        Assert.True(id1 != id2);
        Assert.False(id1.Equals(id2));
    }

    [Fact]
    public void ClusterId_ToString_Formats_As_Hex()
    {
        var clusterId = new ClusterId(255);
        Assert.Contains("00000000000000FF", clusterId.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Empty_Has_Version_Zero() => Assert.Equal(0, ConfigurationId.Empty.Version);

    [Fact]
    public void Empty_Has_Empty_ClusterId() => Assert.Equal(ClusterId.None, ConfigurationId.Empty.ClusterId);

    [Fact]
    public void Constructor_Sets_Version()
    {
        var config = new ConfigurationId(new ClusterId(888), 42);
        Assert.Equal(42, config.Version);
    }

    [Fact]
    public void Constructor_With_ClusterId_And_Version_Sets_Both_Values()
    {
        var config = new ConfigurationId(new ClusterId(888), 42);
        Assert.Equal(new ClusterId(888), config.ClusterId);
        Assert.Equal(42, config.Version);
    }

    [Fact]
    public void Constructor_With_ClusterId_And_Version_Sets_Both()
    {
        var clusterId = new ClusterId(123);
        var config = new ConfigurationId(clusterId, 456);
        Assert.Equal(clusterId, config.ClusterId);
        Assert.Equal(456, config.Version);
    }

    [Fact]
    public void Next_Returns_Incremented_Version()
    {
        var config = new ConfigurationId(new ClusterId(888), 10);
        var next = config.Next();
        Assert.Equal(11, next.Version);
    }

    [Fact]
    public void Next_Preserves_ClusterId()
    {
        var clusterId = new ClusterId(999);
        var config = new ConfigurationId(clusterId, 10);
        var next = config.Next();
        Assert.Equal(clusterId, next.ClusterId);
        Assert.Equal(11, next.Version);
    }

    [Fact]
    public void Equality_Same_Version_Returns_True()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 5);
        var config2 = new ConfigurationId(new ClusterId(888), 5);
        Assert.True(config1 == config2);
        Assert.False(config1 != config2);
        Assert.True(config1.Equals(config2));
    }

    [Fact]
    public void Equality_Different_Version_Returns_False()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 5);
        var config2 = new ConfigurationId(new ClusterId(888), 6);
        Assert.False(config1 == config2);
        Assert.True(config1 != config2);
        Assert.False(config1.Equals(config2));
    }

    [Fact]
    public void Equality_Same_ClusterId_And_Version_Returns_True()
    {
        var clusterId = new ClusterId(100);
        var config1 = new ConfigurationId(clusterId, 5);
        var config2 = new ConfigurationId(clusterId, 5);
        Assert.True(config1 == config2);
        Assert.True(config1.Equals(config2));
    }

    [Fact]
    public void Equality_Different_ClusterId_Same_Version_Returns_False()
    {
        var config1 = new ConfigurationId(new ClusterId(100), 5);
        var config2 = new ConfigurationId(new ClusterId(200), 5);
        Assert.False(config1 == config2);
        Assert.False(config1.Equals(config2));
    }

    [Fact]
    public void Comparison_LessThan()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 5);
        var config2 = new ConfigurationId(new ClusterId(888), 10);
        Assert.True(config1 < config2);
        Assert.False(config2 < config1);
    }

    [Fact]
    public void Comparison_GreaterThan()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 10);
        var config2 = new ConfigurationId(new ClusterId(888), 5);
        Assert.True(config1 > config2);
        Assert.False(config2 > config1);
    }

    [Fact]
    public void Comparison_LessThanOrEqual()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 5);
        var config2 = new ConfigurationId(new ClusterId(888), 10);
        var config3 = new ConfigurationId(new ClusterId(888), 5);
        Assert.True(config1 <= config2);
        Assert.True(config1 <= config3);
    }

    [Fact]
    public void Comparison_GreaterThanOrEqual()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 10);
        var config2 = new ConfigurationId(new ClusterId(888), 5);
        var config3 = new ConfigurationId(new ClusterId(888), 10);
        Assert.True(config1 >= config2);
        Assert.True(config1 >= config3);
    }

    [Fact]
    public void CompareTo_With_Same_ClusterId_Compares_Versions()
    {
        var clusterId = new ClusterId(100);
        var config1 = new ConfigurationId(clusterId, 5);
        var config2 = new ConfigurationId(clusterId, 10);
        Assert.True(config1.CompareTo(config2) < 0);
        Assert.True(config2.CompareTo(config1) > 0);
    }

    [Fact]
    public void CompareTo_With_Empty_ClusterId_On_Left_Throws()
    {
        var config1 = new ConfigurationId(ClusterId.None, 5);
        var config2 = new ConfigurationId(new ClusterId(100), 10);

        var ex = Assert.Throws<InvalidOperationException>(() => config1.CompareTo(config2));
        Assert.Contains("Cannot compare ConfigurationIds with different ClusterIds", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CompareTo_With_Empty_ClusterId_On_Right_Throws()
    {
        var config1 = new ConfigurationId(new ClusterId(100), 10);
        var config2 = new ConfigurationId(ClusterId.None, 5);

        var ex = Assert.Throws<InvalidOperationException>(() => config1.CompareTo(config2));
        Assert.Contains("Cannot compare ConfigurationIds with different ClusterIds", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CompareTo_With_Both_Empty_ClusterIds_Compares_Versions()
    {
        var config1 = new ConfigurationId(ClusterId.None, 5);
        var config2 = new ConfigurationId(ClusterId.None, 10);
        Assert.True(config1.CompareTo(config2) < 0);
    }

    [Fact]
    public void CompareTo_With_Different_NonEmpty_ClusterIds_Throws()
    {
        var config1 = new ConfigurationId(new ClusterId(100), 5);
        var config2 = new ConfigurationId(new ClusterId(200), 10);

        var ex = Assert.Throws<InvalidOperationException>(() => config1.CompareTo(config2));
        Assert.Contains("Cannot compare ConfigurationIds with different ClusterIds", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void LessThan_With_Different_NonEmpty_ClusterIds_Throws()
    {
        var config1 = new ConfigurationId(new ClusterId(100), 5);
        var config2 = new ConfigurationId(new ClusterId(200), 10);

        Assert.Throws<InvalidOperationException>(() => _ = config1 < config2);
    }

    [Fact]
    public void GreaterThan_With_Different_NonEmpty_ClusterIds_Throws()
    {
        var config1 = new ConfigurationId(new ClusterId(100), 5);
        var config2 = new ConfigurationId(new ClusterId(200), 10);

        Assert.Throws<InvalidOperationException>(() => _ = config1 > config2);
    }

    [Fact]
    public void GetHashCode_Same_Version_Same_HashCode()
    {
        var config1 = new ConfigurationId(new ClusterId(888), 42);
        var config2 = new ConfigurationId(new ClusterId(888), 42);
        Assert.Equal(config1.GetHashCode(), config2.GetHashCode());
    }

    [Fact]
    public void GetHashCode_Same_ClusterId_And_Version_Same_HashCode()
    {
        var clusterId = new ClusterId(100);
        var config1 = new ConfigurationId(clusterId, 42);
        var config2 = new ConfigurationId(clusterId, 42);
        Assert.Equal(config1.GetHashCode(), config2.GetHashCode());
    }

    [Fact]
    public void ToProtobuf_Converts_Correctly()
    {
        var clusterId = new ClusterId(12345);
        var config = new ConfigurationId(clusterId, 67890);
        var proto = config.ToProtobuf();

        Assert.Equal(12345UL, proto.ClusterId);
        Assert.Equal(67890L, proto.Version);
    }

    [Fact]
    public void FromProtobuf_Converts_Correctly()
    {
        var proto = new Pb.ConfigurationId { ClusterId = 12345, Version = 67890 };
        var config = ConfigurationId.FromProtobuf(proto);

        Assert.Equal(12345, config.ClusterId.Value);
        Assert.Equal(67890, config.Version);
    }

    [Fact]
    public void FromProtobuf_Null_Returns_Empty()
    {
        var config = ConfigurationId.FromProtobuf(proto: null);
        Assert.Equal(ConfigurationId.Empty, config);
    }

    [Fact]
    public void ToConfigurationId_Extension_Converts_Correctly()
    {
        var proto = new Pb.ConfigurationId { ClusterId = 111, Version = 222 };
        var config = proto.ToConfigurationId();

        Assert.Equal(111, config.ClusterId.Value);
        Assert.Equal(222, config.Version);
    }

    [Fact]
    public void Roundtrip_Protobuf_Preserves_Values()
    {
        var original = new ConfigurationId(new ClusterId(999), 888);
        var proto = original.ToProtobuf();
        var restored = ConfigurationId.FromProtobuf(proto);

        Assert.Equal(original, restored);
    }

    [Fact]
    public void Property_Comparison_With_Empty_ClusterId_Works_Correctly()
    {
        Gen.Long[1, 1000].Select(Gen.Long[1, 1000])
            .Sample((v1, v2) =>
            {
                var config1 = new ConfigurationId(new ClusterId(888), v1);
                var config2 = new ConfigurationId(new ClusterId(888), v2);

                if (v1 < v2) return config1 < config2;
                if (v1 > v2) return config1 > config2;
                return config1 == config2;
            });
    }

    [Fact]
    public void Property_Comparison_With_Same_ClusterId_Works_Correctly()
    {
        Gen.Long[1, 1000].Select(Gen.Long[1, 1000], Gen.Long[1, long.MaxValue])
            .Sample((v1, v2, clusterIdValue) =>
            {
                var clusterId = new ClusterId(clusterIdValue);
                var config1 = new ConfigurationId(clusterId, v1);
                var config2 = new ConfigurationId(clusterId, v2);

                if (v1 < v2) return config1 < config2;
                if (v1 > v2) return config1 > config2;
                return config1 == config2;
            });
    }
}
