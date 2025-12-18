using CsCheck;

namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for ConfigurationId functionality.
/// </summary>
public class ConfigurationIdTests
{
    [Fact]
    public void Empty_Has_Version_Zero()
    {
        Assert.Equal(0, ConfigurationId.Empty.Version);
    }

    [Fact]
    public void Constructor_Sets_Version()
    {
        var config = new ConfigurationId(42);
        Assert.Equal(42, config.Version);
    }

    [Fact]
    public void Next_Returns_Incremented_Version()
    {
        var config = new ConfigurationId(10);
        var next = config.Next();
        Assert.Equal(11, next.Version);
    }

    [Fact]
    public void Equality_Same_Version_Returns_True()
    {
        var config1 = new ConfigurationId(5);
        var config2 = new ConfigurationId(5);
        Assert.True(config1 == config2);
        Assert.False(config1 != config2);
        Assert.True(config1.Equals(config2));
    }

    [Fact]
    public void Equality_Different_Version_Returns_False()
    {
        var config1 = new ConfigurationId(5);
        var config2 = new ConfigurationId(6);
        Assert.False(config1 == config2);
        Assert.True(config1 != config2);
        Assert.False(config1.Equals(config2));
    }

    [Fact]
    public void Comparison_LessThan()
    {
        var config1 = new ConfigurationId(5);
        var config2 = new ConfigurationId(10);
        Assert.True(config1 < config2);
        Assert.False(config2 < config1);
    }

    [Fact]
    public void Comparison_GreaterThan()
    {
        var config1 = new ConfigurationId(10);
        var config2 = new ConfigurationId(5);
        Assert.True(config1 > config2);
        Assert.False(config2 > config1);
    }

    [Fact]
    public void Comparison_LessThanOrEqual()
    {
        var config1 = new ConfigurationId(5);
        var config2 = new ConfigurationId(10);
        var config3 = new ConfigurationId(5);
        Assert.True(config1 <= config2);
        Assert.True(config1 <= config3);
    }

    [Fact]
    public void Comparison_GreaterThanOrEqual()
    {
        var config1 = new ConfigurationId(10);
        var config2 = new ConfigurationId(5);
        var config3 = new ConfigurationId(10);
        Assert.True(config1 >= config2);
        Assert.True(config1 >= config3);
    }

    [Fact]
    public void GetHashCode_Same_Version_Same_HashCode()
    {
        var config1 = new ConfigurationId(42);
        var config2 = new ConfigurationId(42);
        Assert.Equal(config1.GetHashCode(), config2.GetHashCode());
    }

    [Fact]
    public void ToString_Returns_Version_String()
    {
        var config = new ConfigurationId(789);
        Assert.Equal("ConfigurationId(v789)", config.ToString());
    }

    #region Property-Based Tests

    [Fact]
    public void Property_Next_Increases_Version()
    {
        Gen.Long[1, 1000]
            .Sample(version =>
            {
                var config = new ConfigurationId(version);
                var next = config.Next();

                return next.Version == version + 1;
            });
    }

    [Fact]
    public void Property_Equality_Works_Correctly()
    {
        Gen.Long[1, 1000]
            .Sample(version =>
            {
                var config1 = new ConfigurationId(version);
                var config2 = new ConfigurationId(version);
                var config3 = new ConfigurationId(version + 1);

                return config1 == config2 && config1 != config3;
            });
    }

    [Fact]
    public void Property_Comparison_Works_Correctly()
    {
        Gen.Select(Gen.Long[1, 1000], Gen.Long[1, 1000])
            .Sample((v1, v2) =>
            {
                var config1 = new ConfigurationId(v1);
                var config2 = new ConfigurationId(v2);

                if (v1 < v2) return config1 < config2;
                if (v1 > v2) return config1 > config2;
                return config1 == config2;
            });
    }

    #endregion

    #region ClusterId Tests

    [Fact]
    public void Constructor_With_ClusterId_Sets_Both()
    {
        var config = new ConfigurationId(42, 0x12345678);
        Assert.Equal(42, config.Version);
        Assert.Equal(0x12345678, config.ClusterId);
    }

    [Fact]
    public void Next_Preserves_ClusterId()
    {
        var config = new ConfigurationId(10, 0xABCD1234);
        var next = config.Next();
        Assert.Equal(11, next.Version);
        Assert.Equal(0xABCD1234, next.ClusterId);
    }

    [Fact]
    public void Equality_Same_Version_Different_ClusterId_Returns_False()
    {
        var config1 = new ConfigurationId(5, 0x11111111);
        var config2 = new ConfigurationId(5, 0x22222222);
        Assert.False(config1 == config2);
        Assert.True(config1 != config2);
        Assert.False(config1.Equals(config2));
    }

    [Fact]
    public void Equality_Same_Version_Same_ClusterId_Returns_True()
    {
        var config1 = new ConfigurationId(5, 0x12345678);
        var config2 = new ConfigurationId(5, 0x12345678);
        Assert.True(config1 == config2);
        Assert.False(config1 != config2);
        Assert.True(config1.Equals(config2));
    }

    [Fact]
    public void Comparison_DifferentClusterId_Throws_InvalidOperationException()
    {
        var config1 = new ConfigurationId(5, 0x11111111);
        var config2 = new ConfigurationId(10, 0x22222222);

        Assert.Throws<InvalidOperationException>(() => config1 < config2);
        Assert.Throws<InvalidOperationException>(() => config1 > config2);
        Assert.Throws<InvalidOperationException>(() => config1 <= config2);
        Assert.Throws<InvalidOperationException>(() => config1 >= config2);
        Assert.Throws<InvalidOperationException>(() => config1.CompareTo(config2));
    }

    [Fact]
    public void Comparison_SameClusterId_Works_Correctly()
    {
        var config1 = new ConfigurationId(5, 0x12345678);
        var config2 = new ConfigurationId(10, 0x12345678);

        Assert.True(config1 < config2);
        Assert.False(config1 > config2);
        Assert.True(config1 <= config2);
        Assert.False(config1 >= config2);
        Assert.True(config1.CompareTo(config2) < 0);
    }

    [Fact]
    public void ToString_With_ClusterId_Includes_ClusterId()
    {
        var config = new ConfigurationId(789, 0x12345678);
        Assert.Equal("ConfigurationId(v789, c12345678)", config.ToString());
    }

    [Fact]
    public void GetHashCode_Different_ClusterId_Different_HashCode()
    {
        var config1 = new ConfigurationId(42, 0x11111111);
        var config2 = new ConfigurationId(42, 0x22222222);
        // Note: Not guaranteed to be different, but very likely for these values
        Assert.NotEqual(config1.GetHashCode(), config2.GetHashCode());
    }

    #endregion
}