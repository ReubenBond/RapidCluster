namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for RapidClusterProtocolOptions configuration functionality.
/// </summary>
public class RapidClusterProtocolOptionsTests
{
    [Fact]
    public void FailureDetectorInterval_HasDefaultValue()
    {
        var options = new RapidClusterProtocolOptions();
        Assert.Equal(TimeSpan.FromSeconds(1), options.FailureDetectorInterval);
    }

    [Fact]
    public void FailureDetectorInterval_CanBeConfigured()
    {
        var options = new RapidClusterProtocolOptions
        {
            FailureDetectorInterval = TimeSpan.FromMilliseconds(500)
        };
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.FailureDetectorInterval);
    }

    [Fact]
    public void UnstableModeTimeout_HasDefaultValue()
    {
        var options = new RapidClusterProtocolOptions();
        Assert.Equal(TimeSpan.FromSeconds(5), options.UnstableModeTimeout);
    }

    [Fact]
    public void UnstableModeTimeout_CanBeConfigured()
    {
        var options = new RapidClusterProtocolOptions
        {
            UnstableModeTimeout = TimeSpan.FromSeconds(10)
        };
        Assert.Equal(TimeSpan.FromSeconds(10), options.UnstableModeTimeout);
    }
}
