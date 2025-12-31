namespace RapidCluster.Unit.Tests;

/// <summary>
/// Tests for RapidClusterProtocolOptions configuration functionality.
/// </summary>
public class RapidClusterProtocolOptionsTests
{
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
            UnstableModeTimeout = TimeSpan.FromSeconds(10),
        };
        Assert.Equal(TimeSpan.FromSeconds(10), options.UnstableModeTimeout);
    }

    [Fact]
    public void BatchingWindow_HasDefaultValue()
    {
        var options = new RapidClusterProtocolOptions();
        Assert.Equal(TimeSpan.FromMilliseconds(100), options.BatchingWindow);
    }

    [Fact]
    public void BatchingWindow_CanBeConfigured()
    {
        var options = new RapidClusterProtocolOptions
        {
            BatchingWindow = TimeSpan.FromMilliseconds(200),
        };
        Assert.Equal(TimeSpan.FromMilliseconds(200), options.BatchingWindow);
    }

    [Fact]
    public void ConsensusFallbackTimeoutBaseDelay_HasDefaultValue()
    {
        var options = new RapidClusterProtocolOptions();
        Assert.Equal(TimeSpan.FromMilliseconds(500), options.ConsensusFallbackTimeoutBaseDelay);
    }

    [Fact]
    public void ConsensusFallbackTimeoutBaseDelay_CanBeConfigured()
    {
        var options = new RapidClusterProtocolOptions
        {
            ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(1000),
        };
        Assert.Equal(TimeSpan.FromMilliseconds(1000), options.ConsensusFallbackTimeoutBaseDelay);
    }

    [Fact]
    public void LeaveMessageTimeout_HasDefaultValue()
    {
        var options = new RapidClusterProtocolOptions();
        Assert.Equal(TimeSpan.FromMilliseconds(1500), options.LeaveMessageTimeout);
    }

    [Fact]
    public void LeaveMessageTimeout_CanBeConfigured()
    {
        var options = new RapidClusterProtocolOptions
        {
            LeaveMessageTimeout = TimeSpan.FromMilliseconds(2000),
        };
        Assert.Equal(TimeSpan.FromMilliseconds(2000), options.LeaveMessageTimeout);
    }
}
