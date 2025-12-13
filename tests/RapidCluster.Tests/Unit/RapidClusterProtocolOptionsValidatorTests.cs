namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for RapidClusterProtocolOptionsValidator validation logic.
/// </summary>
public class RapidClusterProtocolOptionsValidatorTests
{
    private readonly RapidClusterProtocolOptionsValidator _validator = new();


    [Fact]
    public void ValidateGrpcTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.GrpcTimeout = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcTimeoutNegativeFails()
    {
        var options = CreateValidOptions();
        options.GrpcTimeout = TimeSpan.FromSeconds(-1);

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
    }

    [Fact]
    public void ValidateGrpcTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcTimeout = TimeSpan.FromSeconds(10);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateGrpcDefaultRetriesNegativeFails()
    {
        var options = CreateValidOptions();
        options.GrpcDefaultRetries = -1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcDefaultRetries", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcDefaultRetriesZeroSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcDefaultRetries = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateGrpcDefaultRetriesPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcDefaultRetries = 5;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateGrpcJoinTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.GrpcJoinTimeout = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcJoinTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcJoinTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcJoinTimeout = TimeSpan.FromSeconds(5);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateGrpcProbeTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.GrpcProbeTimeout = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcProbeTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcProbeTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcProbeTimeout = TimeSpan.FromMilliseconds(500);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateFailureDetectorIntervalZeroFails()
    {
        var options = CreateValidOptions();
        options.FailureDetectorInterval = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("FailureDetectorInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorIntervalPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.FailureDetectorInterval = TimeSpan.FromSeconds(1);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateBatchingWindowZeroFails()
    {
        var options = CreateValidOptions();
        options.BatchingWindow = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("BatchingWindow", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateBatchingWindowPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.BatchingWindow = TimeSpan.FromMilliseconds(100);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateConsensusFallbackTimeoutBaseDelayZeroFails()
    {
        var options = CreateValidOptions();
        options.ConsensusFallbackTimeoutBaseDelay = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("ConsensusFallbackTimeoutBaseDelay", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateConsensusFallbackTimeoutBaseDelayPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(500);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateLeaveMessageTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.LeaveMessageTimeout = TimeSpan.Zero;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("LeaveMessageTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLeaveMessageTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.LeaveMessageTimeout = TimeSpan.FromMilliseconds(1500);

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateObserversPerSubjectZeroFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("ObserversPerSubject", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateObserversPerSubjectNegativeFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = -1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
    }

    [Fact]
    public void ValidateObserversPerSubjectBelowMinimumFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 2; // Below minimum of 3

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("at least 3", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateObserversPerSubjectMinimumSucceeds()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 3; // Minimum value
        options.HighWatermark = 2;
        options.LowWatermark = 1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateObserversPerSubjectPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void ValidateHighWatermarkZeroFails()
    {
        var options = CreateValidOptions();
        options.HighWatermark = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("HighWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateHighWatermarkNegativeFails()
    {
        var options = CreateValidOptions();
        options.HighWatermark = -1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
    }

    [Fact]
    public void ValidateHighWatermarkPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.HighWatermark = 9;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateHighWatermarkEqualToObserversPerSubjectFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 10; // Must be < K

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than ObserversPerSubject", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateHighWatermarkGreaterThanObserversPerSubjectFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 11; // Must be < K

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than ObserversPerSubject", result.FailureMessage, StringComparison.Ordinal);
    }



    [Fact]
    public void ValidateLowWatermarkNegativeFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = -1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("LowWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkZeroFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("LowWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 3;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateLowWatermarkEqualToHighWatermarkFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 9;
        options.HighWatermark = 9;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than HighWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkGreaterThanHighWatermarkFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 10;
        options.HighWatermark = 5;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than HighWatermark", result.FailureMessage, StringComparison.Ordinal);
    }



    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresZeroFails()
    {
        var options = CreateValidOptions();
        options.FailureDetectorConsecutiveFailures = 0;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("FailureDetectorConsecutiveFailures", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresNegativeFails()
    {
        var options = CreateValidOptions();
        options.FailureDetectorConsecutiveFailures = -1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Failed);
        Assert.Contains("FailureDetectorConsecutiveFailures", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresOneSucceeds()
    {
        var options = CreateValidOptions();
        options.FailureDetectorConsecutiveFailures = 1;

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresDefaultSucceeds()
    {
        var options = CreateValidOptions();
        // Default is 3

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
        Assert.Equal(3, options.FailureDetectorConsecutiveFailures);
    }



    [Fact]
    public void ValidateAllDefaultValuesSucceeds()
    {
        var options = new RapidClusterProtocolOptions();

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateAllValidCustomValuesSucceeds()
    {
        var options = new RapidClusterProtocolOptions
        {
            GrpcTimeout = TimeSpan.FromSeconds(5),
            GrpcDefaultRetries = 3,
            GrpcJoinTimeout = TimeSpan.FromSeconds(3),
            GrpcProbeTimeout = TimeSpan.FromMilliseconds(250),
            FailureDetectorInterval = TimeSpan.FromMilliseconds(500),
            BatchingWindow = TimeSpan.FromMilliseconds(50),
            ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(250),
            LeaveMessageTimeout = TimeSpan.FromMilliseconds(1000)
        };

        var result = _validator.Validate(null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateWithNameWorks()
    {
        var options = CreateValidOptions();

        var result = _validator.Validate("TestOptions", options);

        Assert.True(result.Succeeded);
    }



    [Fact]
    public void GetEffectiveParameters_SingleNodeCluster_ReturnsZeros()
    {
        var options = CreateValidOptions();

        var (effectiveK, effectiveH, effectiveL) = options.GetEffectiveParameters(1);

        Assert.Equal(0, effectiveK);
        Assert.Equal(0, effectiveH);
        Assert.Equal(0, effectiveL);
    }

    [Fact]
    public void GetEffectiveParameters_LargeCluster_ReturnsConfiguredValues()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 9;
        options.LowWatermark = 3;

        var (effectiveK, effectiveH, effectiveL) = options.GetEffectiveParameters(100);

        Assert.Equal(10, effectiveK);
        Assert.Equal(9, effectiveH);
        Assert.Equal(3, effectiveL);
    }

    [Fact]
    public void GetEffectiveParameters_SmallCluster_ScalesValues()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 9;
        options.LowWatermark = 3;

        // 4-node cluster can only have 3 observers per subject
        var (effectiveK, effectiveH, effectiveL) = options.GetEffectiveParameters(4);

        Assert.Equal(3, effectiveK);
        // H and L should be scaled down proportionally, maintaining K >= H >= L >= 1
        // For small clusters, strict inequality isn't always achievable
        Assert.True(effectiveK >= effectiveH, $"K >= H failed: {effectiveK} >= {effectiveH}");
        Assert.True(effectiveH >= effectiveL, $"H >= L failed: {effectiveH} >= {effectiveL}");
        Assert.True(effectiveL >= 1, $"L >= 1 failed: {effectiveL}");
    }

    [Fact]
    public void GetEffectiveParameters_TwoNodeCluster_ReturnsMinimalValues()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 9;
        options.LowWatermark = 3;

        // 2-node cluster can only have 1 observer per subject
        var (effectiveK, effectiveH, effectiveL) = options.GetEffectiveParameters(2);

        // With effectiveK=1, this is below minimum so returns scaled values
        Assert.Equal(1, effectiveK);
    }

    [Fact]
    public void GetEffectiveParameters_ThreeNodeCluster_ReturnsMinimalValues()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 9;
        options.LowWatermark = 3;

        // 3-node cluster can have 2 observers per subject
        var (effectiveK, effectiveH, effectiveL) = options.GetEffectiveParameters(3);

        Assert.Equal(2, effectiveK);
    }

    [Fact]
    public void GetEffectiveParameters_MaintainsConstraints()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 9;
        options.LowWatermark = 3;

        for (var clusterSize = 4; clusterSize <= 20; clusterSize++)
        {
            var (effectiveK, effectiveH, effectiveL) = options.GetEffectiveParameters(clusterSize);

            // Verify constraint K >= H >= L >= 1 (relaxed for small clusters)
            Assert.True(effectiveK >= effectiveH, $"K >= H failed for cluster size {clusterSize}: {effectiveK} >= {effectiveH}");
            Assert.True(effectiveH >= effectiveL, $"H >= L failed for cluster size {clusterSize}: {effectiveH} >= {effectiveL}");
            Assert.True(effectiveL >= 1, $"L >= 1 failed for cluster size {clusterSize}: {effectiveL}");
        }
    }


    private static RapidClusterProtocolOptions CreateValidOptions() => new();
}
