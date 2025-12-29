namespace RapidCluster.Unit.Tests;

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

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcTimeoutNegativeFails()
    {
        var options = CreateValidOptions();
        options.GrpcTimeout = TimeSpan.FromSeconds(-1);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
    }

    [Fact]
    public void ValidateGrpcTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcTimeout = TimeSpan.FromSeconds(10);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateGrpcDefaultRetriesNegativeFails()
    {
        var options = CreateValidOptions();
        options.GrpcDefaultRetries = -1;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcDefaultRetries", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcDefaultRetriesZeroSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcDefaultRetries = 0;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateGrpcDefaultRetriesPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcDefaultRetries = 5;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateGrpcJoinTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.GrpcJoinTimeout = TimeSpan.Zero;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcJoinTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcJoinTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcJoinTimeout = TimeSpan.FromSeconds(5);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateGrpcProbeTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.GrpcProbeTimeout = TimeSpan.Zero;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("GrpcProbeTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateGrpcProbeTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.GrpcProbeTimeout = TimeSpan.FromMilliseconds(500);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateFailureDetectorIntervalZeroFails()
    {
        var options = CreateValidOptions();
        options.FailureDetectorInterval = TimeSpan.Zero;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("FailureDetectorInterval", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorIntervalPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.FailureDetectorInterval = TimeSpan.FromSeconds(1);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateBatchingWindowZeroFails()
    {
        var options = CreateValidOptions();
        options.BatchingWindow = TimeSpan.Zero;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("BatchingWindow", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateBatchingWindowPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.BatchingWindow = TimeSpan.FromMilliseconds(100);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateConsensusFallbackTimeoutBaseDelayZeroFails()
    {
        var options = CreateValidOptions();
        options.ConsensusFallbackTimeoutBaseDelay = TimeSpan.Zero;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("ConsensusFallbackTimeoutBaseDelay", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateConsensusFallbackTimeoutBaseDelayPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.ConsensusFallbackTimeoutBaseDelay = TimeSpan.FromMilliseconds(500);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateLeaveMessageTimeoutZeroFails()
    {
        var options = CreateValidOptions();
        options.LeaveMessageTimeout = TimeSpan.Zero;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("LeaveMessageTimeout", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLeaveMessageTimeoutPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.LeaveMessageTimeout = TimeSpan.FromMilliseconds(1500);

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateObserversPerSubjectZeroFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 0;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("ObserversPerSubject", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateObserversPerSubjectNegativeFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = -1;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
    }

    [Fact]
    public void ValidateObserversPerSubjectBelowMinimumFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 2; // Below minimum of 3

        var result = _validator.Validate(name: null, options);

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

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateObserversPerSubjectPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateHighWatermarkZeroFails()
    {
        var options = CreateValidOptions();
        options.HighWatermark = 0;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("HighWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateHighWatermarkNegativeFails()
    {
        var options = CreateValidOptions();
        options.HighWatermark = -1;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
    }

    [Fact]
    public void ValidateHighWatermarkPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.HighWatermark = 9;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateHighWatermarkEqualToObserversPerSubjectFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 10; // Must be < K

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than ObserversPerSubject", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateHighWatermarkGreaterThanObserversPerSubjectFails()
    {
        var options = CreateValidOptions();
        options.ObserversPerSubject = 10;
        options.HighWatermark = 11; // Must be < K

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than ObserversPerSubject", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkNegativeFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = -1;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("LowWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkZeroFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 0;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("LowWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkPositiveSucceeds()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 3;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateLowWatermarkEqualToHighWatermarkFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 9;
        options.HighWatermark = 9;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than HighWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateLowWatermarkGreaterThanHighWatermarkFails()
    {
        var options = CreateValidOptions();
        options.LowWatermark = 10;
        options.HighWatermark = 5;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("must be less than HighWatermark", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresZeroFails()
    {
        var options = CreateValidOptions();
        options.FailureDetectorConsecutiveFailures = 0;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("FailureDetectorConsecutiveFailures", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresNegativeFails()
    {
        var options = CreateValidOptions();
        options.FailureDetectorConsecutiveFailures = -1;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Failed);
        Assert.Contains("FailureDetectorConsecutiveFailures", result.FailureMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresOneSucceeds()
    {
        var options = CreateValidOptions();
        options.FailureDetectorConsecutiveFailures = 1;

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateFailureDetectorConsecutiveFailuresDefaultSucceeds()
    {
        var options = CreateValidOptions();

        // Default is 3
        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
        Assert.Equal(3, options.FailureDetectorConsecutiveFailures);
    }

    [Fact]
    public void ValidateAllDefaultValuesSucceeds()
    {
        var options = new RapidClusterProtocolOptions();

        var result = _validator.Validate(name: null, options);

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
            LeaveMessageTimeout = TimeSpan.FromMilliseconds(1000),
        };

        var result = _validator.Validate(name: null, options);

        Assert.True(result.Succeeded);
    }

    [Fact]
    public void ValidateWithNameWorks()
    {
        var options = CreateValidOptions();

        var result = _validator.Validate("TestOptions", options);

        Assert.True(result.Succeeded);
    }

    private static RapidClusterProtocolOptions CreateValidOptions() => new();
}
