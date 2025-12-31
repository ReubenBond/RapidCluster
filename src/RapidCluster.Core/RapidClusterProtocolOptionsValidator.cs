using System.Globalization;
using Microsoft.Extensions.Options;

namespace RapidCluster;

/// <summary>
/// Validates RapidClusterProtocolOptions configuration at startup.
/// </summary>
internal sealed class RapidClusterProtocolOptionsValidator : IValidateOptions<RapidClusterProtocolOptions>
{
    public ValidateOptionsResult Validate(string? name, RapidClusterProtocolOptions options)
    {
        if (options.BatchingWindow <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("BatchingWindow must be positive");
        }

        if (options.ConsensusFallbackTimeoutBaseDelay <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("ConsensusFallbackTimeoutBaseDelay must be positive");
        }

        if (options.LeaveMessageTimeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail("LeaveMessageTimeout must be positive");
        }

        // Validate K, H, L constraints from the Rapid paper: K >= 3, K > H > L > 0
        if (options.ObserversPerSubject < RapidClusterProtocolOptions.MinObserversPerSubject)
        {
            return ValidateOptionsResult.Fail(
                $"ObserversPerSubject must be at least {RapidClusterProtocolOptions.MinObserversPerSubject}. " +
                "The Rapid protocol requires a minimum of 3 observers per subject for proper failure detection.");
        }

        if (options.HighWatermark <= 0)
        {
            return ValidateOptionsResult.Fail("HighWatermark must be positive");
        }

        if (options.LowWatermark <= 0)
        {
            return ValidateOptionsResult.Fail("LowWatermark must be positive");
        }

        // K > H: Need at least one more observer than required for stable detection
        if (options.HighWatermark >= options.ObserversPerSubject)
        {
            return ValidateOptionsResult.Fail(string.Create(CultureInfo.InvariantCulture,
                $"HighWatermark ({options.HighWatermark}) must be less than ObserversPerSubject ({options.ObserversPerSubject}). ") +
                "The protocol requires K > H to allow for some observer failures.");
        }

        // H > L: Need a gap between stable and unstable thresholds
        if (options.LowWatermark >= options.HighWatermark)
        {
            return ValidateOptionsResult.Fail(string.Create(CultureInfo.InvariantCulture,
                $"LowWatermark ({options.LowWatermark}) must be less than HighWatermark ({options.HighWatermark}). ") +
                "The gap between H and L is required for almost-everywhere agreement.");
        }

        return ValidateOptionsResult.Success;
    }
}
