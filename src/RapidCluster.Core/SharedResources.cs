namespace RapidCluster;

/// <summary>
/// Holds all resources that are shared across a single instance of Rapid.
/// </summary>
/// <remarks>
/// Creates a new SharedResources instance.
/// </remarks>
/// <param name="timeProvider">Optional TimeProvider for time-related operations.</param>
/// <param name="taskScheduler">Optional TaskScheduler for scheduling tasks.</param>
/// <param name="random">Optional Random instance for random number generation.</param>
/// <param name="guidFactory">Optional factory function for generating GUIDs.</param>
/// <param name="shuttingDownToken">Cancellation token that signals when shutdown begins.</param>
public sealed class SharedResources(
    TimeProvider? timeProvider = null,
    TaskScheduler? taskScheduler = null,
    Random? random = null,
    Func<Guid>? guidFactory = null,
    CancellationToken shuttingDownToken = default)
{
    private readonly Random _random = random ?? Random.Shared;
    private readonly Func<Guid> _guidFactory = guidFactory ?? Guid.NewGuid;

    /// <summary>
    /// Gets the TimeProvider used for all time-related operations.
    /// </summary>
    public TimeProvider TimeProvider { get; } = timeProvider ?? TimeProvider.System;

    /// <summary>
    /// Gets the TaskScheduler used for scheduling tasks.
    /// </summary>
    public TaskScheduler TaskScheduler { get; } = taskScheduler ?? TaskScheduler.Default;

    /// <summary>
    /// Generates a random double value between 0.0 and 1.0.
    /// </summary>
#pragma warning disable CA5394 // Do not use insecure randomness. Justification: this is not security-sensitive code.
    public double NextRandomDouble() => _random.NextDouble();
#pragma warning restore CA5394

    /// <summary>
    /// Generates a new GUID.
    /// </summary>
    public Guid NewGuid() => _guidFactory();

    /// <summary>
    /// Gets a cancellation token that is cancelled when shutdown begins.
    /// </summary>
    public CancellationToken ShuttingDownToken { get; } = shuttingDownToken;

    /// <summary>
    /// Gets a value indicating whether gets whether shutdown has been initiated.
    /// </summary>
    public bool IsShuttingDown => ShuttingDownToken.IsCancellationRequested;
}
