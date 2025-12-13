using System.Diagnostics;

namespace Clockwork;

/// <summary>
/// Shared time source for all simulation components.
/// Provides a single source of truth for the current simulated time,
/// allowing multiple <see cref="SimulationTaskQueue"/> instances to share
/// a unified view of time while maintaining separate task queues.
/// 
/// This class uses a real lock instead of the SingleThreadedGuard because
/// it needs to be accessed from cross-thread contexts (e.g., when
/// SynchronizationContext.Post is called from thread pool threads due to
/// CancellationToken callbacks or other async work escaping the simulation).
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class SimulationClock
{
    private readonly Lock _lock = new();
    private TimeSpan _currentTime;

    /// <summary>
    /// Creates a new simulation clock with the specified start date/time and initial time offset.
    /// </summary>
    /// <param name="startDateTime">The starting date/time for the simulation.</param>
    /// <param name="initialTime">The initial time offset. Default is <see cref="TimeSpan.Zero"/>.</param>
    public SimulationClock(DateTimeOffset startDateTime, TimeSpan initialTime = default)
    {
        StartDateTime = startDateTime;
        _currentTime = initialTime;
    }

    /// <summary>
    /// Gets the starting date/time for the simulation.
    /// </summary>
    public DateTimeOffset StartDateTime { get; }

    /// <summary>
    /// Gets the current simulated time as an offset from the start.
    /// </summary>
    public TimeSpan CurrentTime
    {
        get
        {
            lock (_lock)
            {
                return _currentTime;
            }
        }
    }

    /// <summary>
    /// Gets the current simulated date/time (StartDateTime + CurrentTime).
    /// </summary>
    public DateTimeOffset UtcNow => StartDateTime + CurrentTime;

    /// <summary>
    /// Advances the current time by the specified amount.
    /// </summary>
    /// <param name="delta">The amount to advance. Must be non-negative.</param>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="delta"/> is negative.</exception>
    public void Advance(TimeSpan delta)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(delta, TimeSpan.Zero);
        lock (_lock)
        {
            _currentTime += delta;
        }
    }

    private string DebuggerDisplay => $"SimulationClock({CurrentTime:hh\\:mm\\:ss\\.fff})";
}
