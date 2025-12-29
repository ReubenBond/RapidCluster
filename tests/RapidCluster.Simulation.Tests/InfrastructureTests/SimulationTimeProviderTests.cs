using System.Globalization;
using Clockwork;

namespace RapidCluster.Simulation.Tests.InfrastructureTests;

/// <summary>
/// Tests for SimulationTimeProvider, modeled after Microsoft's FakeTimeProviderTests.
/// </summary>
public class SimulationTimeProviderTests
{
    private static readonly TimeSpan InfiniteTimeout = TimeSpan.FromMilliseconds(-1);

    /// <summary>
    /// Helper class that wraps the simulation components and provides convenience methods for testing.
    /// </summary>
    private sealed class TestTimeProvider
    {
        public SimulationClock Clock { get; }
        public SimulationTaskQueue TaskQueue { get; }
        public SimulationTimeProvider TimeProvider { get; }
        public DateTimeOffset Start { get; }

        public TestTimeProvider(DateTimeOffset? startDateTime = null)
        {
            Start = startDateTime ?? new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
            var guard = new SingleThreadedGuard();
            Clock = new SimulationClock(Start);
            TaskQueue = new SimulationTaskQueue(Clock, guard);
            TimeProvider = new SimulationTimeProvider(TaskQueue, Clock);
        }

        // Convenience delegations to TimeProvider
        public DateTimeOffset GetUtcNow() => TimeProvider.GetUtcNow();
        public long GetTimestamp() => TimeProvider.GetTimestamp();
        public long TimestampFrequency => TimeProvider.TimestampFrequency;
        public TimeSpan GetElapsedTime(long startingTimestamp) => TimeProvider.GetElapsedTime(startingTimestamp);
        public TimeSpan GetElapsedTime(long startingTimestamp, long endingTimestamp) => TimeProvider.GetElapsedTime(startingTimestamp, endingTimestamp);
        public ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
            => TimeProvider.CreateTimer(callback, state, dueTime, period);

        // Test-only convenience methods
        public void Advance(TimeSpan delta) => Clock.Advance(delta);

        public void SetUtcNow(DateTimeOffset value)
        {
            var delta = value - GetUtcNow();
            if (delta < TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(value), string.Create(CultureInfo.InvariantCulture, $"Cannot go back in time. Current time is {GetUtcNow()}."));
            Clock.Advance(delta);
        }

        public TimeSpan? TimeUntilNextTimer
        {
            get
            {
                var nextDueTime = TaskQueue.NextWaitingDueTime;
                if (!nextDueTime.HasValue)
                    return null;

                var currentTime = TaskQueue.UtcNow;
                var duration = nextDueTime.Value - currentTime;
                return duration > TimeSpan.Zero ? duration : TimeSpan.Zero;
            }
        }

        public bool AdvanceToNextTimer()
        {
            var nextDueTime = TaskQueue.NextWaitingDueTime;
            if (!nextDueTime.HasValue)
                return false;

            var delta = nextDueTime.Value - TaskQueue.UtcNow;
            if (delta > TimeSpan.Zero)
                Clock.Advance(delta);

            return true;
        }

        public IReadOnlyList<(DateTimeOffset DueTime, TimeSpan Period)> GetPendingTimers()
        {
            var result = SimulationTimer.GetTimers(TaskQueue);
            return [.. result.OrderBy(t => t.DueTime)];
        }

        public int PendingTimerCount => SimulationTimer.GetPendingTimerCount(TaskQueue);

        public void RunOnce() => TaskQueue.RunOnce();
        public void RunUntilIdle() => TaskQueue.RunUntilIdle();
    }

    [Fact]
    public void ConstructorDefaultInitializationSetsExpectedValues()
    {
        var p = new TestTimeProvider();

        var now = p.GetUtcNow();
        var timestamp = p.GetTimestamp();
        var frequency = p.TimestampFrequency;

        Assert.Equal(2000, now.Year);
        Assert.Equal(1, now.Month);
        Assert.Equal(1, now.Day);
        Assert.Equal(0, now.Hour);
        Assert.Equal(0, now.Minute);
        Assert.Equal(0, now.Second);
        Assert.Equal(0, now.Millisecond);
        Assert.Equal(TimeSpan.Zero, now.Offset);
        Assert.Equal(TimeSpan.TicksPerSecond, frequency);

        var timestamp2 = p.GetTimestamp();
        var frequency2 = p.TimestampFrequency;
        var now2 = p.GetUtcNow();

        Assert.Equal(now, now2);
        Assert.Equal(frequency, frequency2);
        Assert.Equal(timestamp, timestamp2);
    }

    [Fact]
    public void ConstructorInitializesWithCustomDateTimeOffset()
    {
        var customTime = new DateTimeOffset(2023, 6, 15, 10, 30, 45, TimeSpan.Zero);
        var p = new TestTimeProvider(customTime);

        var now = p.GetUtcNow();

        Assert.Equal(2023, now.Year);
        Assert.Equal(6, now.Month);
        Assert.Equal(15, now.Day);
        Assert.Equal(10, now.Hour);
        Assert.Equal(30, now.Minute);
        Assert.Equal(45, now.Second);
        Assert.Equal(customTime, p.Start);
    }

    [Fact]
    public void GetTimestampWithoutAdvanceDoesNotChange()
    {
        var p = new TestTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, 0, TimeSpan.Zero));

        var timestamp1 = p.GetTimestamp();
        var timestamp2 = p.GetTimestamp();

        Assert.Equal(timestamp1, timestamp2);
    }

    [Fact]
    public void GetTimestampAfterAdvanceChanges()
    {
        var p = new TestTimeProvider();
        var timestamp1 = p.GetTimestamp();

        p.Advance(TimeSpan.FromSeconds(1));
        var timestamp2 = p.GetTimestamp();

        Assert.True(timestamp2 > timestamp1);
    }

    [Fact]
    public void GetElapsedTimeAfterAdvanceReturnsCorrectDuration()
    {
        var p = new TestTimeProvider();
        var start = p.GetTimestamp();

        p.Advance(TimeSpan.FromSeconds(1));
        var elapsed = p.GetElapsedTime(start);

        Assert.Equal(TimeSpan.FromSeconds(1), elapsed);
    }

    [Fact]
    public void AdvanceForwardAdvancesByProperAmount()
    {
        var p = new TestTimeProvider(new DateTimeOffset(2001, 2, 3, 4, 5, 6, TimeSpan.Zero));

        var initialTimeUtcNow = p.GetUtcNow();
        var initialTimestamp = p.GetTimestamp();

        p.Advance(TimeSpan.FromMilliseconds(1234));

        var finalTimeUtcNow = p.GetUtcNow();
        var finalTimeTimestamp = p.GetTimestamp();

        var utcDelta = finalTimeUtcNow - initialTimeUtcNow;
        var perfDelta = finalTimeTimestamp - initialTimestamp;
        var elapsedTime = p.GetElapsedTime(initialTimestamp, finalTimeTimestamp);

        Assert.Equal(1, utcDelta.Seconds);
        Assert.Equal(234, utcDelta.Milliseconds);
        Assert.Equal(1234D, utcDelta.TotalMilliseconds);
        Assert.Equal(1.234D, (double)perfDelta / p.TimestampFrequency, 3);
        Assert.Equal(1234, elapsedTime.TotalMilliseconds);
    }

    [Fact]
    public void AdvanceByZeroDoesNotChangeTime()
    {
        var p = new TestTimeProvider();
        var before = p.GetUtcNow();

        p.Advance(TimeSpan.Zero);

        Assert.Equal(before, p.GetUtcNow());
    }

    [Fact]
    public void AdvanceBackwardsThrowsArgumentOutOfRangeException()
    {
        var p = new TestTimeProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() => p.Advance(TimeSpan.FromTicks(-1)));
    }

    [Fact]
    public void AdvanceMultipleSmallIncrementsAccumulatesCorrectly()
    {
        var p = new TestTimeProvider();
        var start = p.GetUtcNow();

        for (var i = 0; i < 100; i++)
        {
            p.Advance(TimeSpan.FromMilliseconds(10));
        }

        var elapsed = p.GetUtcNow() - start;
        Assert.Equal(1000, elapsed.TotalMilliseconds);
    }

    [Fact]
    public void SetUtcNowForwardAdvancesByProperAmount()
    {
        var p = new TestTimeProvider(new DateTimeOffset(2001, 2, 3, 4, 5, 6, TimeSpan.Zero));

        var initialTimeUtcNow = p.GetUtcNow();
        var initialTimestamp = p.GetTimestamp();

        p.SetUtcNow(p.GetUtcNow().AddMilliseconds(1234));

        var finalTimeUtcNow = p.GetUtcNow();
        var finalTimeTimestamp = p.GetTimestamp();

        var utcDelta = finalTimeUtcNow - initialTimeUtcNow;
        var perfDelta = finalTimeTimestamp - initialTimestamp;
        var elapsedTime = p.GetElapsedTime(initialTimestamp, finalTimeTimestamp);

        Assert.Equal(1, utcDelta.Seconds);
        Assert.Equal(234, utcDelta.Milliseconds);
        Assert.Equal(1234D, utcDelta.TotalMilliseconds);
        Assert.Equal(1.234D, (double)perfDelta / p.TimestampFrequency, 3);
        Assert.Equal(1234, elapsedTime.TotalMilliseconds);
    }

    [Fact]
    public void SetUtcNowBackwardsThrowsArgumentOutOfRangeException()
    {
        var p = new TestTimeProvider();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            p.SetUtcNow(p.GetUtcNow() - TimeSpan.FromTicks(1)));
    }

    [Fact]
    public void SetUtcNowToSameTimeDoesNotThrow()
    {
        var p = new TestTimeProvider();
        var currentTime = p.GetUtcNow();

        p.SetUtcNow(currentTime);

        Assert.Equal(currentTime, p.GetUtcNow());
    }

    [Fact]
    public void CreateTimerWithDueTimeCreatesWaiter()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        Assert.Equal(0, callCount);
        Assert.True(p.PendingTimerCount > 0);
        Assert.Equal(1, p.PendingTimerCount);
    }

    [Fact]
    public void CreateTimerWithZeroDueTimeSchedulesCallback()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.Zero, TimeSpan.Zero);

        // Timer with TimeSpan.Zero schedules callback to task queue
        Assert.Equal(0, callCount);
        p.RunOnce();
        Assert.Equal(1, callCount);
    }

    [Fact]
    public void TimerCallbackFiresAfterAdvance()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        Assert.Equal(0, callCount);

        p.Advance(TimeSpan.FromMilliseconds(999));
        p.RunUntilIdle();
        Assert.Equal(0, callCount);

        p.Advance(TimeSpan.FromMilliseconds(1));
        p.RunUntilIdle();
        Assert.Equal(1, callCount);
    }

    [Fact]
    public void TimerCallbackPassesState()
    {
        var p = new TestTimeProvider();
        object? receivedState = null;
        var expectedState = new object();

        using var timer = p.CreateTimer(state => receivedState = state, expectedState, TimeSpan.Zero, TimeSpan.Zero);
        p.RunOnce();

        Assert.Same(expectedState, receivedState);
    }

    [Fact]
    public void PeriodicTimerFiresRepeatedly()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(1, callCount);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(2, callCount);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(3, callCount);
    }

    [Fact]
    public void OneShotTimerFiresOnlyOnce()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(1, callCount);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(1, callCount); // Should still be 1

        p.Advance(TimeSpan.FromSeconds(10));
        p.RunUntilIdle();
        Assert.Equal(1, callCount); // Should still be 1
    }

    [Fact]
    public void DisposedTimerDoesNotFire()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        timer.Dispose();

        p.Advance(TimeSpan.FromSeconds(10));
        p.RunUntilIdle();
        Assert.Equal(0, callCount);
    }

    [Fact]
    public void ChangedTimerUsesNewValues()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(10), TimeSpan.Zero);

        // Change to fire sooner
        timer.Change(TimeSpan.FromMilliseconds(100), TimeSpan.Zero);

        p.Advance(TimeSpan.FromMilliseconds(100));
        p.RunUntilIdle();
        Assert.Equal(1, callCount);
    }

    [Fact]
    public void TimerChangedToInfiniteDoesNotFire()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        // Change to infinite (disabled)
        timer.Change(Timeout.InfiniteTimeSpan, TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(100));
        p.RunUntilIdle();
        Assert.Equal(0, callCount);
    }

    [Fact]
    public void MultipleTimersFireInOrder()
    {
        var p = new TestTimeProvider();
        var firedOrder = new List<int>();

        using var timer1 = p.CreateTimer(_ => firedOrder.Add(1), state: null, TimeSpan.FromSeconds(3), TimeSpan.Zero);
        using var timer2 = p.CreateTimer(_ => firedOrder.Add(2), state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        using var timer3 = p.CreateTimer(_ => firedOrder.Add(3), state: null, TimeSpan.FromSeconds(2), TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(5));
        p.RunUntilIdle();

        Assert.Equal([2, 3, 1], firedOrder);
    }

    [Fact]
    public void TimersWithSameDueTimeFireInScheduledOrder()
    {
        var p = new TestTimeProvider();
        var firedOrder = new List<int>();

        using var timer1 = p.CreateTimer(_ => firedOrder.Add(1), state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        using var timer2 = p.CreateTimer(_ => firedOrder.Add(2), state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        using var timer3 = p.CreateTimer(_ => firedOrder.Add(3), state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();

        Assert.Equal([1, 2, 3], firedOrder);
    }

    [Fact]
    public void AdvanceTimeInCallbackPreventsInfiniteLoop()
    {
        var p = new TestTimeProvider();
        var oneSecond = TimeSpan.FromSeconds(1);
        var callCount = 0;

        using var timer = p.CreateTimer(_ =>
        {
            callCount++;
            // Advance the time with exactly the same amount as the period of the timer.
            // This could lead to an infinite loop where this callback repeatedly gets invoked.
            // A correct implementation will adjust the timer's wake time.
            p.Advance(oneSecond);
        }, state: null, TimeSpan.Zero, oneSecond);

        // Execute only the currently ready items (not items added during execution)
        // This prevents infinite loops when callbacks enqueue more items
        p.RunUntilIdle();

        // Should not hang and call count should be limited
        Assert.True(callCount >= 1, "Timer should have fired at least once");
        Assert.True(callCount < 1000, "Timer should not have fired infinitely");
    }

    [Fact]
    public void TimerCallbackExceptionPropagates()
    {
        var p = new TestTimeProvider();

        using var timer1 = p.CreateTimer(_ => throw new InvalidOperationException("Test exception"),
state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(3));
        // This should throw due to timer1's callback - exceptions propagate from timer callbacks
        var ex = Assert.Throws<InvalidOperationException>(p.RunUntilIdle);
        Assert.Equal("Test exception", ex.Message);
    }

    [Fact]
    public void AdvanceToNextTimerWithPendingTimer()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(5), TimeSpan.Zero);

        var result = p.AdvanceToNextTimer();
        p.RunUntilIdle();

        Assert.True(result);
        Assert.Equal(1, callCount);
    }

    [Fact]
    public void AdvanceToNextTimerWithNoTimersReturnsFalse()
    {
        var p = new TestTimeProvider();

        var result = p.AdvanceToNextTimer();

        Assert.False(result);
    }

    [Fact]
    public void AdvanceToNextTimerMultipleTimersFiresOnlyNext()
    {
        var p = new TestTimeProvider();
        var firedTimers = new List<int>();

        using var timer1 = p.CreateTimer(_ => firedTimers.Add(1), state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        using var timer2 = p.CreateTimer(_ => firedTimers.Add(2), state: null, TimeSpan.FromSeconds(2), TimeSpan.Zero);
        using var timer3 = p.CreateTimer(_ => firedTimers.Add(3), state: null, TimeSpan.FromSeconds(3), TimeSpan.Zero);

        p.AdvanceToNextTimer();
        p.RunUntilIdle();
        Assert.Equal([1], firedTimers);

        p.AdvanceToNextTimer();
        p.RunUntilIdle();
        Assert.Equal([1, 2], firedTimers);

        p.AdvanceToNextTimer();
        p.RunUntilIdle();
        Assert.Equal([1, 2, 3], firedTimers);
    }

    [Fact]
    public void TimeUntilNextTimerNoPendingTimersReturnsNull()
    {
        var p = new TestTimeProvider();

        Assert.Null(p.TimeUntilNextTimer);
    }

    [Fact]
    public void TimeUntilNextTimerWithPendingTimerReturnsCorrectDuration()
    {
        var p = new TestTimeProvider();

        using var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(5), TimeSpan.Zero);

        Assert.Equal(TimeSpan.FromSeconds(5), p.TimeUntilNextTimer);
    }

    [Fact]
    public void TimeUntilNextTimerAfterPartialAdvanceReturnsRemainingTime()
    {
        var p = new TestTimeProvider();

        using var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(5), TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(3));

        Assert.Equal(TimeSpan.FromSeconds(2), p.TimeUntilNextTimer);
    }

    [Fact]
    public void TimeUntilNextTimerPeriodicTimerShowsNextPeriod()
    {
        var p = new TestTimeProvider();
        var callCount = 0;

        // Use a periodic timer so it stays registered
        using var timer = p.CreateTimer(_ => callCount++, state: null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(10));

        // After timer fires, next time should be period away
        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(1, callCount);
        Assert.Equal(TimeSpan.FromSeconds(10), p.TimeUntilNextTimer);
    }

    [Fact]
    public void GetPendingTimersNoPendingReturnsEmpty()
    {
        var p = new TestTimeProvider();

        var timers = p.GetPendingTimers();

        Assert.Empty(timers);
    }

    [Fact]
    public void GetPendingTimersWithTimersReturnsAllPending()
    {
        var p = new TestTimeProvider();

        using var timer1 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        using var timer2 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(2), TimeSpan.Zero);

        var timers = p.GetPendingTimers();

        Assert.Equal(2, timers.Count);
    }

    [Fact]
    public void GetPendingTimersOrderedByWakeTime()
    {
        var p = new TestTimeProvider();

        using var timer1 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(3), TimeSpan.Zero);
        using var timer2 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        using var timer3 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(2), TimeSpan.Zero);

        var timers = p.GetPendingTimers();

        Assert.Equal(3, timers.Count);
        Assert.True(timers[0].DueTime < timers[1].DueTime);
        Assert.True(timers[1].DueTime < timers[2].DueTime);
    }

    [Fact]
    public async Task DelayZeroDelayCompletesImmediately()
    {
        var p = new TestTimeProvider();

        var task = Task.Delay(TimeSpan.Zero, p.TimeProvider, TestContext.Current.CancellationToken);

        Assert.True(task.IsCompleted);
        await task;
    }

    [Fact]
    public async Task DelayAwaitedCompletesSuccessfully()
    {
        var p = new TestTimeProvider();

        var delay = Task.Delay(TimeSpan.FromMilliseconds(1), p.TimeProvider, TestContext.Current.CancellationToken);
        Assert.False(delay.IsCompleted);

        p.Advance(TimeSpan.FromMilliseconds(1));
        p.RunUntilIdle();
        await delay;

        Assert.True(delay.IsCompleted);
        Assert.False(delay.IsFaulted);
        Assert.False(delay.IsCanceled);
    }

    [Fact]
    public async Task DelayCancelledTokenThrowsTaskCanceledException()
    {
        var p = new TestTimeProvider();

        using var cts = new CancellationTokenSource();
        var delay = Task.Delay(InfiniteTimeout, p.TimeProvider, cts.Token);
        Assert.False(delay.IsCompleted);

        cts.SafeCancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await delay);
    }

    [Fact]
    public async Task DelayWhenTimeAdvancedCompletesWithoutCancellation()
    {
        var p = new TestTimeProvider();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1000));

        var task = Task.Delay(TimeSpan.FromMilliseconds(10000), p.TimeProvider, cts.Token);

        p.Advance(TimeSpan.FromMilliseconds(10000));
        p.RunUntilIdle();

        await task;

        Assert.False(cts.Token.IsCancellationRequested);
    }

    [Fact]
    public void DelayMultipleDelaysRegisteredInCorrectOrder()
    {
        var p = new TestTimeProvider();
        var ct = TestContext.Current.CancellationToken;

        // Install sync context so continuations are captured by the simulation
        using var _ = p.TaskQueue.SynchronizationContext.Install();

        // Create delays - they immediately register timers with the provider
        var delay1 = Task.Delay(TimeSpan.FromSeconds(3), p.TimeProvider, ct);
        var delay2 = Task.Delay(TimeSpan.FromSeconds(1), p.TimeProvider, ct);
        var delay3 = Task.Delay(TimeSpan.FromSeconds(2), p.TimeProvider, ct);

        // Verify timers are registered with correct due times
        var pendingTimers = p.GetPendingTimers();
        Assert.Equal(3, pendingTimers.Count);

        // Timers should be ordered by wake time (1s, 2s, 3s)
        Assert.Equal(p.Start + TimeSpan.FromSeconds(1), pendingTimers[0].DueTime);
        Assert.Equal(p.Start + TimeSpan.FromSeconds(2), pendingTimers[1].DueTime);
        Assert.Equal(p.Start + TimeSpan.FromSeconds(3), pendingTimers[2].DueTime);

        // Advance time and verify delays complete in order
        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.True(delay2.IsCompleted);
        Assert.False(delay3.IsCompleted);
        Assert.False(delay1.IsCompleted);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.True(delay3.IsCompleted);
        Assert.False(delay1.IsCompleted);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.True(delay1.IsCompleted);
    }

    [Fact]
    public void DelayMultipleDelaysContinuationsRunInCorrectOrder()
    {
        var p = new TestTimeProvider();
        var scheduler = new SimulationTaskScheduler(p.TaskQueue);
        var completionOrder = new List<int>();
        var ct = TestContext.Current.CancellationToken;

        // Install sync context so continuations are captured by the simulation
        using var _ = p.TaskQueue.SynchronizationContext.Install();

        // Create delays with continuations that record completion order
        var delay1 = Task.Delay(TimeSpan.FromSeconds(3), p.TimeProvider, ct).ContinueWith(_ => completionOrder.Add(1), ct, TaskContinuationOptions.None, scheduler);
        var delay2 = Task.Delay(TimeSpan.FromSeconds(1), p.TimeProvider, ct).ContinueWith(_ => completionOrder.Add(2), ct, TaskContinuationOptions.None, scheduler);
        var delay3 = Task.Delay(TimeSpan.FromSeconds(2), p.TimeProvider, ct).ContinueWith(_ => completionOrder.Add(3), ct, TaskContinuationOptions.None, scheduler);

        // Advance time past all delays and execute
        p.Advance(TimeSpan.FromSeconds(5));
        p.RunUntilIdle();

        // Continuations should run in order of their delay durations: 1s (2), 2s (3), 3s (1)
        Assert.Equal([2, 3, 1], completionOrder);
    }

    [Fact]
    public void ToStringDefaultReturnsProperFormat()
    {
        var p = new TestTimeProvider();

        var result = p.TimeProvider.ToString();

        Assert.Equal("2000-01-01T00:00:00.000", result);
    }

    [Fact]
    public void ToStringCustomTimeReturnsProperFormat()
    {
        var dto = new DateTimeOffset(new DateTime(2022, 1, 2, 3, 4, 5, 6), TimeSpan.Zero);
        var p = new TestTimeProvider(dto);

        Assert.Equal("2022-01-02T03:04:05.006", p.TimeProvider.ToString());
    }

    [Fact]
    public void ToStringAfterAdvanceReturnsUpdatedTime()
    {
        var p = new TestTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));

        p.Advance(TimeSpan.FromHours(1).Add(TimeSpan.FromMinutes(30)));

        Assert.Equal("2000-01-01T01:30:00.000", p.TimeProvider.ToString());
    }

    [Fact]
    public void PendingTimerCountNoTimersReturnsZero()
    {
        var p = new TestTimeProvider();

        Assert.Equal(0, p.PendingTimerCount);
    }

    [Fact]
    public void PendingTimerCountWithTimerReturnsOne()
    {
        var p = new TestTimeProvider();

        using var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        Assert.True(p.PendingTimerCount > 0);
    }

    [Fact]
    public void PendingTimerCountAfterTimerFiresReturnsZero()
    {
        var p = new TestTimeProvider();

        using var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);

        p.Advance(TimeSpan.FromSeconds(1));

        Assert.Equal(0, p.PendingTimerCount);
    }

    [Fact]
    public void PendingTimerCountTracksCorrectly()
    {
        var p = new TestTimeProvider();

        Assert.Equal(0, p.PendingTimerCount);

        using var timer1 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        Assert.Equal(1, p.PendingTimerCount);

        using var timer2 = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(2), TimeSpan.Zero);
        Assert.Equal(2, p.PendingTimerCount);

        p.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(1, p.PendingTimerCount);

        p.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(0, p.PendingTimerCount);
    }

    [Fact]
    public void PendingTimerCountWithPeriodicTimerStaysConstant()
    {
        var p = new TestTimeProvider();

        using var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));

        Assert.Equal(1, p.PendingTimerCount);

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle(); // Execute to trigger rescheduling of periodic timer
        Assert.Equal(1, p.PendingTimerCount); // Periodic timer stays registered

        p.Advance(TimeSpan.FromSeconds(1));
        p.RunUntilIdle();
        Assert.Equal(1, p.PendingTimerCount);
    }

    [Fact]
    public void TimerDisposeRemovesFromPending()
    {
        var p = new TestTimeProvider();

        var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        Assert.Equal(1, p.PendingTimerCount);

        timer.Dispose();
        Assert.Equal(0, p.PendingTimerCount);
    }

    [Fact]
    public async Task TimerDisposeAsyncRemovesFromPending()
    {
        var p = new TestTimeProvider();

        var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        Assert.Equal(1, p.PendingTimerCount);

        await timer.DisposeAsync();
        Assert.Equal(0, p.PendingTimerCount);
    }

    [Fact]
    public void TimerDoubleDisposeDoesNotThrow()
    {
        var p = new TestTimeProvider();

        var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        timer.Dispose();
        timer.Dispose(); // Should not throw
    }

    [Fact]
    public void TimerChangeAfterDisposeReturnsFalse()
    {
        var p = new TestTimeProvider();

        var timer = p.CreateTimer(_ => { }, state: null, TimeSpan.FromSeconds(1), TimeSpan.Zero);
        timer.Dispose();

        var result = timer.Change(TimeSpan.FromSeconds(1), TimeSpan.Zero);
        Assert.False(result);
    }

    [Fact]
    public void ConcurrentAccessThrowsInvalidOperationException()
    {
        // Create a task queue with a guard we can test
        var guard = new SingleThreadedGuard();
        var clock = new SimulationClock(DateTimeOffset.UtcNow);
        var taskQueue = new SimulationTaskQueue(clock, guard);

        using var thread1EnteredGuard = new ManualResetEventSlim(initialState: false);
        using var thread2CanProceed = new ManualResetEventSlim(initialState: false);
        InvalidOperationException? caughtException = null;

        // Schedule an item that will block inside the guard
        // We use a custom approach: have thread 1 enter the guard and signal,
        // then thread 2 tries to enter and should fail
        var thread1 = new Thread(() =>
        {
            // Schedule an action - this enters the guard
            taskQueue.EnqueueAfter(() =>
            {
                // This callback runs OUTSIDE the guard, so we need a different approach
            }, TimeSpan.Zero);

            // Enter the guard and hold it while signaling thread 2
            using (guard.Enter())
            {
                thread1EnteredGuard.Set();
                thread2CanProceed.Wait(); // Hold the guard until thread 2 has tried
            }
        });

        var thread2 = new Thread(() =>
        {
            thread1EnteredGuard.Wait(); // Wait for thread 1 to hold the guard
            try
            {
                // Try to access a guarded method - should throw
                _ = taskQueue.HasItems;
            }
            catch (InvalidOperationException ex)
            {
                caughtException = ex;
            }
            finally
            {
                thread2CanProceed.Set(); // Let thread 1 release the guard
            }
        });

        thread1.Start();
        thread2.Start();
        thread1.Join(TimeSpan.FromSeconds(5));
        thread2.Join(TimeSpan.FromSeconds(5));

        Assert.NotNull(caughtException);
        Assert.Contains("single-threaded", caughtException.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void StartReturnsInitialTime()
    {
        var startTime = new DateTimeOffset(2023, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var p = new TestTimeProvider(startTime);

        Assert.Equal(startTime, p.Start);
    }

    [Fact]
    public void StartUnchangedAfterAdvance()
    {
        var startTime = new DateTimeOffset(2023, 6, 15, 12, 0, 0, TimeSpan.Zero);
        var p = new TestTimeProvider(startTime);

        p.Advance(TimeSpan.FromHours(5));

        Assert.Equal(startTime, p.Start);
        Assert.NotEqual(startTime, p.GetUtcNow());
    }
}
