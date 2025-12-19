using Clockwork;

namespace RapidCluster.Simulation.Tests.InfrastructureTests;

/// <summary>
/// Tests for the simulation task scheduler and synchronization context.
/// </summary>
public sealed class SimulationSchedulerTests
{
    private static (SimulationTaskQueue TaskQueue, SimulationClock Clock, SimulationTaskScheduler Scheduler) CreateComponents()
    {
        var guard = new SingleThreadedGuard();
        var clock = new SimulationClock(DateTimeOffset.UtcNow);
        var taskQueue = new SimulationTaskQueue(clock, guard);
        var scheduler = new SimulationTaskScheduler(taskQueue);
        return (taskQueue, clock, scheduler);
    }

    [Fact]
    public void QueuedTasksAreNotExecutedAutomatically()
    {
        var (taskQueue, _, scheduler) = CreateComponents();
        var executed = false;

        var task = new Task(() => executed = true);
        task.Start(scheduler);

        Assert.False(executed);
        Assert.Single(scheduler.Tasks);
    }

    [Fact]
    public void StepExecutesSingleTask()
    {
        var (taskQueue, _, scheduler) = CreateComponents();
        var executed = false;

        var task = new Task(() => executed = true);
        task.Start(scheduler);

        var result = taskQueue.RunOnce();

        Assert.True(result);
        Assert.True(executed);
        Assert.Empty(scheduler.Tasks);
    }

    [Fact]
    public void StepAllExecutesAllTasks()
    {
        var (taskQueue, _, scheduler) = CreateComponents();
        var count = 0;

        for (var i = 0; i < 5; i++)
        {
            var task = new Task(() => Interlocked.Increment(ref count));
            task.Start(scheduler);
        }

        var executed = taskQueue.RunUntilIdle();

        Assert.Equal(5, executed);
        Assert.Equal(5, count);
        Assert.Empty(scheduler.Tasks);
    }

    [Fact]
    public void StepWithCountLimitsExecution()
    {
        var (taskQueue, _, scheduler) = CreateComponents();
        var count = 0;

        for (var i = 0; i < 10; i++)
        {
            var task = new Task(() => Interlocked.Increment(ref count));
            task.Start(scheduler);
        }

        var executed = 0;
        for (var i = 0; i < 3 && taskQueue.RunOnce(); i++)
        {
            executed++;
        }

        Assert.Equal(3, executed);
        Assert.Equal(3, count);
        Assert.Equal(7, scheduler.Tasks.Count);
    }

    [Fact]
    public void TasksExecuteInFifoOrder()
    {
        var (taskQueue, _, scheduler) = CreateComponents();
        var order = new List<int>();

        for (var i = 0; i < 5; i++)
        {
            var index = i;
            var task = new Task(() => order.Add(index));
            task.Start(scheduler);
        }

        taskQueue.RunUntilIdle();

        Assert.Equal([0, 1, 2, 3, 4], order);
    }

    [Fact]
    public void SynchronizationContextPostRoutesToScheduler()
    {
        var (taskQueue, _, _) = CreateComponents();
        var syncContext = taskQueue.SynchronizationContext;
        var executed = false;

        syncContext.Post(_ => executed = true, null);

        Assert.False(executed);
        taskQueue.RunOnce();
        Assert.True(executed);
    }

    [Fact]
    public void SynchronizationContextSendExecutesSynchronously()
    {
        var (taskQueue, _, _) = CreateComponents();
        var syncContext = taskQueue.SynchronizationContext;
        var executed = false;

        Assert.Throws<InvalidOperationException>(() => syncContext.Send(_ => executed = true, null));

        Assert.False(executed);
    }

    [Fact]
    public void SynchronizationContextCreateCopyReturnsNewInstance()
    {
        var (taskQueue, _, _) = CreateComponents();
        var syncContext = taskQueue.SynchronizationContext;

        var copy = syncContext.CreateCopy();

        Assert.NotSame(syncContext, copy);
        Assert.IsType<SynchronizationContext>(copy, exactMatch: false);
    }

    [Fact]
    public void SchedulerWithTaskQueueOrdersByTime()
    {
        var (taskQueue, clock, scheduler) = CreateComponents();
        var order = new List<string>();

        // Queue first task
        var task1 = new Task(() => order.Add("first"));
        task1.Start(scheduler);

        // Advance time and queue second task
        clock.Advance(TimeSpan.FromSeconds(1));
        var task2 = new Task(() => order.Add("second"));
        task2.Start(scheduler);

        taskQueue.RunUntilIdle();

        // First task should execute before second (queued at earlier time)
        Assert.Equal(["first", "second"], order);
    }

    [Fact]
    public void TryExecuteOneReturnsFalseWhenEmpty()
    {
        var (taskQueue, _, _) = CreateComponents();

        var result = taskQueue.RunOnce();

        Assert.False(result);
    }

    [Fact]
    public void HasPendingTasksReflectsQueueState()
    {
        var (taskQueue, _, scheduler) = CreateComponents();

        Assert.Empty(scheduler.Tasks);

        var task = new Task(() => { });
        task.Start(scheduler);

        Assert.True(scheduler.Tasks.Count > 0);

        taskQueue.RunOnce();

        Assert.Empty(scheduler.Tasks);
    }
}
