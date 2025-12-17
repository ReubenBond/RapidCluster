using System.Diagnostics.CodeAnalysis;

namespace RapidCluster.Unit.Tests;

[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public class BroadcastChannelTests
{
    [Fact]
    public async Task MoveNextAsync_ReturnsCurrentValue_WhenSubscribingAfterPublish()
    {
        // Arrange
        using var channel = new BroadcastChannel<int>();

        // Publish first element before subscribing
        channel.Writer.TryPublish(1);

        // Get enumerator - should receive element 1 on first MoveNextAsync
        var enumerator = channel.Reader.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        // Act - first MoveNextAsync should return the current value (replay behavior)
        var result = await enumerator.MoveNextAsync();

        // Assert
        Assert.True(result, "MoveNextAsync should return true");
        Assert.Equal(1, enumerator.Current);
    }

    [Fact]
    public async Task MoveNextAsync_CompletesImmediately_WhenNextElementIsAlreadyAvailable()
    {
        // Arrange
        using var channel = new BroadcastChannel<int>();

        // Publish first element
        channel.Writer.TryPublish(1);

        // Get enumerator - should start at element 1
        var enumerator = channel.Reader.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        // Move to first element (replay)
        await enumerator.MoveNextAsync();

        // Publish second element - this should complete the "next" TCS of element 1
        channel.Writer.TryPublish(2);

        // Act - call MoveNextAsync
        var task = enumerator.MoveNextAsync();

        // Assert - task should be completed synchronously
        Assert.True(task.IsCompleted, "MoveNextAsync should complete immediately when next element is available");
        Assert.True(await task, "MoveNextAsync should return true");
        Assert.Equal(2, enumerator.Current);
    }

    [Fact]
    public void MoveNextAsync_DoesNotCompleteImmediately_WhenNextElementIsNotAvailable()
    {
        // Arrange
        using var channel = new BroadcastChannel<int>();

        // Publish first element
        channel.Writer.TryPublish(1);

        // Get enumerator - should start at element 1
        var enumerator = channel.Reader.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        // Move to first element (replay)
        var firstMove = enumerator.MoveNextAsync();
        Assert.True(firstMove.IsCompleted, "First MoveNextAsync should complete immediately (replay)");

        // Act - call MoveNextAsync WITHOUT publishing next element
        var task = enumerator.MoveNextAsync();

        // Assert - task should NOT be completed
        Assert.False(task.IsCompleted, "MoveNextAsync should not complete when next element is not available");
    }

    [Fact]
    public async Task MoveNextAsync_WaitsForValue_WhenSubscribingBeforeFirstPublish()
    {
        // Arrange
        using var channel = new BroadcastChannel<int>();

        // Get enumerator before any publish
        var enumerator = channel.Reader.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        // Act - call MoveNextAsync, it should wait
        var task = enumerator.MoveNextAsync();
        Assert.False(task.IsCompleted, "MoveNextAsync should wait when no value published yet");

        // Now publish
        channel.Writer.TryPublish(42);

        // Assert - should now complete with the value
        var result = await task;
        Assert.True(result, "MoveNextAsync should return true");
        Assert.Equal(42, enumerator.Current);
    }

    [Fact]
    public async Task MoveNextAsync_ReturnsFalse_WhenChannelIsCompleted()
    {
        // Arrange
        using var channel = new BroadcastChannel<int>();
        channel.Writer.TryPublish(1);

        var enumerator = channel.Reader.GetAsyncEnumerator(TestContext.Current.CancellationToken);

        // Move to first element
        await enumerator.MoveNextAsync();

        // Complete the channel
        channel.Writer.Complete();

        // Act
        var result = await enumerator.MoveNextAsync();

        // Assert
        Assert.False(result, "MoveNextAsync should return false when channel is completed");
    }
}
