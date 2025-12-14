using System.Diagnostics;
using System.Reactive.Subjects;

namespace RapidCluster;

/// <summary>
/// Contains sentinel values used by <see cref="BroadcastChannel{T}"/>.
/// </summary>
internal static class BroadcastChannel
{
    internal static readonly object InitialValue = new();
    internal static readonly object DisposedValue = new();
}

/// <summary>
/// A broadcast channel that allows multiple subscribers to receive the same items.
/// Provides <see cref="Reader"/> for consumers and <see cref="Writer"/> for producers.
/// </summary>
/// <typeparam name="T">The type of items in the channel.</typeparam>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class BroadcastChannel<T> : IDisposable
{
    private readonly ReplaySubject<T> _subject = new(1);
    private readonly Lock _lock = new();
    private Element _current;
    private bool _isCompleted;

    /// <summary>
    /// Creates a new broadcast channel.
    /// </summary>
    public BroadcastChannel()
    {
        _current = Element.CreateInitial();
        Reader = new BroadcastChannelReader<T>(this);
        Writer = new BroadcastChannelWriter<T>(this);
    }

    /// <summary>
    /// Gets the reader for consuming items from this channel.
    /// Implements both <see cref="IAsyncEnumerable{T}"/> and <see cref="IObservable{T}"/>.
    /// </summary>
    public BroadcastChannelReader<T> Reader { get; }

    /// <summary>
    /// Gets the writer for publishing items to this channel.
    /// </summary>
    public BroadcastChannelWriter<T> Writer { get; }

    /// <summary>
    /// Gets whether the channel has been completed.
    /// </summary>
    internal bool IsCompleted => _isCompleted;

    /// <summary>
    /// Gets the current element for new enumerators.
    /// </summary>
    internal Element Current => _current;

    /// <summary>
    /// Gets the subject for IObservable subscriptions.
    /// </summary>
    internal IObservable<T> Observable => _subject;

    /// <summary>
    /// Publishes an item to all current and future subscribers.
    /// </summary>
    /// <param name="item">The item to publish.</param>
    /// <returns>True if published successfully, false if completed.</returns>
    internal bool TryPublish(T item)
    {
        if (_isCompleted) return false;

        lock (_lock)
        {
            if (_isCompleted) return false;

            // Update the async enumerable chain
            var newElement = new Element(item);
            var prev = _current;
            _current = newElement;
            prev.SetNext(newElement);
        }

        // Notify IObservable subscribers via Subject
        _subject.OnNext(item);

        return true;
    }

    /// <summary>
    /// Publishes an item to all current and future subscribers.
    /// Throws if the channel has been completed.
    /// </summary>
    /// <param name="item">The item to publish.</param>
    internal void Publish(T item)
    {
        if (!TryPublish(item))
        {
            throw new InvalidOperationException("The channel has been completed.");
        }
    }

    /// <summary>
    /// Completes the channel, signaling to all subscribers that no more items will be published.
    /// </summary>
    internal void Complete()
    {
        if (_isCompleted) return;

        lock (_lock)
        {
            if (_isCompleted) return;

            _isCompleted = true;

            // Complete the async enumerable chain
            var disposed = Element.CreateDisposed();
            var prev = _current;
            _current = disposed;
            prev.SetNext(disposed);
        }

        // Complete IObservable subscribers via Subject
        _subject.OnCompleted();
        _subject.Dispose();
    }

    /// <summary>
    /// Completes the channel with an error, notifying all subscribers.
    /// </summary>
    /// <param name="error">The error to propagate to subscribers.</param>
    internal void CompleteWithError(Exception error)
    {
        ArgumentNullException.ThrowIfNull(error);

        if (_isCompleted) return;

        lock (_lock)
        {
            if (_isCompleted) return;

            _isCompleted = true;

            // Complete the async enumerable chain
            var disposed = Element.CreateDisposed();
            var prev = _current;
            _current = disposed;
            prev.SetNext(disposed);
        }

        // Notify IObservable subscribers of the error via Subject
        _subject.OnError(error);
        _subject.Dispose();
    }

    /// <summary>
    /// Disposes the channel, completing it if not already completed.
    /// </summary>
    public void Dispose() => Complete();

    private string DebuggerDisplay => $"BroadcastChannel<{typeof(T).Name}>({(_isCompleted ? "Completed" : "Active")})";

    internal sealed class Element
    {
        private readonly TaskCompletionSource<Element> _next;
        private readonly object? _value;

        public Element(T value)
            : this(value, new TaskCompletionSource<Element>(TaskCreationOptions.RunContinuationsAsynchronously))
        {
        }

        private Element(object? value, TaskCompletionSource<Element> next)
        {
            _value = value;
            _next = next;
        }

        public static Element CreateInitial() => new(
            BroadcastChannel.InitialValue,
            new TaskCompletionSource<Element>(TaskCreationOptions.RunContinuationsAsynchronously));

        public static Element CreateDisposed()
        {
            // Use a self-referential completed task instead of an exception.
            // This avoids unobserved task exceptions when no one awaits NextAsync()
            // on the disposed element. The IsDisposed check in MoveNextAsync prevents
            // any subscriber from actually calling NextAsync() on a disposed element.
            var tcs = new TaskCompletionSource<Element>(TaskCreationOptions.RunContinuationsAsynchronously);
            var disposed = new Element(BroadcastChannel.DisposedValue, tcs);
            tcs.SetResult(disposed); // Self-referential: NextAsync returns itself
            return disposed;
        }

        public bool IsValid => !IsInitial && !IsDisposed;
        public bool IsInitial => ReferenceEquals(_value, BroadcastChannel.InitialValue);
        public bool IsDisposed => ReferenceEquals(_value, BroadcastChannel.DisposedValue);

        public T Value
        {
            get
            {
                if (!IsValid) throw new InvalidOperationException("Element does not have a valid value.");
                return (T)_value!;
            }
        }

        public Task<Element> NextAsync() => _next.Task;

        public void SetNext(Element next) => _next.SetResult(next);
    }
}

/// <summary>
/// A read-only view of a broadcast channel that allows multiple subscribers to receive the same items.
/// Implements both <see cref="IAsyncEnumerable{T}"/> and <see cref="IObservable{T}"/> for flexibility.
/// </summary>
/// <typeparam name="T">The type of items in the channel.</typeparam>
/// <remarks>
/// <para>
/// This type provides a consumer view of a broadcast channel. It does not expose publishing methods.
/// Use <see cref="BroadcastChannelWriter{T}"/> to publish items.
/// </para>
/// <para>
/// For <see cref="IAsyncEnumerable{T}"/>: Each subscriber gets their own enumerator that starts from
/// the current position and receives all subsequent items.
/// </para>
/// <para>
/// For <see cref="IObservable{T}"/>: Subscribers receive notifications immediately when items are published.
/// The observable inherently supports multiple observers without needing <c>Publish()</c> from Rx.NET
/// because the underlying <see cref="Subject{T}"/> multicasts to all subscribers.
/// </para>
/// </remarks>
public sealed class BroadcastChannelReader<T> : IAsyncEnumerable<T>, IObservable<T>
{
    private readonly BroadcastChannel<T> _channel;

    internal BroadcastChannelReader(BroadcastChannel<T> channel)
    {
        _channel = channel;
    }

    /// <inheritdoc />
    public IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        => new AsyncEnumerator(_channel.Current, cancellationToken);

    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<T> observer) => _channel.Observable.Subscribe(observer);

    private sealed class AsyncEnumerator : IAsyncEnumerator<T>
    {
        private readonly CancellationToken _cancellationToken;
        private BroadcastChannel<T>.Element _current;

        public AsyncEnumerator(BroadcastChannel<T>.Element initial, CancellationToken cancellationToken)
        {
            _cancellationToken = cancellationToken;

            // If there's no valid current value, just use the initial element directly.
            // Otherwise, create an initial placeholder that points to the valid element.
            // This way, the first MoveNextAsync() will return the current value immediately,
            // matching the replay behavior of the IObservable path (ReplaySubject with buffer 1).
            if (!initial.IsValid)
            {
                _current = initial;
            }
            else
            {
                var result = BroadcastChannel<T>.Element.CreateInitial();
                result.SetNext(initial);
                _current = result;
            }
        }

        public T Current => _current.Value;

        public ValueTask<bool> MoveNextAsync()
        {
            if (_current.IsDisposed)
            {
                return ValueTask.FromResult(false);
            }

            var nextTask = _current.NextAsync();

            // Fast path: if next element is already available, complete synchronously.
            // This is critical for simulation tests where async continuations may not
            // be pumped by the custom scheduler.
            if (nextTask.IsCompletedSuccessfully)
            {
                // CA1849: Safe to access .Result since we've verified IsCompletedSuccessfully
#pragma warning disable CA1849
                _current = nextTask.Result;
#pragma warning restore CA1849
                return ValueTask.FromResult(_current.IsValid);
            }

            // Slow path: await asynchronously
            return MoveNextAsyncCore(nextTask);
        }

        private async ValueTask<bool> MoveNextAsyncCore(Task<BroadcastChannel<T>.Element> nextTask)
        {
            try
            {
                _current = await nextTask.WaitAsync(_cancellationToken).ConfigureAwait(true);
                return _current.IsValid;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

/// <summary>
/// A writer for publishing items to a <see cref="BroadcastChannel{T}"/>.
/// </summary>
/// <typeparam name="T">The type of items to publish.</typeparam>
/// <remarks>
/// This type provides a producer view of a broadcast channel. Use it to publish items
/// and complete the channel. The associated <see cref="BroadcastChannelReader{T}"/> provides
/// the consumer interface for subscribers.
/// </remarks>
public sealed class BroadcastChannelWriter<T> : IDisposable
{
    private readonly BroadcastChannel<T> _channel;
    private bool _disposed;

    internal BroadcastChannelWriter(BroadcastChannel<T> channel)
    {
        _channel = channel;
    }

    /// <summary>
    /// Publishes an item to all current and future subscribers.
    /// </summary>
    /// <param name="item">The item to publish.</param>
    /// <returns>True if published successfully, false if the channel has been completed.</returns>
    public bool TryPublish(T item)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return _channel.TryPublish(item);
    }

    /// <summary>
    /// Publishes an item to all current and future subscribers.
    /// Throws if the channel has been completed.
    /// </summary>
    /// <param name="item">The item to publish.</param>
    public void Publish(T item)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _channel.Publish(item);
    }

    /// <summary>
    /// Completes the channel, signaling to all subscribers that no more items will be published.
    /// </summary>
    public void Complete()
    {
        if (_disposed) return;
        _channel.Complete();
    }

    /// <summary>
    /// Completes the channel with an error, notifying all subscribers.
    /// </summary>
    /// <param name="error">The error to propagate to subscribers.</param>
    public void CompleteWithError(Exception error)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _channel.CompleteWithError(error);
    }

    /// <summary>
    /// Disposes the writer, completing the channel if it hasn't been completed yet.
    /// </summary>
    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _channel.Complete();
    }
}
