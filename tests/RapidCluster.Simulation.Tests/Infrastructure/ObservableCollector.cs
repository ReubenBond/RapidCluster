using System.Reactive.Linq;

namespace RapidCluster.Simulation.Tests.Infrastructure;

/// <summary>
/// Collects items from an observable stream for later inspection in tests.
/// </summary>
/// <typeparam name="T">The type of items in the observable stream.</typeparam>
/// <remarks>
/// Usage pattern:
/// <code>
/// using var collector = new ObservableCollector&lt;MembershipView&gt;(node.ViewAccessor.Updates);
/// 
/// // Run simulation or perform operations...
/// harness.WaitForConvergence(expectedSize: 3);
/// 
/// // Inspect collected items
/// var viewsWithSize3 = collector.Items.Where(v => v.Size == 3).ToList();
/// </code>
/// </remarks>
public sealed class ObservableCollector<T> : IDisposable
{
    private readonly List<T> _items = [];
    private readonly Lock _lock = new();
    private readonly IDisposable _subscription;
    private bool _disposed;

    /// <summary>
    /// Creates a new collector that subscribes to the given observable stream.
    /// </summary>
    /// <param name="source">The observable stream to collect from.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="source"/> is null.</exception>
    public ObservableCollector(IObservable<T> source)
    {
        ArgumentNullException.ThrowIfNull(source);
        _subscription = source.Subscribe(OnNext, OnError, OnCompleted);
    }

    /// <summary>
    /// Gets a snapshot of all collected items.
    /// </summary>
    public IReadOnlyList<T> Items
    {
        get
        {
            lock (_lock)
            {
                return [.. _items];
            }
        }
    }

    /// <summary>
    /// Gets the number of collected items.
    /// </summary>
    public int Count
    {
        get
        {
            lock (_lock)
            {
                return _items.Count;
            }
        }
    }

    /// <summary>
    /// Gets whether the observable has completed.
    /// </summary>
    public bool IsCompleted { get; private set; }

    /// <summary>
    /// Gets the error if the observable terminated with an error.
    /// </summary>
    public Exception? Error { get; private set; }

    private void OnNext(T item)
    {
        lock (_lock)
        {
            _items.Add(item);
        }
    }

    private void OnError(Exception error)
    {
        Error = error;
        IsCompleted = true;
    }

    private void OnCompleted()
    {
        IsCompleted = true;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _subscription.Dispose();
    }
}
