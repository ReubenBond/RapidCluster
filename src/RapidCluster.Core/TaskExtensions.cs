using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace RapidCluster;

internal static partial class TaskExtensions
{
    private static readonly Action<Task> IgnoreTaskContinuation = t => { _ = t.Exception; };

    public static async Task<T?> WithDefaultOnException<T>(this Task<T> task)
    {
        await ((Task)task).ConfigureAwait(ConfigureAwaitOptions.ContinueOnCapturedContext | ConfigureAwaitOptions.SuppressThrowing);
        return task switch
        {
            { IsCompletedSuccessfully: true } => await task.ConfigureAwait(true),
            _ => default
        };
    }

    /// <summary>
    /// Observes and ignores a potential exception on a given Task.
    /// If a Task fails and throws an exception which is never observed, it will be caught by the .NET finalizer thread.
    /// This function awaits the given task and if the exception is thrown, it observes this exception and simply ignores it.
    /// This will prevent the escalation of this exception to the .NET finalizer thread.
    /// </summary>
    /// <param name="task">The task to be ignored.</param>
    public static void Ignore(this Task task)
    {
        if (task.IsCompleted)
        {
            _ = task.Exception;
        }
        else
        {
            task.ContinueWith(
                IgnoreTaskContinuation,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>
    /// Observes and ignores a potential exception on a given Task.
    /// If a Task fails and throws an exception which is never observed, it will be caught by the .NET finalizer thread.
    /// This function awaits the given task and if the exception is thrown, it observes this exception and simply ignores it.
    /// This will prevent the escalation of this exception to the .NET finalizer thread.
    /// </summary>
    /// <param name="task">The task to be ignored.</param>
    public static async void Ignore<TResponse>(this AsyncUnaryCall<TResponse> task)
    {
#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            await task.ConfigureAwait(true);
        }
        catch
        {
            // Ignore
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    /// <summary>
    /// Cancels the <see cref="CancellationTokenSource"/> without throwing if a registered callback throws.
    /// Uses the synchronous Cancel method to avoid CA1849 warnings in async contexts where
    /// CancelAsync would be preferred but synchronous cancellation is intentional.
    /// </summary>
    /// <param name="cts">The cancellation token source to cancel.</param>
    /// <param name="logger">Optional logger to log exceptions from cancellation callbacks.</param>
    public static void SafeCancel(this CancellationTokenSource cts, ILogger? logger = null)
    {
#pragma warning disable CA1849 // Call async methods when in an async method
#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            cts.Cancel(throwOnFirstException: false);
        }
        catch (Exception ex)
        {
            if (logger is not null)
            {
                LogCancellationException(logger, ex);
            }
        }
#pragma warning restore CA1031 // Do not catch general exception types
#pragma warning restore CA1849 // Call async methods when in an async method
    }

    [LoggerMessage(Level = LogLevel.Warning, Message = "Exception occurred during CancellationTokenSource.Cancel")]
    private static partial void LogCancellationException(ILogger logger, Exception ex);
}
