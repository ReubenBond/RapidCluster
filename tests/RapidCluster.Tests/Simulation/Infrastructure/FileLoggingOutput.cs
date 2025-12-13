using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Text;
using Microsoft.Extensions.Logging;

namespace RapidCluster.Tests.Simulation.Infrastructure;

/// <summary>
/// The log output which all <see cref="FileLogger"/> instances share to log messages to.
/// </summary>
internal sealed class FileLoggingOutput : IDisposable
{
    private static readonly ConcurrentDictionary<FileLoggingOutput, object?> Instances = new();
    private readonly TimeSpan _flushInterval = Debugger.IsAttached ? TimeSpan.FromMilliseconds(10) : TimeSpan.FromSeconds(1);
    private readonly Lock _lock = new();
    private readonly string _logFileName;
    private readonly TimeProvider _timeProvider;
    private DateTimeOffset _lastFlush;
#pragma warning disable CA2213 // Disposable fields should be disposed - _logOutput is disposed in Dispose(bool)
    private StreamWriter? _logOutput;
#pragma warning restore CA2213

    static FileLoggingOutput()
    {
        AppDomain.CurrentDomain.ProcessExit += CurrentDomain_ProcessExit;

        static void CurrentDomain_ProcessExit(object? sender, EventArgs args)
        {
            foreach (var instance in Instances)
            {
                instance.Key.Dispose();
            }
        }
    }

    public FileLoggingOutput(string fileName, TimeProvider? timeProvider = null)
    {
        _logFileName = fileName;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _lastFlush = _timeProvider.GetUtcNow();

        // Ensure directory exists
        var directory = Path.GetDirectoryName(fileName);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        // Use append mode and allow shared read/write access like Orleans
        _logOutput = new StreamWriter(
            File.Open(fileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite),
            Encoding.UTF8);

        Instances[this] = null;
    }

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter,
        string category)
    {
        var logMessage = FormatMessage(
            _timeProvider.GetUtcNow(),
            logLevel,
            category,
            formatter(state, exception),
            exception,
            eventId);

        lock (_lock)
        {
            if (_logOutput == null) return;

            _logOutput.WriteLine(logMessage);
            var now = _timeProvider.GetUtcNow();
            if (now - _lastFlush > _flushInterval)
            {
                _lastFlush = now;
                _logOutput.Flush();
            }
        }
    }

    private static string FormatMessage(
        DateTimeOffset timestamp,
        LogLevel logLevel,
        string caller,
        string message,
        Exception? exception,
        EventId errorCode)
    {
        if (logLevel == LogLevel.Error)
            message = "!!!!!!!!!! " + message;

        var exc = exception != null ? PrintException(exception) : string.Empty;
        var msg = $"[{timestamp:yyyy-MM-dd HH:mm:ss.fff} {Environment.CurrentManagedThreadId}\t{logLevel}\t{errorCode}\t{caller}]\t{message}\t{exc}";

        return msg;
    }

    private static string PrintException(Exception? exception)
    {
        if (exception == null)
            return string.Empty;

        var sb = new StringBuilder();
        PrintException(sb, exception, 0);
        return sb.ToString();
    }

    private static void PrintException(StringBuilder sb, Exception exception, int level)
    {
        if (exception == null) return;

        sb.Append($"{Environment.NewLine}Exc level {level}: {exception.GetType()}: {exception.Message}");

        if (exception.StackTrace is { } stack)
        {
            sb.Append($"{Environment.NewLine}{stack}");
        }

        if (exception is ReflectionTypeLoadException typeLoadException)
        {
            var loaderExceptions = typeLoadException.LoaderExceptions;
            if (loaderExceptions == null || loaderExceptions.Length == 0)
            {
                sb.Append("No LoaderExceptions found");
            }
            else
            {
                foreach (var inner in loaderExceptions)
                {
                    if (inner is not null)
                    {
                        // call recursively on all loader exceptions. Same level for all.
                        PrintException(sb, inner, level + 1);
                    }
                }
            }
        }
        else if (exception.InnerException != null)
        {
            if (exception is AggregateException { InnerExceptions: { Count: > 1 } innerExceptions })
            {
                foreach (var inner in innerExceptions)
                {
                    // call recursively on all inner exceptions. Same level for all.
                    PrintException(sb, inner, level + 1);
                }
            }
            else
            {
                // call recursively on a single inner exception.
                PrintException(sb, exception.InnerException, level + 1);
            }
        }
    }

    public void Dispose()
    {
        try
        {
            lock (_lock)
            {
                if (_logOutput is { } output)
                {
                    _logOutput = null;
                    Instances.TryRemove(this, out _);

                    // Dispose the output, which will flush all buffers.
                    output.Dispose();
                }
            }
        }
#pragma warning disable CA1031 // Do not catch general exception types - intentional for robustness during disposal
        catch (Exception exc)
#pragma warning restore CA1031
        {
            // Intentionally swallow exception during disposal - nothing useful we can do here
            _ = exc;
        }
    }
}

/// <summary>
/// A logger provider that writes log messages to a file.
/// Modeled after Orleans.TestingHost.Logging for robust file logging during tests.
/// </summary>
internal sealed class FileLoggerProvider(string filePath, TimeProvider? timeProvider = null) : ILoggerProvider
{
    private readonly FileLoggingOutput _output = new(filePath, timeProvider);
    private bool _disposed;

    public ILogger CreateLogger(string categoryName) => new FileLogger(categoryName, _output);

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _output.Dispose();
    }
}

/// <summary>
/// A logger that writes to a file via the FileLoggingOutput.
/// Modeled after Orleans.TestingHost.Logging.FileLogger.
/// </summary>
internal sealed class FileLogger(string categoryName, FileLoggingOutput output) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => NullScope.Instance;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter)
    {
        if (!IsEnabled(logLevel)) return;
        output.Log(logLevel, eventId, state, exception, formatter, categoryName);
    }

    private sealed class NullScope : IDisposable
    {
        public static NullScope Instance { get; } = new NullScope();

        private NullScope()
        {
        }

        public void Dispose()
        {
        }
    }
}
