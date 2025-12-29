namespace RapidCluster;

internal sealed class OneShotTimer : IDisposable
{
    private ITimer? _timer;
    private int _token;

    public void Dispose()
    {
        System.Threading.Interlocked.Increment(ref _token);
        _timer?.Dispose();
        _timer = null;
    }

    public void Schedule(TimeProvider timeProvider, TimeSpan dueTime, Action callback)
    {
        Dispose();

        var token = System.Threading.Interlocked.Increment(ref _token);
        _timer = timeProvider.CreateTimer(
            static state =>
            {
                var (timer, expectedToken, callback) = ((OneShotTimer Timer, int Token, Action Callback))state!;
                if (expectedToken != System.Threading.Volatile.Read(ref timer._token))
                {
                    return;
                }

                callback();
            },
            state: (this, token, callback),
            dueTime: dueTime,
            period: System.Threading.Timeout.InfiniteTimeSpan);
    }
}
