using RapidCluster.Pb;

namespace RapidCluster.Monitoring;

/// <summary>
/// Factory for creating edge failure detector instances.
/// </summary>
public interface IEdgeFailureDetectorFactory
{
    /// <summary>
    /// Creates a new edge failure detector instance for monitoring a subject node.
    /// </summary>
    /// <param name="subject">The endpoint to monitor.</param>
    /// <param name="notifier">Action to invoke when the subject is detected as failed.</param>
    /// <returns>A new edge failure detector instance.</returns>
    IEdgeFailureDetector CreateInstance(Endpoint subject, Action notifier);
}

/// <summary>
/// Interface for monitoring a single edge (connection) to a subject node.
/// Implementations detect when the monitored node has failed and invoke a callback.
/// </summary>
public interface IEdgeFailureDetector : IDisposable
{
    /// <summary>
    /// Starts monitoring the subject node for failures.
    /// </summary>
    void Start();
}
