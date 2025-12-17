using Microsoft.Extensions.Logging;

namespace RapidCluster.Tests.Simulation.Infrastructure.Logging;

/// <summary>
/// High-performance logger for InMemoryMessagingClient using LoggerMessage source generators.
/// </summary>
internal sealed partial class InMemoryMessagingClientLogger(ILogger<InMemoryMessagingClient> logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger => _logger;

    [LoggerMessage(Level = LogLevel.Trace, Message = "Attempting to send {MessageType} from {Local} to {Remote}")]
    public partial void AttemptingSend(object messageType, string local, string remote);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Message {MessageType} from {Local} to {Remote} blocked by network partition")]
    public partial void MessageBlockedByPartition(object messageType, string local, string remote);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Message {MessageType} from {Local} to {Remote} dropped (simulated packet loss)")]
    public partial void MessageDroppedPacketLoss(object messageType, string local, string remote);

    [LoggerMessage(Level = LogLevel.Error, Message = "Target node {Remote} not found when sending from {Local}")]
    public partial void TargetNodeNotFound(string remote, string local);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Scheduling delivery of {MessageType} from {Local} to {Remote}")]
    public partial void SchedulingDelivery(object messageType, string local, string remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Target node {Remote} was crashed before message delivery from {Local}")]
    public partial void TargetNodeCrashedBeforeDelivery(string remote, string local);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Delivering {MessageType} from {Local} to {Remote}")]
    public partial void DeliveringMessage(object messageType, string local, string remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Message {MessageType} from {Local} to {Remote} failed: {Error}")]
    public partial void MessageDeliveryFailed(object messageType, string local, string remote, string error);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Message {MessageType} from {Local} to {Remote} timed out after {Timeout}")]
    public partial void MessageTimedOut(object messageType, string local, string remote, TimeSpan timeout);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Simulating {DelayMs}ms delay for message from {Local} to {Remote}")]
    public partial void SimulatingDelay(double delayMs, string local, string remote);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Best-effort message to {Remote} timed out: {Message}")]
    public partial void BestEffortTimedOut(string remote, string message);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Best-effort message to {Remote} failed: {Message}")]
    public partial void BestEffortFailed(string remote, string message);
}
