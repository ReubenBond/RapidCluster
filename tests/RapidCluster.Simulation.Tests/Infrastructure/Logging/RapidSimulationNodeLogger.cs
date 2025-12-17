using Microsoft.Extensions.Logging;

namespace RapidCluster.Simulation.Tests.Infrastructure.Logging;

/// <summary>
/// High-performance logger for RapidSimulationNode using LoggerMessage source generators.
/// Note: Suspend/Resume logging is handled by SimulationNodeContext in the Clockwork library.
/// </summary>
internal sealed partial class RapidSimulationNodeLogger(ILogger<RapidSimulationNode> logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger => _logger;

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {Address} initialized with {MembershipSize} members, ConfigId={ConfigId}")]
    public partial void NodeInitialized(string address, int membershipSize, long configId);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Node {Address} handling request of type {RequestType}")]
    public partial void HandlingRequest(string address, object requestType);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {Address} leaving cluster gracefully")]
    public partial void NodeLeaving(string address);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {Address} completed graceful leave")]
    public partial void NodeLeftGracefully(string address);

    [LoggerMessage(Level = LogLevel.Information, Message = "Node {Address} shutting down")]
    public partial void NodeShuttingDown(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} Shutdown called but node is not initialized")]
    public partial void ShutdownCalledNotInitialized(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} destroying")]
    public partial void NodeDestroying(string address);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Node {Address} destroyed")]
    public partial void NodeDestroyed(string address);
}
