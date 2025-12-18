using Microsoft.Extensions.Logging;
using RapidCluster.Pb;

namespace RapidCluster.Logging;

internal sealed partial class MembershipRefreshCoordinatorLogger(ILogger<MembershipRefreshCoordinator> logger)
{
    private readonly ILogger _logger = logger;

    /// <summary>
    /// Gets the underlying logger instance.
    /// </summary>
    public ILogger Logger => _logger;

    [LoggerMessage(Level = LogLevel.Debug, Message = "Starting membership refresh coordinator")]
    public partial void Starting();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Stopping membership refresh coordinator")]
    public partial void Stopping();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Disposed membership refresh coordinator")]
    public partial void Disposed();

    [LoggerMessage(EventName = nameof(StaleViewDetected), Level = LogLevel.Information, Message = "Stale view detected from {RemoteEndpoint}: remote config {RemoteConfigVersion} > local config {LocalConfigVersion}")]
    private partial void StaleViewDetectedCore(LoggableEndpoint remoteEndpoint, long remoteConfigVersion, long localConfigVersion);
    public void StaleViewDetected(Endpoint remoteEndpoint, long remoteConfigVersion, long localConfigVersion) => StaleViewDetectedCore(new(remoteEndpoint), remoteConfigVersion, localConfigVersion);

    [LoggerMessage(EventName = nameof(SkippingRefresh), Level = LogLevel.Debug, Message = "Skipping refresh - received config {ReceivedConfigVersion} not newer than local config {LocalConfigVersion}")]
    public partial void SkippingRefresh(long receivedConfigVersion, long localConfigVersion);

    [LoggerMessage(EventName = nameof(RequestingMembershipView), Level = LogLevel.Debug, Message = "Requesting membership view from {RemoteEndpoint}")]
    private partial void RequestingMembershipViewCore(LoggableEndpoint remoteEndpoint);
    public void RequestingMembershipView(Endpoint remoteEndpoint) => RequestingMembershipViewCore(new(remoteEndpoint));

    [LoggerMessage(EventName = nameof(MembershipViewRefreshed), Level = LogLevel.Information, Message = "Successfully refreshed membership view from {RemoteEndpoint}: new config {NewConfigVersion}, {MemberCount} members")]
    private partial void MembershipViewRefreshedCore(LoggableEndpoint remoteEndpoint, long newConfigVersion, int memberCount);
    public void MembershipViewRefreshed(Endpoint remoteEndpoint, long newConfigVersion, int memberCount) => MembershipViewRefreshedCore(new(remoteEndpoint), newConfigVersion, memberCount);

    [LoggerMessage(EventName = nameof(MembershipViewRefreshFailed), Level = LogLevel.Warning, Message = "Failed to refresh membership view from {RemoteEndpoint}: {Message}")]
    private partial void MembershipViewRefreshFailedCore(LoggableEndpoint remoteEndpoint, string message);
    public void MembershipViewRefreshFailed(Endpoint remoteEndpoint, string message) => MembershipViewRefreshFailedCore(new(remoteEndpoint), message);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Refresh loop exiting due to cancellation")]
    public partial void RefreshLoopExiting();

    [LoggerMessage(Level = LogLevel.Debug, Message = "Processing refresh request for config version {ConfigVersion} from {RemoteEndpoint}")]
    private partial void ProcessingRefreshRequestCore(long configVersion, LoggableEndpoint remoteEndpoint);
    public void ProcessingRefreshRequest(long configVersion, Endpoint remoteEndpoint) => ProcessingRefreshRequestCore(configVersion, new(remoteEndpoint));
}
