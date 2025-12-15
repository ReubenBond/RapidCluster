using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// Health check that reports the cluster membership status.
/// Reports healthy when the node is part of a cluster with at least one member.
/// </summary>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed class ClusterMembershipHealthCheck : IHealthCheck
{
    private readonly IRapidCluster _cluster;

    /// <summary>
    /// Initializes a new instance of the <see cref="ClusterMembershipHealthCheck"/> class.
    /// </summary>
    /// <param name="cluster">The RapidCluster instance.</param>
    public ClusterMembershipHealthCheck(IRapidCluster cluster)
    {
        _cluster = cluster;
    }

    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var view = _cluster.CurrentView;

        if (view is null)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                "Cluster view not yet available - node may be joining"));
        }

        var memberCount = view.Members.Length;
        var configId = view.ConfigurationId;

        if (memberCount == 0)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy(
                "Not a member of any cluster"));
        }

        var data = new Dictionary<string, object>
        {
            ["MemberCount"] = memberCount,
            ["ConfigurationId"] = configId,
        };

        return Task.FromResult(HealthCheckResult.Healthy(
            $"Cluster member (size={memberCount}, configId={configId})",
            data));
    }
}
