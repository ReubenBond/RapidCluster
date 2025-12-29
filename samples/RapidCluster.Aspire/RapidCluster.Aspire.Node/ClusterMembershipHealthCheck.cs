using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace RapidCluster.Aspire.Node;

/// <summary>
/// Health check that reports the cluster membership status.
/// Reports healthy when the node is part of a cluster with at least one member.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="ClusterMembershipHealthCheck"/> class.
/// </remarks>
/// <param name="cluster">The RapidCluster instance.</param>
[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Instantiated via DI")]
internal sealed class ClusterMembershipHealthCheck(IRapidCluster cluster) : IHealthCheck
{
    /// <inheritdoc/>
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        var view = cluster.CurrentView;

        if (view is null)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                "Cluster view not yet available - node may be joining"));
        }

        var memberCount = view.Members.Length;
        var configId = view.ConfigurationId;

        if (memberCount == 0)
        {
            return Task.FromResult(HealthCheckResult.Degraded("Not a member of any cluster"));
        }

        var data = new Dictionary<string, object>
(StringComparer.Ordinal)
        {
            ["MemberCount"] = memberCount,
            ["ConfigurationId"] = configId,
        };

        return Task.FromResult(HealthCheckResult.Healthy(
            string.Create(CultureInfo.InvariantCulture, $"Cluster member (size={memberCount}, configId={configId})"),
            data));
    }
}
