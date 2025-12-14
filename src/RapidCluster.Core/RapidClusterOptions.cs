using System.Diagnostics.CodeAnalysis;
using System.Net;

namespace RapidCluster;

/// <summary>
/// Configuration options for RapidCluster.
/// </summary>
public sealed class RapidClusterOptions
{
    /// <summary>
    /// The endpoint this node listens on.
    /// Use <see cref="IPEndPoint"/> for IP addresses or <see cref="DnsEndPoint"/> for hostnames.
    /// </summary>
    public EndPoint ListenAddress { get; set; } = null!;

    /// <summary>
    /// The seed node endpoints to join. If null/empty or all entries equal ListenAddress, starts a new cluster.
    /// Nodes are tried in round-robin order until join succeeds.
    /// </summary>
    /// <remarks>
    /// This property provides backward compatibility. For more advanced seed discovery,
    /// register an <see cref="Discovery.ISeedProvider"/> implementation in the DI container.
    /// </remarks>
    public IReadOnlyList<EndPoint>? SeedAddresses { get; set; }

    /// <summary>
    /// Metadata for this node.
    /// </summary>
    [SuppressMessage("Usage", "CA2227:Collection properties should be read only", Justification = "Options class needs settable properties for configuration binding")]
    public Dictionary<string, byte[]> Metadata { get; set; } = [];

    /// <summary>
    /// Number of nodes expected to form the initial cluster.
    /// When set to a value greater than 0, seed nodes will wait until this many nodes
    /// have contacted them before starting consensus. This ensures the initial cluster
    /// forms atomically with the expected size.
    /// Set to 0 to disable (default behavior - cluster starts immediately).
    /// </summary>
    /// <remarks>
    /// This is useful in environments where multiple seed nodes start simultaneously
    /// and you want to ensure the cluster forms with a minimum size before accepting
    /// application traffic.
    /// </remarks>
    public int BootstrapExpect { get; set; }

    /// <summary>
    /// Timeout for waiting for <see cref="BootstrapExpect"/> nodes to contact the seed.
    /// If the timeout expires before the expected number of nodes join, the seed will
    /// start the cluster with whatever nodes have contacted it.
    /// Default: 5 minutes.
    /// </summary>
    public TimeSpan BootstrapTimeout { get; set; } = TimeSpan.FromMinutes(5);
}
