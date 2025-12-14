using Google.Protobuf;
using RapidCluster.Pb;

namespace RapidCluster;

/// <summary>
/// Configuration options for RapidCluster.
/// </summary>
public sealed class RapidClusterOptions
{
    /// <summary>
    /// The endpoint this node listens on.
    /// </summary>
    public Endpoint ListenAddress { get; set; } = null!;

    /// <summary>
    /// The seed node endpoints to join. If null/empty or all entries equal ListenAddress, starts a new cluster.
    /// Nodes are tried in round-robin order until join succeeds.
    /// </summary>
#pragma warning disable CA2227 // Collection properties should be read only - Options classes need settable collections
    public IReadOnlyList<Endpoint>? SeedAddresses { get; set; }
#pragma warning restore CA2227

    /// <summary>
    /// Metadata for this node.
    /// </summary>
    public Metadata Metadata { get; set; } = new();

    /// <summary>
    /// Sets metadata from a dictionary.
    /// </summary>
    public void SetMetadata(Dictionary<string, ByteString> metadata)
    {
        ArgumentNullException.ThrowIfNull(metadata);
        Metadata = new Metadata();
        foreach (var kvp in metadata)
        {
            Metadata.Metadata_.Add(kvp.Key, kvp.Value);
        }
    }
}
