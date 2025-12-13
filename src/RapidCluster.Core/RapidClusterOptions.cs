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
    /// The seed node endpoint to join. If null or equal to ListenAddress, starts a new cluster.
    /// </summary>
    public Endpoint? SeedAddress { get; set; }

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
