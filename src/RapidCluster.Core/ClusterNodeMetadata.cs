using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Text;

namespace RapidCluster;

/// <summary>
/// Represents metadata associated with a cluster node.
/// Provides a read-only dictionary of string keys to binary values.
/// </summary>
[DebuggerDisplay("{DebuggerDisplay,nq}")]
[SuppressMessage("Naming", "CA1710:Identifiers should have correct suffix", Justification = "ClusterMetadata is a semantic name; adding 'Dictionary' suffix would be awkward")]
public sealed class ClusterNodeMetadata : IReadOnlyDictionary<string, ReadOnlyMemory<byte>>
{
    private static readonly Dictionary<string, ReadOnlyMemory<byte>> EmptyData = [];

    /// <summary>
    /// An empty metadata instance with no entries.
    /// </summary>
    public static ClusterNodeMetadata Empty { get; } = new(EmptyData);

    private readonly Dictionary<string, ReadOnlyMemory<byte>> _data;

    /// <summary>
    /// Creates a new ClusterMetadata instance with the specified data.
    /// </summary>
    /// <param name="data">The metadata dictionary.</param>
    internal ClusterNodeMetadata(Dictionary<string, ReadOnlyMemory<byte>> data)
    {
        ArgumentNullException.ThrowIfNull(data);
        _data = data;
    }

    /// <inheritdoc/>
    public ReadOnlyMemory<byte> this[string key] => _data[key];

    /// <inheritdoc/>
    public IEnumerable<string> Keys => _data.Keys;

    /// <inheritdoc/>
    public IEnumerable<ReadOnlyMemory<byte>> Values => _data.Values;

    /// <inheritdoc/>
    public int Count => _data.Count;

    /// <inheritdoc/>
    public bool ContainsKey(string key) => _data.ContainsKey(key);

    /// <inheritdoc/>
    public bool TryGetValue(string key, out ReadOnlyMemory<byte> value) => _data.TryGetValue(key, out value);

    /// <inheritdoc/>
    public IEnumerator<KeyValuePair<string, ReadOnlyMemory<byte>>> GetEnumerator() => _data.GetEnumerator();

    /// <inheritdoc/>
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Gets a metadata value as a UTF-8 string.
    /// </summary>
    /// <param name="key">The key to look up.</param>
    /// <returns>The value as a string, or null if the key doesn't exist.</returns>
    public string? GetString(string key)
    {
        if (!_data.TryGetValue(key, out var value))
        {
            return null;
        }
        return Encoding.UTF8.GetString(value.Span);
    }

    private string DebuggerDisplay => $"ClusterMetadata(Count={Count})";
}
