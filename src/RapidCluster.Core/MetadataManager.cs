using RapidCluster.Pb;

namespace RapidCluster;

internal sealed class MetadataManager
{
    // Use SortedDictionary for deterministic iteration order (important for simulation tests)
    // Use EndpointComparer to ignore MonotonicNodeId when comparing endpoints
    private readonly SortedDictionary<Endpoint, Metadata> _metadata = new(EndpointComparer.Instance);
    private readonly Lock _lock = new();

    public void Add(Endpoint endpoint, Metadata metadata)
    {
        lock (_lock)
        {
            _metadata[endpoint] = metadata;
        }
    }

    public void AddMetadata(IReadOnlyDictionary<Endpoint, Metadata> metadata)
    {
        lock (_lock)
        {
            foreach (var kvp in metadata)
            {
                _metadata[kvp.Key] = kvp.Value;
            }
        }
    }

    public Metadata? Get(Endpoint endpoint)
    {
        lock (_lock)
        {
            return _metadata.TryGetValue(endpoint, out var metadata) ? metadata : null;
        }
    }

    public void RemoveNode(Endpoint endpoint)
    {
        lock (_lock)
        {
            _metadata.Remove(endpoint);
        }
    }

    public void Clear()
    {
        lock (_lock)
        {
            _metadata.Clear();
        }
    }

    public IReadOnlyDictionary<Endpoint, Metadata> GetAllMetadata()
    {
        lock (_lock)
        {
            // Use EndpointAddressComparer to ignore MonotonicNodeId when comparing endpoints
            return new Dictionary<Endpoint, Metadata>(_metadata, EndpointAddressComparer.Instance);
        }
    }
}
