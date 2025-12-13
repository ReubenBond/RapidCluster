using RapidCluster.Pb;

namespace RapidCluster.Exceptions;

public sealed class UuidAlreadySeenException : Exception
{
    public UuidAlreadySeenException(Endpoint node, NodeId nodeId)
        : this($"Endpoint add attempt with identifier already seen: {{host: {node ?? throw new ArgumentNullException(nameof(node))}, identifier: {nodeId ?? throw new ArgumentNullException(nameof(nodeId))}}}")
    {
    }

    public UuidAlreadySeenException()
    {
    }

    public UuidAlreadySeenException(string message) : base(message)
    {
    }

    public UuidAlreadySeenException(string message, Exception innerException) : base(message, innerException)
    {
    }
}

