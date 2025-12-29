using RapidCluster.Pb;

namespace RapidCluster.Exceptions;

public sealed class NodeNotInRingException : Exception
{
    public NodeNotInRingException(Endpoint node) : this(node?.ToString() ?? throw new ArgumentNullException(nameof(node)))
    {
    }
    public NodeNotInRingException()
    {
    }

    public NodeNotInRingException(string message) : base(message)
    {
    }

    public NodeNotInRingException(string message, Exception innerException) : base(message, innerException)
    {
    }
}
