namespace RapidCluster.Exceptions;

public sealed class JoinException : Exception
{
    public JoinException(string message) : base(message)
    {
    }

    public JoinException(string message, Exception innerException) : base(message, innerException)
    {
    }

    public JoinException()
    {
    }
}
