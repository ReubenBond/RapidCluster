namespace RapidCluster.Exceptions;

/// <summary>
/// Exception thrown when a node attempts to bootstrap but discovers that
/// a cluster has already been formed. The node should fall back to the
/// normal join protocol instead.
/// </summary>
public sealed class BootstrapAlreadyCompleteException : Exception
{
    /// <summary>
    /// The configuration ID of the already-formed cluster.
    /// </summary>
    public ConfigurationId? ClusterConfigurationId { get; }

    public BootstrapAlreadyCompleteException()
    {
    }

    public BootstrapAlreadyCompleteException(string message) : base(message)
    {
    }

    public BootstrapAlreadyCompleteException(string message, Exception innerException) : base(message, innerException)
    {
    }

    public BootstrapAlreadyCompleteException(string message, ConfigurationId clusterConfigurationId) : base(message)
    {
        ClusterConfigurationId = clusterConfigurationId;
    }
}
