namespace RapidCluster.Grpc;

/// <summary>
/// Configuration options for RapidCluster gRPC transport.
/// </summary>
public sealed class RapidClusterGrpcOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether to use HTTPS for gRPC communication. Default: false (use HTTP).
    /// </summary>
    /// <remarks>
    /// <para>
    /// When enabled, gRPC clients will connect using https:// scheme instead of http://.
    /// This is required when the server uses TLS and Http1AndHttp2 protocol mode,
    /// which allows both HTTP/1.1 (for health checks) and HTTP/2 (for gRPC) on the same port.
    /// </para>
    /// <para>
    /// In development environments with self-signed certificates, you may need to
    /// configure the HttpClient to trust the development certificate.
    /// </para>
    /// </remarks>
    public bool UseHttps { get; set; }
}
