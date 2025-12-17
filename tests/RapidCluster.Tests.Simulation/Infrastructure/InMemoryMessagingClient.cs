using Clockwork;
using Microsoft.Extensions.Logging;
using RapidCluster.Messaging;
using RapidCluster.Pb;
using RapidCluster.Tests.Simulation.Infrastructure.Logging;

namespace RapidCluster.Tests.Simulation.Infrastructure;

/// <summary>
/// In-memory messaging client for simulation testing.
/// Routes messages through the SimulationNetwork instead of gRPC.
/// 
/// Messages are delivered by scheduling work on the target node's SimulationTaskQueue.
/// This provides deterministic execution where messages are processed when the simulation steps.
/// </summary>
internal sealed class InMemoryMessagingClient(
    RapidSimulationCluster harness,
    RapidSimulationNode sourceNode,
    Endpoint localEndpoint,
    RapidClusterProtocolOptions options) : IMessagingClient
{
    private readonly TimeSpan _messageTimeout = options.GrpcTimeout;
    private readonly InMemoryMessagingClientLogger _log = new(harness.LoggerFactory.CreateLogger<InMemoryMessagingClient>());
    private readonly CancellationTokenSource _disposeCts = new();
    private bool _disposed;

    public void SendOneWayMessage(Endpoint remote, RapidClusterRequest request, DeliveryFailureCallback? onDeliveryFailure, CancellationToken cancellationToken)
    {
        var sendTask = SendMessageAsync(remote, request, cancellationToken);
        sendTask.Ignore();

        if (onDeliveryFailure is not null)
        {
            sendTask.ContinueWith(
                static (t, state) =>
                {
                    var (callback, endpoint) = ((DeliveryFailureCallback, Endpoint))state!;
                    if (t.IsFaulted)
                    {
                        callback(endpoint);
                    }
                },
                (onDeliveryFailure, remote),
                harness.TeardownCancellationToken,
                TaskContinuationOptions.NotOnRanToCompletion,
                sourceNode.Context.TaskScheduler).Ignore();
        }
    }

    public async Task<RapidClusterResponse> SendMessageAsync(Endpoint remote, RapidClusterRequest request, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        using var sourceCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _disposeCts.Token);
        var localAddr = RapidClusterUtils.Loggable(localEndpoint);
        var remoteAddr = RapidClusterUtils.Loggable(remote);
        var targetNode = harness.GetNode(remoteAddr) ?? throw new SimulatedNetworkException($"No target node found for address {remoteAddr}");
        var sourceContext = sourceNode.Context;
        var targetContext = targetNode.Context;
        var network = harness.Network;
        var networkStatus = network.CheckDelivery(localAddr, remoteAddr);
        var delay = network.GetMessageDelay();
        if (networkStatus is DeliveryStatus.Dropped)
        {
            await Task.Delay(_messageTimeout, sourceContext.TimeProvider, sourceCts.Token).ConfigureAwait(true);
            throw new SimulatedNetworkException($"Message from {localAddr} to {remoteAddr} was dropped by the network.");
        }
        else if (networkStatus is DeliveryStatus.Partitioned)
        {
            throw new SimulatedNetworkException($"Message from {localAddr} to {remoteAddr} could not be delivered due to network partition.");
        }

        // Schedule delivery on the target node.
        var targetCancellation = targetNode.TeardownCancellationToken;
        var deliveryTask = Task.Factory.StartNew(
            DeliverMessageAsync,
            targetCancellation,
            TaskCreationOptions.None,
            targetContext.TaskScheduler)
            .Unwrap();

        // Ignore the task in case the source is terminated before it resolves.
        deliveryTask.Ignore();

        var result = await deliveryTask.WaitAsync(_messageTimeout, sourceContext.TimeProvider, sourceCts.Token).ConfigureAwait(true);
        return result;

        async Task<RapidClusterResponse> DeliverMessageAsync()
        {
            await Task.Delay(network.GetMessageDelay(), targetContext.TimeProvider, targetCancellation).ConfigureAwait(true);
            return network.CheckDelivery(localAddr, remoteAddr) switch
            {
                DeliveryStatus.Dropped => throw new SimulatedNetworkException($"Message from {localAddr} to {remoteAddr} was dropped by the network."),
                DeliveryStatus.Partitioned => throw new SimulatedNetworkException($"Message from {localAddr} to {remoteAddr} could not be delivered due to network partition."),
                DeliveryStatus.Success => await targetNode.HandleRequestAsync(request, targetCancellation).ConfigureAwait(true),
                _ => throw new InvalidOperationException("Unrecognized delivery status."),
            };
        }
    }

    public ValueTask DisposeAsync()
    {
        if (_disposed) return ValueTask.CompletedTask;
        _disposed = true;

        _disposeCts.SafeCancel(_log.Logger);
        _disposeCts.Dispose();

        return ValueTask.CompletedTask;
    }

    private sealed class SimulatedNetworkException : Exception
    {
        public SimulatedNetworkException() { }
        public SimulatedNetworkException(string message) : base(message) { }
        public SimulatedNetworkException(string message, Exception innerException) : base(message, innerException) { }
    }
}
