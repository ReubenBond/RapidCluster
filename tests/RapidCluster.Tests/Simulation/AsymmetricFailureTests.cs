using System.Diagnostics.CodeAnalysis;
using Clockwork;
using RapidCluster.Tests.Simulation.Infrastructure;

namespace RapidCluster.Tests.Simulation;

/// <summary>
/// Tests for asymmetric network failure scenarios using the simulation harness.
/// Asymmetric failures occur when communication works in one direction but not the other,
/// which can lead to complex edge cases in failure detection and consensus protocols.
/// </summary>
[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores", Justification = "Test naming convention")]
public sealed class AsymmetricFailureTests : IAsyncLifetime
{
    private RapidSimulationCluster _harness = null!;
    private const int TestSeed = 56789;

    public ValueTask InitializeAsync()
    {
        _harness = new RapidSimulationCluster(seed: TestSeed);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await _harness.DisposeAsync();
    }


    [Fact]
    public void OneWayPartition_SourceCannotReachTarget()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        var nodeA = nodes[0];
        var nodeB = nodes[1];

        // Act: Create one-way partition from A to B (A cannot reach B, but B can reach A)
        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);
        _harness.Network.CreatePartition(addrA, addrB);

        // Assert: B can still send to A (check delivery status)
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrB, addrA));
        Assert.Equal(DeliveryStatus.Partitioned, _harness.Network.CheckDelivery(addrA, addrB));
    }

    [Fact]
    public void OneWayPartition_TargetCanReachSource()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        var nodeA = nodes[0];
        var nodeB = nodes[1];
        var nodeC = nodes[2];

        // Act: Create one-way partition from A to B
        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);
        var addrC = RapidClusterUtils.Loggable(nodeC.Address);
        _harness.Network.CreatePartition(addrA, addrB);

        // Assert: A can still communicate with C
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrA, addrC));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrC, addrA));

        // Assert: B can still communicate with C
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrB, addrC));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrC, addrB));
    }

    [Fact]
    public void OneWayPartition_DoesNotAffectOtherNodes()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var nodeA = nodes[0];
        var nodeB = nodes[1];
        var nodeC = nodes[2];
        var nodeD = nodes[3];

        // Act: Create one-way partition from A to B
        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);
        _harness.Network.CreatePartition(addrA, addrB);

        // Assert: Communication between C and D is unaffected
        var addrC = RapidClusterUtils.Loggable(nodeC.Address);
        var addrD = RapidClusterUtils.Loggable(nodeD.Address);
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrC, addrD));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrD, addrC));

        // Assert: A can still reach C and D
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrA, addrC));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrA, addrD));
    }

    [Fact]
    public void OneWayPartition_HealRestoresCommunication()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        var nodeA = nodes[0];
        var nodeB = nodes[1];

        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);

        // Create one-way partition
        _harness.Network.CreatePartition(addrA, addrB);
        Assert.Equal(DeliveryStatus.Partitioned, _harness.Network.CheckDelivery(addrA, addrB));

        // Act: Heal the partition
        _harness.Network.HealPartition(addrA, addrB);

        // Assert: Communication is restored
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrA, addrB));
    }

    [Fact]
    public void OneWayPartition_MultiplePartitionsCanCoexist()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var addrA = RapidClusterUtils.Loggable(nodes[0].Address);
        var addrB = RapidClusterUtils.Loggable(nodes[1].Address);
        var addrC = RapidClusterUtils.Loggable(nodes[2].Address);
        var addrD = RapidClusterUtils.Loggable(nodes[3].Address);

        // Act: Create multiple one-way partitions
        _harness.Network.CreatePartition(addrA, addrB); // A cannot reach B
        _harness.Network.CreatePartition(addrC, addrD); // C cannot reach D

        // Assert: Both partitions are active
        Assert.Equal(DeliveryStatus.Partitioned, _harness.Network.CheckDelivery(addrA, addrB));
        Assert.Equal(DeliveryStatus.Partitioned, _harness.Network.CheckDelivery(addrC, addrD));

        // Assert: Reverse directions work
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrB, addrA));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrD, addrC));
    }



    [Fact]
    public void AsymmetricPartition_ObserverCannotReachMonitoredNode()
    {
        // Arrange: Create a 4-node cluster for quorum
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var observer = nodes[0];
        var monitored = nodes[1];

        // Act: Create one-way partition where observer cannot reach monitored
        // but monitored can reach observer (probes can't be sent, but responses would work if probes arrived)
        var addrObserver = RapidClusterUtils.Loggable(observer.Address);
        var addrMonitored = RapidClusterUtils.Loggable(monitored.Address);
        _harness.Network.CreatePartition(addrObserver, addrMonitored);

        // Run simulation to allow failure detection to potentially trigger
        // Since observer can't send probes to monitored, it might report monitored as failed
        _harness.RunForDuration(TimeSpan.FromSeconds(10));

        // The cluster should remain stable since monitored can still communicate with other nodes
        // and those nodes can vouch for monitored being alive
        Assert.True(nodes[2].MembershipSize >= 3, "Cluster should remain mostly stable");
        Assert.True(nodes[3].MembershipSize >= 3, "Cluster should remain mostly stable");
    }

    [Fact]
    public void AsymmetricPartition_MonitoredCannotReachObserver()
    {
        // Arrange: Create a 4-node cluster for quorum
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var observer = nodes[0];
        var monitored = nodes[1];

        // Act: Create one-way partition where monitored cannot reach observer
        // (probes arrive but responses can't be sent back)
        var addrObserver = RapidClusterUtils.Loggable(observer.Address);
        var addrMonitored = RapidClusterUtils.Loggable(monitored.Address);
        _harness.Network.CreatePartition(addrMonitored, addrObserver);

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(10));

        // The monitored node might be reported as failed by the observer
        // since probe responses can't get back
        // The cluster should eventually reach consensus
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(1));

        // All remaining nodes should have consistent view
        var remainingNodes = _harness.Nodes;
        if (remainingNodes.Count > 1)
        {
            var sizes = remainingNodes.Select(n => n.MembershipSize).Distinct().ToList();
            Assert.True(sizes.Count <= 2, "Nodes should converge to at most 2 different views during partition");
        }
    }

    [Fact]
    public void AsymmetricPartition_ProbeTimeoutDueToOneWayFailure()
    {
        // Arrange: Create a 5-node cluster for better fault tolerance
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        var nodeA = nodes[0];
        var nodeB = nodes[1];

        // Act: Create asymmetric partition - A can send to B, but B cannot respond to A
        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);
        _harness.Network.CreatePartition(addrB, addrA);

        // Advance time significantly to trigger failure detection
        _harness.RunForDuration(TimeSpan.FromSeconds(30));

        // Wait for cluster to stabilize
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // The cluster should eventually reach a stable state
        // Due to asymmetric failure, node B might be reported as failed by A
        Assert.True(_harness.Nodes.Count >= 3, "At least 3 nodes should remain for quorum");
    }

    [Fact]
    public void AsymmetricPartition_ClusterConvergesAfterHeal()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var nodeA = nodes[0];
        var nodeB = nodes[1];

        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);

        // Act: Create and then heal asymmetric partition
        _harness.Network.CreatePartition(addrA, addrB);
        _harness.RunForDuration(TimeSpan.FromSeconds(5));

        // Heal the partition before failure detection kicks in
        _harness.Network.HealPartition(addrA, addrB);

        // Wait for stabilization
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromSeconds(30));

        // Cluster should remain intact
        Assert.Equal(4, _harness.Nodes.Count);
    }



    [Fact]
    public void AsymmetricPartition_ChainedOneWayFailures()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        var addrA = RapidClusterUtils.Loggable(nodes[0].Address);
        var addrB = RapidClusterUtils.Loggable(nodes[1].Address);
        var addrC = RapidClusterUtils.Loggable(nodes[2].Address);

        // Act: Create a chain of one-way partitions: A -> B -> C
        // A cannot reach B, B cannot reach C
        _harness.Network.CreatePartition(addrA, addrB);
        _harness.Network.CreatePartition(addrB, addrC);

        // Assert: A can still reach C directly
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrA, addrC));

        // Assert: Reverse direction works
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrB, addrA));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrC, addrB));

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(15));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(1));

        // Cluster should maintain quorum capability
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should maintain quorum");
    }

    [Fact]
    public void AsymmetricPartition_StarTopologyFailure()
    {
        // Arrange: Create a 5-node cluster where one node is central
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        var centralNode = nodes[0];
        var addrCentral = RapidClusterUtils.Loggable(centralNode.Address);

        // Act: Create one-way partitions where central node cannot reach any other node
        // but other nodes can reach the central node
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            _harness.Network.CreatePartition(addrCentral, addr);
        }

        // Assert: Other nodes can still reach central
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr, addrCentral));
        }

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(20));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // The central node might be removed or the cluster adapts
        // Key assertion: cluster remains operational
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should remain operational");
    }

    [Fact]
    public void AsymmetricPartition_ReverseStarTopologyFailure()
    {
        // Arrange: Create a 5-node cluster
        var nodes = _harness.CreateCluster(size: 5);
        _harness.WaitForConvergence(expectedSize: 5);

        var centralNode = nodes[0];
        var addrCentral = RapidClusterUtils.Loggable(centralNode.Address);

        // Act: Create one-way partitions where no other node can reach the central node
        // but central node can reach all others
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            _harness.Network.CreatePartition(addr, addrCentral);
        }

        // Assert: Central can still reach others
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrCentral, addr));
        }

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(20));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Cluster should adapt - central may be removed as other nodes can't reach it
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should remain operational");
    }

    [Fact]
    public void AsymmetricPartition_OscillatingFailure()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var nodeA = nodes[0];
        var nodeB = nodes[1];
        var addrA = RapidClusterUtils.Loggable(nodeA.Address);
        var addrB = RapidClusterUtils.Loggable(nodeB.Address);

        // Act: Oscillate the partition direction multiple times
        for (var i = 0; i < 3; i++)
        {
            // A cannot reach B
            _harness.Network.CreatePartition(addrA, addrB);
            _harness.RunForDuration(TimeSpan.FromSeconds(2));

            // Heal and reverse
            _harness.Network.HealPartition(addrA, addrB);
            _harness.Network.CreatePartition(addrB, addrA);
            _harness.RunForDuration(TimeSpan.FromSeconds(2));

            // Heal
            _harness.Network.HealPartition(addrB, addrA);
            _harness.RunForDuration(TimeSpan.FromSeconds(2));
        }

        // Wait for stabilization
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(1));

        // Cluster should recover
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should recover from oscillating failures");
    }

    [Fact]
    public void AsymmetricPartition_PartialMeshFailure()
    {
        // Arrange: Create a 6-node cluster
        var nodes = _harness.CreateCluster(size: 6);
        _harness.WaitForConvergence(expectedSize: 6);

        // Create two groups: [0,1,2] and [3,4,5]
        // Group 1 cannot reach Group 2, but Group 2 can reach Group 1

        var group1 = nodes.Take(3).ToList();
        var group2 = nodes.Skip(3).ToList();

        // Act: Create one-way partitions from group1 to group2
        foreach (var src in group1)
        {
            foreach (var dst in group2)
            {
                var addrSrc = RapidClusterUtils.Loggable(src.Address);
                var addrDst = RapidClusterUtils.Loggable(dst.Address);
                _harness.Network.CreatePartition(addrSrc, addrDst);
            }
        }

        // Assert: Group 2 can still reach Group 1
        foreach (var src in group2)
        {
            foreach (var dst in group1)
            {
                var addrSrc = RapidClusterUtils.Loggable(src.Address);
                var addrDst = RapidClusterUtils.Loggable(dst.Address);
                Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrSrc, addrDst));
            }
        }

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(30));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Cluster should adapt - some nodes may be removed
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should maintain quorum");
    }



    [Fact]
    public void AsymmetricPartition_DuringJoin()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        var seedNode = nodes[0];
        var addrSeed = RapidClusterUtils.Loggable(seedNode.Address);

        // Create one-way partition from seed to one of the other nodes
        var addrOther = RapidClusterUtils.Loggable(nodes[1].Address);
        _harness.Network.CreatePartition(addrSeed, addrOther);

        // Act: Try to join a new node through the seed
        // The join might succeed through alternative paths
        var newNode = _harness.CreateJoinerNode(seedNode, nodeId: 10);

        // Wait for potential convergence
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(1));

        // Assert: New node should be initialized (join completed)
        Assert.True(newNode.IsInitialized, "New node should complete join despite asymmetric partition");
    }

    [Fact]
    public void AsymmetricPartition_DuringLeave()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var leavingNode = nodes[3];
        var seedNode = nodes[0];

        // Create one-way partition - leaving node cannot reach seed
        var addrLeaving = RapidClusterUtils.Loggable(leavingNode.Address);
        var addrSeed = RapidClusterUtils.Loggable(seedNode.Address);
        _harness.Network.CreatePartition(addrLeaving, addrSeed);

        // Act: Attempt graceful leave
        _harness.RemoveNodeGracefully(leavingNode);

        // Wait for convergence
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(1));

        // Assert: Remaining nodes should converge
        var remainingNodes = _harness.Nodes;
        Assert.True(remainingNodes.Count == 3, "Leaving node should be removed");
    }

    [Fact]
    public void AsymmetricPartition_MultipleConcurrentJoins()
    {
        // Arrange: Create a 10-node cluster so MultiNodeCutDetector is used (K > 1)
        // This provides better fault tolerance for asymmetric partition scenarios
        var nodes = _harness.CreateCluster(size: 10);
        _harness.WaitForConvergence(expectedSize: 10);

        // Create asymmetric partitions between existing nodes
        var addr0 = RapidClusterUtils.Loggable(nodes[0].Address);
        var addr1 = RapidClusterUtils.Loggable(nodes[1].Address);
        _harness.Network.CreatePartition(addr0, addr1);

        // Act: Join multiple nodes in sequence
        var newNode1 = _harness.CreateJoinerNode(nodes[0], nodeId: 20);
        var newNode2 = _harness.CreateJoinerNode(nodes[2], nodeId: 21);

        // Wait for convergence
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Assert: Both new nodes should join successfully
        Assert.True(newNode1.IsInitialized, "First new node should join");
        Assert.True(newNode2.IsInitialized, "Second new node should join");
    }



    [Fact]
    public void AsymmetricPartition_SingleNodeCannotSendToAnyone()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var isolatedSender = nodes[0];
        var addrIsolated = RapidClusterUtils.Loggable(isolatedSender.Address);

        // Act: Create one-way partitions - isolated node cannot send to anyone
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            _harness.Network.CreatePartition(addrIsolated, addr);
        }

        // All other nodes can still communicate with each other and with isolated
        foreach (var src in nodes.Skip(1))
        {
            foreach (var dst in nodes.Where(n => n != src))
            {
                var addrSrc = RapidClusterUtils.Loggable(src.Address);
                var addrDst = RapidClusterUtils.Loggable(dst.Address);
                if (dst != isolatedSender)
                {
                    Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrSrc, addrDst));
                }
            }
        }

        // Run simulation - isolated sender should eventually be detected as failed
        _harness.RunForDuration(TimeSpan.FromSeconds(30));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Cluster should remain operational with 3+ nodes
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should remain operational");
    }

    [Fact]
    public void AsymmetricPartition_SingleNodeCannotReceiveFromAnyone()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var isolatedReceiver = nodes[0];
        var addrIsolated = RapidClusterUtils.Loggable(isolatedReceiver.Address);

        // Act: Create one-way partitions - no one can send to isolated node
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            _harness.Network.CreatePartition(addr, addrIsolated);
        }

        // Isolated node can still send to others
        foreach (var node in nodes.Skip(1))
        {
            var addr = RapidClusterUtils.Loggable(node.Address);
            Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addrIsolated, addr));
        }

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(30));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Cluster should remain operational
        Assert.True(_harness.Nodes.Count >= 3, "Cluster should remain operational");
    }

    [Fact]
    public void AsymmetricPartition_HealAllRestoresFullConnectivity()
    {
        // Arrange: Create a 4-node cluster with multiple asymmetric partitions
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var addr0 = RapidClusterUtils.Loggable(nodes[0].Address);
        var addr1 = RapidClusterUtils.Loggable(nodes[1].Address);
        var addr2 = RapidClusterUtils.Loggable(nodes[2].Address);
        var addr3 = RapidClusterUtils.Loggable(nodes[3].Address);

        // Create multiple one-way partitions
        _harness.Network.CreatePartition(addr0, addr1);
        _harness.Network.CreatePartition(addr1, addr2);
        _harness.Network.CreatePartition(addr2, addr3);
        _harness.Network.CreatePartition(addr3, addr0);

        // Act: Heal all partitions
        _harness.Network.HealAllPartitions();

        // Assert: All communication restored
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr0, addr1));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr1, addr2));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr2, addr3));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr3, addr0));
    }

    [Fact]
    public void AsymmetricPartition_RapidToggling()
    {
        // Arrange: Create a 3-node cluster
        var nodes = _harness.CreateCluster(size: 3);
        _harness.WaitForConvergence(expectedSize: 3);

        var addr0 = RapidClusterUtils.Loggable(nodes[0].Address);
        var addr1 = RapidClusterUtils.Loggable(nodes[1].Address);

        // Act: Rapidly toggle partition multiple times
        for (var i = 0; i < 10; i++)
        {
            _harness.Network.CreatePartition(addr0, addr1);
            _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMilliseconds(100));

            _harness.Network.HealPartition(addr0, addr1);
            _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMilliseconds(100));
        }

        // Give time to stabilize
        _harness.RunForDuration(TimeSpan.FromSeconds(10));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(1));

        // Assert: Cluster should still be operational
        Assert.True(_harness.Nodes.Count >= 2, "Cluster should survive rapid partition toggling");
    }

    [Fact]
    public void AsymmetricPartition_CyclicPartitions()
    {
        // Arrange: Create a 4-node cluster
        var nodes = _harness.CreateCluster(size: 4);
        _harness.WaitForConvergence(expectedSize: 4);

        var addr0 = RapidClusterUtils.Loggable(nodes[0].Address);
        var addr1 = RapidClusterUtils.Loggable(nodes[1].Address);
        var addr2 = RapidClusterUtils.Loggable(nodes[2].Address);
        var addr3 = RapidClusterUtils.Loggable(nodes[3].Address);

        // Act: Create cyclic one-way partitions: 0->1, 1->2, 2->3, 3->0
        // Forms a cycle where each node can't reach the next in sequence
        _harness.Network.CreatePartition(addr0, addr1);
        _harness.Network.CreatePartition(addr1, addr2);
        _harness.Network.CreatePartition(addr2, addr3);
        _harness.Network.CreatePartition(addr3, addr0);

        // Each node can still reach some nodes (just not the next in cycle)
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr0, addr2));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr0, addr3));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr1, addr0));
        Assert.Equal(DeliveryStatus.Success, _harness.Network.CheckDelivery(addr1, addr3));

        // Run simulation
        _harness.RunForDuration(TimeSpan.FromSeconds(20));
        _harness.RunUntilIdle(maxSimulatedTime: TimeSpan.FromMinutes(2));

        // Cluster should adapt somehow
        Assert.True(_harness.Nodes.Count >= 2, "Cluster should handle cyclic partitions");
    }

}
