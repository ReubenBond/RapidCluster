using CsCheck;
using Google.Protobuf;
using RapidCluster.Exceptions;
using RapidCluster.Pb;

namespace RapidCluster.Tests.Unit;

/// <summary>
/// Tests for MembershipViewBuilder and MembershipView.
/// </summary>
public class MembershipViewTests
{
    private const int K = 10;

    /// <summary>
    /// Add a single node and verify whether it appears on all rings
    /// </summary>
    [Fact]
    public void OneRingAddition()
    {
        var builder = new MembershipViewBuilder(K);
        var addr = Utils.HostFromParts("127.0.0.1", 123, Utils.GetNextNodeId());

        builder.RingAdd(addr);
        var view = builder.Build();

        // With 1 node and K=10, actual ring count is clamped to 1
        for (var k = 0; k < view.RingCount; k++)
        {
            var list = view.GetRing(k);
            Assert.Single(list);
            foreach (var address in list)
            {
                Assert.Equal(address, addr);
            }
        }
    }

    /// <summary>
    /// Add multiple nodes and verify whether they appear on all rings
    /// </summary>
    [Fact]
    public void MultipleRingAdditions()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 10;

        for (var i = 0; i < numNodes; i++)
        {
            builder.RingAdd(Utils.HostFromParts("127.0.0.1", i, Utils.GetNextNodeId()));
        }

        var view = builder.Build();

        // With 10 nodes and K=10, actual ring count is clamped to 9 (nodes-1)
        for (var k = 0; k < view.RingCount; k++)
        {
            var list = view.GetRing(k);
            Assert.Equal(numNodes, list.Length);
        }
    }

    /// <summary>
    /// Add multiple nodes twice and verify whether the rings reject duplicates
    /// </summary>
    [Fact]
    public void RingReAdditions()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 10;
        const int startPort = 0;

        for (var i = 0; i < numNodes; i++)
        {
            builder.RingAdd(Utils.HostFromParts("127.0.0.1", startPort + i, Utils.GetNextNodeId()));
        }

        for (var k = 0; k < K; k++)
        {
            var list = builder.GetRing(k);
            Assert.Equal(numNodes, list.Count);
        }

        var numThrows = 0;
        for (var i = 0; i < numNodes; i++)
        {
            try
            {
                builder.RingAdd(Utils.HostFromParts("127.0.0.1", startPort + i, Utils.GetNextNodeId()));
            }
            catch (NodeAlreadyInRingException)
            {
                numThrows++;
            }
        }

        Assert.Equal(numNodes, numThrows);
    }

    /// <summary>
    /// Delete nodes that were never added and verify whether the object rejects those attempts
    /// </summary>
    [Fact]
    public void RingDeletionsOnly()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 10;
        var numThrows = 0;

        for (var i = 0; i < numNodes; i++)
        {
            try
            {
                builder.RingDelete(Utils.HostFromParts("127.0.0.1", i));
            }
            catch (NodeNotInRingException)
            {
                numThrows++;
            }
        }

        Assert.Equal(numNodes, numThrows);
    }

    /// <summary>
    /// Add nodes and then delete them.
    /// </summary>
    [Fact]
    public void RingAdditionsAndDeletions()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 10;

        for (var i = 0; i < numNodes; i++)
        {
            builder.RingAdd(Utils.HostFromParts("127.0.0.1", i, Utils.GetNextNodeId()));
        }

        for (var i = 0; i < numNodes; i++)
        {
            builder.RingDelete(Utils.HostFromParts("127.0.0.1", i));
        }

        var view = builder.Build();

        // With all nodes deleted, the view has 0 nodes so ring count is clamped to 1
        for (var k = 0; k < view.RingCount; k++)
        {
            var list = view.GetRing(k);
            Assert.Empty(list);
        }
    }

    /// <summary>
    /// Verify the edge case of monitoring relationships in a single node case.
    /// </summary>
    [Fact]
    public void MonitoringRelationshipEdge()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());

        builder.RingAdd(n1);
        var view = builder.Build();

        Assert.Empty(view.GetSubjectsOf(n1));
        Assert.Empty(view.GetObserversOf(n1));

        var n2 = Utils.HostFromParts("127.0.0.1", 2);

        Assert.Throws<NodeNotInRingException>(() => view.GetSubjectsOf(n2));
        Assert.Throws<NodeNotInRingException>(() => view.GetObserversOf(n2));
    }

    /// <summary>
    /// Verify the edge case of monitoring relationships in an empty view case.
    /// </summary>
    [Fact]
    public void MonitoringRelationshipEmpty()
    {
        var view = new MembershipViewBuilder(K).Build();
        var n = Utils.HostFromParts("127.0.0.1", 1);

        Assert.Throws<NodeNotInRingException>(() => view.GetSubjectsOf(n));
        Assert.Throws<NodeNotInRingException>(() => view.GetObserversOf(n));
    }

    /// <summary>
    /// Verify the monitoring relationships in a two node setting
    /// </summary>
    [Fact]
    public void MonitoringRelationshipTwoNodes()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        var n2 = Utils.HostFromParts("127.0.0.1", 2, Utils.GetNextNodeId());

        builder.RingAdd(n1);
        builder.RingAdd(n2);
        var view = builder.Build();

        // With 2 nodes, actual ring count is clamped to 1
        Assert.Equal(view.RingCount, view.GetSubjectsOf(n1).Length);
        Assert.Equal(view.RingCount, view.GetObserversOf(n1).Length);
        Assert.Single(view.GetSubjectsOf(n1).ToHashSet());
        Assert.Single(view.GetObserversOf(n1).ToHashSet());
    }

    /// <summary>
    /// Verify the monitoring relationships in a three node setting with delete
    /// </summary>
    [Fact]
    public void MonitoringRelationshipThreeNodesWithDelete()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        var n2 = Utils.HostFromParts("127.0.0.1", 2, Utils.GetNextNodeId());
        var n3 = Utils.HostFromParts("127.0.0.1", 3, Utils.GetNextNodeId());

        builder.RingAdd(n1);
        builder.RingAdd(n2);
        builder.RingAdd(n3);
        var view = builder.Build();

        // With 3 nodes and max K=10, actual ring count is clamped to 2
        Assert.Equal(view.RingCount, view.GetSubjectsOf(n1).Length);
        Assert.Equal(view.RingCount, view.GetObserversOf(n1).Length);
        // With 2 rings and 3 nodes, there may be 1 or 2 unique subjects/observers
        // depending on hash ordering
        Assert.True(view.GetSubjectsOf(n1).ToHashSet().Count >= 1);
        Assert.True(view.GetObserversOf(n1).ToHashSet().Count >= 1);

        // Create new view without n2
        var builder2 = view.ToBuilder();
        builder2.RingDelete(n2);
        var view2 = builder2.Build();

        // With 2 nodes and K=10 (from previous view), actual ring count is clamped to 1
        Assert.Equal(view2.RingCount, view2.GetSubjectsOf(n1).Length);
        Assert.Equal(view2.RingCount, view2.GetObserversOf(n1).Length);
        Assert.Single(view2.GetSubjectsOf(n1).ToHashSet());
        Assert.Single(view2.GetObserversOf(n1).ToHashSet());
    }

    /// <summary>
    /// Verify configuration ID changes with membership changes
    /// </summary>
    [Fact]
    public void ConfigurationIdChanges()
    {
        var builder1 = new MembershipViewBuilder(K);
        var view1 = builder1.Build();
        var initialConfig = view1.ConfigurationId;

        var builder2 = view1.ToBuilder();
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        builder2.RingAdd(n1);
        var view2 = builder2.Build(view1.ConfigurationId);

        var configAfterAdd = view2.ConfigurationId;
        Assert.NotEqual(initialConfig, configAfterAdd);
        Assert.Equal(initialConfig.Version + 1, configAfterAdd.Version);

        var builder3 = view2.ToBuilder();
        var n2 = Utils.HostFromParts("127.0.0.1", 2, Utils.GetNextNodeId());
        builder3.RingAdd(n2);
        var view3 = builder3.Build(view2.ConfigurationId);

        var configAfterSecondAdd = view3.ConfigurationId;
        Assert.NotEqual(configAfterAdd, configAfterSecondAdd);
        Assert.Equal(configAfterAdd.Version + 1, configAfterSecondAdd.Version);

        var builder4 = view3.ToBuilder();
        builder4.RingDelete(n1);
        var view4 = builder4.Build(view3.ConfigurationId);

        var configAfterDelete = view4.ConfigurationId;
        Assert.NotEqual(configAfterSecondAdd, configAfterDelete);
        Assert.Equal(configAfterSecondAdd.Version + 1, configAfterDelete.Version);
    }

    /// <summary>
    /// Verify membership size tracking
    /// </summary>
    [Fact]
    public void MembershipSize()
    {
        var builder = new MembershipViewBuilder(K);
        Assert.Equal(0, builder.GetMembershipSize());

        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        builder.RingAdd(n1);
        Assert.Equal(1, builder.GetMembershipSize());

        var n2 = Utils.HostFromParts("127.0.0.1", 2, Utils.GetNextNodeId());
        builder.RingAdd(n2);
        Assert.Equal(2, builder.GetMembershipSize());

        builder.RingDelete(n1);
        Assert.Equal(1, builder.GetMembershipSize());

        builder.RingDelete(n2);
        Assert.Equal(0, builder.GetMembershipSize());
    }

    /// <summary>
    /// Verify IsHostPresent method
    /// </summary>
    [Fact]
    public void HostPresence()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());

        Assert.False(builder.IsHostPresent(n1));

        builder.RingAdd(n1);

        Assert.True(builder.IsHostPresent(n1));

        builder.RingDelete(n1);

        Assert.False(builder.IsHostPresent(n1));
    }

    /// <summary>
    /// Verify safe to join checks
    /// </summary>
    [Fact]
    public void SafeToJoinChecks()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        var n2 = Utils.HostFromParts("127.0.0.1", 2);

        // Empty view is always safe to join
        Assert.Equal(JoinStatusCode.SafeToJoin, builder.IsSafeToJoin(n1));

        builder.RingAdd(n1);

        // Different node should be safe
        Assert.Equal(JoinStatusCode.SafeToJoin, builder.IsSafeToJoin(n2));

        // Same node should not be safe (hostname already in ring)
        Assert.Equal(JoinStatusCode.HostnameAlreadyInRing, builder.IsSafeToJoin(n1));
    }

    /// <summary>
    /// Verify the monitoring relationships in a multi node setting
    /// </summary>
    [Fact]
    public void MonitoringRelationshipMultipleNodes()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 200;
        var list = new List<Endpoint>();

        for (var i = 0; i < numNodes; i++)
        {
            var n = Utils.HostFromParts("127.0.0.1", i, Utils.GetNextNodeId());
            list.Add(n);
            builder.RingAdd(n);
        }

        var view = builder.Build();

        for (var i = 0; i < numNodes; i++)
        {
            var numSubjects = view.GetSubjectsOf(list[i]).Length;
            var numObservers = view.GetObserversOf(list[i]).Length;
            Assert.True(K == numSubjects, $"NumSubjects: {numSubjects}");
            Assert.True(K == numObservers, $"NumObservers: {numObservers}");
        }
    }

    /// <summary>
    /// Verify the monitoring relationships during bootstrap
    /// </summary>
    [Fact]
    public void MonitoringRelationshipBootstrap()
    {
        var builder = new MembershipViewBuilder(K);
        const int serverPort = 1234;
        var n = Utils.HostFromParts("127.0.0.1", serverPort, Utils.GetNextNodeId());
        builder.RingAdd(n);
        var view = builder.Build();

        var joiningNode = Utils.HostFromParts("127.0.0.1", serverPort + 1);
        var expectedObservers = view.GetExpectedObserversOf(joiningNode);
        // With 1 node, actual ring count is clamped to 1
        Assert.Equal(view.RingCount, expectedObservers.Length);
        Assert.Single(expectedObservers.Distinct());
        Assert.Equal(n, expectedObservers[0]);
    }

    /// <summary>
    /// Verify the monitoring relationships during bootstrap with up to K nodes
    /// </summary>
    [Fact]
    public void MonitoringRelationshipBootstrapMultiple()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 20;
        const int serverPortBase = 1234;
        var joiningNode = Utils.HostFromParts("127.0.0.1", serverPortBase - 1);
        var numObservers = 0;

        for (var i = 0; i < numNodes; i++)
        {
            var n = Utils.HostFromParts("127.0.0.1", serverPortBase + i, Utils.GetNextNodeId());
            builder.RingAdd(n);

            var view = builder.Build();
            var numObserversActual = view.GetExpectedObserversOf(joiningNode).Length;
            Assert.True(numObservers <= numObserversActual);
            numObservers = numObserversActual;

            // Need to create new builder from view to continue adding
            builder = view.ToBuilder();
        }

        // With numNodes > K+1, actual ring count should be K (clamped to nodes-1)
        // The final view has 20 nodes, so ring count = min(K, 19) = 10
        // Expected observers should be equal to the ring count
        Assert.True(numObservers > 0);
        Assert.True(K >= numObservers);
    }

    /// <summary>
    /// Test for adding same host again (should throw NodeAlreadyInRingException)
    /// </summary>
    [Fact]
    public void NodeAdditionSameHostThrows()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        builder.RingAdd(n1);

        // Same host with different ID should throw NodeAlreadyInRingException
        var n2 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        Assert.Throws<NodeAlreadyInRingException>(() => builder.RingAdd(n2));

        // Only n1 should have been added
        Assert.Single(builder.GetRing(0));
    }

    /// <summary>
    /// Ensure that N different configuration IDs are generated when N nodes are added to the rings.
    /// With version-based ConfigurationId, each build should increment the version.
    /// </summary>
    [Fact]
    public void NodeConfigurationChange()
    {
        var builder = new MembershipViewBuilder(K);
        const int numNodes = 200;
        var set = new HashSet<long>(numNodes);
        var previousConfigId = ConfigurationId.Empty;

        for (var i = 0; i < numNodes; i++)
        {
            var n = Utils.HostFromParts("127.0.0.1", i, Utils.GetNextNodeId());
            builder.RingAdd(n);
            var view = builder.Build(previousConfigId);
            set.Add(view.ConfigurationId);
            previousConfigId = view.ConfigurationId;
            builder = view.ToBuilder();
        }

        Assert.Equal(numNodes, set.Count); // should be 1000 different configurations (versions 1-1000)
    }

    /// <summary>
    /// Add endpoints to two membership view objects in different orders.
    /// With version-based ConfigurationId, both sequences produce incrementing versions
    /// and the final version should be the same (both reach version N after N additions).
    /// </summary>
    [Fact]
    public void NodeConfigurationsAcrossMViews()
    {
        var builder1 = new MembershipViewBuilder(K);
        var builder2 = new MembershipViewBuilder(K);
        const int numNodes = 200;
        var list1 = new List<long>(numNodes);
        var list2 = new List<long>(numNodes);
        var previousConfigId1 = ConfigurationId.Empty;
        var previousConfigId2 = ConfigurationId.Empty;

        for (var i = 0; i < numNodes; i++)
        {
            var n = Utils.HostFromParts("127.0.0.1", i, Utils.GetNextNodeId());
            builder1.RingAdd(n);
            var view1 = builder1.Build(previousConfigId1);
            list1.Add(view1.ConfigurationId);
            previousConfigId1 = view1.ConfigurationId;
            builder1 = view1.ToBuilder();
        }

        for (var i = numNodes - 1; i >= 0; i--)
        {
            var n = Utils.HostFromParts("127.0.0.1", i, Utils.GetNextNodeId());
            builder2.RingAdd(n);
            var view2 = builder2.Build(previousConfigId2);
            list2.Add(view2.ConfigurationId);
            previousConfigId2 = view2.ConfigurationId;
            builder2 = view2.ToBuilder();
        }

        Assert.Equal(numNodes, list1.Count);
        Assert.Equal(numNodes, list2.Count);

        // With version-based ConfigurationId, both sequences produce the same versions (1, 2, 3, ..., N)
        // regardless of the order in which nodes are added
        for (var i = 0; i < numNodes; i++)
        {
            Assert.Equal(list1[i], list2[i]);
            Assert.Equal(i + 1, list1[i]); // Version should be i+1 (1-indexed)
        }
    }

    /// <summary>
    /// Verify that ToBuilder creates a proper builder from a view
    /// </summary>
    [Fact]
    public void ToBuilderAndBuild()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        builder.RingAdd(n1);
        var view = builder.Build();

        // Verify the view has correct values
        // With 1 node and K=10, actual ring count is clamped to 1
        Assert.Equal(1, view.RingCount);
        Assert.Single(view.Members);
        Assert.Equal(n1, view.Members[0]);

        // Create a new builder from the view and add another node
        var builder2 = view.ToBuilder();
        var n2 = Utils.HostFromParts("127.0.0.1", 2, Utils.GetNextNodeId());
        builder2.RingAdd(n2);
        var view2 = builder2.Build(view.ConfigurationId);

        // The original view should still show the old state
        Assert.Single(view.Members);
        Assert.NotEqual(view.ConfigurationId, view2.ConfigurationId);
        Assert.Equal(view.ConfigurationId.Version + 1, view2.ConfigurationId.Version);

        // The new view should have the updated state
        Assert.Equal(2, view2.Members.Length);
    }

    /// <summary>
    /// Verify that builder becomes sealed after Build is called
    /// </summary>
    [Fact]
    public void BuilderBecomesSealed()
    {
        var builder = new MembershipViewBuilder(K);
        var n1 = Utils.HostFromParts("127.0.0.1", 1, Utils.GetNextNodeId());
        builder.RingAdd(n1);

        var view = builder.Build();
        Assert.NotNull(view);

        // Builder should be sealed now - all operations should throw
        Assert.Throws<InvalidOperationException>(() => builder.RingAdd(Utils.HostFromParts("127.0.0.1", 2, Utils.GetNextNodeId())));
        Assert.Throws<InvalidOperationException>(() => builder.RingDelete(n1));
        Assert.Throws<InvalidOperationException>(() => builder.GetRing(0));
        Assert.Throws<InvalidOperationException>(() => builder.GetMembershipSize());
        Assert.Throws<InvalidOperationException>(() => builder.IsHostPresent(n1));
        Assert.Throws<InvalidOperationException>(() => builder.IsSafeToJoin(n1));
        Assert.Throws<InvalidOperationException>(builder.Build);
        Assert.Throws<InvalidOperationException>(() => _ = builder.MaxRingCount);
    }

    #region Property-Based Tests

    /// <summary>
    /// Generator for a list of unique endpoints with their NodeIds.
    /// </summary>
    private static Gen<List<Endpoint>> GenUniqueNodes(int minCount, int maxCount)
    {
        return Gen.Int[minCount, maxCount].SelectMany(count =>
            Gen.Select(
                Gen.Int[1, 255].Array[count].Where(a => a.Distinct().Count() == count),
                Gen.Int[1000, 65535].Array[count],
                Gen.Long.Array[count].Where(a => a.Distinct().Count() == count)
            ).Select((octets, ports, nodeIds) =>
            {
                var result = new List<Endpoint>(count);
                for (var i = 0; i < count; i++)
                {
                    var endpoint = new Endpoint
                    {
                        Hostname = ByteString.CopyFromUtf8($"127.0.0.{octets[i]}"),
                        Port = ports[i],
                        NodeId = nodeIds[i]
                    };
                    result.Add(endpoint);
                }
                return result;
            }));
    }

    /// <summary>
    /// Generator for K values (number of rings).
    /// </summary>
    private static readonly Gen<int> GenK = Gen.Int[1, 10];

    [Fact]
    public void Property_RingCount_Is_Clamped_Correctly()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Ring count is clamped: Math.Clamp(k, 1, Math.Max(1, nodeCount - 1))
                var expectedRingCount = Math.Clamp(k, 1, Math.Max(1, nodes.Count - 1));
                return view.RingCount == expectedRingCount;
            });
    }

    [Fact]
    public void Property_Size_Equals_AddedNodes()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                return view.Size == nodes.Count;
            });
    }

    [Fact]
    public void Property_GetRing_Returns_Correct_Size()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Each ring should have the same number of elements as there are nodes
                // Use actual ring count, not requested k (which may be clamped)
                for (var ringNumber = 0; ringNumber < view.RingCount; ringNumber++)
                {
                    var ring = view.GetRing(ringNumber);
                    if (ring.Length != nodes.Count) return false;
                }
                return true;
            });
    }

    [Fact]
    public void Property_IsHostPresent_Returns_True_For_Added_Nodes()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // All added nodes should be present
                return nodes.All(n => view.IsHostPresent(n));
            });
    }

    [Fact]
    public void Property_GetObserversOf_Returns_Up_To_K_Observers()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var testNode = nodes[0];
                var observers = view.GetObserversOf(testNode);

                // Observers count should be exactly the actual ring count (one per ring)
                return observers.Length == view.RingCount;
            });
    }

    [Fact]
    public void Property_GetExpectedObserversOf_Does_Not_Include_Self()
    {
        Gen.Select(GenK, GenUniqueNodes(2, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var testNode = nodes[0];
                var expectedObservers = view.GetExpectedObserversOf(testNode);

                // Self should never be an observer of itself
                return !expectedObservers.Contains(testNode);
            });
    }

    [Fact]
    public void Property_IsMember_And_IsHostPresent_Are_Equivalent()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // IsMember and IsHostPresent should return the same result for all nodes
                foreach (var endpoint in nodes)
                {
                    if (view.IsMember(endpoint) != view.IsHostPresent(endpoint))
                        return false;
                }

                // Both should return false for a node not in the view
                var notInView = new Endpoint { Hostname = ByteString.CopyFromUtf8("10.0.0.1"), Port = 9999 };
                return !view.IsMember(notInView) && !view.IsHostPresent(notInView);
            });
    }

    [Fact]
    public void Property_GetSubjectsOf_Returns_RingCount_Subjects()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var testNode = nodes[0];
                var subjects = view.GetSubjectsOf(testNode);

                // Subjects count should be exactly the actual ring count (one per ring)
                return subjects.Length == view.RingCount;
            });
    }

    [Fact]
    public void Property_Observers_And_Subjects_Are_All_Members()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var testNode = nodes[0];
                var observers = view.GetObserversOf(testNode);
                var subjects = view.GetSubjectsOf(testNode);

                // All observers and subjects should be members of the view
                return observers.All(view.IsMember) && subjects.All(view.IsMember);
            });
    }

    [Fact]
    public void Property_GetRingNumbers_Returns_Valid_Ring_Indices()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                var observer = nodes[0];
                var subjects = view.GetSubjectsOf(observer);

                // For each subject, GetRingNumbers should return at least one valid ring index
                foreach (var subject in subjects.Distinct())
                {
                    var ringNumbers = view.GetRingNumbers(observer, subject);
                    if (ringNumbers.Length == 0) return false;
                    if (ringNumbers.Any(r => r < 0 || r >= view.RingCount)) return false;
                }
                return true;
            });
    }

    [Fact]
    public void Property_Members_Contains_All_Added_Endpoints()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // All added endpoints should be in Members
                var members = view.Members.ToHashSet();
                return nodes.All(n => members.Contains(n));
            });
    }

    [Fact]
    public void Property_Configuration_Roundtrip_Preserves_Data()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Get configuration and verify it contains the same data
                var config = view.Configuration;
                return config.Endpoints.Length == view.Size &&
                       config.Endpoints.SequenceEqual(view.Members);
            });
    }

    [Fact]
    public void Property_IsSafeToJoin_Rejects_Existing_Hosts()
    {
        Gen.Select(GenK, GenUniqueNodes(2, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Existing hosts should not be safe to join
                var existingHost = nodes[0];
                return view.IsSafeToJoin(existingHost) == JoinStatusCode.HostnameAlreadyInRing;
            });
    }

    [Fact]
    public void Property_IsSafeToJoin_Allows_New_Host()
    {
        Gen.Select(GenK, GenUniqueNodes(1, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // New host should be safe to join
                var newHost = new Endpoint { Hostname = ByteString.CopyFromUtf8("10.0.0.1"), Port = 9999 };
                return view.IsSafeToJoin(newHost) == JoinStatusCode.SafeToJoin;
            });
    }

    [Fact]
    public void Property_ToBuilder_Creates_Independent_Copy()
    {
        Gen.Select(GenK, GenUniqueNodes(2, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view1 = builder.Build();

                // Create a new builder and modify it
                var builder2 = view1.ToBuilder();
                var newEndpoint = new Endpoint { Hostname = ByteString.CopyFromUtf8("10.0.0.1"), Port = 9999, NodeId = 999999 };
                builder2.RingAdd(newEndpoint);
                var view2 = builder2.Build(view1.ConfigurationId);

                // Original view should be unchanged
                return view1.Size == nodes.Count && view2.Size == nodes.Count + 1;
            });
    }

    [Fact]
    public void Property_Delete_Removes_Node_From_View()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view1 = builder.Build();

                // Delete a node
                var builder2 = view1.ToBuilder();
                var nodeToDelete = nodes[0];
                builder2.RingDelete(nodeToDelete);
                var view2 = builder2.Build(view1.ConfigurationId);

                // Node should no longer be a member
                return view2.Size == nodes.Count - 1 &&
                       !view2.IsMember(nodeToDelete) &&
                       view1.IsMember(nodeToDelete); // Original unchanged
            });
    }

    [Fact]
    public void Property_All_Rings_Have_Same_Nodes()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // All rings should contain the same set of nodes (just in different order)
                var expectedNodes = nodes.ToHashSet();
                for (var ringIndex = 0; ringIndex < view.RingCount; ringIndex++)
                {
                    var ring = view.GetRing(ringIndex);
                    var ringNodes = ring.ToHashSet();
                    if (!ringNodes.SetEquals(expectedNodes)) return false;
                }
                return true;
            });
    }

    [Fact]
    public void Property_GetExpectedObserversOf_Returns_RingCount_Observers()
    {
        Gen.Select(GenK, GenUniqueNodes(2, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // For a new joining node, expected observers count equals ring count
                var joiningNode = new Endpoint { Hostname = ByteString.CopyFromUtf8("10.0.0.1"), Port = 9999 };
                var expectedObservers = view.GetExpectedObserversOf(joiningNode);
                return expectedObservers.Length == view.RingCount;
            });
    }

    [Fact]
    public void Property_Empty_View_Has_No_Members()
    {
        GenK.Sample(k =>
        {
            var builder = new MembershipViewBuilder(k);
            var view = builder.Build();

            return view.Size == 0 &&
                   view.Members.Length == 0;
        });
    }

    [Fact]
    public void Property_ConfigurationId_Increments_On_Build()
    {
        Gen.Select(GenK, GenUniqueNodes(2, 10))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                builder.RingAdd(nodes[0]);
                var view1 = builder.Build();

                var builder2 = view1.ToBuilder();
                builder2.RingAdd(nodes[1]);
                var view2 = builder2.Build(view1.ConfigurationId);

                return view2.ConfigurationId.Version == view1.ConfigurationId.Version + 1;
            });
    }

    [Fact]
    public void Property_GetRing_Throws_For_Invalid_Index()
    {
        Gen.Select(GenK, GenUniqueNodes(2, 20))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // Negative index should throw
                var threwForNegative = false;
                try { view.GetRing(-1); }
                catch (ArgumentOutOfRangeException) { threwForNegative = true; }

                // Index >= RingCount should throw
                var threwForTooLarge = false;
                try { view.GetRing(view.RingCount); }
                catch (ArgumentOutOfRangeException) { threwForTooLarge = true; }

                return threwForNegative && threwForTooLarge;
            });
    }

    [Fact]
    public void Property_Observer_Subject_Relationship_Is_Symmetric()
    {
        Gen.Select(GenK, GenUniqueNodes(3, 15))
            .Sample((k, nodes) =>
            {
                var builder = new MembershipViewBuilder(k);
                foreach (var endpoint in nodes)
                {
                    builder.RingAdd(endpoint);
                }
                var view = builder.Build();

                // For each node, if B is an observer of A, then A is a subject of B
                foreach (var endpoint in nodes)
                {
                    var observers = view.GetObserversOf(endpoint);
                    foreach (var observer in observers)
                    {
                        var subjectsOfObserver = view.GetSubjectsOf(observer);
                        if (!subjectsOfObserver.Contains(endpoint))
                            return false;
                    }
                }
                return true;
            });
    }

    #endregion
}
