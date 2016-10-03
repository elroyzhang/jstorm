/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.jstorm.scheduler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;

import com.tencent.jstorm.daemon.nimbus.StandaloneNimbus;
import com.tencent.jstorm.utils.CoreUtil;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.Config;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.scheduler.Cluster;
import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.SchedulerAssignmentImpl;
import org.apache.storm.scheduler.SupervisorDetails;
import org.apache.storm.scheduler.Topologies;
import org.apache.storm.scheduler.TopologyDetails;
import org.apache.storm.scheduler.WorkerSlot;
import junit.framework.Assert;

@ClojureClass(className = "backtype.storm.scheduler-test")
public class SchedulerTest {
  private static ExecutorDetails ed1;
  private static ExecutorDetails ed2;
  private static WorkerSlot ws1;
  private static WorkerSlot ws2;

  @BeforeClass
  public static void setUp() {
    ed1 = new ExecutorDetails(1, 5);
    ed2 = new ExecutorDetails(6, 10);
    ws1 = new WorkerSlot("supervisor1", 1);
    ws2 = new WorkerSlot("supervisor2", 1);
  }

  @Test
  @ClojureClass(className = "backtype.storm.scheduler-test#test-supervisor-details")
  public void testSupervisorDetails() {
    Map<ExecutorDetails, WorkerSlot> executorToSlot =
        new HashMap<ExecutorDetails, WorkerSlot>();
    executorToSlot.put(ed1, ws1);
    executorToSlot.put(ed2, ws2);
    String topologyId = "topology1";
    SchedulerAssignmentImpl assignment =
        new SchedulerAssignmentImpl(topologyId, executorToSlot);
    List<ExecutorDetails> executors =
        Arrays.asList(new ExecutorDetails(11, 15), new ExecutorDetails(16, 20));
    // test assign
    assignment.assign(ws1, executors);
    Map<ExecutorDetails, WorkerSlot> result = assignment.getExecutorToSlot();
    // {[6, 10]=supervisor2:1:false, [11, 15]=supervisor1:1:false, [1,
    // 5]=supervisor1:1:false, [16, 20]=supervisor1:1:false}
    Assert.assertEquals(4, result.size());

    Assert.assertTrue(result.containsKey(new ExecutorDetails(1, 5)));
    Assert.assertEquals(ws1, result.get(new ExecutorDetails(1, 5)));

    Assert.assertTrue(result.containsKey(new ExecutorDetails(6, 10)));
    Assert.assertEquals(ws2, result.get(new ExecutorDetails(6, 10)));

    Assert.assertTrue(result.containsKey(new ExecutorDetails(11, 15)));
    Assert.assertEquals(ws1, result.get(new ExecutorDetails(11, 15)));

    Assert.assertTrue(result.containsKey(new ExecutorDetails(16, 20)));
    Assert.assertEquals(ws1, result.get(new ExecutorDetails(16, 20)));

    // test isSlotOccupied
    Assert.assertTrue(assignment.isSlotOccupied(ws2));
    Assert.assertTrue(assignment.isSlotOccupied(ws1));

    // test isExecutorAssigned
    Assert.assertTrue(assignment.isExecutorAssigned(new ExecutorDetails(1, 5)));
    Assert.assertFalse(
        assignment.isExecutorAssigned(new ExecutorDetails(21, 25)));

    // test unassignBySlot
    assignment.unassignBySlot(ws1);
    Assert.assertTrue(
        assignment.getExecutorToSlot().containsKey(new ExecutorDetails(6, 10)));
    Assert.assertEquals(ws2,
        assignment.getExecutorToSlot().get(new ExecutorDetails(6, 10)));
  }

  @Test
  @ClojureClass(className = "backtype.storm.scheduler_test#test-topologies")
  public void testTopologies() {
    ExecutorDetails executor1 = new ExecutorDetails(1, 5);
    ExecutorDetails executor2 = new ExecutorDetails(6, 10);

    Map topologyConf = new HashMap<String, String>();
    topologyConf.put(Config.TOPOLOGY_NAME, "topology-name-1");

    Map executorToComponents = new HashMap<ExecutorDetails, String>();
    executorToComponents.put(executor1, "spout1");
    executorToComponents.put(executor2, "bolt1");

    TopologyDetails topology1 = new TopologyDetails("topology1", topologyConf,
        new StormTopology(), 1, executorToComponents);

    // test topology.selectExecutorToComponents
    List<ExecutorDetails> list = new ArrayList<ExecutorDetails>();
    list.add(executor1);
    Map<ExecutorDetails, String> executorToComp =
        topology1.selectExecutorToComponent(list);

    Map executorToComponents1 = new HashMap<ExecutorDetails, String>();
    executorToComponents1.put(executor1, "spout1");
    Assert.assertEquals(executorToComp, executorToComponents1);

    // test topologies.getById
    topologyConf = new HashMap();
    topologyConf.put(Config.TOPOLOGY_NAME, "topology-name-2");
    TopologyDetails topology2 = new TopologyDetails("topology2", topologyConf,
        new StormTopology(), 1, new HashMap<ExecutorDetails, String>());
    Map<String, TopologyDetails> nameToTopologyDetails =
        new HashMap<String, TopologyDetails>();
    nameToTopologyDetails.put("topology1", topology1);
    nameToTopologyDetails.put("topology2", topology2);
    Topologies topologies = new Topologies(nameToTopologyDetails);
    Assert.assertEquals("topology1", topologies.getById("topology1").getId());

    // test topologies.getByName
    Assert.assertEquals("topology2",
        topologies.getByName("topology-name-2").getId());

  }

  @Test
  @ClojureClass(className = "backtype.storm.scheduler-test#test-cluster")
  public void testCluster() {
    List l1 = Arrays.asList(1, 3, 5, 7, 9);
    List l2 = Arrays.asList(2, 4, 6, 8, 10);

    SupervisorDetails supervisor1 = new SupervisorDetails("supervisor1",
        "192.168.0.1", new ArrayList(), l1);
    SupervisorDetails supervisor2 = new SupervisorDetails("supervisor2",
        "192.168.0.2", new ArrayList(), l2);
    ExecutorDetails executor1 = new ExecutorDetails(1, 5);
    ExecutorDetails executor2 = new ExecutorDetails(6, 10);
    ExecutorDetails executor3 = new ExecutorDetails(11, 15);
    ExecutorDetails executor11 = new ExecutorDetails(100, 105);
    ExecutorDetails executor12 = new ExecutorDetails(106, 110);
    ExecutorDetails executor21 = new ExecutorDetails(201, 205);
    ExecutorDetails executor22 = new ExecutorDetails(206, 210);

    // topology1 needs scheduling: executor3 is NOT assigned a slot.
    Map<String, String> topologyConf = new HashMap<String, String>();
    topologyConf.put(Config.TOPOLOGY_NAME, "topology-name-1");
    Map<ExecutorDetails, String> executorToComponents =
        new HashMap<ExecutorDetails, String>();
    executorToComponents.put(executor1, "spout1");
    executorToComponents.put(executor2, "bolt1");
    executorToComponents.put(executor3, "bolt2");

    TopologyDetails topology1 = new TopologyDetails("topology1", topologyConf,
        new StormTopology(), 2, executorToComponents);

    // topology2 is fully scheduled
    Map<String, String> topologyConf1 = new HashMap<String, String>();
    topologyConf1.put(Config.TOPOLOGY_NAME, "topology-name-2");
    Map<ExecutorDetails, String> executorToComponents1 =
        new HashMap<ExecutorDetails, String>();
    executorToComponents1.put(executor11, "spout11");
    executorToComponents1.put(executor12, "bolt2");
    TopologyDetails topology2 = new TopologyDetails("topology2", topologyConf1,
        new StormTopology(), 2, executorToComponents1);

    // topology3 needs scheduling, since the assignment is squeezed
    Map<String, String> topologyConf2 = new HashMap<String, String>();
    topologyConf2.put(Config.TOPOLOGY_NAME, "topology-name-3");
    Map<ExecutorDetails, String> executorToComponents2 =
        new HashMap<ExecutorDetails, String>();
    executorToComponents2.put(executor21, "spout21");
    executorToComponents2.put(executor22, "bolt22");
    TopologyDetails topology3 = new TopologyDetails("topology3", topologyConf2,
        new StormTopology(), 2, executorToComponents2);

    Map<String, TopologyDetails> parmentTopologies =
        new HashMap<String, TopologyDetails>();
    parmentTopologies.put("topology1", topology1);
    parmentTopologies.put("topology2", topology2);
    parmentTopologies.put("topology3", topology3);
    Topologies topologies = new Topologies(parmentTopologies);

    Map<ExecutorDetails, WorkerSlot> executorToSlot1 =
        new HashMap<ExecutorDetails, WorkerSlot>();
    executorToSlot1.put(executor1, new WorkerSlot("supervisor1", 1));
    executorToSlot1.put(executor2, new WorkerSlot("supervisor2", 2));

    Map<ExecutorDetails, WorkerSlot> executorToSlot2 =
        new HashMap<ExecutorDetails, WorkerSlot>();
    executorToSlot2.put(executor11, new WorkerSlot("supervisor1", 3));
    executorToSlot2.put(executor12, new WorkerSlot("supervisor2", 4));

    Map<ExecutorDetails, WorkerSlot> executorToSlot3 =
        new HashMap<ExecutorDetails, WorkerSlot>();
    executorToSlot3.put(executor21, new WorkerSlot("supervisor1", 5));
    executorToSlot3.put(executor22, new WorkerSlot("supervisor1", 5));

    SchedulerAssignmentImpl assignment1 =
        new SchedulerAssignmentImpl("topology1", executorToSlot1);
    SchedulerAssignmentImpl assignment2 =
        new SchedulerAssignmentImpl("topology2", executorToSlot2);
    SchedulerAssignmentImpl assignment3 =
        new SchedulerAssignmentImpl("topology3", executorToSlot3);

    StandaloneNimbus nimbus = new StandaloneNimbus();
    Map<String, SupervisorDetails> supervisors =
        new HashMap<String, SupervisorDetails>();
    Map<String, SchedulerAssignmentImpl> assignments =
        new HashMap<String, SchedulerAssignmentImpl>();
    supervisors.put("supervisor1", supervisor1);
    supervisors.put("supervisor2", supervisor2);
    assignments.put("topology1", assignment1);
    assignments.put("topology2", assignment2);
    assignments.put("topology3", assignment3);
    Cluster cluster =
        new Cluster(nimbus, supervisors, assignments, topologyConf);

    // test Cluster constructor
    Set<String> supervisorNames = cluster.getSupervisors().keySet();
    Assert.assertEquals(2, supervisorNames.size());
    Assert.assertTrue(supervisorNames.contains("supervisor1"));
    Assert.assertTrue(supervisorNames.contains("supervisor2"));

    Set<String> assignmentNames = cluster.getAssignments().keySet();
    Assert.assertEquals(3, assignmentNames.size());
    Assert.assertTrue(assignmentNames.contains("topology1"));
    Assert.assertTrue(assignmentNames.contains("topology2"));
    Assert.assertTrue(assignmentNames.contains("topology3"));

    // test cluster.getUnassignedExecutors
    Collection<ExecutorDetails> unassignedExecutors =
        cluster.getUnassignedExecutors(topology1);
    Assert.assertEquals(1, unassignedExecutors.size());
    Assert.assertTrue(unassignedExecutors.contains(executor3));
    Assert.assertTrue(cluster.getUnassignedExecutors(topology2).isEmpty());

    // test Cluster.needsScheduling
    Assert.assertEquals(true, cluster.needsScheduling(topology1));
    Assert.assertEquals(false, cluster.needsScheduling(topology2));
    Assert.assertEquals(true, cluster.needsScheduling(topology3));

    // test Cluster.needSchedulingTopogies
    Set<String> topologyIds = new HashSet<String>();
    for (TopologyDetails topology : cluster
        .needsSchedulingTopologies(topologies)) {
      topologyIds.add(topology.getId());
    }
    Assert.assertEquals(2, topologyIds.size());
    Assert.assertTrue(topologyIds.contains("topology1"));
    Assert.assertTrue(topologyIds.contains("topology3"));

    // test Cluster.getNeedsSchedulingComponentToExecutors
    Map<ExecutorDetails, String> executorDetailsToComponent =
        cluster.getNeedsSchedulingExecutorToComponents(topology1);
    Assert.assertEquals(1, executorDetailsToComponent.size());
    Assert.assertEquals("bolt2", executorDetailsToComponent.get(executor3));
    Assert.assertEquals(0,
        cluster.getNeedsSchedulingExecutorToComponents(topology2).size());
    Assert.assertEquals(0,
        cluster.getNeedsSchedulingComponentToExecutors(topology3).size());

    // test Cluster.getUsedPorts
    Set<Integer> testSet = cluster.getUsedPorts(supervisor1);
    Assert.assertEquals(3, testSet.size());
    Assert.assertTrue(testSet.contains(1));
    Assert.assertTrue(testSet.contains(3));
    Assert.assertTrue(testSet.contains(5));

    Set<Integer> testSet1 = cluster.getUsedPorts(supervisor2);
    Assert.assertEquals(2, testSet1.size());
    Assert.assertTrue(testSet1.contains(2));
    Assert.assertTrue(testSet1.contains(4));

    // test Cluster.getAvailablePorts
    Set<Integer> testSet2 = cluster.getAvailablePorts(supervisor1);
    Assert.assertEquals(2, testSet2.size());
    Assert.assertTrue(testSet2.contains(7));
    Assert.assertTrue(testSet2.contains(9));
    Set<Integer> testSet3 = cluster.getAvailablePorts(supervisor2);
    Assert.assertEquals(3, testSet3.size());
    Assert.assertTrue(testSet3.contains(6));
    Assert.assertTrue(testSet3.contains(8));
    Assert.assertTrue(testSet3.contains(10));

    // test Cluster.getAvailableSlots
    List<WorkerSlot> listSlots = cluster.getAvailableSlots(supervisor1);
    Set<WorkerSlot> slotSet = new HashSet<WorkerSlot>(listSlots);

    Set<WorkerSlot> testSet4 = new HashSet<WorkerSlot>();
    testSet4.add(new WorkerSlot("supervisor1", 7));
    testSet4.add(new WorkerSlot("supervisor1", 9));

    Set<WorkerSlot> diffSets = CoreUtil.set_difference(slotSet, testSet4);
    Assert.assertTrue((null == diffSets || diffSets.size() == 0));

    List<WorkerSlot> listSlots1 = cluster.getAvailableSlots(supervisor2);
    Set<WorkerSlot> slotSet1 = new HashSet<WorkerSlot>(listSlots1);

    Set<WorkerSlot> testSet5 = new HashSet<WorkerSlot>();
    testSet5.add(new WorkerSlot("supervisor2", 6));
    testSet5.add(new WorkerSlot("supervisor2", 8));
    testSet5.add(new WorkerSlot("supervisor2", 10));

    Set<WorkerSlot> diffSets1 = CoreUtil.set_difference(slotSet1, testSet5);
    Assert.assertTrue((null == diffSets1) || (diffSets1.size() == 0));

    // test cluster.getAvailableSlots
    List<WorkerSlot> listSlots2 = cluster.getAvailableSlots();
    Set<WorkerSlot> slotSet2 = new HashSet<WorkerSlot>(listSlots2);

    Set<WorkerSlot> testSet6 = new HashSet<WorkerSlot>();
    testSet6.add(new WorkerSlot("supervisor1", 7));
    testSet6.add(new WorkerSlot("supervisor1", 9));
    testSet6.add(new WorkerSlot("supervisor2", 6));
    testSet6.add(new WorkerSlot("supervisor2", 8));
    testSet6.add(new WorkerSlot("supervisor2", 10));

    Set<WorkerSlot> diffSets2 = CoreUtil.set_difference(slotSet2, testSet6);
    Assert.assertTrue((null == diffSets2) || (diffSets2.size() == 0));

    // test Cluster.getAssignedNumWorkers
    Assert.assertTrue(cluster.getAssignedNumWorkers(topology1) == 2);
    Assert.assertTrue(cluster.getAssignedNumWorkers(topology2) == 2);
    Assert.assertTrue(cluster.getAssignedNumWorkers(topology3) == 1);

    // test Cluster.isSlotOccupied
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 1)));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 3)));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 5)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 7)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 9)));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 2)));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 4)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 6)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 8)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor2", 10)));

    // test Cluster.getAssignmentById
    Assert.assertEquals(assignment1, cluster.getAssignmentById("topology1"));
    Assert.assertEquals(assignment2, cluster.getAssignmentById("topology2"));
    Assert.assertEquals(assignment3, cluster.getAssignmentById("topology3"));

    // test cluster.getSupervisorsById
    Assert.assertEquals(supervisor1, cluster.getSupervisorById("supervisor1"));
    Assert.assertEquals(supervisor2, cluster.getSupervisorById("supervisor2"));

    // test Cluster.getSupervisorByHost
    List<SupervisorDetails> listHost =
        cluster.getSupervisorsByHost("192.168.0.1");
    Set<SupervisorDetails> setHost = new HashSet<SupervisorDetails>(listHost);

    Set<SupervisorDetails> testSetHost = new HashSet<SupervisorDetails>();
    testSetHost.add(supervisor1);

    Set<SupervisorDetails> diffSets3 =
        CoreUtil.set_difference(testSetHost, setHost);
    Assert.assertTrue((null == diffSets3) || (diffSets3.size() == 0));

    List<SupervisorDetails> listHost1 =
        cluster.getSupervisorsByHost("192.168.0.2");
    Set<SupervisorDetails> setHost1 = new HashSet<SupervisorDetails>(listHost1);

    Set<SupervisorDetails> testSetHost1 = new HashSet<SupervisorDetails>();
    testSetHost1.add(supervisor2);

    Set<SupervisorDetails> diffSets4 =
        CoreUtil.set_difference(testSetHost1, setHost1);
    Assert.assertTrue((null == diffSets4) || (diffSets4.size() == 0));

    // ==== the following tests will change the state of the cluster, so put
    // it
    // here at the end ====
    // test Cluster.assign
    List<ExecutorDetails> listExe = new ArrayList<ExecutorDetails>();
    listExe.add(executor3);
    cluster.assign(new WorkerSlot("supervisor1", 7), "topology1", listExe);
    Assert.assertFalse(cluster.needsScheduling(topology1));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 7)));

    // revert the change
    cluster.freeSlot(new WorkerSlot("supervisor1", 7));

    // test Cluster.assign:if a executor is already assigned, there will be
    // an exception
    List<ExecutorDetails> executor1Parameter = new ArrayList<ExecutorDetails>();
    executor1Parameter.add(executor1);

    boolean flag = true;
    try {
      cluster.assign(new WorkerSlot("supervisor1", 9), "topology1",
          executor1Parameter);
      flag = false;
    } catch (Exception e) {
      flag = true;
    }
    Assert.assertTrue(flag);

    // test Cluster.freeSlot
    cluster.freeSlot(new WorkerSlot("supervisor", 7));
    Assert.assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor", 7)));

    // test Cluster.freeSlots
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 1)));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 3)));
    Assert.assertTrue(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 5)));
    List<WorkerSlot> testFreeSlotsParameter = new ArrayList<WorkerSlot>();
    testFreeSlotsParameter.add(new WorkerSlot("supervisor1", 1));
    testFreeSlotsParameter.add(new WorkerSlot("supervisor1", 3));
    testFreeSlotsParameter.add(new WorkerSlot("supervisor1", 5));
    cluster.freeSlots(testFreeSlotsParameter);

    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 1)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 3)));
    Assert
        .assertFalse(cluster.isSlotOccupied(new WorkerSlot("supervisor1", 5)));
  }
}
