package com.tencent.jstorm.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.storm.scheduler.SchedulerUtils;
import org.junit.Test;

import org.apache.storm.scheduler.ExecutorDetails;
import org.apache.storm.scheduler.WorkerSlot;

public class SchedulerUtilsTest {
  @Test
  public void testAssignExecutor2Worker() {
    String nodeId1 = "host1";
    String nodeId2 = "host2";
    String nodeId3 = "host3";

    List<WorkerSlot> assignedSlots = new ArrayList<WorkerSlot>();
    assignedSlots.add(new WorkerSlot(nodeId1, 6700));
    assignedSlots.add(new WorkerSlot(nodeId3, 6704));
    assignedSlots.add(new WorkerSlot(nodeId2, 6708));
    assignedSlots.add(new WorkerSlot(nodeId1, 6701));
    assignedSlots.add(new WorkerSlot(nodeId3, 6705));
    assignedSlots.add(new WorkerSlot(nodeId2, 6709));
    assignedSlots.add(new WorkerSlot(nodeId1, 6702));
    assignedSlots.add(new WorkerSlot(nodeId3, 6706));
    assignedSlots.add(new WorkerSlot(nodeId1, 6703));

    System.out.println(assignedSlots.toString());
    // construct executors
    List<ExecutorDetails> tasks = new ArrayList<ExecutorDetails>();
    for (int i = 0; i < 10; i++) {
      tasks.add(new ExecutorDetails(i, i));
    }
    Collections.shuffle(tasks);
    Map<ExecutorDetails, WorkerSlot> assignment =
        SchedulerUtils.assignExecutor2Worker(
            new HashSet<ExecutorDetails>(tasks), assignedSlots);
    Map<ExecutorDetails, WorkerSlot> expected =
        new HashMap<ExecutorDetails, WorkerSlot>();

  }

  @Test
  public void testSortSlots() {
    String nodeId1 = "host1";
    String nodeId2 = "host2";
    String nodeId3 = "host3";

    List<WorkerSlot> testSlots = new ArrayList<WorkerSlot>();
    testSlots.add(new WorkerSlot(nodeId1, 6701));
    testSlots.add(new WorkerSlot(nodeId1, 6700));
    testSlots.add(new WorkerSlot(nodeId1, 6703));
    testSlots.add(new WorkerSlot(nodeId1, 6702));

    testSlots.add(new WorkerSlot(nodeId3, 6705));
    testSlots.add(new WorkerSlot(nodeId3, 6706));
    testSlots.add(new WorkerSlot(nodeId3, 6704));

    testSlots.add(new WorkerSlot(nodeId2, 6709));
    testSlots.add(new WorkerSlot(nodeId2, 6708));

    // List<WorkerSlot> sortedList = SchedulerUtils.sortSlots(testSlots);

    List<WorkerSlot> expectedList = new ArrayList<WorkerSlot>();
    expectedList.add(new WorkerSlot(nodeId1, 6700));
    expectedList.add(new WorkerSlot(nodeId3, 6704));
    expectedList.add(new WorkerSlot(nodeId2, 6708));
    expectedList.add(new WorkerSlot(nodeId1, 6701));
    expectedList.add(new WorkerSlot(nodeId3, 6705));
    expectedList.add(new WorkerSlot(nodeId2, 6709));
    expectedList.add(new WorkerSlot(nodeId1, 6702));
    expectedList.add(new WorkerSlot(nodeId3, 6706));
    expectedList.add(new WorkerSlot(nodeId1, 6703));

    // Assert.assertTrue(Utils.listEqual(sortedList, expectedList));
  }

  @Test
  public void testSortSlots2() {
    String nodeId1 = "10.209.16.217";
    String nodeId2 = "10.219.5.22";
    String nodeId3 = "10.208.154.208";
    String nodeId4 = "10.149.28.18";

    List<WorkerSlot> testSlots = new ArrayList<WorkerSlot>();

    addSlots(testSlots, nodeId1, 24);
    addSlots(testSlots, nodeId2, 24);
    addSlots(testSlots, nodeId3, 16);
    addSlots(testSlots, nodeId4, 8);
    List<WorkerSlot> sortedList = SchedulerUtils.sortSlots(testSlots, null);
    System.out.println(sortedList);

  }

  private void addSlots(List<WorkerSlot> testSlots, String host, int size) {
    int port = 6700;
    for (int i = 0; i < size; i++) {
      testSlots.add(new WorkerSlot(host, port));
      port++;
    }

  }

}
