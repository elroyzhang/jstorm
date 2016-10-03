package com.tencent.example;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.SupervisorInfo;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;

import com.tencent.jstorm.daemon.nimbus.NimbusUtils;

public class ZkPerformanceTest {

  private static Map STORM_CONF = Utils.readStormConfig();
  private static IStormClusterState stormClusterState;

  static {
    try {
      if (Utils.isZkAuthenticationConfiguredStormServer(STORM_CONF)) {
        stormClusterState = ClusterUtils.mkStormClusterState(STORM_CONF,
            NimbusUtils.nimbusZKAcls(),
            new ClusterStateContext(DaemonType.NIMBUS));
      } else {
        stormClusterState = ClusterUtils.mkStormClusterState(STORM_CONF, null,
            new ClusterStateContext(DaemonType.NIMBUS));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void testGetAllSupervisorFromZk() {
    try {
      long begin = Time.currentTimeMillis();
      Map<String, SupervisorInfo> supervisorInfos =
          NimbusUtils.allSupervisorInfo(stormClusterState);
      long end = Time.currentTimeMillis();
      System.out
          .println("testGetAllSupervisorFromZk cost time: " + (end - begin));
      System.out.println("SupervisorInfo.size:" + supervisorInfos.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void testGetAllTopologyFromZk() {
    try {
      long begin = Time.currentTimeMillis();
      Map<String, StormBase> bases =
          StormCommon.topologyBases(stormClusterState);
      for (Entry<String, StormBase> entry : bases.entrySet()) {
        String id = entry.getKey();
        StormBase base = entry.getValue();
        long getAssignmentBegin = Time.currentTimeMillis();
        Assignment assignment = stormClusterState.assignmentInfo(id, null);
        System.out.println(id + " stormClusterState.assignmentInfo cost time: "
            + (Time.currentTimeMillis() - getAssignmentBegin));
      }
      long end = Time.currentTimeMillis();
      System.out
          .println("testGetAllTopologyFromZk cost time: " + (end - begin));
      System.out.println("Topology.size:" + bases.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void testGetAllNimbusFromZk() {
    try {
      long begin = Time.currentTimeMillis();
      List<NimbusSummary> nimbuses = stormClusterState.nimbuses();
      long end = Time.currentTimeMillis();
      System.out.println("testGetAllNimbusFromZk cost time: " + (end - begin));
      System.out.println("Nimbus.size:" + nimbuses.size());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) {
    testGetAllSupervisorFromZk();
    testGetAllTopologyFromZk();
    testGetAllNimbusFromZk();
  }

}
