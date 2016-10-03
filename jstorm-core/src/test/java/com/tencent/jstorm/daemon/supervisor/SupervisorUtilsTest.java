package com.tencent.jstorm.daemon.supervisor;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.daemon.supervisor.SupervisorUtils;
import org.apache.storm.daemon.supervisor.workermanager.DefaultWorkerManager;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SupervisorUtilsTest {
  private final static Logger LOG =
      LoggerFactory.getLogger(SupervisorUtilsTest.class);

  @Test
  public void testSubstituteStringChildopts() {
    String workerId = Utils.uuid();
    String topologyId = Utils.uuid();
    long port = 6701;
    int memOnheap = 1024;
    StringBuilder sb = new StringBuilder();
    sb.append("-Did=%ID%").append(" ");
    sb.append("-Dsupervisor.childopts=Xmx256m").append(" ");
    sb.append("-DworkerId=%WORKER-ID%").append(" ");
    sb.append("-DtopologyId=%TOPOLOGY-ID%").append(" ");
    sb.append("-DworkerPort=%WORKER-PORT%").append(" ");
    sb.append("-Xmx%HEAP-MEM%m").append(" ");
    sb.append("-XX:+PrintGCDetails").append(" ");
    sb.append("-Xloggc:artifacts/gc.log");
    List<String> opts = new DefaultWorkerManager().substituteChildopts(sb.toString(),
        workerId, topologyId, port, memOnheap);
    List<String> expectedValue = new ArrayList<String>();
    expectedValue.add("-Did=" + port);
    expectedValue.add("-Dsupervisor.childopts=Xmx256m");
    expectedValue.add("-DworkerId=" + workerId);
    expectedValue.add("-DtopologyId=" + topologyId);
    expectedValue.add("-DworkerPort=" + port);
    expectedValue.add("-Xmx" + memOnheap + "m");
    expectedValue.add("-XX:+PrintGCDetails");
    expectedValue.add("-Xloggc:artifacts/gc.log");
    Assert.assertEquals(expectedValue, opts);
  }

  @Test
  public void testSubstituteCollectionChildopts() {
    String workerId = Utils.uuid();
    String topologyId = Utils.uuid();
    long port = 6701;
    int memOnheap = 2048;
    List<String> lst = new ArrayList<String>();
    lst.add("-Did=%ID%");
    lst.add("-Dsupervisor.childopts=Xmx256m");
    lst.add("-DworkerId=%WORKER-ID%");
    lst.add("-DtopologyId=%TOPOLOGY-ID%");
    lst.add("-DworkerPort=%WORKER-PORT%");
    lst.add("-Xmx%HEAP-MEM%m");
    lst.add("-XX:+PrintGCDetails");
    lst.add("-Xloggc:artifacts/gc.log");
    List<String> opts = new DefaultWorkerManager().substituteChildopts(lst, workerId,
        topologyId, port, memOnheap);
    List<String> expectedValue = new ArrayList<String>();
    expectedValue.add("-Did=" + port);
    expectedValue.add("-Dsupervisor.childopts=Xmx256m");
    expectedValue.add("-DworkerId=" + workerId);
    expectedValue.add("-DtopologyId=" + topologyId);
    expectedValue.add("-DworkerPort=" + port);
    expectedValue.add("-Xmx" + memOnheap + "m");
    expectedValue.add("-XX:+PrintGCDetails");
    expectedValue.add("-Xloggc:artifacts/gc.log");
    Assert.assertEquals(expectedValue, opts);
  }
}
