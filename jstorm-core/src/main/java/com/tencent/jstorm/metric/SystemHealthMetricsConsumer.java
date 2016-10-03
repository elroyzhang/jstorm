package com.tencent.jstorm.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Constants;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.WorkerJvmInfo;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.MapKeyConstants;

/**
 * get the jvm memory info config : ```yaml topology.metrics.consumer.register:
 * - class: "com.tencent.jstorm.metric.SystemHealthMetricsConsumer"
 * parallelism.hint: 1
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 10:46:14 AM May 20, 2016
 */
public class SystemHealthMetricsConsumer implements IMetricsConsumer {
  public static final Logger LOG =
      LoggerFactory.getLogger(SystemHealthMetricsConsumer.class);

  private IStormClusterState stormClusterState;

  private List<String> workerJvmInfoNodes = new ArrayList<String>();
  private String topologyId = "";

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, Object registrationArgument,
      TopologyContext context, IErrorReporter errorReporter) {
    try {
      topologyId = context.getStormId();
      List<ACL> acls = Utils.getWorkerACL(stormConf);
      IStateStorage stateStore = ClusterUtils.mkStateStorage(stormConf,
          stormConf, acls, new ClusterStateContext(DaemonType.WORKER));
      this.stormClusterState = ClusterUtils.mkStormClusterState(stateStore,
          acls, new ClusterStateContext());
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handleDataPoints(TaskInfo taskInfo,
      Collection<DataPoint> dataPoints) {
    String componentId = taskInfo.srcComponentId;
    if (!Constants.SYSTEM_COMPONENT_ID.equals(componentId)) {
      return;
    }
    int taskId = taskInfo.srcTaskId;
    String host = taskInfo.srcWorkerHost;
    int port = taskInfo.srcWorkerPort;
    Map<String, Map<String, String>> jvmInfo =
        new HashMap<String, Map<String, String>>();
    Map<String, String> workerInfo = new HashMap<String, String>();
    workerInfo.put("host", host);
    workerInfo.put("port", String.valueOf(port));
    workerInfo.put("taskId", String.valueOf(taskId));
    workerInfo.put("componentId", componentId);
    jvmInfo.put(MapKeyConstants.WROKER_INFO, workerInfo);
    Map<String, String> jvmTime = new HashMap<String, String>();
    jvmInfo.put(MapKeyConstants.JVM_TIME, jvmTime);
    for (DataPoint dataPoint : dataPoints) {
      String name = dataPoint.name;
      Object value = dataPoint.value;
      if (MapKeyConstants.START_TIME_SECS.equals(name)
          || MapKeyConstants.UP_TIME_SECS.equals(name)) {
        jvmTime.put(name, String.valueOf(value));
      } else if (MapKeyConstants.GC_PS_SCAVANGE.equals(name)
          || MapKeyConstants.GC_PS_MARK_SWEEP.equals(name)
          || MapKeyConstants.MEMORY_NONHEAP.equals(name)
          || MapKeyConstants.MEMORY_HEAP.equals(name)
          || MapKeyConstants.PROCESS_INFO.equals(name)) {
        Map<String, String> valueMap = new HashMap<String, String>();
        for (Map.Entry<String, Object> entry : ((Map<String, Object>) value)
            .entrySet()) {
          valueMap.put(entry.getKey(), String.valueOf(entry.getValue()));
        }
        jvmInfo.put(name, valueMap);
      }
    }
    WorkerJvmInfo workerJvmInfo = new WorkerJvmInfo();
    workerJvmInfo.set_topologyId(topologyId);
    workerJvmInfo.set_value(jvmInfo);
    stormClusterState.setWorkerJvmInfo(host, Long.valueOf(port), workerJvmInfo);
    LOG.debug("set worker jvm info into zk : " + host + ":" + port);
    workerJvmInfoNodes.add(host + ":" + port);
  }

  @Override
  public void cleanup() {
    for (String node : workerJvmInfoNodes) {
      String[] nodeArr = node.split(":");
      String host = nodeArr[0];
      Long port = Long.valueOf(nodeArr[1]);
      stormClusterState.removeWorkerJvmNode(host, port);
      LOG.debug("delete worker jvm node : " + node);
    }
  }

}