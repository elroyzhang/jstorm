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
package com.tencent.jstorm.daemon.worker.heartbeat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.ClusterWorkerHeartbeat;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.common.Common;
import com.tencent.jstorm.daemon.executor.ExecutorShutdown;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 11:15:24 AM Mar 15, 2016
 */
public class WorkerHeartbeatRunable implements Runnable {
  private static final Logger LOG =
      LoggerFactory.getLogger(WorkerHeartbeatRunable.class);
  private IStormClusterState stormClusterState;
  private String stormId;
  private Utils.UptimeComputer uptime;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private String node;
  private int port;
  private WorkerData workerData;
  private boolean isDebug = false;
  private Map<Integer, Integer> virtualToRealPort;

  /** EMPTY HBs */
  private static Map<ExecutorInfo, ExecutorStats> EMPTY_STATS =
      new HashMap<ExecutorInfo, ExecutorStats>();

  @SuppressWarnings("unchecked")
  public WorkerHeartbeatRunable(WorkerData workerData) throws Exception {
    this.workerData = workerData;
    this.stormClusterState = workerData.getStormClusterState();
    this.stormId = workerData.getTopologyId();
    this.node = workerData.getAssignmentId();
    this.port = workerData.getPort();
    this.stormConf = workerData.getStormConf();
    this.virtualToRealPort =
        (Map<Integer, Integer>) stormConf.get("storm.virtual.real.ports");
    if (virtualToRealPort != null && virtualToRealPort.containsKey(port)) {
      this.port = virtualToRealPort.get(port);
    }
    this.uptime = workerData.getUptime();
    this.isDebug =
        Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);

    /** mk Empty Stats */
    mkEmptyStats();
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#do-executor-heartbeats")
  private void doExecutorHeartbeats() {
    List<ExecutorShutdown> executors = workerData.getShutdownExecutors();
    // stats is how we know what executors are assigned to this worker
    Map<ExecutorInfo, ExecutorStats> stats =
        new HashMap<ExecutorInfo, ExecutorStats>();
    if (executors == null) {
      LOG.warn("ExecutorShutdowns is null");
      // return; INACTIVE Topology's dead worker(e.g. kill -9 XXX) cannot heart
      // beat to Zookeeper
      stats = EMPTY_STATS;
    } else {
      for (ExecutorShutdown executor : executors) {
        List<Long> list = executor.get_executor_id();
        ExecutorInfo executorInfo = new ExecutorInfo(list.get(0).intValue(),
            list.get(list.size() - 1).intValue());
        stats.put(executorInfo, executor.render_stats());
      }
    }

    Integer currtime = Time.currentTimeSecs();
    ClusterWorkerHeartbeat zkHb =
        new ClusterWorkerHeartbeat(stormId, stats, currtime, uptime.upTime());
    try {
      // do the zookeeper heartbeat
      if (TopologyStatus.KILLED.equals(workerData.getTopologyStatus().get())) {
        // topology is about to be deleted, return; issue #171
        return;
      }
      stormClusterState.workerHeartbeat(stormId, node, Long.valueOf(port),
          zkHb);
    } catch (Exception e) {
      LOG.error("Failed to update heartbeat to zookeeper for "
          + CoreUtil.stringifyError(e));
      return;
    }
    if (isDebug) {
      LOG.info("WorkerHeartbeat:" + zkHb.toString());
    }
  }

  @Override
  public void run() {
    doExecutorHeartbeats();
  }

  private void mkEmptyStats() throws Exception {
    List<List<Long>> executorList = this.workerData.getExecutors();
    for (List<Long> list : executorList) {
      ExecutorInfo executorInfo = new ExecutorInfo(list.get(0).intValue(),
          list.get(list.size() - 1).intValue());

      WorkerTopologyContext workerContext = Common.workerContext(workerData);
      String componentId = workerContext.getComponentId(list.get(0).intValue());
      ComponentType componentType =
          ExecutorUtils.componentType(workerContext, componentId);
      CommonStats stats = ExecutorUtils.mkExecutorStats(componentType,
          ConfigUtils.samplingRate(stormConf));
      if (stats instanceof BoltExecutorStats) {
        EMPTY_STATS.put(executorInfo,
            ((BoltExecutorStats) stats).renderStats());
      } else if (stats instanceof SpoutExecutorStats) {
        EMPTY_STATS.put(executorInfo,
            ((SpoutExecutorStats) stats).renderStats());
      }
    }
  }
}
