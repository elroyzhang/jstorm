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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.LSWorkerHeartbeat;
import org.apache.storm.utils.LocalState;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.ConfigUtils;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.localstate.LocalStateUtils;
import com.tencent.jstorm.utils.CoreUtil;

public class WorkerLocalHeartbeatRunable implements Runnable {
  private static Logger LOG =
      LoggerFactory.getLogger(WorkerLocalHeartbeatRunable.class);

  private Map<Object, Object> conf;
  private String worker_id;
  private Integer port;
  private String topologyId;
  private List<List<Long>> executors;
  private int processId;
  private boolean isDebug = false;
  private Map<Integer, Integer> virtualToRealPort;

  @SuppressWarnings("unchecked")
  public WorkerLocalHeartbeatRunable(WorkerData workerData) {
    this.conf = workerData.getStormConf();
    this.worker_id = workerData.getWorkerId();
    this.port = workerData.getPort();
    this.virtualToRealPort =
        (Map<Integer, Integer>) conf.get("storm.virtual.real.ports");
    if (virtualToRealPort != null && virtualToRealPort.containsKey(port)) {
      this.port = virtualToRealPort.get(port);
    }
    this.topologyId = workerData.getTopologyId();
    this.executors = workerData.getExecutors();
    this.processId = workerData.getProcessId();
    this.isDebug = Utils.getBoolean(conf.get(Config.TOPOLOGY_DEBUG), false);
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#do-heartbeat")
  public void doHeartbeat() throws IOException {

    int currtime = Time.currentTimeSecs();
    List<ExecutorInfo> executorInfos = new ArrayList<ExecutorInfo>();
    for (List<Long> list : executors) {
      executorInfos.add(new ExecutorInfo(list.get(0).intValue(),
          list.get(list.size() - 1).intValue()));
    }
    if (isDebug) {

      LSWorkerHeartbeat hb = new LSWorkerHeartbeat(currtime, topologyId,
          executorInfos, port, Integer.valueOf(processId));
      LOG.info("Doing heartbeat " + hb.toString());
    }
    // do the local-file-system heartbeat.
    LocalState localState = ConfigUtils.workerState(conf, worker_id);
    LocalStateUtils.putLSWorkerHeartbeat(localState, currtime, topologyId,
        executorInfos, port, Integer.valueOf(processId));
    // this is just in case supervisor is down so that disk doesn't fill up.
    // it shouldn't take supervisor 120 seconds between listing dir
    // and reading it
    localState.cleanup(8);
  }

  @Override
  public void run() {
    try {
      doHeartbeat();
    } catch (IOException e) {
      LOG.error("Failed doing Worker HeartBeat ", CoreUtil.stringifyError(e));
    }
  }

}
