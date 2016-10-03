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
package com.tencent.jstorm.daemon.worker.threads;

import java.util.Map;

import org.apache.storm.cluster.IStormClusterState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.generated.StormBase;
import org.apache.storm.generated.TopologyStatus;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy ablecao
 * @ModifiedTime 11:16:01 AM Mar 15, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worker#refresh-storm-active")
public class RefreshStormActive extends RunnableCallback {
  private static final long serialVersionUID = 1L;

  private static Logger LOG = LoggerFactory.getLogger(RefreshStormActive.class);

  private WorkerData workerData;

  private IStormClusterState stormClusterState;
  private String topologyId;

  public RefreshStormActive(WorkerData workerData, RunnableCallback callback,
      @SuppressWarnings("rawtypes") Map conf) {
    this.workerData = workerData;
    this.stormClusterState = workerData.getStormClusterState();
    this.topologyId = workerData.getTopologyId();
  }

  public RefreshStormActive(WorkerData workerData,
      @SuppressWarnings("rawtypes") Map conf) {
    this(workerData, null, conf);
  }

  @Override
  public void run() {

    try {
      if (workerData.getWorkerActiveFlag().get()) {
        if (TopologyStatus.KILLED
            .equals(workerData.getTopologyStatus().get())) {
          // topology is about to be deleted, return; issue #171
          return;
        }
        TopologyStatus baseStatus = TopologyStatus.ACTIVE;
        StormBase base = stormClusterState.stormBase(topologyId, this);
        if (base == null) {
          baseStatus = TopologyStatus.KILLED;
        } else {
          baseStatus = base.get_status();
          workerData.getStormComponentToDebugAtom()
              .set(base.get_component_debug());
        }

        TopologyStatus oldTopologyStatus = workerData.getTopologyStatus().get();
        boolean isActive = baseStatus.equals(TopologyStatus.ACTIVE)
            && workerData.getWorkerActiveFlag().get();
        workerData.getStormActiveAtom().set(isActive);

        LOG.debug("Event debug options {}",
            workerData.getStormComponentToDebugAtom().get());
        if (baseStatus.equals(oldTopologyStatus)) {
          return;
        }
        LOG.info("Old TopologyStatus:" + oldTopologyStatus
            + ", new TopologyStatus:" + baseStatus);
        workerData.getTopologyStatus().set((baseStatus));
      }
    } catch (Exception e) {
      LOG.error("Failed to get topology from ZK {}",
          CoreUtil.stringifyError(e));
    }
  }
}
