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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.grouping.Load;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.DisruptorQueue.QueueMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.daemon.worker.WorkerUtils;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy yuzhongliu
 * @ModifiedTime 12:39:25 PM Jan 21, 2016
 */
@ClojureClass(className = "backtype.storm.daemon.worker#mk-refresh-load")
public class RefreshLoad implements Runnable {
  private static Logger LOG = LoggerFactory.getLogger(RefreshLoad.class);
  private WorkerData workerData;
  private Set<Integer> outboundTasks;
  private Set<Integer> localTasks;
  private Set<Integer> remoteTasks;
  private AtomicLong nextUpdate;
  private Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap;
  private final static int LOAD_REFRESH_INTERVAL_MS = 5000;

  public RefreshLoad(WorkerData workerData) throws Exception {
    this.workerData = workerData;
    this.outboundTasks = WorkerUtils.workerOutboundTasks(workerData);

    this.localTasks = workerData.getTaskids();
    this.remoteTasks = CoreUtil.set_difference(outboundTasks, localTasks);
    this.nextUpdate = new AtomicLong(0);
    this.shortExecutorReceiveQueueMap =
        workerData.getShortExecutorReceiveQueueMap();
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-refresh-load")
  public void run() {
    if (workerData.getStormActiveAtom().get()) {
      try {
        LoadMapping loadMapping = workerData.getLoadMapping();
        Map<Integer, Double> localPop = new HashMap<Integer, Double>();
        for (Map.Entry<Integer, DisruptorQueue> entry : shortExecutorReceiveQueueMap
            .entrySet()) {
          QueueMetrics qMetrics = entry.getValue().getMetrics();
          localPop.put(entry.getKey(),
              1.0D * qMetrics.population() / qMetrics.capacity());
        }
        Map<Integer, Load> remoteLoad = new HashMap<Integer, Load>();
        for (Map.Entry<String, IConnection> entry : workerData
            .getCachedNodeportToSocket().entrySet()) {
          IConnection conn = entry.getValue();
          remoteLoad.putAll(conn.getLoad(remoteTasks));
        }
        long now = System.currentTimeMillis();
        loadMapping.setLocal(localPop);
        loadMapping.setRemote(remoteLoad);
        if (now > nextUpdate.get()) {
          workerData.getReceiver().sendLoadMetrics(localPop);
          nextUpdate.set(now + LOAD_REFRESH_INTERVAL_MS);
        }
      } catch (Exception e) {
        LOG.error("Failed to refresh load ", CoreUtil.stringifyError(e));
        throw new RuntimeException(e);
      }
    }
  }
}
