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
package com.tencent.jstorm.daemon.executor.bolt;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Constants;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.utils.DisruptorQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 5:09:30 PM Feb 25, 2016
 */
public class BoltFn extends RunnableCallback {
  private static final Logger LOG = LoggerFactory.getLogger(BoltFn.class);

  private static final long serialVersionUID = 1L;

  private BoltEventHandler boltEventHandler;
  private ExecutorData executorData;

  public BoltFn(ExecutorData executorData,
      Map<String, String> initialCredentials,
      Map<Integer, TaskData> taskDatas) {
    this.executorData = executorData;
    this.boltEventHandler = new BoltEventHandler(executorData);
    try {
      // If topology was started in inactive state, don't call prepare
      // bolt until it's activated first.
      while (!executorData.getWorker().getStormActiveAtom().get()) {
        LOG.debug("Storm not active yet. Sleep 100 ms ...");
        Thread.sleep(100);
      }
      for (Map.Entry<Integer, TaskData> tt : taskDatas.entrySet()) {
        Integer taskId = tt.getKey();
        TaskData taskData = tt.getValue();
        IBolt boltObj = (IBolt) taskData.getObject();

        if (boltObj instanceof ICredentialsListener) {
          ((ICredentialsListener) boltObj).setCredentials(initialCredentials);
        }
        TopologyContext userContext = taskData.getUserContext();
        if (executorData.getComponentId()
            .equals(Constants.SYSTEM_COMPONENT_ID)) {
          Map<String, DisruptorQueue> queues =
              new HashMap<String, DisruptorQueue>();
          queues.put("sendqueue", executorData.getBatchTransferQueue());
          queues.put("receive", executorData.getReceiveQueue());
          queues.put("transfer", executorData.getWorker().getTransferQueue());
          BuiltinMetricsUtil.registerQueueMetrics(queues,
              executorData.getStormConf(), userContext);
          BuiltinMetricsUtil.registerIconnectionClientMetrics(
              executorData.getWorker().getCachedNodeportToSocket(),
              executorData.getStormConf(), userContext);
          BuiltinMetricsUtil.registerIconnectionServerMetric(
              executorData.getWorker().getReceiver(),
              executorData.getStormConf(), userContext);
        } else {
          Map<String, DisruptorQueue> queues =
              new HashMap<String, DisruptorQueue>();
          queues.put("sendqueue", executorData.getBatchTransferQueue());
          queues.put("receive", executorData.getReceiveQueue());
          BuiltinMetricsUtil.registerQueueMetrics(queues,
              executorData.getStormConf(), userContext);
        }

        boltObj.prepare(executorData.getStormConf(), taskData.getUserContext(),
            new OutputCollector(new BoltOutputCollector(taskData,
                executorData.getReportError(), executorData.getStormConf(),
                executorData.getTransferFn(), executorData.getWorkerContext(),
                taskId, null, executorData.getStats())));
      }
    } catch (Exception e) {
      LOG.error("Bolt Executor init error {}", CoreUtil.stringifyError(e));
    }

    this.executorData.getOpenOrPrepareWasCalled().reset(true);

  }

  @Override
  public Long call() throws Exception {
    executorData.getReceiveQueue().consumeBatchWhenAvailable(boltEventHandler);
    return 0L;
  }

}
