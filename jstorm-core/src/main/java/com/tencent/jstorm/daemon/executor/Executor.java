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
package com.tencent.jstorm.daemon.executor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.daemon.Acker;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.SmartThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.bolt.MkThreadsBolt;
import com.tencent.jstorm.daemon.executor.spout.MkThreadsSpout;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.worker.WorkerData;
import com.tencent.jstorm.disruptor.ExecutorDisruptorBackpressureHandler;
import com.tencent.jstorm.utils.thread.RunnableCallback;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "org.apache.storm.daemon.executor")
public class Executor implements Serializable {
  private static final long serialVersionUID = 1L;
  private final static Logger LOG = LoggerFactory.getLogger(Executor.class);
  private WorkerData workerData;
  private ExecutorData executorData;
  private Map<Integer, TaskData> taskDatas;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private Map<String, String> initialCredentials;

  public Executor(WorkerData workerData, List<Long> executorId,
      Map<String, String> initialCredentials) throws Exception {
    this.workerData = workerData;
    this.executorData = new ExecutorData(workerData, executorId);
    this.taskDatas = executorData.getTaskDatas();
    this.stormConf = executorData.getStormConf();
    this.initialCredentials = initialCredentials;
    LOG.info("Loading task " + executorData.getComponentId() + ":"
        + executorData.getTaskId());
  }

  @ClojureClass(className = "org.apache.storm.daemon.executor#mk-threads")
  public RunnableCallback mkThreads(ExecutorData executorData,
      Map<Integer, TaskData> taskDatas, Map<String, String> initialCredentials)
          throws Exception {
    ComponentType componentType = executorData.getComponentType();
    if (componentType.equals(ComponentType.BOLT)) {
      return new MkThreadsBolt(executorData, taskDatas, initialCredentials);
    } else if (componentType.equals(ComponentType.SPOUT)) {
      return new MkThreadsSpout(executorData, taskDatas, initialCredentials);
    }
    return null;
  }

  public ExecutorShutdown execute() throws Exception {
    List<SmartThread> threads = new ArrayList<SmartThread>();
    // starting the batch-transfer->worker ensures that anything publishing to
    // that queue doesn't block (because it's a single threaded queue and the
    // caching/consumer started trick isn't thread-safe)
    StartBatchTransferToWorkerHandler batchTransferToWorker =
        new StartBatchTransferToWorkerHandler(workerData, executorData);
    SmartThread systemThreads = Utils.asyncLoop(batchTransferToWorker,
        executorData.getBatchTransferQueue().getName(),
        executorData.getReportErrorAndDie());
    threads.add(systemThreads);

    RunnableCallback baseExecutor =
        mkThreads(executorData, taskDatas, initialCredentials);
    SmartThread executor_threads = Utils.asyncLoop(baseExecutor, false,
        executorData.getReportErrorAndDie(), Thread.NORM_PRIORITY, true, true,
        executorData.getComponentId() + "-executor" + executorData.getTaskId());
    threads.add(executor_threads);

    //
    ExecutorDisruptorBackpressureHandler disruptorHandler =
        new ExecutorDisruptorBackpressureHandler(executorData);
    executorData.getReceiveQueue()
        .registerBackpressureCallback(disruptorHandler);
    double highWaterMark = Utils.getDouble(executorData.getStormConf()
        .get(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK), 0.9);
    double lowWaterMark = Utils.getDouble(executorData.getStormConf()
        .get(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK), 0.4);
    boolean isBackPressureEnable = Utils.getBoolean(
        workerData.getStormConf().get(Config.TOPOLOGY_BACKPRESSURE_ENABLE),
        false);
    executorData.getReceiveQueue().setHighWaterMark(highWaterMark);
    executorData.getReceiveQueue().setLowWaterMark(lowWaterMark);
    executorData.getReceiveQueue().setEnableBackpressure(isBackPressureEnable);

    setupTicks(workerData, executorData);
    setupMetrics(executorData);
    LOG.info("Finished loading executor componentId:{}, executorId:{}",
        executorData.getComponentId(),
        executorData.getExecutorInfo().toString());
    return new ExecutorShutdown(executorData, workerData.getAssignmentId(),
        workerData.getPort(), threads);
  }

  @ClojureClass(className = "org.apache.storm.daemon.executor#setup-metrics!")
  private void setupMetrics(ExecutorData executorData) {
    Set<Integer> intervals =
        executorData.getIntervalToTaskToMetricRegistry().keySet();
    for (Integer interval : intervals) {
      Runnable metricsRunnable = new MetricsRunable(executorData, interval);
      executorData.getWorker().getUserTimer().scheduleRecurring(interval,
          interval, metricsRunnable);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.executor#setup-ticks!")
  private void setupTicks(WorkerData workerData, ExecutorData executorData) {
    Integer tickTimeSecs =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS), null);
    if (tickTimeSecs != null) {
      boolean isSystemId = Utils.isSystemId(executorData.getComponentId());
      boolean isEnableMessageTimeOuts = Utils.getBoolean(
          stormConf.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS), true);
      boolean isSpout =
          executorData.getComponentType().equals(ComponentType.SPOUT);
      if ((isSystemId
          && !Acker.ACKER_COMPONENT_ID.equals(executorData.getComponentId()))
          || (!isEnableMessageTimeOuts && isSpout)) {
        LOG.info(
            "Timeouts disabled for executor " + executorData.getComponentId()
                + " : " + executorData.getExecutorInfo());
      } else {
        Runnable ticksRunnable = new TicksRunable(executorData, tickTimeSecs);
        workerData.getUserTimer().scheduleRecurring(tickTimeSecs, tickTimeSecs,
            ticksRunnable);
        LOG.info(
            "Timeouts enabled for executor " + executorData.getComponentId()
                + " : " + executorData.getExecutorInfo() + " for "
                + tickTimeSecs + " secs");
      }
    }
  }

  public static ExecutorShutdown mkExecutorShutdownDameon(WorkerData workerData,
      List<Long> executorId, Map<String, String> initialCredentials)
          throws Exception {
    Executor executor =
        new Executor(workerData, executorId, initialCredentials);
    return executor.execute();
  }
}
