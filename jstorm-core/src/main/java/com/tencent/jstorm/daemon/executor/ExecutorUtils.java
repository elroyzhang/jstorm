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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.exec.util.MapUtils;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.metric.api.IMetricsConsumer.DataPoint;
import org.apache.storm.metric.api.IMetricsConsumer.TaskInfo;
import org.apache.storm.spout.ISpoutWaitStrategy;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.dsl.ProducerType;
import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.ConfigUtils;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.ReflectionUtils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class ExecutorUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExecutorUtils.class);

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "org.apache.storm.daemon.executor#init-spout-wait-strategy")
  public static ISpoutWaitStrategy initSpoutWaitStrategy(Map stormConf)
      throws ClassNotFoundException {
    // default : backtype.storm.spout.SleepSpoutWaitStrategy
    ISpoutWaitStrategy spoutWaitStrategy = ReflectionUtils.newInstance(
        Utils.getString(stormConf.get(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY),
            "backtype.storm.spout.SleepSpoutWaitStrategy"));
    spoutWaitStrategy.prepare(stormConf);
    return spoutWaitStrategy;
  }

  /**
   * Send sampled data to the eventlogger if the global or component level debug
   * flag is set (via nimbus api).
   *
   * @param executorData
   * @param taskData
   * @param values
   * @param componentId
   * @param messageId
   * @param rand
   * @throws InsufficientCapacityException
   */
  @ClojureClass(className = "org.apache.storm.daemon.executor#send-to-eventlogger")
  public static void sendToEventlogger(ExecutorData executorData,
      TaskData taskData, List<Object> values, String componentId,
      Object messageId, Random random) throws InsufficientCapacityException {
    Map<String, DebugOptions> componentToDebug =
        executorData.getStormComponentToDebugAtom().get();
    // component level debug
    DebugOptions options = componentToDebug.get(componentId);
    if (options == null) {
      // global level debug
      options = componentToDebug.get(executorData.getStormId());
    }
    double spct;
    if ((options != null) && options.is_enable()) {
      spct = options.get_samplingpct();
    } else {
      spct = 0d;
    }
    // the thread's initialized random number generator is used to generate
    // uniformily distributed random numbers.
    if ((spct > 0) && (100 * random.nextDouble() < spct)) {
      List<Object> tuple = Lists.newArrayList(componentId, messageId,
          System.currentTimeMillis(), values);
      TaskUtils.sendUnanchored(taskData, StormCommon.EVENTLOGGER_STREAM_ID,
          tuple);
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "org.apache.storm.daemon.executor#executor-max-spout-pending")
  public static Integer executorMaxSpoutPending(Map stormConf, int numTasks) {
    Integer p =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING), null);
    if (p != null) {
      return p * numTasks;
    }
    return null;
  }

  @SuppressWarnings("rawtypes")
  public static DisruptorQueue batchTransferToworker(Map stormConf,
      List<Long> executorInfo) {

    int bufferSize = Utils
        .getInt(stormConf.get(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE), 1024);
    int waitTimeOutMillis = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS), 1000);
    int batchSize =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE), 100);
    int batchTimeoutMillis = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS), 1);

    DisruptorQueue disruptorQueue = new DisruptorQueue(
        "executor-" + executorInfo.get(0) + "-send-queue", ProducerType.SINGLE,
        bufferSize, waitTimeOutMillis, batchSize, batchTimeoutMillis);

    return disruptorQueue;
  }

  @ClojureClass(className = "org.apache.storm.daemon.executor#executor-type")
  public static ComponentType componentType(WorkerTopologyContext context,
      String componentId) {
    StormTopology topology = context.getRawTopology();
    Map<String, Bolt> bolts = topology.get_bolts();
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    if (bolts.containsKey(componentId)) {
      return ComponentType.BOLT;
    }
    if (spouts.containsKey(componentId)) {
      return ComponentType.SPOUT;
    } else {
      throw new RuntimeException(
          "Could not find " + componentId + " in topology " + topology);
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.executor#mk-executor-stats")
  public static CommonStats mkExecutorStats(ComponentType componentType,
      Integer samplerate) {
    if (componentType.equals(ComponentType.SPOUT)) {
      return new SpoutExecutorStats(samplerate);

    } else if (componentType.equals(ComponentType.BOLT)) {
      return new BoltExecutorStats(samplerate);
    }
    return null;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "org.apache.storm.daemon.executor#normalized-component-conf")
  public static Map normalizedComponentConf(Map stormConf,
      WorkerTopologyContext generalContext, String componentId) {
    List<Object> to_remove = ConfigUtils.All_CONFIGS();
    to_remove.remove(Config.TOPOLOGY_DEBUG);
    to_remove.remove(Config.TOPOLOGY_MAX_SPOUT_PENDING);
    to_remove.remove(Config.TOPOLOGY_MAX_TASK_PARALLELISM);
    to_remove.remove(Config.TOPOLOGY_TRANSACTIONAL_ID);
    to_remove.remove(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
    to_remove.remove(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS);
    to_remove.remove(Config.TOPOLOGY_SPOUT_WAIT_STRATEGY);
    to_remove.remove(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT);
    to_remove.remove(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS);
    to_remove.remove(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT);
    to_remove.remove(Config.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS);
    to_remove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_FIELD_NAME);
    to_remove.remove(Config.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM);
    to_remove.remove(Config.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS);
    to_remove.remove(Config.TOPOLOGY_BOLTS_MESSAGE_ID_FIELD_NAME);
    to_remove.remove(Config.TOPOLOGY_STATE_PROVIDER);
    to_remove.remove(Config.TOPOLOGY_STATE_PROVIDER_CONFIG);

    Map specConf = new HashMap();
    String jsonConf =
        generalContext.getComponentCommon(componentId).get_json_conf();
    if (jsonConf != null) {
      specConf = (Map) CoreUtil.from_json(jsonConf);
    }

    for (Object p : to_remove) {
      specConf.remove(p);
    }

    return MapUtils.merge(stormConf, specConf);
  }

  @ClojureClass(className = "org.apache.storm.daemon.executor#metrics-tick")
  public static void metricsTick(ExecutorData executorData, TaskData taskData,
      TupleImpl tuple) {
    Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricRegistry =
        executorData.getIntervalToTaskToMetricRegistry();
    WorkerTopologyContext workerContext = executorData.getWorkerContext();
    int interval = tuple.getInteger(0);
    int taskId = taskData.getTaskId();
    Map<String, IMetric> nameToImetrics =
        intervalToTaskToMetricRegistry.get(interval).get(taskId);
    long now = Time.currentTimeSecs();
    TaskInfo taskInfo = null;
    String localhost;
    try {
      localhost = Utils.getString(
          executorData.getConf().get(Config.STORM_LOCAL_HOSTNAME),
          Utils.localHostname());
    } catch (UnknownHostException e1) {
      localhost = "localhost";
    }
    taskInfo = new IMetricsConsumer.TaskInfo(localhost,
        workerContext.getThisWorkerPort(), executorData.getComponentId(),
        taskId, now, interval);

    List<DataPoint> dataPoints = new ArrayList<DataPoint>();
    for (Map.Entry<String, IMetric> nameTometric : nameToImetrics.entrySet()) {
      String name = nameTometric.getKey();
      IMetric imetirc = nameTometric.getValue();
      Object value = imetirc.getValueAndReset();
      if (value != null) {
        DataPoint dataPoint = new IMetricsConsumer.DataPoint(name, value);
        dataPoints.add(dataPoint);
      }
    }

    try {
      if (!dataPoints.isEmpty()) {
        TaskUtils.sendUnanchored(taskData, Constants.METRICS_STREAM_ID,
            Lists.newArrayList((Object) taskInfo, (Object) dataPoints));
      }
    } catch (InsufficientCapacityException e) {
      if (taskData.getExecutorData().getDebug()) {
        LOG.info(
            "Metrics tick TASK: " + taskId + " TUPLE: "
                + tuple.getValues().toString() + "failed {}!",
            CoreUtil.stringifyError(e));
      }
      e.printStackTrace();
    }
  }
}
