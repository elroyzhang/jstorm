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
package com.tencent.jstorm.daemon.worker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Assignment;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.LogConfig;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.LogLevelAction;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.messaging.ConnectionWithStatus;
import org.apache.storm.messaging.ConnectionWithStatus.Status;
import org.apache.storm.messaging.DeserializingConnectionCallback;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.ThriftTopologyUtils;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.lmax.disruptor.dsl.ProducerType;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.common.Common;
import com.tencent.jstorm.daemon.worker.transfer.TransferLocalFn;

public class WorkerUtils {
  private static Logger LOG = LoggerFactory.getLogger(WorkerUtils.class);

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.worker#read-worker-executors")
  public static List<List<Long>> readWorkerExecutors(Map stormConf,
      IStormClusterState stormClusterState, String stormId, String assignmentId,
      int port) throws Exception {
    LOG.info("Reading Assignments.");
    Map<Integer, Integer> virtualToRealPort =
        (Map<Integer, Integer>) stormConf.get("storm.virtual.real.ports");
    if (virtualToRealPort != null && virtualToRealPort.containsKey(port)) {
      port = virtualToRealPort.get(port);
    }
    Assignment assignment = stormClusterState.assignmentInfo(stormId, null);
    if (assignment == null) {
      String errMsg = "Failed to get Assignment of " + stormId;
      LOG.error(errMsg);
      throw new RuntimeException(errMsg);
    }
    Map<List<Long>, NodeInfo> executorToNodePort =
        assignment.get_executor_node_port();
    List<List<Long>> executors = new ArrayList<List<Long>>();

    executors.add(Constants.SYSTEM_EXECUTOR_ID);

    for (Map.Entry<List<Long>, NodeInfo> enp : executorToNodePort.entrySet()) {
      List<Long> executor = enp.getKey();
      NodeInfo nodeport = enp.getValue();
      if (nodeport.get_node().equals(assignmentId)
          && nodeport.get_port_iterator().next() == port) {
        executors.add(executor);
      }
    }
    if (executors.size() == 0) {
      LOG.warn("No Executors running current on port:{} and assignment:{}",
          port, assignment);
    }
    return executors;

  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-outbound-tasks")
  public static Set<Integer> workerOutboundTasks(WorkerData workerData)
      throws Exception {
    // Returns seq of task-ids that receive messages from this worker

    WorkerTopologyContext context = Common.workerContext(workerData);
    Set<Integer> rtn = new HashSet<Integer>();

    Set<Integer> taskIds = workerData.getTaskids();
    Set<String> components = new HashSet<String>();
    for (Integer taskId : taskIds) {
      String componentId = context.getComponentId(taskId);
      // streamId to componentId to the Grouping used
      Map<String, Map<String, Grouping>> targets =
          context.getTargets(componentId);
      // componentId to the Grouping used
      for (Map<String, Grouping> e : targets.values()) {
        components.addAll(e.keySet());
      }
    }

    Map<String, List<Integer>> reverseMap =
        Utils.reverseMap(workerData.getTasksToComponent());
    if (reverseMap != null) {
      for (String k : components) {
        if (reverseMap.containsKey(k)) {
          rtn.addAll(reverseMap.get(k));
        }
      }
    }
    return rtn;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-receive-queue-map")
  public static Map<List<Long>, DisruptorQueue> mkReceiveQueueMap(Map stormConf,
      List<List<Long>> executors) {
    Map<List<Long>, DisruptorQueue> result =
        new HashMap<List<Long>, DisruptorQueue>();
    int bufferSize = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE), 1024);

    int waitTimeOutMillis = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS), 1000);
    int batchSize =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE), 100);
    int batchTimeoutMillis = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS), 1);

    for (List<Long> e : executors) {
      DisruptorQueue queue =
          new DisruptorQueue("receive-queue-" + e.get(0), ProducerType.MULTI,
              bufferSize, waitTimeOutMillis, batchSize, batchTimeoutMillis);
      result.put(e, queue);
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#stream->fields")
  public static Map<String, Fields> streamToFields(StormTopology topology,
      String component) {

    Map<String, Fields> streamToFieldsMap = new HashMap<String, Fields>();

    Map<String, StreamInfo> streamInfoMap = ThriftTopologyUtils
        .getComponentCommon(topology, component).get_streams();
    for (Entry<String, StreamInfo> entry : streamInfoMap.entrySet()) {
      String s = entry.getKey();
      StreamInfo info = entry.getValue();
      streamToFieldsMap.put(s, new Fields(info.get_output_fields()));
    }

    return streamToFieldsMap;

  }

  @ClojureClass(className = "backtype.storm.daemon.worker#component->stream->fields")
  public static Map<String, Map<String, Fields>> componentToStreamToFields(
      StormTopology topology) {
    HashMap<String, Map<String, Fields>> componentToStreamToFields =
        new HashMap<String, Map<String, Fields>>();
    Set<String> components = ThriftTopologyUtils.getComponentIds(topology);
    for (String component : components) {
      componentToStreamToFields.put(component,
          streamToFields(topology, component));
    }
    return componentToStreamToFields;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-default-resources")
  public static Map<String, Object> mkDefaultResources(Map conf) {
    Map<String, Object> result = new HashMap<String, Object>();
    int threadPoolSize = Utils
        .getInt(conf.get(Config.TOPOLOGY_WORKER_SHARED_THREAD_POOL_SIZE), 4);
    result.put(WorkerTopologyContext.SHARED_EXECUTOR,
        Executors.newFixedThreadPool(threadPoolSize));
    return result;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-user-resources")
  public static Map<String, Object> mkUserResources(Map conf) {
    // TODO need to invoke a hook provided by the topology,
    // giving it a chance to create user resources.
    // this would be part of the initialization hook need to separate
    // workertopologycontext into WorkerContext and WorkerUserContext.
    // actually just do it via interfaces. just need to make sure to hide
    // setResource from tasks
    return new HashMap<String, Object>();
  }

  /**
   * Check whether this messaging connection is ready to send data
   * 
   * @param connection
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#is-connection-ready")
  public static boolean isConnectionReady(IConnection connection) {
    if (connection instanceof ConnectionWithStatus) {
      Status status = ((ConnectionWithStatus) connection).status();
      return status.equals(ConnectionWithStatus.Status.Ready);
    }
    return true;
  }

  /**
   * all connections are ready
   * 
   * @param worker WorkerData
   * @return
   */
  @ClojureClass(className = "backtype.storm.daemon.worker#all-connections-ready")
  public static boolean allConnectionsReady(WorkerData worker) {
    Collection<IConnection> connections =
        worker.getCachedNodeportToSocket().values();
    for (IConnection connection : connections) {
      if (!isConnectionReady(connection)) {
        return false;
      }
    }
    return true;
  }

  public static Map<Integer, DisruptorQueue> mkShortExecutorReceiveQueueMap(
      Map<List<Long>, DisruptorQueue> executorReceiveQueueMap) {
    Map<Integer, DisruptorQueue> result =
        new HashMap<Integer, DisruptorQueue>();
    for (Map.Entry<List<Long>, DisruptorQueue> ed : executorReceiveQueueMap
        .entrySet()) {
      List<Long> e = ed.getKey();
      DisruptorQueue dq = ed.getValue();
      result.put(e.get(0).intValue(), dq);
    }
    return result;
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#register-callbacks")
  public static void registerCallBacks(WorkerData workerData) throws Exception {
    LOG.info("Registering IConnectionCallbacks for "
        + workerData.getAssignmentId() + ":" + workerData.getPort());
    DeserializingConnectionCallback dc =
        new DeserializingConnectionCallback(workerData.getStormConf(),
            Common.workerContext(workerData), new TransferLocalFn(workerData));
    workerData.getReceiver().registerRecv(dc);
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#endpoint->string")
  public static String endpointToString(NodeInfo nodeInfo) throws Exception {
    return nodeInfo.get_node() + "/" + nodeInfo.get_port_iterator().next();
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#string->endpoint")
  public static NodeInfo stringToEndpoint(String nodeInfo) throws Exception {
    String[] s = nodeInfo.split("/");
    return new NodeInfo(s[0], Sets.newHashSet(Long.valueOf(s[1])));
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#get-logger-levels")
  public static Map<String, Level> getLoggerLevels() {
    Map<String, Level> ret = new HashMap<String, Level>();
    Configuration logConfiguration =
        ((LoggerContext) LogManager.getContext(false)).getConfiguration();
    Map<String, LoggerConfig> loggers = logConfiguration.getLoggers();
    for (Entry<String, LoggerConfig> e : loggers.entrySet()) {
      ret.put(e.getValue().toString(), e.getValue().getLevel());
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#set-logger-level")
  public static void setLoggerLevel(LoggerContext loggerContext,
      String loggerName, Level newLevel) {
    Configuration config = loggerContext.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(loggerName);
    if (!loggerConfig.toString().equalsIgnoreCase(loggerName)) {
      // create a new config. Make it additive (true) s.t. inherit
      // parents appenders
      LoggerConfig newLoggerConfig =
          new LoggerConfig(loggerName, newLevel, true);
      LOG.info("Adding config for: {}  with level: {}", newLoggerConfig,
          newLevel);
      config.addLogger(loggerName, newLoggerConfig);
    } else {
      LOG.info("Setting {}  log level to: {}", loggerConfig, newLevel);
      loggerConfig.setLevel(newLevel);
    }
  }

  /**
   * function called on timer to reset log levels last set to DEBUG also called
   * from process-log-config-change
   * 
   * @param loggerContext
   * @param loggerName
   * @param newLevel
   */
  @ClojureClass(className = "org.apache.storm.daemon.worker#reset-log-levels")
  public static void resetLogLevels(
      ConcurrentHashMap<String, LogLevel> latestLogConfigAtom) {
    LoggerContext loggerContext =
        ((LoggerContext) LogManager.getContext(false));
    for (Entry<String, LogLevel> e : latestLogConfigAtom.entrySet()) {
      String loggerName = e.getKey();
      LogLevel loggerSetting = e.getValue();
      long timeoutEpoch = loggerSetting.get_reset_log_level_timeout_epoch();
      String resetLogLevel = loggerSetting.get_reset_log_level();
      if (Time.currentTimeSecs() > timeoutEpoch) {
        LOG.info("{} : Resetting level to {}", loggerName, resetLogLevel);
        setLoggerLevel(loggerContext, loggerName, Level.toLevel(resetLogLevel));
        latestLogConfigAtom.remove(loggerName);
      }
    }
    loggerContext.updateLoggers();
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#new-log-configs")
  public static ConcurrentHashMap<String, LogLevel> newLogConfigs(
      Map<String, LogLevel> loggers,
      ConcurrentHashMap<String, Level> originalLogLevels) {
    ConcurrentHashMap<String, LogLevel> ret =
        new ConcurrentHashMap<String, LogLevel>();
    // merge named log levels
    for (Entry<String, LogLevel> e : loggers.entrySet()) {
      String msgLoggerName = e.getKey();
      LogLevel loggerLevel = e.getValue();
      String loggerName = "ROOT".equals(msgLoggerName)
          ? LogManager.ROOT_LOGGER_NAME : msgLoggerName;
      // the new-timeouts map now contains logger => timeout
      if (loggerLevel.is_set_reset_log_level_timeout_epoch()) {
        LogLevel newLogLevel = new LogLevel();
        newLogLevel.set_action(loggerLevel.get_action());
        newLogLevel.set_target_log_level(loggerLevel.get_target_log_level());
        Level level = originalLogLevels.get(loggerName);
        level = level != null ? level : Level.INFO;
        newLogLevel.set_reset_log_level(level.name());
        newLogLevel.set_reset_log_level_timeout_epoch(
            loggerLevel.get_reset_log_level_timeout_epoch());
        ret.put(loggerName, newLogLevel);
      }
    }
    return ret;
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#process-log-config-change")
  public static void processLogConfigChange(
      ConcurrentHashMap<String, LogLevel> latestLogConfig,
      ConcurrentHashMap<String, Level> originalLogLevels, LogConfig logConfig) {
    if (logConfig != null) {
      LOG.info("Processing received log config: {}", logConfig);
      // merge log configs together
      Map<String, LogLevel> loggers = logConfig.get_named_logger_level();
      LoggerContext loggerContext =
          ((LoggerContext) LogManager.getContext(false));

      ConcurrentHashMap<String, LogLevel> newLogConfigs =
          newLogConfigs(loggers, originalLogLevels);

      // look for deleted log timeouts
      for (Entry<String, LogLevel> e : latestLogConfig.entrySet()) {
        if (!newLogConfigs.containsKey(e.getKey())) {
          // if we had a timeout, but the timeout is no longer active
          setLoggerLevel(loggerContext, e.getKey(),
              Level.toLevel(e.getValue().get_reset_log_level()));
        }
      }

      // apply new log settings we just received
      // the merged configs are only for the reset logic
      Map<String, LogLevel> namedLoggerLevel =
          logConfig.get_named_logger_level();
      for (Entry<String, LogLevel> e : namedLoggerLevel.entrySet()) {
        String msgLoggerName = e.getKey();
        String loggerName = "ROOT".equals(msgLoggerName)
            ? LogManager.ROOT_LOGGER_NAME : msgLoggerName;
        Level level = Level.toLevel(e.getValue().get_target_log_level());
        LogLevelAction action = e.getValue().get_action();
        if (action.equals(LogLevelAction.UPDATE)) {
          setLoggerLevel(loggerContext, loggerName, level);
        }
      }

      loggerContext.updateLoggers();
      latestLogConfig.clear();
      latestLogConfig.putAll(newLogConfigs);
      LOG.info("New merged log config is {}", latestLogConfig);
    }
  }
}
