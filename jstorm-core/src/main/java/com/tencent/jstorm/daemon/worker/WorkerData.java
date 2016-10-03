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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.Level;
import org.apache.storm.Config;
import org.apache.storm.StormTimer;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.generated.LogLevel;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.TopologyStatus;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.TransportFactory;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lmax.disruptor.dsl.ProducerType;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorShutdown;
import com.tencent.jstorm.daemon.worker.transfer.TransferFn;
import com.tencent.jstorm.utils.CoreUtil;

@ClojureClass(className = "backtype.storm.daemon.worker#worker-data")
public class WorkerData implements Serializable {
  private static final long serialVersionUID = 1L;
  private static Logger LOG = LoggerFactory.getLogger(WorkerData.class);
  private ConcurrentHashMap<String, LogLevel> latestLogConfig;
  private ConcurrentHashMap<String, Level> originalLogLevels;
  @SuppressWarnings("rawtypes")
  private Map conf;
  private IConnection receiver;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private IContext mqContext;
  private final String stormId;
  private final String assignmentId;
  private final String hostName;
  private final Integer port;
  private final String workerId;
  private AtomicBoolean stormActiveAtom;
  private AtomicReference<Map<String, DebugOptions>> stormComponentToDebugAtom;
  private AtomicBoolean workerActiveFlag;
  private AtomicReference<TopologyStatus> topologyStatus;
  private IStateStorage stateStore;
  private IStormClusterState stormClusterState;
  private SortedSet<Integer> taskids;
  private volatile ConcurrentHashMap<String, IConnection> cachedNodeportToSocket;
  private volatile ConcurrentHashMap<Integer, String> cachedTaskToNodeport;
  private List<List<Long>> executors;
  private ConcurrentHashMap<Integer, DisruptorQueue> innerTaskTransfer;
  private Map<Integer, String> tasksToComponent;
  private Map<String, List<Integer>> componentToSortedTasks;
  private Map<String, Object> defaultSharedResources;
  private Map<String, Object> userSharedResources;
  private StormTopology topology;
  private StormTopology systemTopology;
  private DefaultKillFn suicideFn;
  private Utils.UptimeComputer uptime;
  private DisruptorQueue transferQueue;
  private List<ExecutorShutdown> shutdownExecutors;
  private int processId;
  private Map<String, Map<String, Fields>> componentToStreamToFields;
  private Map<List<Long>, DisruptorQueue> executorReceiveQueueMap;
  private Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap;
  private Map<Integer, DisruptorQueue> receiveQueueMap;
  private ReentrantReadWriteLock endpointSocketLock;
  private Map<Integer, Integer> taskToShortExecutor;
  private TransferFn transferFn;
  private LoadMapping loadMapping;
  private AtomicBoolean backpressure;
  private AtomicBoolean transferBackpressure;
  private AtomicBoolean backpressureTrigger;
  private Integer receiverThreadCount;
  @SuppressWarnings("rawtypes")
  private Map<String, Map> assignmentVersions;
  private AtomicBoolean throttleOn;
  private StormTimer heartbeatTimer;
  private StormTimer refreshLoadTimer;
  private StormTimer refreshConnectionsTimer;
  private StormTimer refreshCredentialsTimer;
  private StormTimer resetLogLevelsTimer;
  private StormTimer refreshActiveTimer;
  private StormTimer executorHeartbeatTimer;
  private StormTimer userTimer;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public WorkerData(Map conf, IContext mqContext, String stormId,
      String assignmentId, int port, String workerId, int processId,
      Map stormConf, IStateStorage clusterState,
      IStormClusterState stormClusterState) throws Exception {
    this.latestLogConfig = new ConcurrentHashMap<String, LogLevel>();
    this.originalLogLevels =
        new ConcurrentHashMap<String, Level>(WorkerUtils.getLoggerLevels());
    LOG.info("Started with log levels: {}", this.originalLogLevels);

    this.conf = conf;
    this.stormConf = stormConf;
    this.mqContext = mkMqContext(mqContext);
    this.receiver = this.mqContext.bind(stormId, port);
    this.stormId = stormId;
    this.assignmentId = assignmentId;
    this.hostName = System.getProperty("storm.local.hostname");
    this.port = port;
    this.workerId = workerId;
    this.processId = processId;
    this.stateStore = clusterState;
    this.stormClusterState = stormClusterState;
    this.workerActiveFlag = new AtomicBoolean(false);
    this.stormActiveAtom = new AtomicBoolean(false);
    this.stormComponentToDebugAtom =
        new AtomicReference<Map<String, DebugOptions>>(
            new HashMap<String, DebugOptions>());
    this.topologyStatus = new AtomicReference<>(TopologyStatus.ACTIVE);
    this.innerTaskTransfer = new ConcurrentHashMap<Integer, DisruptorQueue>();
    this.executors = WorkerUtils.readWorkerExecutors(stormConf,
        stormClusterState, stormId, assignmentId, port);

    this.topology = ConfigUtils.readSupervisorTopology(conf, stormId);
    this.systemTopology = StormCommon.systemTopology(stormConf, topology);
    // for optimized access when used in tasks later on
    this.tasksToComponent = StormCommon.stormTaskInfo(topology, stormConf);
    this.componentToStreamToFields =
        WorkerUtils.componentToStreamToFields(systemTopology);
    this.componentToSortedTasks = mkComponentToSortedTasks(tasksToComponent);
    this.endpointSocketLock = new ReentrantReadWriteLock();
    this.cachedNodeportToSocket = new ConcurrentHashMap<String, IConnection>();
    this.cachedTaskToNodeport = new ConcurrentHashMap<Integer, String>();
    this.transferQueue = mkTransferQueue();
    this.executorReceiveQueueMap =
        WorkerUtils.mkReceiveQueueMap(stormConf, executors);
    this.shortExecutorReceiveQueueMap =
        WorkerUtils.mkShortExecutorReceiveQueueMap(executorReceiveQueueMap);
    this.taskToShortExecutor = mkTaskToShortExecutor(executors);
    this.receiveQueueMap = mkTaskReceiveQueueMap(executorReceiveQueueMap);
    this.taskids = new TreeSet(receiveQueueMap.keySet());
    this.suicideFn = mkSuicideFn();
    this.uptime = Utils.makeUptimeComputer();
    this.defaultSharedResources = WorkerUtils.mkDefaultResources(conf);
    this.userSharedResources = WorkerUtils.mkUserResources(conf);
    this.receiverThreadCount = 1;
    // TODO
    // Utils.getInt(stormConf.get(Config.WORKER_RECEIVER_THREAD_COUNT), 1);
    this.transferFn = new TransferFn(this);
    this.loadMapping = new LoadMapping();
    this.assignmentVersions = new HashMap<String, Map>();
    this.backpressure = new AtomicBoolean(false);
    this.transferBackpressure = new AtomicBoolean(false);
    this.backpressureTrigger = new AtomicBoolean(false);
    this.throttleOn = new AtomicBoolean(false);
    this.heartbeatTimer = CoreUtil.mkHaltingTimer("heartbeat-timer");
    this.refreshLoadTimer = CoreUtil.mkHaltingTimer("refresh-load-timer");
    this.refreshConnectionsTimer =
        CoreUtil.mkHaltingTimer("refresh-connections-timer");
    this.refreshCredentialsTimer =
        CoreUtil.mkHaltingTimer("refresh-credentials-timer");
    this.resetLogLevelsTimer =
        CoreUtil.mkHaltingTimer("reset-log-levels-timer");
    this.refreshActiveTimer = CoreUtil.mkHaltingTimer("refresh-active-timer");
    this.executorHeartbeatTimer =
        CoreUtil.mkHaltingTimer("executor-heartbeat-timer");
    this.userTimer = CoreUtil.mkHaltingTimer("user-timer");
    LOG.info("Successfully create WorkerData");
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#mk-suicide-fn")
  private DefaultKillFn mkSuicideFn() {
    return new DefaultKillFn(1, "Worker died");
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#transfer-queue")
  private DisruptorQueue mkTransferQueue() {
    int bufferSize =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE), 1024);
    int waitTimeOutMillis = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_DISRUPTOR_WAIT_TIMEOUT_MILLIS), 1000);
    int batchSize =
        Utils.getInt(stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_SIZE), 100);
    int batchTimeoutMillis = Utils.getInt(
        stormConf.get(Config.TOPOLOGY_DISRUPTOR_BATCH_TIMEOUT_MILLIS), 1);
    return new DisruptorQueue("worker-transfer-queue", ProducerType.MULTI,
        bufferSize, waitTimeOutMillis, batchSize, batchTimeoutMillis);
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#task->short-executor")
  private Map<Integer, Integer> mkTaskToShortExecutor(
      List<List<Long>> executors) {
    Map<Integer, Integer> results = new HashMap<Integer, Integer>();
    for (List<Long> e : executors) {
      for (Integer t : StormCommon.executorIdToTasks(e)) {
        results.put(t, e.get(0).intValue());
      }
    }
    return results;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#component->sorted-tasks")
  private Map<String, List<Integer>> mkComponentToSortedTasks(
      Map<Integer, String> tasksToComponent) {
    Map<String, List<Integer>> componentToSortedTasks =
        Utils.reverseMap(tasksToComponent);
    for (Map.Entry<String, List<Integer>> entry : componentToSortedTasks
        .entrySet()) {
      List<Integer> tasks = entry.getValue();
      Collections.sort(tasks);
    }
    return componentToSortedTasks;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#receive-queue-map")
  private Map<Integer, DisruptorQueue> mkTaskReceiveQueueMap(
      Map<List<Long>, DisruptorQueue> executorReceiveQueueMap) {
    Map<Integer, DisruptorQueue> result =
        new HashMap<Integer, DisruptorQueue>();
    for (Map.Entry<List<Long>, DisruptorQueue> eToD : executorReceiveQueueMap
        .entrySet()) {
      List<Long> e = eToD.getKey();
      DisruptorQueue queue = eToD.getValue();
      for (Integer t : StormCommon.executorIdToTasks(e)) {
        result.put(t, queue);
      }
    }
    return result;
  }

  @ClojureClass(className = "backtype.storm.daemon.worker#worker-data#:mq-context")
  private IContext mkMqContext(IContext mqContext) {
    if (mqContext == null) {
      mqContext = TransportFactory.makeContext(stormConf);
    }
    return mqContext;
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  public ConcurrentHashMap<String, LogLevel> getLatestLogConfig() {
    return latestLogConfig;
  }

  public ConcurrentHashMap<String, Level> getOriginalLogLevels() {
    return originalLogLevels;
  }

  public AtomicBoolean getStormActiveAtom() {
    return stormActiveAtom;
  }

  public void setStormActiveAtom(AtomicBoolean active) {
    this.stormActiveAtom = active;
  }

  @SuppressWarnings("unchecked")
  public Map<Object, Object> getStormConf() {
    return stormConf;
  }

  public IContext getContext() {
    return mqContext;
  }

  public String getTopologyId() {
    return stormId;
  }

  public String getAssignmentId() {
    return assignmentId;
  }

  public Integer getPort() {
    return port;
  }

  public String getWorkerId() {
    return workerId;
  }

  public IStateStorage getZkClusterstate() {
    return stateStore;
  }

  public IStormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public Set<Integer> getTaskids() {
    return taskids;
  }

  public ConcurrentHashMap<String, IConnection> getCachedNodeportToSocket() {
    return cachedNodeportToSocket;
  }

  public void setCachedNodeportToSocket(
      ConcurrentHashMap<String, IConnection> cachedNodeportToSocket) {
    this.cachedNodeportToSocket = cachedNodeportToSocket;
  }

  public ConcurrentHashMap<Integer, DisruptorQueue> getInnerTaskTransfer() {
    return innerTaskTransfer;
  }

  public Map<Integer, String> getTasksToComponent() {
    return tasksToComponent;
  }

  public StormTopology getTopology() {
    return topology;
  }

  public StormTopology getSystemTopology() {
    return systemTopology;
  }

  public DefaultKillFn getSuicideFn() {
    return suicideFn;
  }

  public DisruptorQueue getTransferQueue() {
    return transferQueue;
  }

  public Map<String, List<Integer>> getComponentToSortedTasks() {
    return componentToSortedTasks;
  }

  public Map<String, Object> getDefaultSharedResources() {
    return defaultSharedResources;
  }

  public Map<String, Object> getUserSharedResources() {
    return userSharedResources;
  }

  public AtomicBoolean getBackpressure() {
    return backpressure;
  }

  public void setBackpressure(AtomicBoolean backpressure) {
    this.backpressure = backpressure;
  }

  public AtomicBoolean getTransferBackpressure() {
    return transferBackpressure;
  }

  public void setTransferBackpressure(AtomicBoolean transferBackpressure) {
    this.transferBackpressure = transferBackpressure;
  }

  public AtomicBoolean getBackpressureTrigger() {
    return backpressureTrigger;
  }

  public void setBackpressureTrigger(AtomicBoolean backpressureTrigger) {
    this.backpressureTrigger = backpressureTrigger;
  }

  public List<ExecutorShutdown> getShutdownExecutors() {
    return shutdownExecutors;
  }

  public void setShutdownExecutors(List<ExecutorShutdown> shutdownExecutors) {
    this.shutdownExecutors = shutdownExecutors;
  }

  public List<List<Long>> getExecutors() {
    return executors;
  }

  public void setExecutors(List<List<Long>> executors) {
    this.executors = executors;
  }

  public AtomicBoolean getWorkerActiveFlag() {
    return workerActiveFlag;
  }

  public void setWorkerActiveFlag(AtomicBoolean workerActiveFlag) {
    this.workerActiveFlag = workerActiveFlag;
  }

  public int getProcessId() {
    return processId;
  }

  public void setProcessId(int processId) {
    this.processId = processId;
  }

  public Map<String, Map<String, Fields>> getComponentToStreamToFields() {
    return componentToStreamToFields;
  }

  public void setComponentToStreamToFields(
      Map<String, Map<String, Fields>> componentToStreamToFields) {
    this.componentToStreamToFields = componentToStreamToFields;
  }

  public Map<List<Long>, DisruptorQueue> getExecutorReceiveQueueMap() {
    return executorReceiveQueueMap;
  }

  public void setExecutorReceiveQueueMap(
      Map<List<Long>, DisruptorQueue> executorReceiveQueueMap) {
    this.executorReceiveQueueMap = executorReceiveQueueMap;
  }

  public ConcurrentHashMap<Integer, String> getCachedTaskToNodeport() {
    return cachedTaskToNodeport;
  }

  public void setCachedTaskToNodeport(
      ConcurrentHashMap<Integer, String> cachedTaskToNodeport) {
    this.cachedTaskToNodeport = cachedTaskToNodeport;
  }

  public Map<Integer, Integer> getTaskToShortExecutor() {
    return taskToShortExecutor;
  }

  public void setTaskToShortExecutor(
      Map<Integer, Integer> taskToShortExecutor) {
    this.taskToShortExecutor = taskToShortExecutor;
  }

  public Map<Integer, DisruptorQueue> getShortExecutorReceiveQueueMap() {
    return shortExecutorReceiveQueueMap;
  }

  public void setShortExecutorReceiveQueueMap(
      Map<Integer, DisruptorQueue> shortExecutorReceiveQueueMap) {
    this.shortExecutorReceiveQueueMap = shortExecutorReceiveQueueMap;
  }

  public TransferFn getTransferFn() {
    return transferFn;
  }

  public void setTransferFn(TransferFn transferFn) {
    this.transferFn = transferFn;
  }

  public Utils.UptimeComputer getUptime() {
    return uptime;
  }

  public void setUptime(Utils.UptimeComputer uptime) {
    this.uptime = uptime;
  }

  public Integer getReceiverThreadCount() {
    return receiverThreadCount;
  }

  public void setReceiverThreadCount(Integer receiverThreadCount) {
    this.receiverThreadCount = receiverThreadCount;
  }

  public ReentrantReadWriteLock getEndpointSocketLock() {
    return endpointSocketLock;
  }

  public void setEndpointSocketLock(ReentrantReadWriteLock endpointSocketLock) {
    this.endpointSocketLock = endpointSocketLock;
  }

  @SuppressWarnings("rawtypes")
  public Map<String, Map> getAssignmentVersions() {
    return assignmentVersions;
  }

  @SuppressWarnings("rawtypes")
  public void setAssignmentVersions(Map<String, Map> assignmentVersions) {
    this.assignmentVersions = assignmentVersions;
  }

  public String getHostName() {
    return hostName;
  }

  public IConnection getReceiver() {
    return receiver;
  }

  public void setReceiver(IConnection receiver) {
    this.receiver = receiver;
  }

  public LoadMapping getLoadMapping() {
    return loadMapping;
  }

  public void setLoadMapping(LoadMapping loadMapping) {
    this.loadMapping = loadMapping;
  }

  public AtomicBoolean getThrottleOn() {
    return throttleOn;
  }

  public void setThrottleOn(AtomicBoolean throttleOn) {
    this.throttleOn = throttleOn;
  }

  public AtomicReference<Map<String, DebugOptions>> getStormComponentToDebugAtom() {
    return stormComponentToDebugAtom;
  }

  public void setStormComponentToDebugAtom(
      AtomicReference<Map<String, DebugOptions>> stormComponentToDebugAtom) {
    this.stormComponentToDebugAtom = stormComponentToDebugAtom;
  }

  public AtomicReference<TopologyStatus> getTopologyStatus() {
    return topologyStatus;
  }

  public void setTopologyStatus(
      AtomicReference<TopologyStatus> topologyStatus) {
    this.topologyStatus = topologyStatus;
  }

  public StormTimer getRefreshActiveTimer() {
    return refreshActiveTimer;
  }

  public void setRefreshActiveTimer(StormTimer refreshActiveTimer) {
    this.refreshActiveTimer = refreshActiveTimer;
  }

  public StormTimer getHeartbeatTimer() {
    return heartbeatTimer;
  }

  public StormTimer getRefreshLoadTimer() {
    return refreshLoadTimer;
  }

  public StormTimer getRefreshConnectionsTimer() {
    return refreshConnectionsTimer;
  }

  public StormTimer getRefreshCredentialsTimer() {
    return refreshCredentialsTimer;
  }

  public StormTimer getResetLogLevelsTimer() {
    return resetLogLevelsTimer;
  }

  public StormTimer getExecutorHeartbeatTimer() {
    return executorHeartbeatTimer;
  }

  public StormTimer getUserTimer() {
    return userTimer;
  }

}
