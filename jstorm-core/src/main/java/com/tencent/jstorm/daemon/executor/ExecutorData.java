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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.daemon.metrics.SpoutThrottlingMetrics;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.DebugOptions;
import org.apache.storm.messaging.IContext;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.serialization.KryoTupleDeserializer;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.daemon.common.Common;
import com.tencent.jstorm.daemon.executor.error.ITaskReportErr;
import com.tencent.jstorm.daemon.executor.error.ReportErrorAndDie;
import com.tencent.jstorm.daemon.executor.error.ThrottledReportErrorFn;
import com.tencent.jstorm.daemon.executor.grouping.GroupingUtils;
import com.tencent.jstorm.daemon.executor.grouping.MkGrouper;
import com.tencent.jstorm.daemon.task.Task;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.worker.WorkerData;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy zionwang
 * @ModifiedTime 11:23:24 AM Feb 24, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#mk-executor-data")
public class ExecutorData implements Serializable {
  private static final long serialVersionUID = 1L;
  private Integer taskId;
  private WorkerData worker;
  private List<Long> executorInfo;
  private WorkerTopologyContext workerContext;
  private List<Integer> taskIds;
  private String componentId;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private DisruptorQueue receiveQueue;
  private ComponentType componentType;
  private AtomicReference<Map<String, DebugOptions>> stormComponentToDebugAtom;
  private DisruptorQueue batchTransferQueue;
  private clojure.lang.Atom openOrPrepareWasCalled;
  private String stormId;
  private String hostName;
  @SuppressWarnings("rawtypes")
  private Map conf;
  @SuppressWarnings("rawtypes")
  private Map sharedExecutorData;
  private IStormClusterState stormClusterState;
  private Map<Integer, String> taskToComponent;
  private Map<String, Map<String, MkGrouper>> streamToComponentToGrouper;
  private KryoTupleDeserializer deserializer;
  private IContext context;
  private ExecutorTransferFn transferFn;;
  private ITaskReportErr reportError;
  private ReportErrorAndDie reportErrorAndDie;
  private CommonStats stats;
  private Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricRegistry;
  private Map<Integer, TaskData> taskDatas;
  private ScheduledExecutorService executorScheduler;
  private Counters counters;
  private Callable<Boolean> latenciesSampler;
  private Callable<Boolean> ackSampler;
  private Callable<Boolean> failSampler;
  private AtomicBoolean backpressure;
  private Boolean debug;
  private SpoutThrottlingMetrics spoutThrottlingMetrics;

  @SuppressWarnings("rawtypes")
  public ExecutorData(WorkerData worker, List<Long> executorInfo)
      throws Exception {
    this.taskId = executorInfo.get(0).intValue();
    this.worker = worker;
    this.workerContext = Common.workerContext(worker);
    this.executorInfo = executorInfo;
    this.taskIds = StormCommon.executorIdToTasks(executorInfo);
    this.componentId = workerContext.getComponentId(taskIds.get(0));
    this.openOrPrepareWasCalled = new clojure.lang.Atom(false);
    this.stormConf = ExecutorUtils.normalizedComponentConf(
        worker.getStormConf(), workerContext, componentId);
    this.debug = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_DEBUG), false);
    this.receiveQueue = worker.getExecutorReceiveQueueMap().get(executorInfo);
    this.stormId = worker.getTopologyId();
    this.hostName = worker.getHostName();
    this.conf = worker.getConf();
    this.sharedExecutorData = new HashMap();
    this.stormComponentToDebugAtom = worker.getStormComponentToDebugAtom();
    this.batchTransferQueue =
        ExecutorUtils.batchTransferToworker(stormConf, executorInfo);
    this.transferFn = new ExecutorTransferFn(batchTransferQueue, debug);
    // :suicide-fn (:suicide-fn worker)
    this.stormClusterState = worker.getStormClusterState();
    this.componentType =
        ExecutorUtils.componentType(workerContext, componentId);
    // TODO: should refactor this to be part of the executor specific map (spout
    // or bolt with :common field)
    this.stats = ExecutorUtils.mkExecutorStats(componentType,
        ConfigUtils.samplingRate(stormConf));
    this.intervalToTaskToMetricRegistry =
        new HashMap<Integer, Map<Integer, Map<String, IMetric>>>();
    this.taskToComponent = worker.getTasksToComponent();
    this.streamToComponentToGrouper =
        GroupingUtils.outboundComponents(workerContext, componentId, stormConf);
    this.reportError = new ThrottledReportErrorFn(this);
    // report error and halt worker
    this.reportErrorAndDie =
        new ReportErrorAndDie(reportError, worker.getSuicideFn());
    this.deserializer = new KryoTupleDeserializer(stormConf, workerContext);
    this.latenciesSampler = ConfigUtils.mkStatsSampler(stormConf);
    this.ackSampler = ConfigUtils.mkStatsSampler(stormConf);
    this.failSampler = ConfigUtils.mkStatsSampler(stormConf);

    this.backpressure = new AtomicBoolean(false);
    if (componentType.equals(ComponentType.SPOUT)) {
      setSpoutThrottlingMetrics(new SpoutThrottlingMetrics());
    }
    // TODO: add in the executor-specific stuff in a :specific... or make a
    // spout-data, bolt-data function?
    // ///////////////////////
    this.context = worker.getContext();
    this.taskDatas = mkTaskDatas(taskIds);
    this.executorScheduler = Executors.newScheduledThreadPool(8);
    this.counters = new Counters();

  }

  private Map<Integer, TaskData> mkTaskDatas(List<Integer> taskIds)
      throws Exception {
    Map<Integer, TaskData> taskDatas = new HashMap<Integer, TaskData>();
    for (Integer taskId : taskIds) {
      TaskData taskData = Task.mkTask(this, taskId);
      taskDatas.put(taskId, taskData);
    }
    return taskDatas;
  }

  public WorkerData getWorker() {
    return worker;
  }

  public void setWorker(WorkerData worker) {
    this.worker = worker;
  }

  public List<Long> getExecutorInfo() {
    return executorInfo;
  }

  public void setExecutorInfo(List<Long> executorInfo) {
    this.executorInfo = executorInfo;
  }

  public WorkerTopologyContext getWorkerContext() {
    return workerContext;
  }

  public void setWorkerContext(WorkerTopologyContext workerContext) {
    this.workerContext = workerContext;
  }

  public List<Integer> getTaskIds() {
    return taskIds;
  }

  public void setTaskIds(List<Integer> taskIds) {
    this.taskIds = taskIds;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  @SuppressWarnings("rawtypes")
  public Map getStormConf() {
    return stormConf;
  }

  @SuppressWarnings("rawtypes")
  public void setStormConf(Map stormConf) {
    this.stormConf = stormConf;
  }

  public ComponentType getComponentType() {
    return componentType;
  }

  public AtomicReference<Map<String, DebugOptions>> getStormComponentToDebugAtom() {
    return stormComponentToDebugAtom;
  }

  public void setStormComponentToDebugAtom(
      AtomicReference<Map<String, DebugOptions>> stormComponentToDebugAtom) {
    this.stormComponentToDebugAtom = stormComponentToDebugAtom;
  }

  public DisruptorQueue getBatchTransferQueue() {
    return batchTransferQueue;
  }

  public void setBatchTransferQueue(DisruptorQueue batchTransferQueue) {
    this.batchTransferQueue = batchTransferQueue;
  }

  public AtomicBoolean getBackpressure() {
    return backpressure;
  }

  public void setBackpressure(AtomicBoolean backpressure) {
    this.backpressure = backpressure;
  }

  public clojure.lang.Atom getOpenOrPrepareWasCalled() {
    return openOrPrepareWasCalled;
  }

  public void setOpenOrPrepareWasCalled(
      clojure.lang.Atom openOrPrepareWasCalled) {
    this.openOrPrepareWasCalled = openOrPrepareWasCalled;
  }

  public String getStormId() {
    return stormId;
  }

  public void setStormId(String stormId) {
    this.stormId = stormId;
  }

  @SuppressWarnings("rawtypes")
  public Map getConf() {
    return conf;
  }

  @SuppressWarnings("rawtypes")
  public void setConf(Map conf) {
    this.conf = conf;
  }

  @SuppressWarnings("rawtypes")
  public Map getSharedExecutorData() {
    return sharedExecutorData;
  }

  @SuppressWarnings("rawtypes")
  public void setSharedExecutorData(Map sharedExecutorData) {
    this.sharedExecutorData = sharedExecutorData;
  }

  public IStormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public void setStormClusterState(IStormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public Map<Integer, String> getTaskToComponent() {
    return taskToComponent;
  }

  public void setTaskToComponent(Map<Integer, String> taskToComponent) {
    this.taskToComponent = taskToComponent;
  }

  public KryoTupleDeserializer getDeserializer() {
    return deserializer;
  }

  public void setDeserializer(KryoTupleDeserializer deserializer) {
    this.deserializer = deserializer;
  }

  public IContext getContext() {
    return context;
  }

  public void setContext(IContext context) {
    this.context = context;
  }

  public ExecutorTransferFn getTransferFn() {
    return transferFn;
  }

  public void setTransferFn(ExecutorTransferFn transferFn) {
    this.transferFn = transferFn;
  }

  public Map<Integer, Map<Integer, Map<String, IMetric>>> getIntervalToTaskToMetricRegistry() {
    return intervalToTaskToMetricRegistry;
  }

  public void setIntervalToTaskToMetricRegistry(
      Map<Integer, Map<Integer, Map<String, IMetric>>> intervalToTaskToMetricRegistry) {
    this.intervalToTaskToMetricRegistry = intervalToTaskToMetricRegistry;
  }

  public Map<String, Map<String, MkGrouper>> getStreamToComponentToGrouper() {
    return streamToComponentToGrouper;
  }

  public void setStreamToComponentToGrouper(
      Map<String, Map<String, MkGrouper>> streamToComponentToGrouper) {
    this.streamToComponentToGrouper = streamToComponentToGrouper;
  }

  public ReportErrorAndDie getReportErrorAndDie() {
    return reportErrorAndDie;
  }

  public void setReportErrorAndDie(ReportErrorAndDie reportErrorAndDie) {
    this.reportErrorAndDie = reportErrorAndDie;
  }

  public DisruptorQueue getReceiveQueue() {
    return receiveQueue;
  }

  public void setReceiveQueue(DisruptorQueue receiveQueue) {
    this.receiveQueue = receiveQueue;
  }

  public Integer getTaskId() {
    return taskId;
  }

  public void setTaskId(Integer taskId) {
    this.taskId = taskId;
  }

  public Map<Integer, TaskData> getTaskDatas() {
    return taskDatas;
  }

  public void setTaskDatas(Map<Integer, TaskData> taskDatas) {
    this.taskDatas = taskDatas;
  }

  public CommonStats getStats() {
    return stats;
  }

  public void setStats(CommonStats stats) {
    this.stats = stats;
  }

  public ScheduledExecutorService getExecutorScheduler() {
    return executorScheduler;
  }

  public void setExecutorScheduler(ScheduledExecutorService executorScheduler) {
    this.executorScheduler = executorScheduler;
  }

  public Counters getCounters() {
    return counters;
  }

  public void setCounters(Counters counters) {
    this.counters = counters;
  }

  public ITaskReportErr getReportError() {
    return reportError;
  }

  public void setReportError(ITaskReportErr reportError) {
    this.reportError = reportError;
  }

  public Callable<Boolean> getLatenciesSampler() {
    return this.latenciesSampler;
  }

  public void setLatenciesSampler(Callable<Boolean> sampler) {
    this.latenciesSampler = sampler;
  }

  public Callable<Boolean> getAckSampler() {
    return ackSampler;
  }

  public void setAckSampler(Callable<Boolean> ackSampler) {
    this.ackSampler = ackSampler;
  }

  public Callable<Boolean> getFailSampler() {
    return failSampler;
  }

  public void setFailSampler(Callable<Boolean> failSampler) {
    this.failSampler = failSampler;
  }

  public String getHostName() {
    return hostName;
  }

  public Boolean getDebug() {
    return debug;
  }

  public void setDebug(Boolean isDebug) {
    this.debug = isDebug;
  }

  public void setHostName(String hostName) {
    this.hostName = hostName;
  }

  public SpoutThrottlingMetrics getSpoutThrottlingMetrics() {
    return spoutThrottlingMetrics;
  }

  public void setSpoutThrottlingMetrics(
      SpoutThrottlingMetrics spoutThrottlingMetrics) {
    this.spoutThrottlingMetrics = spoutThrottlingMetrics;
  }
}
