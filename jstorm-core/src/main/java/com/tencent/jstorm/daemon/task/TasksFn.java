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
package com.tencent.jstorm.daemon.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.LoadMapping;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.grouping.MkGrouper;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 11:06:15 AM Feb 26, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.task#mk-tasks-fn")
public class TasksFn {
  private Logger LOG = LoggerFactory.getLogger(TasksFn.class);
  private TaskData taskData;
  private ExecutorData executorData;
  private LoadMapping loadMapping;
  private String componentId;
  private WorkerTopologyContext workerContext;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private Map<String, Map<String, MkGrouper>> streamToComponentToGrouper;
  private TopologyContext userContext;
  private CommonStats executorStats;
  private int taskId;
  private boolean isDebug = false;
  private Callable<Boolean> emitSampler;

  public TasksFn(TaskData taskData) {
    this.taskData = taskData;
    this.taskId = taskData.getTaskId();
    this.executorData = taskData.getExecutorData();
    this.loadMapping = executorData.getWorker().getLoadMapping();
    this.componentId = executorData.getComponentId();
    this.workerContext = executorData.getWorkerContext();
    this.stormConf = executorData.getStormConf();
    this.streamToComponentToGrouper =
        executorData.getStreamToComponentToGrouper();
    this.userContext = taskData.getUserContext();
    this.executorStats = executorData.getStats();
    this.emitSampler = ConfigUtils.mkStatsSampler(stormConf);
    this.isDebug = executorData.getDebug();
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#mk-tasks-fn#fn")
  public List<Integer> fn(Integer outTaskId, String stream,
      List<Object> values) {

    if (isDebug) {
      LOG.info("Emitting direct: " + outTaskId + "; " + componentId + " "
          + stream + " " + values);
    }

    List<Integer> outTasks = new ArrayList<Integer>();
    if (outTaskId != null) {
      String targetComponent = workerContext.getComponentId(outTaskId);
      Map<String, MkGrouper> componentToGrouping =
          streamToComponentToGrouper.get(stream);
      if (componentToGrouping != null) {
        MkGrouper grouping = componentToGrouping.get(targetComponent);
        if (grouping != null
            && !grouping.getGrouptype().equals(Grouping._Fields.DIRECT)) {
          throw new IllegalArgumentException(
              "Cannot emitDirect to a task expecting a regular grouping");
        }
        outTasks.add(outTaskId);
      }
    }

    applyHooks(userContext, new EmitInfo(values, stream, taskId, outTasks));

    try {
      if (emitSampler.call()) {
        executorStats.emittedTuple(stream);
        if (outTaskId != null) {
          executorStats.transferredTuples(stream, 1);
        }
      }
    } catch (Exception e) {
      LOG.error("emitSampler call error {}", CoreUtil.stringifyError(e));
    }

    return outTasks;
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#mk-tasks-fn#fn")
  public List<Integer> fn(String stream, List<Object> values) {

    if (isDebug) {
      LOG.info("Emitting: " + componentId + " " + stream + " " + values);
    }

    if (!streamToComponentToGrouper.containsKey(stream)) {
      throw new IllegalArgumentException("Unknown stream ID: " + stream);
    }

    List<Integer> outTasks = new ArrayList<Integer>();
    Map<String, MkGrouper> componentToGrouping =
        streamToComponentToGrouper.get(stream);
    if (componentToGrouping != null) {
      for (MkGrouper grouper : componentToGrouping.values()) {
        if (grouper.getGrouptype().equals(Grouping._Fields.DIRECT)) {
          // TODO: this is wrong, need to check how the stream was declared
          throw new IllegalArgumentException(
              "Cannot do regular emit to direct stream");
        }
        List<Integer> compTasks = grouper.grouper(taskId, values, loadMapping);
        outTasks.addAll(compTasks);
      }
    }

    applyHooks(userContext, new EmitInfo(values, stream, taskId, outTasks));

    try {
      if (emitSampler.call()) {
        executorStats.emittedTuple(stream);
        executorStats.transferredTuples(stream, outTasks.size());
      }
    } catch (Exception e) {
      LOG.error("emitSampler call error {}", CoreUtil.stringifyError(e));
    }

    return outTasks;
  }

  @ClojureClass(className = "backtype.storm.daemon.task#apply-hooks")
  private void applyHooks(TopologyContext topologyContext, EmitInfo emitInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks == null || hooks.isEmpty()) {
      return;
    }
    for (ITaskHook hook : hooks) {
      hook.emit(emitInfo);
    }
  }

  public TaskData getTaskData() {
    return taskData;
  }

  public void setTaskData(TaskData taskData) {
    this.taskData = taskData;
  }

  public ExecutorData getExecutorData() {
    return executorData;
  }

  public void setExecutorData(ExecutorData executorData) {
    this.executorData = executorData;
  }

  public String getComponentId() {
    return componentId;
  }

  public void setComponentId(String componentId) {
    this.componentId = componentId;
  }

  public WorkerTopologyContext getWorkerContext() {
    return workerContext;
  }

  public void setWorkerContext(WorkerTopologyContext workerContext) {
    this.workerContext = workerContext;
  }

  @SuppressWarnings("rawtypes")
  public Map getStormConf() {
    return stormConf;
  }

  @SuppressWarnings("rawtypes")
  public void setStormConf(Map stormConf) {
    this.stormConf = stormConf;
  }

  public Map<String, Map<String, MkGrouper>> getStreamToComponentToGrouper() {
    return streamToComponentToGrouper;
  }

  public void setStreamToComponentToGrouper(
      Map<String, Map<String, MkGrouper>> streamToComponentToGrouper) {
    this.streamToComponentToGrouper = streamToComponentToGrouper;
  }

  public TopologyContext getUserContext() {
    return userContext;
  }

  public void setUserContext(TopologyContext userContext) {
    this.userContext = userContext;
  }

  public CommonStats getExecutorStats() {
    return executorStats;
  }

  public void setExecutorStats(CommonStats executorStats) {
    this.executorStats = executorStats;
  }

  public boolean isDebug() {
    return isDebug;
  }

  public void setDebug(boolean isDebug) {
    this.isDebug = isDebug;
  }

  public LoadMapping getLoadMapping() {
    return loadMapping;
  }

  public void setLoadMapping(LoadMapping loadMapping) {
    this.loadMapping = loadMapping;
  }
}
