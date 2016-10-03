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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.storm.Thrift;
import org.apache.storm.generated.Bolt;
import org.apache.storm.generated.ComponentObject;
import org.apache.storm.generated.JavaObject;
import org.apache.storm.generated.ShellComponent;
import org.apache.storm.generated.SpoutSpec;
import org.apache.storm.generated.StateSpoutSpec;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.spout.ShellSpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.lmax.disruptor.InsufficientCapacityException;
import com.tencent.jstorm.ClojureClass;
import org.apache.storm.utils.ConfigUtils;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorTransferFn;
import com.tencent.jstorm.daemon.worker.WorkerData;

public class TaskUtils {

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "org.apache.storm.daemon.task#mk-topology-context-builder")
  private static TopologyContext mkTopologyContextBuilder(WorkerData worker,
      ExecutorData executorData, StormTopology topology, Integer taskId)
      throws Exception {
    Map conf = worker.getConf();
    String stormId = worker.getTopologyId();
    String stormroot = ConfigUtils.supervisorStormDistRoot(conf, stormId);
    String resourcePath = ConfigUtils.supervisorStormResourcesPath(stormroot);
    String workerPidsRoot =
        ConfigUtils.workerPidsRoot(conf, worker.getWorkerId());
    TopologyContext context =
        new TopologyContext(topology, worker.getStormConf(),
            worker.getTasksToComponent(), worker.getComponentToSortedTasks(),
            worker.getComponentToStreamToFields(), stormId, resourcePath,
            workerPidsRoot, taskId, worker.getPort(),
            Lists.newArrayList(worker.getTaskids()),
            worker.getDefaultSharedResources(), worker.getUserSharedResources(),
            executorData.getSharedExecutorData(),
            executorData.getIntervalToTaskToMetricRegistry(),
            executorData.getOpenOrPrepareWasCalled());

    return context;
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#system-topology-context")
  public static TopologyContext systemTopologyContext(WorkerData worker,
      ExecutorData executorData, Integer taskId) throws Exception {
    return mkTopologyContextBuilder(worker, executorData,
        worker.getSystemTopology(), taskId);
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#user-topology-context")
  public static TopologyContext userTopologyContext(WorkerData worker,
      ExecutorData executorData, Integer taskId) throws Exception {
    return mkTopologyContextBuilder(worker, executorData, worker.getTopology(),
        taskId);
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#get-task-object")
  public static Object getTaskObject(StormTopology topology,
      String component_id) {
    Map<String, SpoutSpec> spouts = topology.get_spouts();
    Map<String, Bolt> bolts = topology.get_bolts();
    Map<String, StateSpoutSpec> state_spouts = topology.get_state_spouts();
    ComponentObject obj = null;
    if (spouts.containsKey(component_id)) {
      obj = spouts.get(component_id).get_spout_object();
    } else if (bolts.containsKey(component_id)) {
      obj = bolts.get(component_id).get_bolt_object();
    } else if (state_spouts.containsKey(component_id)) {
      obj = state_spouts.get(component_id).get_state_spout_object();
    }
    if (obj == null) {
      throw new RuntimeException(
          "Could not find " + component_id + " in " + topology.toString());
    }
    Object componentObject = Utils.getSetComponentObject(obj);
    Object rtn = null;
    if (componentObject instanceof JavaObject) {
      rtn = Thrift.instantiateJavaObject((JavaObject) componentObject);
    } else if (componentObject instanceof ShellComponent) {
      if (spouts.containsKey(component_id)) {
        rtn = new ShellSpout((ShellComponent) componentObject);
      } else {
        rtn = new ShellBolt((ShellComponent) componentObject);
      }
    } else {
      rtn = componentObject;
    }
    return rtn;
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#apply-hooks")
  public static void applyHooks(TopologyContext topologyContext,
      SpoutAckInfo spoutAckInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.spoutAck(spoutAckInfo);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#apply-hooks#spoutFail")
  public static void applyHooks(TopologyContext topologyContext,
      SpoutFailInfo spoutFailInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.spoutFail(spoutFailInfo);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#apply-hooks#boltExecute")
  public static void applyHooks(TopologyContext topologyContext,
      BoltExecuteInfo boltExecuteInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.boltExecute(boltExecuteInfo);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#apply-hooks#boltAck")
  public static void applyHooks(TopologyContext topologyContext,
      BoltAckInfo boltAckInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.boltAck(boltAckInfo);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#apply-hooks#boltFail")
  public static void applyHooks(TopologyContext topologyContext,
      BoltFailInfo boltFailInfo) {
    Collection<ITaskHook> hooks = topologyContext.getHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (ITaskHook hook : hooks) {
        hook.boltFail(boltFailInfo);
      }
    }
  }

  @ClojureClass(className = "org.apache.storm.daemon.task#send-unanchored")
  public static void sendUnanchored(TaskData taskData, String stream,
      List<Object> values) throws InsufficientCapacityException {
    // TODO: this is all expensive... should be precomputed
    TasksFn tasksFn = taskData.getTasksFn();
    List<Integer> targetTaskIds = tasksFn.fn(stream, values);
    if (targetTaskIds.size() == 0) {
      return;
    }

    TopologyContext topologyContext = taskData.getSystemContext();
    ExecutorTransferFn transferFn = taskData.getExecutorData().getTransferFn();
    TupleImpl outTuple = new TupleImpl(topologyContext, values,
        topologyContext.getThisTaskId(), stream);

    for (Integer t : targetTaskIds) {
      transferFn.transfer(t, outTuple);
    }
  }
}
