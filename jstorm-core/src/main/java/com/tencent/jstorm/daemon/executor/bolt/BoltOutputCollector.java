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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.storm.Config;
import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.task.IOutputCollector;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.lmax.disruptor.InsufficientCapacityException;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.counter.TaskCounter;
import com.tencent.jstorm.daemon.executor.ExecutorTransferFn;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
import com.tencent.jstorm.daemon.executor.error.ITaskReportErr;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.daemon.task.TasksFn;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#IOutputCollector")
public class BoltOutputCollector implements IOutputCollector {
  private static final Logger LOG =
      LoggerFactory.getLogger(BoltOutputCollector.class);
  private ITaskReportErr reportError;
  private ExecutorTransferFn transferFn;
  private TasksFn tasksFn;
  private WorkerTopologyContext topologyContext;
  private Integer taskId;
  private Random rand;
  private CommonStats executorStats;
  private TaskData taskData;
  private Counters counters;
  private boolean isHasEventlogger = false;
  private boolean isUpdateEnable = false;

  @SuppressWarnings("rawtypes")
  public BoltOutputCollector(TaskData taskData, ITaskReportErr reportError,
      Map stormConf, ExecutorTransferFn transferFn,
      WorkerTopologyContext topologyContext, Integer taskId,
      RotatingMap<Tuple, Long> tuple_start_times, CommonStats executorStats) {
    this.taskData = taskData;
    this.tasksFn = new TasksFn(taskData);
    this.reportError = reportError;
    this.transferFn = transferFn;
    this.topologyContext = topologyContext;
    this.taskId = taskId;
    this.counters = taskData.getExecutorData().getCounters();
    this.rand = new Random(Utils.secureRandomLong());
    this.executorStats = executorStats;
    this.isHasEventlogger = StormCommon.hasEventLoggers(stormConf);
    this.isUpdateEnable = Utils.getBoolean(stormConf.get(Config.TOPOLOGY_BOLTS_UPDATE_ENABLE), false);
  }

  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors,
      List<Object> tuple) {
    if (taskData.getExecutorData().getDebug()) {
      LOG.info("bolt emit: streamId:" + streamId + " anchors:" + anchors
          + " messageId:" + tuple.toString());
    }
    List<Integer> ret = boltEmit(streamId, anchors, tuple, null);
    // counters.incrCounter(TaskCounter.BOLT_EMIT_CNT, 1);
    counters.tpsCounter(TaskCounter.BOLT_EMIT_TPS, String.valueOf(taskId));
    return ret;
  }

  @Override
  public void emitDirect(int taskId, String streamId, Collection<Tuple> anchors,
      List<Object> tuple) {
    if (taskData.getExecutorData().getDebug()) {
      LOG.info("bolt emitDirect: streamId:" + streamId + " anchors:" + anchors
          + " messageId:" + tuple.toString());
    }
    boltEmit(streamId, anchors, tuple, taskId);
    // counters.incrCounter(TaskCounter.BOLT_EMIT_DIRECT_CNT, 1);
    counters.tpsCounter(TaskCounter.BOLT_EMIT_DIRECT_TPS,
        String.valueOf(taskId));
  }

  @Override
  public void ack(Tuple input) {

    TupleImpl tuple = (TupleImpl) input;
    Long ackVal = tuple.getAckVal();
    Map<Long, Long> rootToIds = tuple.getMessageId().getAnchorsToIds();
    for (Map.Entry<Long, Long> entry : rootToIds.entrySet()) {
      Long root = entry.getKey();
      Long id = entry.getValue();
      try {
        TaskUtils.sendUnanchored(taskData, Acker.ACKER_ACK_STREAM_ID,
            Lists.newArrayList((Object) root, CoreUtil.bit_xor(id, ackVal)));
      } catch (InsufficientCapacityException e) {
        if (taskData.getExecutorData().getDebug()) {
          LOG.info(
              "BOLT ack TASK: " + taskId + " TUPLE: "
                  + tuple.getValues().toString() + "failed {}!",
              CoreUtil.stringifyError(e));
        }
      }
    }

    Long delta = tupleTimeDelta(tuple);
    if (taskData.getExecutorData().getDebug()) {
      LOG.info("BOLT ack TASK: " + taskId + " TIME: " + delta + " TUPLE: "
          + tuple.getValues().toString());
    }

    TaskUtils.applyHooks(taskData.getUserContext(),
        new BoltAckInfo(tuple, taskId, delta));

    if (delta != null) {
      ((BoltExecutorStats) executorStats).boltRecordProcessLatencies(
          tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
    }
    try {
      if (taskData.getExecutorData().getAckSampler().call()) {
        ((BoltExecutorStats) executorStats).boltAckedTuple(
            tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
      }
    } catch (Exception e) {
      LOG.error("AckSampler call error {}", CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void update(Tuple input, Object messageId) {
    if(!this.isUpdateEnable) {
      return;
    }
    TupleImpl tuple = (TupleImpl) input;
    Map<Long, Long> rootToIds = tuple.getMessageId().getAnchorsToIds();
    for (Map.Entry<Long, Long> entry : rootToIds.entrySet()) {
      Long root = entry.getKey();
      try {
        TaskUtils.sendUnanchored(taskData, Acker.ACKER_UPDATE_STREAM_ID,
            Lists.newArrayList((Object) root, messageId));
      } catch (InsufficientCapacityException e) {
        if (taskData.getExecutorData().getDebug()) {
          LOG.info(
              "BOLT ack TASK: " + taskId + " TUPLE: "
                  + tuple.getValues().toString() + "failed {}!",
              CoreUtil.stringifyError(e));
        }
      }
    }
  }

  @Override
  public void fail(Tuple tuple) {
    Set<Long> roots = tuple.getMessageId().getAnchors();
    for (Long root : roots) {
      try {
        TaskUtils.sendUnanchored(taskData, Acker.ACKER_FAIL_STREAM_ID,
            Lists.newArrayList((Object) root));
      } catch (InsufficientCapacityException e) {
        if (taskData.getExecutorData().getDebug()) {
          LOG.info(
              "BOLT fail TASK: " + taskId + " TUPLE: "
                  + tuple.getValues().toString() + "failed {}!",
              CoreUtil.stringifyError(e));
        }
      }
    }

    Long delta = tupleTimeDelta((TupleImpl) tuple);
    if (taskData.getExecutorData().getDebug()) {
      LOG.info("BOLT fail TASK: " + taskId + " TIME: " + delta + " TUPLE: "
          + tuple.getValues().toString());
    }
    TaskUtils.applyHooks(taskData.getUserContext(),
        new BoltFailInfo(tuple, taskId, delta));

    try {
      if (taskData.getExecutorData().getFailSampler().call()) {
        ((BoltExecutorStats) executorStats).boltFailedTuple(
            tuple.getSourceComponent(), tuple.getSourceStreamId(), delta);
      }
    } catch (Exception e) {
      LOG.error("FailSampler call error {}", CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void reportError(Throwable error) {
    reportError.report(error);
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#bolt#bolt-emit")
  private List<Integer> boltEmit(String stream, Collection<Tuple> anchors,
      List<Object> values, Integer task) {
    List<Integer> outTasks = new ArrayList<Integer>();
    if (task != null) {
      outTasks = tasksFn.fn(task, stream, values);
    } else {
      outTasks = tasksFn.fn(stream, values);
    }

    for (Integer t : outTasks) {
      Map<Long, Long> anchorsToIds = new HashMap<Long, Long>();
      if (anchors != null) {
        for (Tuple anchor : anchors) {
          TupleImpl a = (TupleImpl) anchor;
          Set<Long> rootIds = a.getMessageId().getAnchorsToIds().keySet();
          if (rootIds != null && rootIds.size() > 0) {
            Long edgeId = MessageId.generateId(rand);
            a.updateAckVal(edgeId);
            for (Long rootId : rootIds) {
              putXor(anchorsToIds, rootId, edgeId);
            }
          }
        }
      }
      TupleImpl tupleExt = new TupleImpl(topologyContext, values, taskId,
          stream, MessageId.makeId(anchorsToIds));
      try {
        transferFn.transfer(t, tupleExt);
      } catch (InsufficientCapacityException e) {
        LOG.error("bolt emit", e);
      }
    }
    if (this.isHasEventlogger) {
      try {
        ExecutorUtils.sendToEventlogger(taskData.getExecutorData(), taskData,
            values, taskData.getExecutorData().getComponentId(), null, rand);
      } catch (InsufficientCapacityException e) {
        LOG.error("send to eventlogger error:{}", e.getMessage());
      }
    }
    return outTasks;
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#tuple-time-delta!")
  public static Long tupleTimeDelta(TupleImpl tuple) {
    Long ms = tuple.getProcessSampleStartTime();
    if (ms != null) {
      return CoreUtil.time_delta_ms(ms);
    }
    return null;
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#put-xor!")
  public static void putXor(Map<Long, Long> pending, Long key, Long id) {
    // synchronized (pending) {
    Long curr = pending.get(key);
    if (curr == null) {
      curr = Long.valueOf(0);
    }
    pending.put(key, CoreUtil.bit_xor(curr, id));
    // }
  }

  @Override
  public void resetTimeout(Tuple input) {
    for (Long root : input.getMessageId().getAnchors()) {
      try {
        TaskUtils.sendUnanchored(taskData, Acker.ACKER_RESET_TIMEOUT_STREAM_ID,
            Lists.newArrayList(root));
      } catch (InsufficientCapacityException e) {
        if (taskData.getExecutorData().getDebug()) {
          LOG.info("Bolt reset timeout, root: {} failed {}!", root,
              CoreUtil.stringifyError(e));
        }
      }
    }
  }
}
