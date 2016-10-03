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
package com.tencent.jstorm.daemon.executor.spout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.storm.daemon.Acker;
import org.apache.storm.daemon.StormCommon;
import org.apache.storm.spout.ISpoutOutputCollector;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.lmax.disruptor.InsufficientCapacityException;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.counter.TaskCounter;
import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorTransferFn;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
import com.tencent.jstorm.daemon.executor.error.ITaskReportErr;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.daemon.task.TaskUtils;
import com.tencent.jstorm.daemon.task.TasksFn;
import com.tencent.jstorm.utils.CoreUtil;

public class SpoutCollector implements ISpoutOutputCollector {
  private static Logger LOG = LoggerFactory.getLogger(SpoutCollector.class);

  private ExecutorTransferFn executorTransferFn;
  private RotatingMap<Long, TupleInfo> pending;
  private WorkerTopologyContext workerContext;

  private ITaskReportErr report_error;

  private boolean isHasAckers = false;
  private boolean isHasEventlogger = false;
  private Random rand;

  private ExecutorData executorData;
  private Integer taskId;
  private MutableLong emittedCount;
  private TaskData taskData;
  private TasksFn tasksFn;
  private Callable<Boolean> latenciesSampler;
  private Counters counters;

  public SpoutCollector(ExecutorData executorData, Integer taskId,
      TaskData taskData, MutableLong emittedCount,
      RotatingMap<Long, TupleInfo> pending) {
    this.executorData = executorData;
    this.taskData = taskData;
    this.taskId = taskId;
    this.tasksFn = new TasksFn(taskData);
    @SuppressWarnings("rawtypes")
    Map stormConf = executorData.getStormConf();
    this.counters = executorData.getCounters();
    this.emittedCount = emittedCount;
    this.executorTransferFn = executorData.getTransferFn();
    this.pending = pending;
    this.workerContext = executorData.getWorkerContext();
    this.isHasAckers = StormCommon.hasAckers(stormConf);
    this.isHasEventlogger = StormCommon.hasEventLoggers(stormConf);
    this.rand = new Random(Utils.secureRandomLong());
    this.report_error = executorData.getReportError();
    this.latenciesSampler = executorData.getLatenciesSampler();
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple,
      Object messageId) {
    if (executorData.getDebug()) {
      LOG.info("spout emit streamId:" + streamId + " tuple:" + tuple.toString()
          + " messageId:" + messageId);
    }
    List<Integer> ret = sendSpoutMsg(streamId, tuple, messageId, null);
    // counters.incrCounter(TaskCounter.SPOUT_EMIT_CNT, 1);
    counters.tpsCounter(TaskCounter.SPOUT_EMIT_TPS, String.valueOf(taskId));
    return ret;
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple,
      Object messageId) {
    if (executorData.getDebug()) {
      LOG.info("spout emitDirect: taskId:" + taskId + " streamId:" + streamId
          + " tuple:" + tuple.toString() + " messageId:" + messageId);
    }
    sendSpoutMsg(streamId, tuple, messageId, taskId);
    // counters.incrCounter(TaskCounter.SPOUT_EMIT_DIRECT_CNT, 1);
    counters.tpsCounter(TaskCounter.SPOUT_EMIT_DIRECT_TPS);
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#mk-threads#spout#send-spout-msg")
  private List<Integer> sendSpoutMsg(String outStreamId, List<Object> values,
      Object messageId, Integer outTaskId) {
    emittedCount.increment();
    List<Integer> outTasks = null;
    if (outTaskId != null) {
      outTasks = tasksFn.fn(outTaskId, outStreamId, values);
    } else {
      outTasks = tasksFn.fn(outStreamId, values);
    }

    boolean isRooted = (messageId != null) && isHasAckers;
    Long rootId = null;
    if (isRooted) {
      rootId = MessageId.generateId(rand);
    }
    List<Long> outIds = new ArrayList<Long>();
    for (Integer outTask : outTasks) {
      MessageId tupleId;
      if (isRooted) {
        long id = MessageId.generateId(rand);
        outIds.add(id);
        tupleId = MessageId.makeRootId(rootId, id);
      } else {
        tupleId = MessageId.makeUnanchored();
      }

      TupleImpl outTuple =
          new TupleImpl(workerContext, values, taskId, outStreamId, tupleId);
      try {
        executorTransferFn.transfer(outTask, outTuple);
      } catch (InsufficientCapacityException e) {
        LOG.error("transfer error:{}", e.getMessage());
      }
    }

    if (this.isHasEventlogger) {
      try {
        ExecutorUtils.sendToEventlogger(executorData, taskData, values,
            executorData.getComponentId(), messageId, rand);
      } catch (InsufficientCapacityException e) {
        LOG.error("send to eventlogger error:{}", e.getMessage());
      }
    }

    if (isRooted && !outIds.isEmpty()) {
      TupleInfo info = new TupleInfo();
      info.setTaskId(taskId);
      info.setMessageId(messageId);
      info.setStream(outStreamId);
      info.setValues(values);
      try {
        if (latenciesSampler.call()) {
          info.setTimestamp(System.currentTimeMillis());
        }
      } catch (Exception e1) {
        LOG.error("LatenciesSampler call error {}",
            CoreUtil.stringifyError(e1));
      }
      pending.put(rootId, info);

      List<Object> ackerTuple = Lists.newArrayList((Object) rootId,
          CoreUtil.bit_xor_vals(outIds), taskId);
      try {
        TaskUtils.sendUnanchored(taskData, Acker.ACKER_INIT_STREAM_ID,
            ackerTuple);
      } catch (InsufficientCapacityException e) {
        if (taskData.getExecutorData().getDebug()) {
          LOG.info("SPOUT ack TASK: " + taskId + " TUPLE: " + values.toString()
              + "failed {}!", CoreUtil.stringifyError(e));
        }
      }
    } else if (messageId != null) {
      TupleInfo info = new TupleInfo();
      info.setStream(outStreamId);
      info.setValues(values);
      Long timeDelta = null;
      try {
        if (latenciesSampler.call()) {
          timeDelta = 0L;
          ((SpoutExecutorStats) executorData.getStats())
              .spoutRecordCompleteLatencies(info.getStream(), timeDelta);
        }
      } catch (Exception e) {
        LOG.error("latenciesSampler call error {}", CoreUtil.stringifyError(e));
      }
      AckSpoutMsg ack = new AckSpoutMsg(executorData, taskData, messageId, info,
          timeDelta, 0L);
      ack.run();
    }
    return outTasks;
  }

  @Override
  public void reportError(Throwable error) {
    report_error.report(error);
  }

  @Override
  public long getPendingCount() {
    // TODO Auto-generated method stub
    return 0;
  }

}
