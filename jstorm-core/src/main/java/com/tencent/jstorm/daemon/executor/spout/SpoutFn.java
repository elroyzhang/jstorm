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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.Config;
import org.apache.storm.ICredentialsListener;
import org.apache.storm.daemon.metrics.BuiltinMetricsUtil;
import org.apache.storm.spout.ISpout;
import org.apache.storm.spout.ISpoutWaitStrategy;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.MutableLong;
import org.apache.storm.utils.RotatingMap;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.daemon.executor.ExecutorData;
import com.tencent.jstorm.daemon.executor.ExecutorUtils;
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
 * @ModifiedTime 5:09:50 PM Feb 25, 2016
 */
public class SpoutFn extends RunnableCallback {
  private static final Logger LOG = LoggerFactory.getLogger(SpoutFn.class);
  private static final long serialVersionUID = 1L;

  private SpoutEventHandler spoutEventHandler;
  private ExecutorData executorData;
  private MutableLong emittedCount;
  private AtomicBoolean lastActive;
  private Integer maxSpoutPending;
  private Map<Integer, TaskData> taskDatas;
  private RotatingMap<Long, TupleInfo> pending;
  private ArrayList<ISpout> spouts;
  private MutableLong emptyEmitStreak;
  private ISpoutWaitStrategy spoutWaitStrategy;

  public SpoutFn(ExecutorData executorData, Map<Integer, TaskData> taskDatas,
      Map<String, String> initialCredentials) {
    this.executorData = executorData;
    this.spoutEventHandler = new SpoutEventHandler(executorData, pending);
    this.emittedCount = new MutableLong(0);
    this.lastActive = new AtomicBoolean(false);
    this.taskDatas = taskDatas;
    this.pending = new RotatingMap<Long, TupleInfo>(2,
        new SpoutExpiredCallback<Long, TupleInfo>(executorData, taskDatas));
    this.emptyEmitStreak = new MutableLong(0);
    this.maxSpoutPending = ExecutorUtils
        .executorMaxSpoutPending(executorData.getStormConf(), taskDatas.size());
    this.spoutEventHandler = new SpoutEventHandler(executorData, pending);
    try {
      this.spoutWaitStrategy =
          ExecutorUtils.initSpoutWaitStrategy(executorData.getStormConf());

      while (!executorData.getWorker().getStormActiveAtom().get()) {
        LOG.debug("topology is in inactive state, wait 100 millis ...");
        Thread.sleep(100);
      }
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }

    this.spouts = new ArrayList<ISpout>();
    TaskData firstTaskData = null;
    for (TaskData taskData : taskDatas.values()) {
      if (firstTaskData == null) {
        firstTaskData = taskData;
      }
      this.spouts.add((ISpout) taskData.getObject());
    }
    executorData.getSpoutThrottlingMetrics().registerAll(
        executorData.getStormConf(), firstTaskData.getUserContext());
    for (Map.Entry<Integer, TaskData> tt : taskDatas.entrySet()) {
      Integer taskId = tt.getKey();
      TaskData taskData = tt.getValue();
      ISpout spoutObj = (ISpout) taskData.getObject();
      taskData.getBuiltinMetrics().registerAll(executorData.getStormConf(),
          taskData.getUserContext());
      Map<String, DisruptorQueue> queues =
          new HashMap<String, DisruptorQueue>();
      queues.put("sendqueue", executorData.getBatchTransferQueue());
      queues.put("receive", executorData.getReceiveQueue());
      BuiltinMetricsUtil.registerQueueMetrics(queues,
          executorData.getStormConf(), taskData.getUserContext());

      if (spoutObj instanceof ICredentialsListener) {
        ((ICredentialsListener) spoutObj).setCredentials(initialCredentials);
      }

      spoutObj.open(executorData.getStormConf(), taskData.getUserContext(),
          new SpoutOutputCollector(new SpoutCollector(executorData, taskId,
              taskData, emittedCount, pending)));
    }

    executorData.getOpenOrPrepareWasCalled().reset(true);

  }

  @Override
  public Long call() throws Exception {
    // This design requires that spouts be non-blocking
    executorData.getReceiveQueue().consumeBatch(spoutEventHandler);

    boolean isActive = executorData.getWorker().getStormActiveAtom().get();
    Long currCount = emittedCount.get();
    boolean isBackPressureEnabled = Utils.getBoolean(
        executorData.getStormConf().get(Config.TOPOLOGY_BACKPRESSURE_ENABLE),
        false);
    boolean isThrottleOn = isBackPressureEnabled
        && this.executorData.getWorker().getThrottleOn().get();
    boolean isReachedMaxSpoutPending =
        this.maxSpoutPending != null && (pending.size() >= maxSpoutPending);

    if (isActive) {
      // activated
      if (!lastActive.get()) {
        lastActive.set(true);
        LOG.info("Activating spout " + executorData.getComponentId() + ":"
            + taskDatas.keySet().toString());
        for (ISpout spout : spouts) {
          spout.activate();
        }
      }
      if (!this.executorData.getBatchTransferQueue().isFull() && !isThrottleOn
          && !isReachedMaxSpoutPending) {
        for (ISpout spout : spouts) {
          spout.nextTuple();
        }
      }
    } else {
      // deactivated
      if (lastActive.get()) {
        lastActive.set(false);
        LOG.info("Deactivating spout " + executorData.getComponentId() + ":"
            + taskDatas.keySet().toString());
        for (ISpout spout : spouts) {
          spout.deactivate();
        }
      }
      // TODO: log that it's getting throttled
      LOG.debug("Storm not active yet. Sleep 100 ms ...");
      try {
        Time.sleep(100);
        executorData.getSpoutThrottlingMetrics()
            .skippedInactive(executorData.getStats());
      } catch (InterruptedException e) {
      }
    }

    if (currCount.longValue() == emittedCount.get() && isActive) {
      emptyEmitStreak.increment();
      spoutWaitStrategy.emptyEmit(emptyEmitStreak.get());
      if (isThrottleOn) {
        executorData.getSpoutThrottlingMetrics()
            .skippedThrottle(executorData.getStats());
        if (isReachedMaxSpoutPending) {
          executorData.getSpoutThrottlingMetrics()
              .skippedMaxSpout(executorData.getStats());
        }
      }
    } else {
      emptyEmitStreak.set(0);
    }
    return 0L;
  }
}
