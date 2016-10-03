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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.Constants;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.spout.ISpout;
import org.apache.storm.stats.BoltExecutorStats;
import org.apache.storm.stats.CommonStats;
import org.apache.storm.stats.SpoutExecutorStats;
import org.apache.storm.task.IBolt;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.tuple.AddressedTuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.DisruptorQueue;
import org.apache.storm.utils.Utils.SmartThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.counter.Counters;
import com.tencent.jstorm.daemon.task.TaskData;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 3:38:07 PM Feb 25, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.executor#mk-executor#shutdown")
public class ExecutorShutdown implements ShutdownableDameon, RunningExecutor {
  private static final Logger LOG =
      LoggerFactory.getLogger(ExecutorShutdown.class);
  private List<Long> executorInfo;
  public static final byte QUIT_MSG = (byte) 0xff;
  private Map<Integer, TaskData> taskDatas;
  private List<SmartThread> all_threads;
  private IStormClusterState stormClusterState;
  private ExecutorData executorData;

  public ExecutorShutdown(ExecutorData executorData, String node, int port,
      List<SmartThread> all_threads) {
    this.executorData = executorData;
    this.taskDatas = executorData.getTaskDatas();
    this.executorInfo = executorData.getExecutorInfo();
    this.all_threads = all_threads;
    this.stormClusterState = executorData.getStormClusterState();
  }

  @Override
  public void shutdown() {
    LOG.info("Shutting down executor {}:{}", executorData.getComponentId(),
        executorData.getExecutorInfo().toString());

    LOG.info("Close receive queue.");
    executorData.getReceiveQueue().haltWithInterrupt();
    LOG.info("Close batch transfer queue.");
    executorData.getBatchTransferQueue().haltWithInterrupt();

    LOG.info("Close executor scheduler.");
    executorData.getExecutorScheduler().shutdown();
    // all thread will check the taskStatus
    // once it has been set SHUTDOWN, it will quit
    // waiting 100ms for executor thread shutting it's own
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
    }

    LOG.info("Close executor threads.");
    for (SmartThread thr : all_threads) {
      try {
        thr.interrupt();
        thr.join();
        LOG.info("Shut down  thread {}", thr.getName());
      } catch (InterruptedException e) {
        LOG.error("AsyncLoopThread shutdown error: {}",
            CoreUtil.stringifyError(e));
      }
    }

    // userContext hooks cleanup
    LOG.info("Cleanup hooks.");
    for (TaskData taskData : taskDatas.values()) {
      TopologyContext userContext = taskData.getUserContext();
      for (ITaskHook hook : userContext.getHooks()) {
        hook.cleanup();
      }
    }

    try {
      LOG.info("Close zookeeper connection.");
      stormClusterState.disconnect();
    } catch (Exception e) {
      LOG.info("Disconnect zkCluster error {} ", CoreUtil.stringifyError(e));
    }

    LOG.info("Close component.");
    if ((Boolean) executorData.getOpenOrPrepareWasCalled().deref()) {
      for (TaskData t : taskDatas.values()) {
        Object obj = t.getObject();
        closeComponent(obj);
      }
    }
    LOG.info("Shut down executor componentId:" + executorData.getComponentId()
        + ", executorType: " + executorData.getComponentType().toString()
        + ", executorId:" + executorData.getExecutorInfo().toString());
  }

  public void join() throws InterruptedException {
    for (SmartThread t : all_threads) {
      t.join();
    }
  }

  @ClojureClass(className = "backtype.storm.daemon.executor#close-component")
  private void closeComponent(Object _task_obj) {
    if (_task_obj instanceof IBolt) {
      ((IBolt) _task_obj).cleanup();
    }
    if (_task_obj instanceof ISpout) {
      ((ISpout) _task_obj).close();
    }
  }

  @Override
  public boolean isWaiting() {
    return false;
  }

  @Override
  public void run() {
    shutdown();
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-executor#reify#get_executor_id")
  public List<Long> get_executor_id() {
    return executorInfo;
  }

  public Counters render_counters() {
    return executorData.getCounters();
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-executor#reify#render-stats")
  public ExecutorStats render_stats() {
    CommonStats stats = executorData.getStats();
    if (stats instanceof BoltExecutorStats) {
      return ((BoltExecutorStats) stats).renderStats();
    } else if (stats instanceof SpoutExecutorStats) {
      return ((SpoutExecutorStats) stats).renderStats();
    }
    return null;
  }

  @Override
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-executor#reify#get-backpressure-flag")
  public AtomicBoolean get_backpressure_flag() {
    return executorData.getBackpressure();
  };

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.executor#mk-executor#reify#credentials-changed")
  public void crendentials_changed(Credentials creds) {
    DisruptorQueue receiveQueue = executorData.getReceiveQueue();
    WorkerTopologyContext context = executorData.getWorkerContext();
    List credslist = new ArrayList();
    credslist.add(creds);
    AddressedTuple val = new AddressedTuple(AddressedTuple.BROADCAST_DEST,
        new TupleImpl(context, credslist, (int)Constants.SYSTEM_TASK_ID,
            Constants.CREDENTIALS_CHANGED_STREAM_ID));
    receiveQueue.publish(val);
  };
}
