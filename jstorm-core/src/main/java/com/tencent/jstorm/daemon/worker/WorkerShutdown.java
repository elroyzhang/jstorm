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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hooks.BaseWorkerHook;
import org.apache.storm.messaging.IConnection;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.SmartThread;
import org.apache.storm.utils.WorkerBackpressureThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.ExecutorShutdown;
import com.tencent.jstorm.daemon.executor.ShutdownableDameon;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy leongu
 * @ModifiedTime 9:26:02 AM Mar 1, 2016
 */
@ClojureClass(className = "org.apache.storm.daemon.worker#mk-worker#ret")
public class WorkerShutdown implements ShutdownableDameon {
  private static Logger LOG = LoggerFactory.getLogger(WorkerShutdown.class);
  private List<ExecutorShutdown> executors;
  private ConcurrentHashMap<String, IConnection> cachedNodeportSocket;
  private WorkerBackpressureThread backpressureThread;
  private IStormClusterState stormClusterState;
  private IStateStorage clusterState;
  private WorkerData workerData;
  private String stormId;
  private String assignmentId;
  private int port;
  private SmartThread transferThread;

  public WorkerShutdown(WorkerData workerData, List<ExecutorShutdown> executors,
      WorkerBackpressureThread backpressureThread, SmartThread transferThread) {
    this.workerData = workerData;
    this.stormId = workerData.getTopologyId();
    this.assignmentId = workerData.getAssignmentId();
    this.port = workerData.getPort();
    this.executors = executors;
    this.backpressureThread = backpressureThread;
    this.transferThread = transferThread;
    this.cachedNodeportSocket = workerData.getCachedNodeportToSocket();
    this.stormClusterState = workerData.getStormClusterState();
    this.clusterState = workerData.getZkClusterstate();
    Runtime.getRuntime().addShutdownHook(new Thread(this));
  }

  @Override
  public void shutdown() {
    workerData.getStormActiveAtom().set(false);

    LOG.info("Shutting down worker " + workerData.getTopologyId() + " "
        + workerData.getAssignmentId() + " " + workerData.getPort());

    for (IConnection socket : cachedNodeportSocket.values()) {
      // this will do best effort flushing since the linger period
      // was set on creation
      socket.close();
    }

    LOG.info("Terminating messaging context");
    LOG.info("Shutting down executors");
    for (ShutdownableDameon executor : executors) {
      executor.shutdown();
    }
    LOG.info("Shut down executors");

    // this is fine because the only time this is shared is when it's a local
    // context,in which case it's a noop
    workerData.getContext().term();
    LOG.info("Shutting down transfer thread");
    workerData.getTransferQueue().haltWithInterrupt();

    try {
      transferThread.interrupt();
      transferThread.join();
      LOG.info("Shut down transfer thread");
    } catch (InterruptedException e1) {
      LOG.error("join thread", e1);
    }

    try {
      backpressureThread.interrupt();
      backpressureThread.join();
      LOG.info("Shut down backpressure thread");
    } catch (InterruptedException e1) {
      LOG.error("join thread", e1);
    }

    try {
      workerData.getHeartbeatTimer().close();
    } catch (Exception e) {
      LOG.error("close Heartbeat Timer Exception {}",
          CoreUtil.stringifyError(e));
    }
    try {
      workerData.getRefreshConnectionsTimer().close();
    } catch (Exception e) {
      LOG.error("close RefreshConnections Timer Exception {}",
          CoreUtil.stringifyError(e));
    }
    try {
      workerData.getRefreshActiveTimer().close();
    } catch (Exception e) {
      LOG.error("close RefreshActive Timer Exception {}",
          CoreUtil.stringifyError(e));
    }
    try {
      workerData.getExecutorHeartbeatTimer().close();
    } catch (Exception e) {
      LOG.error("close ExecutorHeartbeat Timer Exception {}",
          CoreUtil.stringifyError(e));
    }

    try {
      workerData.getRefreshCredentialsTimer().close();
    } catch (Exception e) {
      LOG.error("close RefreshCredentials Timer Exception {}",
          CoreUtil.stringifyError(e));
    }
    try {
      workerData.getResetLogLevelsTimer().close();
    } catch (Exception e) {
      LOG.error("close ResetLogLevels Timer Exception {}",
          CoreUtil.stringifyError(e));
    }
    try {
      workerData.getRefreshLoadTimer().close();
    } catch (Exception e) {
      LOG.error("close RefreshLoad Timer Exception {}",
          CoreUtil.stringifyError(e));
    }
    closeResources(workerData);

    LOG.info("Trigger any worker shutdown hooks");
    runWorkerShutdownHooks(workerData);

    try {
      stormClusterState.removeWorkerHeartbeat(stormId, assignmentId,
          Long.valueOf(port));
      LOG.info("Removed worker heartbeat stormId:{}, port:{}", assignmentId,
          port);
      stormClusterState.removeWorkerBackpressure(stormId, assignmentId,
          Long.valueOf(port));
      LOG.info("Removed worker backpressure stormId:{}, port:{}", assignmentId,
          port);
      LOG.info("Disconnecting from storm cluster state context");
      stormClusterState.disconnect();
      clusterState.close();
    } catch (Exception e) {
      LOG.info("Shutdown error {} ", CoreUtil.stringifyError(e));
    }

    LOG.info("Shut down worker " + stormId + " " + assignmentId + " " + port);
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#close-resources")
  private void closeResources(WorkerData workerData) {
    Map<String, Object> dr = workerData.getDefaultSharedResources();
    LOG.info("Shutting down default resources");
    ExecutorService es =
        (ExecutorService) dr.get(WorkerTopologyContext.SHARED_EXECUTOR);
    es.shutdownNow();
    LOG.info("Shut down default resources");
  }

  @ClojureClass(className = "org.apache.storm.daemon.worker#run-worker-shutdown-hooks")
  private void runWorkerShutdownHooks(WorkerData workerData) {
    StormTopology topology = workerData.getTopology();
    List<ByteBuffer> hooks = topology.get_worker_hooks();
    for (ByteBuffer hook : hooks) {
      byte[] hookBytes = Utils.toByteArray(hook);
      BaseWorkerHook deserHook =
          Utils.javaDeserialize(hookBytes, BaseWorkerHook.class);
      deserHook.shutdown();
    }
  }

  public void join() throws InterruptedException {
    for (ExecutorShutdown task : executors) {
      task.join();
    }
  }

  @Override
  public boolean isWaiting() {

    return workerData.getHeartbeatTimer().isTimerWaiting()
        && workerData.getRefreshConnectionsTimer().isTimerWaiting()
        && workerData.getRefreshLoadTimer().isTimerWaiting()
        && workerData.getRefreshCredentialsTimer().isTimerWaiting()
        && workerData.getRefreshActiveTimer().isTimerWaiting()
        && workerData.getExecutorHeartbeatTimer().isTimerWaiting()
        && workerData.getUserTimer().isTimerWaiting();
  }

  @Override
  public void run() {
    shutdown();
  }
}
