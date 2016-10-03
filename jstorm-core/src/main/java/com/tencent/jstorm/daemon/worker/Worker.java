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

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.security.auth.Subject;

import org.apache.storm.Config;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Credentials;
import org.apache.storm.messaging.IContext;
import org.apache.storm.security.auth.AuthUtils;
import org.apache.storm.security.auth.IAutoCredentials;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.Utils.SmartThread;
import org.apache.storm.utils.WorkerBackpressureThread;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.executor.Executor;
import com.tencent.jstorm.daemon.executor.ExecutorShutdown;
import com.tencent.jstorm.daemon.worker.heartbeat.WorkerHeartbeatRunable;
import com.tencent.jstorm.daemon.worker.heartbeat.WorkerLocalHeartbeatRunable;
import com.tencent.jstorm.daemon.worker.threads.ActivateWorkerWhenAllConnectionsReady;
import com.tencent.jstorm.daemon.worker.threads.RefreshConnections;
import com.tencent.jstorm.daemon.worker.threads.RefreshLoad;
import com.tencent.jstorm.daemon.worker.threads.RefreshStormActive;
import com.tencent.jstorm.daemon.worker.transfer.TransferTuplesThread;
import com.tencent.jstorm.disruptor.CheckCredentialsChangedCallback;
import com.tencent.jstorm.disruptor.CheckLogConfigChanged;
import com.tencent.jstorm.disruptor.CheckThrottleChangedCallback;
import com.tencent.jstorm.disruptor.RunCredentialsAndThrottleCheckThread;
import com.tencent.jstorm.disruptor.TopologybackPressureCallback;
import com.tencent.jstorm.disruptor.WorkerBackpressureHandler;
import com.tencent.jstorm.disruptor.WorkerDisruptorBackpressureHandler;
import com.tencent.jstorm.utils.CoreUtil;

@ClojureClass(className = "backtype.storm.daemon.worker")
public class Worker {
  private static Logger LOG = LoggerFactory.getLogger(Worker.class);
  private WorkerData workerData;

  @SuppressWarnings("rawtypes")
  private Map conf;
  private IContext sharedMqContext;
  private String stormId;
  private String assignmentId;
  private int port;
  private String workerId;
  @SuppressWarnings("rawtypes")
  private Map stormConf;
  private List<ACL> acls;
  private IStateStorage stateStore;
  private IStormClusterState stormClusterState;
  private Credentials initialCredentials;
  private Collection<IAutoCredentials> autoCreds;
  private Subject subject;
  private int workerProcessId;
  private static WorkerShutdown worker = null;
  private AtomicReference<Credentials> credentials;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  public Worker(Map conf, IContext sharedMqContext, String stormId,
      String assignmentId, int port, String workerId) throws Exception {
    this.conf = conf;
    this.sharedMqContext = sharedMqContext;
    this.stormId = stormId;
    this.assignmentId = assignmentId;
    this.port = port;
    this.workerId = workerId;
    this.stormConf = ConfigUtils.readSupervisorStormConf(conf, stormId);

    // For docker, integer->integer map will be transfer to string->integer BUG
    this.stormConf.put("storm.real.virtual.ports",
        conf.get("storm.real.virtual.ports"));
    this.stormConf.put("storm.virtual.real.ports",
        conf.get("storm.virtual.real.ports"));

    // if nimbus leader yaml has storm.local.hostname is will pass to current
    // worker, replace it
    this.stormConf.put("storm.local.hostname",
        conf.get("storm.local.hostname"));

    this.stormConf = overrideLoginConfigWithSystemProperty(stormConf);
    this.acls = Utils.getWorkerACL(stormConf);
    this.stateStore = ClusterUtils.mkStateStorage(conf, stormConf, acls,
        new ClusterStateContext(DaemonType.WORKER));
    this.stormClusterState = ClusterUtils.mkStormClusterState(stateStore, acls,
        new ClusterStateContext());
    this.initialCredentials = stormClusterState.credentials(stormId, null);
    this.credentials = new AtomicReference<Credentials>(initialCredentials);
    this.autoCreds = AuthUtils.GetAutoCredentials(stormConf);
    this.workerProcessId = Utils.getInt(Utils.processPid(), 0);
    this.subject = AuthUtils.populateSubject(null, autoCreds,
        initialCredentials.get_creds());
    String clusterMode = ConfigUtils.clusterMode(conf);
    boolean isDistributeMode = clusterMode.endsWith("distributed");
    if (isDistributeMode) {
      String pidPath = ConfigUtils.workerPidPath(conf, workerId,
          String.valueOf(workerProcessId));
      CoreUtil.touch(pidPath);
      CoreUtil.spit(ConfigUtils.workerArtifactsPidPath(conf, stormId, port),
          String.valueOf(workerProcessId));
      LOG.info("Current worker's pid is " + pidPath);
    }
  }

  public WorkerShutdown execute() throws Exception {

    return Subject.doAs(subject,
        new PrivilegedExceptionAction<WorkerShutdown>() {

          @Override
          public WorkerShutdown run() throws Exception {
            workerData = new WorkerData(conf, sharedMqContext, stormId,
                assignmentId, port, workerId, workerProcessId, stormConf,
                stateStore, stormClusterState);
            // 1 Worker Local Heartbeat
            Runnable heartbeatFn = new WorkerLocalHeartbeatRunable(workerData);
            // do this here so that the worker process dies if this fails
            // it's important that worker heartbeat to supervisor ASAP when
            // launching so
            // that the supervisor knows it's running (and can move on)
            heartbeatFn.run();

            // launch heartbeat threads immediately so that slow-loading tasks
            // don't
            // cause the worker to timeout to the supervisor
            int heartbeatFrequence = Utils
                .getInt(conf.get(Config.WORKER_HEARTBEAT_FREQUENCY_SECS), 10);
            workerData.getHeartbeatTimer().scheduleRecurring(0,
                heartbeatFrequence, heartbeatFn);

            // 2 register Receive handle
            WorkerUtils.registerCallBacks(workerData);

            // 3 Refresh Connections
            int refConnfrequence = Utils.getInt(
                workerData.getStormConf().get(Config.TASK_REFRESH_POLL_SECS),
                10);
            Runnable refreshConnections = new RefreshConnections(workerData);
            refreshConnections.run();
            workerData.getRefreshConnectionsTimer().scheduleRecurring(0,
                refConnfrequence, refreshConnections);

            // 4 reset log levels timer
            int resetLogLevelPollSecs = Utils.getInt(workerData.getStormConf()
                .get(Config.WORKER_LOG_LEVEL_RESET_POLL_SECS), 30);
            Runnable resetLogLevels = new Runnable() {
              @Override
              public void run() {
                WorkerUtils.resetLogLevels(workerData.getLatestLogConfig());
              }
            };
            workerData.getResetLogLevelsTimer().scheduleRecurring(0,
                resetLogLevelPollSecs, resetLogLevels);

            // 5 Transfer Tuples
            TransferTuplesThread transferTuples =
                new TransferTuplesThread(workerData);
            SmartThread transferThread = Utils.asyncLoop(transferTuples);

            // 6 ActivateWorkerWhenAllConnectionsReady
            Runnable activeWhenAllConsReady =
                new ActivateWorkerWhenAllConnectionsReady(workerData);
            workerData.getRefreshActiveTimer().scheduleRecurring(0, 1,
                activeWhenAllConsReady);

            // 7 Refresh Storm Active
            int activeRecurSecs =
                Utils.getInt(conf.get(Config.TASK_REFRESH_POLL_SECS), 10);
            Runnable refreshStormActive =
                new RefreshStormActive(workerData, conf);
            workerData.getRefreshActiveTimer().scheduleRecurring(0,
                activeRecurSecs, refreshStormActive);

            // 8 Worker Executors HeartBeats (before init executors, see issue 188)
            int heartbeatRecurSecs = Utils
                .getInt(stormConf.get(Config.TASK_HEARTBEAT_FREQUENCY_SECS), 3);
            Runnable whr = new WorkerHeartbeatRunable(workerData);
            workerData.getExecutorHeartbeatTimer().scheduleRecurring(0,
                heartbeatRecurSecs, whr);

            // 9 shutdown executor callbacks
            List<ExecutorShutdown> shutdownExecutors = createExecutors();
            workerData.setShutdownExecutors(shutdownExecutors);


            WorkerDisruptorBackpressureHandler disruptorHandler =
                new WorkerDisruptorBackpressureHandler(workerData);
            workerData.getTransferQueue()
                .registerBackpressureCallback(disruptorHandler);
            double highWaterMark = Utils.getDouble(workerData.getConf()
                .get(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK), 0.9);
            double lowWatherMark = Utils.getDouble(workerData.getConf()
                .get(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK), 0.4);
            boolean isBackPressureEnable =
                Utils.getBoolean(workerData.getStormConf()
                    .get(Config.TOPOLOGY_BACKPRESSURE_ENABLE), false);
            workerData.getTransferQueue().setHighWaterMark(highWaterMark)
                .setLowWaterMark(lowWatherMark)
                .setEnableBackpressure(isBackPressureEnable);

            WorkerBackpressureHandler backpressureHandler =
                new WorkerBackpressureHandler(workerData, shutdownExecutors);
            WorkerBackpressureThread backpressureThread =
                new WorkerBackpressureThread(
                    workerData.getBackpressureTrigger(), workerData,
                    backpressureHandler);
            if (isBackPressureEnable) {
              backpressureThread.start();
              stormClusterState.topologyBackpressure(stormId,
                  new TopologybackPressureCallback(stormId, workerData,
                      stormClusterState));
            }

            // 10 establish log setting callback
            CheckLogConfigChanged logConfigChangedCallBack =
                new CheckLogConfigChanged(stormId, workerData);
            logConfigChangedCallBack.establishLogSettingCallback();

            // 11 check Credentials and backapress changed
            int checkCredentialsRecurSecs = Utils.getInt(
                workerData.getConf().get(Config.TASK_CREDENTIALS_POLL_SECS), 0);
            CheckCredentialsChangedCallback checkCredentialsChanged =
                new CheckCredentialsChangedCallback(stormId, workerData,
                    credentials, autoCreds, subject);
            CheckThrottleChangedCallback throttleCallback =
                new CheckThrottleChangedCallback(stormId, workerData);
            workerData.getStormClusterState().credentials(stormId,
                checkCredentialsChanged);
            Runnable runCredentialsAndThrottleCheckThread =
                new RunCredentialsAndThrottleCheckThread(workerData,
                    checkCredentialsChanged, throttleCallback);
            workerData.getRefreshCredentialsTimer().scheduleRecurring(0,
                checkCredentialsRecurSecs,
                runCredentialsAndThrottleCheckThread);

            // 12 queue load
            if (!Utils.getBoolean(
                conf.get(Config.TOPOLOGY_DISABLE_LOADAWARE_MESSAGING), false)) {
              Runnable refreshLoad = new RefreshLoad(workerData);
              workerData.getRefreshLoadTimer().scheduleRecurringWithJitter(0, 1,
                  500, refreshLoad);
            }

            return new WorkerShutdown(workerData, shutdownExecutors,
                backpressureThread, transferThread);
          }
        });
  }

  private List<ExecutorShutdown> createExecutors() throws Exception {
    List<ExecutorShutdown> shutdownExecutors =
        new ArrayList<ExecutorShutdown>();
    List<List<Long>> executors = workerData.getExecutors();
    for (List<Long> e : executors) {
      ExecutorShutdown t = Executor.mkExecutorShutdownDameon(workerData, e,
          initialCredentials.get_creds());
      shutdownExecutors.add(t);
    }
    return shutdownExecutors;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.worker#override-login-config-with-system-property")
  private Map overrideLoginConfigWithSystemProperty(Map conf) {
    String loginConfFile =
        System.getProperty("java.security.auth.login.config");
    if (loginConfFile != null) {
      conf.put("java.security.auth.login.config", loginConfFile);
    }
    return conf;
  }

  /**
   * TODO: should worker even take the storm-id as input? this should be
   * deducable from cluster state (by searching through assignments) what about
   * if there's inconsistency in assignments? -> but nimbus should guarantee
   * this consistency
   * 
   * @param conf
   * @param sharedMqContext
   * @param stormId
   * @param assignmentId
   * @param port
   * @param workerId
   * @return
   * @throws Exception
   */
  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.worker#mk-worker")
  public static WorkerShutdown mkWorker(Map conf, IContext sharedMqContext,
      String stormId, String assignmentId, int port, String workerId)
          throws Exception {
    Object[] objs = { stormId, assignmentId, String.valueOf(port), workerId,
        conf.toString() };
    LOG.info("Launching worker for {} on {}:{} with id {} and conf {}", objs);
    Worker w = new Worker(conf, sharedMqContext, stormId, assignmentId, port,
        workerId);
    return w.execute();
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.worker#-main")
  public static void main(String[] args) {
    Utils.setupDefaultUncaughtExceptionHandler();
    if (args.length != 4) {
      LOG.error("args length should be 4!");
      return;
    }

    String stormId = args[0];
    String assignmentId = args[1];
    String portStr = args[2];
    String workerId = args[3];
    Map conf = ConfigUtils.readStormConfig();
    if (ConfigUtils.isLocalMode(conf)) {
      throw new IllegalArgumentException("Cannot start server in local mode!");
    }
    try {
      worker = mkWorker(conf, null, stormId, assignmentId,
          Integer.parseInt(portStr), workerId);
      Utils.addShutdownHookWithForceKillIn1Sec(new Runnable() {
        @Override
        public void run() {
          if (worker != null) {
            worker.shutdown();
          }
        }
      });
    } catch (Exception e) {
      Object[] eobjs =
          { stormId, portStr, workerId, CoreUtil.stringifyError(e) };
      LOG.error(
          "Failed to create worker topologyId:{},port:{},workerId:{} for {}. ",
          eobjs);
      System.exit(1);
    }
  }
}
