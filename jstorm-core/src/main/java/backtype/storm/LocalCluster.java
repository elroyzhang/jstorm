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
package backtype.storm;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.cluster.ZKStateStorage;
import org.apache.storm.daemon.supervisor.SupervisorManager;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Credentials;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.RebalanceOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.messaging.IContext;
import org.apache.storm.messaging.local.Context;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.thrift.TException;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.nimbus.NimbusData;
import com.tencent.jstorm.daemon.nimbus.ServiceHandler;
import com.tencent.jstorm.daemon.nimbus.StandaloneNimbus;
import com.tencent.jstorm.daemon.nimbus.threads.CleanInboxRunnable;
import com.tencent.jstorm.daemon.nimbus.threads.MonitorRunnable;
import com.tencent.jstorm.psim.ProcessSimulator;
import com.tencent.jstorm.utils.CoreUtil;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "backtype.storm.LocalCluster")
public class LocalCluster implements ILocalCluster {
  private static final Logger LOG = LoggerFactory.getLogger(LocalCluster.class);
  private ServiceHandler serviceHandler;
  private NimbusData nimbusData;
  private Vector<String> tmpDirs = new Vector<String>();
  private ThriftServer thriftServer;
  private NIOServerCnxnFactory zkServer;
  private int zkPort;
  private IContext context;
  private IStateStorage clusterState = null;
  private IStormClusterState stormClusterState;
  private Vector<SupervisorManager> supervisorDaemons =
      new Vector<SupervisorManager>();
  private Map state;

  private int supervisors = 2;
  private int portsPerSupervisor = 3;
  private Map daemonConf = new HashMap();
  private INimbus inimbus = null;
  private int supervisorSlotPortMin = 1024;

  public LocalCluster() {
    this.daemonConf = new HashMap();
    this.daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);

    try {
      state = this.init();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.testing#mk-local-storm-cluster")
  public LocalCluster(int supervisors, int portsPerSupervisor, Map daemonConf,
      INimbus inimbus, int supervisorSlotPortMin) {
    this.supervisors = supervisors;
    this.portsPerSupervisor = portsPerSupervisor;
    this.daemonConf = daemonConf;
    this.inimbus = inimbus;
    this.supervisorSlotPortMin = supervisorSlotPortMin;

    try {
      state = this.init();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @ClojureClass(className = "backtype.storm.LocalCluster#-init")
  protected Map init() throws Exception {
    Map<String, Object> zkConf = Utils.readDefaultConfig();
    String zkTmp = CoreUtil.localTempPath();
    zkConf.put(Config.STORM_LOCAL_DIR, zkTmp);
    int zkport = Utils.getInt(zkConf.get(Config.STORM_ZOOKEEPER_PORT), 2182);
    String stormLocalDir =
        Utils.getString(zkConf.get(Config.STORM_LOCAL_DIR), "storm-local");
    List list = Zookeeper.mkInprocessZookeeper(stormLocalDir, zkport);
    this.zkServer = (NIOServerCnxnFactory) list.get(1);
    this.zkPort = ((Long) list.get(0)).intValue();

    this.tmpDirs.add(zkTmp);
    Thread.sleep(3000);

    Map<Object, Object> conf = Utils.readStormConfig();
    conf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
    conf.put(Config.ZMQ_LINGER_MILLIS, 0);
    conf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, false);
    conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 50);
    conf.put(Config.STORM_CLUSTER_MODE, "local");
    conf.put(Config.STORM_ZOOKEEPER_PORT, zkPort);
    conf.put(Config.STORM_ZOOKEEPER_SERVERS, Arrays.asList("localhost"));
    conf.putAll(this.daemonConf);
    this.daemonConf = conf;

    INimbus iNimbus = new StandaloneNimbus();
    this.launchLocalServer(iNimbus);

    this.context = mkSharedContext(this.daemonConf);
    this.clusterState = new ZKStateStorage(daemonConf, conf, null, null);
    this.stormClusterState = null; // TODO
    // new MkStormClusterState(daemonConf, null, null, null);

    for (int i = 0; i < this.supervisors; i++) {
      backtype.storm.testing.Testing.addSupervisor(this,
          this.portsPerSupervisor, new HashMap(), null);
    }

    state = new HashMap<String, Object>();
    state.put("nimbus", this.serviceHandler);
    state.put("port-counter", this.supervisorSlotPortMin);
    state.put("daemon-conf", this.daemonConf);
    state.put("supervisors", this.supervisorDaemons);
    state.put("cluster-state", this.clusterState);
    state.put("storm-cluster-state", this.stormClusterState);
    state.put("tmp-dirs", this.tmpDirs);
    state.put("zookeeper", this.zkServer);
    state.put("shared-context", this.context);
    return state;
  }

  @Override
  public void submitTopology(String topologyName, Map conf,
      StormTopology topology) {
    SubmitOptions options = new SubmitOptions(TopologyInitialStatus.ACTIVE);
    submitTopologyWithOpts(topologyName, conf, topology, options);
  }

  @Override
  public void submitTopologyWithOpts(String topologyName, Map conf,
      StormTopology topology, SubmitOptions submitOpts) {
    try {
      if (!Utils.isValidConf(conf))
        throw new IllegalArgumentException(
            "Topology conf is not json-serializable");

      serviceHandler.submitTopologyWithOpts(topologyName, null,
          JSONValue.toJSONString(conf), topology, submitOpts);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void killTopology(String topologyName) {
    killTopologyWithOpts(topologyName, new KillOptions());
  }

  @Override
  public void killTopologyWithOpts(String name, KillOptions options) {
    try {
      serviceHandler.killTopologyWithOpts(name, options);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
    }
  }

  @Override
  public void activate(String topologyName) throws NotAliveException {
    try {
      serviceHandler.activate(topologyName);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void deactivate(String topologyName) throws NotAliveException {
    try {
      serviceHandler.deactivate(topologyName);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void rebalance(String name, RebalanceOptions options)
      throws NotAliveException {
    try {
      serviceHandler.rebalance(name, options);
    } catch (TException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  @Override
  public void shutdown() {
    if (state == null) {
      return;
    }
    if (serviceHandler != null) {
      serviceHandler.shutdown();
    }
    if (nimbusData != null) {
      nimbusData.cleanup();
    }
    if (this.clusterState != null) {
      this.clusterState.close();
    }
    if (this.stormClusterState != null) {
      try {
        this.stormClusterState.disconnect();
      } catch (Exception e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    Vector<SupervisorManager> supervisorDaemons =
        (Vector<SupervisorManager>) state.get("supervisors");
    for (SupervisorManager daemon : supervisorDaemons) {
      daemon.shutdownAllWorkers();
      daemon.shutdown();
    }

    ProcessSimulator.killAllProcesses();

    if (zkServer != null) {
      LOG.info("Shutting down in process zookeeper");
      Zookeeper.shutdownInprocessZookeeper(zkServer);
      LOG.info("Done shutting down in process zookeeper");
    }

    // delete all tmp path
    for (String dir : tmpDirs) {
      try {
        LOG.info("Deleting temporary path {}", dir);
        Utils.forceDelete(dir);
      } catch (Exception e) {
        LOG.warn("Failed to clean up LocalCluster tmp dirs for {}",
            CoreUtil.stringifyError(e));
      }
    }

    this.state = null;
    // Runtime.getRuntime().exit(0);
  }

  @Override
  public String getTopologyConf(String id) {
    String ret = null;
    try {
      ret = this.serviceHandler.getTopologyConf(id);
    } catch (NotAliveException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public StormTopology getTopology(String id) {
    StormTopology ret = null;
    try {
      ret = this.serviceHandler.getTopology(id);
    } catch (NotAliveException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public ClusterSummary getClusterInfo() {
    ClusterSummary ret = null;
    try {
      ret = this.serviceHandler.getClusterInfo();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public TopologyInfo getTopologyInfo(String id) {
    TopologyInfo ret = null;
    try {
      ret = this.serviceHandler.getTopologyInfo(id);
    } catch (NotAliveException e) {
      e.printStackTrace();
    } catch (TException e) {
      e.printStackTrace();
    }
    return ret;
  }

  @Override
  public Map getState() {
    return state;
  }

  public void launchLocalServer(INimbus inimbus) throws Exception {
    initLocalShutdownHook();

    String nimbusTmp = CoreUtil.localTempPath();
    Map<String, Object> nimbusConf = new HashMap<String, Object>();
    nimbusConf.putAll(this.daemonConf);
    nimbusConf.put(Config.STORM_LOCAL_DIR, nimbusTmp);
    this.tmpDirs.add(nimbusTmp);

    LOG.info("Begin to start nimbus with conf {} ", nimbusConf.toString());

    this.nimbusData = new LocalNimbusData(nimbusConf, inimbus);
    this.serviceHandler = new ServiceHandler(nimbusConf, inimbus);
    scheduleLocalNimbusMonitor(nimbusConf);
    scheduleLocalNimbusInboxCleaner(nimbusConf);
  }

  private void scheduleLocalNimbusMonitor(Map conf) throws Exception {
    // Schedule Nimbus monitor
    int monitorFreqSecs =
        Utils.getInt(conf.get(Config.NIMBUS_MONITOR_FREQ_SECS), 10);
    Runnable r1 = new MonitorRunnable(nimbusData);
    nimbusData.getTimer().scheduleRecurring(0, monitorFreqSecs, r1);
    LOG.info("Successfully init Monitor thread");
  }

  /**
   * Right now, every 600 seconds, nimbus will clean jar under
   * /LOCAL-DIR/nimbus/inbox, which is the uploading topology directory
   * 
   * @param conf
   * @throws IOException
   */
  @SuppressWarnings("rawtypes")
  private void scheduleLocalNimbusInboxCleaner(Map conf) throws IOException {
    // Schedule Nimbus inbox cleaner
    String dirLocation = ConfigUtils.masterInbox(conf);
    int inboxJarExpirationSecs =
        Utils.getInt(conf.get(Config.NIMBUS_INBOX_JAR_EXPIRATION_SECS), 3600);
    Runnable cleanInbox =
        new CleanInboxRunnable(dirLocation, inboxJarExpirationSecs);
    int cleanupInboxFreqSecs =
        Utils.getInt(conf.get(Config.NIMBUS_CLEANUP_INBOX_FREQ_SECS), 600);
    nimbusData.getTimer().scheduleRecurring(0, cleanupInboxFreqSecs,
        cleanInbox);
    LOG.info("Successfully init " + dirLocation + " cleaner");
  }

  private void initLocalShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      public void run() {
        LocalCluster.this.shutdown();
      }
    });
  }

  @ClojureClass(className = "backtype.storm.testing#mk-shared-context")
  public static IContext mkSharedContext(Map conf) {
    Boolean isLocalModeZMQ =
        Utils.getBoolean(conf.get(Config.STORM_LOCAL_MODE_ZMQ), false);
    if (!isLocalModeZMQ) {
      Context context = new Context();
      context.prepare(conf);
      return context;
    }
    return null;
  }

  public Vector<String> getTmpDirs() {
    return this.tmpDirs;
  }

  public void addTmpDir(String dir) {
    this.tmpDirs.add(dir);
  }

  public Vector<SupervisorManager> getSupervisorDaemons() {
    return this.supervisorDaemons;
  }

  public void setSupervisorsDaemons(Vector<SupervisorManager> daemons) {
    this.supervisorDaemons = daemons;
  }

  public IContext getSharedContext() {
    return this.context;
  }

  public void setSharedContext(IContext sharedContext) {
    this.context = sharedContext;
  }

  public Map getDaemonConf() {
    return this.daemonConf;
  }

  public void setDaemonConf(Map conf) {
    this.daemonConf = conf;
  }

  public int getPortCounter() {
    return this.supervisorSlotPortMin;
  }

  public void setPortCounter(int counter) {
    this.supervisorSlotPortMin = counter;
  }

  public IStormClusterState getStormClusterState() {
    return this.stormClusterState;
  }

  public void setStormClusterState(IStormClusterState stormClusterState) {
    this.stormClusterState = stormClusterState;
  }

  public IStateStorage getClusterState() {
    return this.clusterState;
  }

  public void setClusterState(IStateStorage clusterState) {
    this.clusterState = clusterState;
  }

  public ServiceHandler getNimbus() {
    return this.serviceHandler;
  }

  public void setNimbus(ServiceHandler nimbus) {
    this.serviceHandler = nimbus;
  }

  public NIOServerCnxnFactory getZookeeper() {
    return this.zkServer;
  }

  public void setZookeeper(NIOServerCnxnFactory zk) {
    this.zkServer = zk;
  }

  @Override
  public void uploadNewCredentials(String topologyName, Credentials creds) {
    // TODO Auto-generated method stub

  }
}
