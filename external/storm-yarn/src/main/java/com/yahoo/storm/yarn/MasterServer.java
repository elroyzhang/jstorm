/*
 * Copyright (c) 2013 Yahoo! Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.storm.yarn;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.generated.NotAliveException;
import org.apache.storm.generated.TopologyInfo;
import org.apache.storm.security.auth.ServerInfo;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;

import com.tencent.jstorm.cluster.StormZkClusterState;
import com.tencent.jstorm.utils.ServerUtils;
import com.yahoo.storm.yarn.generated.StormMaster;
import com.yahoo.storm.yarn.generated.StormMaster.Processor;

public class MasterServer extends ThriftServer {
  private static final Logger LOG = LoggerFactory.getLogger(MasterServer.class);
  private static StormMasterServerHandler _handler;
  ApplicationAttemptId appAttemptID;
  Timer timer = new Timer();
  long delay = 1000;
  long period = 10000;
  String host;
  long nimbusPort;
  long uiPort;
  private static ScheduledExecutorService masterScheduler;

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    LOG.info("Starting the AM!!!!");

    Options opts = new Options();
    opts.addOption("app_attempt_id", true, "App Attempt ID. Not to be used "
        + "unless for testing purposes");

    CommandLine cl = new GnuParser().parse(opts, args);

    ApplicationAttemptId appAttemptID;
    Map<String, String> envs = System.getenv();
    if (cl.hasOption("app_attempt_id")) {
      String appIdStr = cl.getOptionValue("app_attempt_id", "");
      appAttemptID = ConverterUtils.toApplicationAttemptId(appIdStr);
    } else if (envs.containsKey(ApplicationConstants.Environment.CONTAINER_ID
        .name())) {

      ContainerId containerId =
          ConverterUtils.toContainerId(envs
              .get(ApplicationConstants.Environment.CONTAINER_ID.name()));
      appAttemptID = containerId.getApplicationAttemptId();
      LOG.info("appAttemptID from env:" + appAttemptID.toString());
    } else {
      LOG.error("appAttemptID is not specified for storm master");
      throw new Exception("appAttemptID is not specified for storm master");
    }

    @SuppressWarnings("rawtypes")
    Map storm_conf = Config.readStormConfig(null);
    Util.rmNulls(storm_conf);

    YarnConfiguration hadoopConf = new YarnConfiguration();

    final String host = InetAddress.getLocalHost().getHostName();
    storm_conf.put("nimbus.host", host);

    StormAMRMClient rmClient =
        new StormAMRMClient(appAttemptID, storm_conf, hadoopConf);
    rmClient.init(hadoopConf);
    rmClient.start();

    BlockingQueue<Container> launcherQueue =
        new LinkedBlockingQueue<Container>();

    boolean masterServerRelaunch = false;
    int port = Utils.getInt(storm_conf.get(Config.MASTER_THRIFT_PORT));
    Integer prePort = Util.readIdFromLocalFile(Config.MASTER_THRIFT_PORT_FILE);
    ServerSocket serverSocket = null;
    if (prePort == null) {// 第一次启动
      if (port < 0) {// allocate an appmaster thrift port
        try {
          serverSocket = new ServerSocket(0);
          port = serverSocket.getLocalPort();
          LOG.info("assignServerSocketPort port=" + port);
        } catch (IOException e) {
          throw e;
        }
        LOG.info("allocate appmaster.thrift.port=" + port);
      }
      Util.writeIdToLocalFile(Config.MASTER_THRIFT_PORT_FILE, port);
    } else {// 之前已经启动过，使用前面的port
      port = prePort.intValue();
      masterServerRelaunch = true;
    }

    MasterServer server =
        new MasterServer(storm_conf, rmClient, appAttemptID, port);
    try {
      LOG.info("now server.nimbusPort:" + server.nimbusPort
          + ", server.uiPort:" + server.uiPort);
      final String target = host + ":" + port;
      InetSocketAddress addr = NetUtils.createSocketAddr(target);
      String appTrackingUrl = host + ":" + server.uiPort;
      LOG.info("register storm ui url to AM " + appTrackingUrl);
      if (!masterServerRelaunch) {
        RegisterApplicationMasterResponse resp =
            rmClient.registerApplicationMaster(addr.getHostName(), port,
                appTrackingUrl);
        LOG.info("Got a registration response " + resp);
        LOG.info("Max Capability " + resp.getMaximumResourceCapability());
        rmClient.setMaxResource(resp.getMaximumResourceCapability());
      }

      masterScheduler = Executors.newScheduledThreadPool(8);

      // master heartbeat thread
      int masterHeartBeatInterval =
          Utils.getInt(storm_conf.get(Config.MASTER_HEARTBEAT_INTERVAL_MILLIS),
              1000);
      MasterHeartbeatThread heartbeatThread =
          new MasterHeartbeatThread(_handler, rmClient, launcherQueue);
      masterScheduler.scheduleAtFixedRate(heartbeatThread, 0,
          masterHeartBeatInterval / 1000, TimeUnit.SECONDS);

      // launcher thread
      // LauncherThread lacuncherThread =
      // new LauncherThread(rmClient, launcherQueue);
      // masterScheduler.scheduleAtFixedRate(lacuncherThread, 0, 10,
      // TimeUnit.SECONDS);

      rmClient.startAllSupervisors();

      uploadTopologyInfo(appAttemptID, storm_conf, hadoopConf, rmClient, server);

      // supervisor monitor thread
      // SupervisorMonitorThread supervisorMonitor =
      // new SupervisorMonitorThread(storm_conf, _handler, numContainerNeeded);
      // masterScheduler.scheduleAtFixedRate(supervisorMonitor, 0, 10,
      // TimeUnit.SECONDS);

      // Node Manager Checker Thread
      NMCheckerThread nmCheckerThread = new NMCheckerThread(_handler);
      masterScheduler.scheduleAtFixedRate(nmCheckerThread, 0, 60,
          TimeUnit.SECONDS);

      LOG.info("Starting Master Thrift Server");
      if (serverSocket != null) {
        serverSocket.close();
      }
      server.serve();
      LOG.info("StormAMRMClient::unregisterApplicationMaster");
      rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          "AllDone", null);
    } finally {
      if (server.isServing()) {
        LOG.info("Stop Master Thrift Server");
        server.stop();
      }
      LOG.info("Stop RM client");
      rmClient.stop();
      if (masterScheduler != null) {
        masterScheduler.shutdown();
      }
    }
    System.exit(0);
  }

  @SuppressWarnings("rawtypes")
  private static void uploadTopologyInfo(ApplicationAttemptId appAttemptID,
      Map storm_conf, YarnConfiguration hadoopConf, StormAMRMClient rmClient,
      MasterServer server) throws IOException, YarnException {
    if (new File("./UserTopology.jar").exists()
        && !server.isSubmitTopology(storm_conf, hadoopConf, appAttemptID)) {
      LOG.info("first Attempt, start UserTopologyClass");
      if (!server.startUserTopologyClass(storm_conf)) {
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.KILLED,
            "startUserClass error", null);
      } else {
        server.submitTopologyJar(storm_conf, hadoopConf, appAttemptID);
      }
    } else {
      LOG.info("Not the first Attempt skip submit UserTopologyClass");
    }
  }

  @SuppressWarnings("rawtypes")
  private boolean startUserTopologyClass(Map storm_conf) {
    Process _process = null;
    try {
      boolean nimbusReady = false;
      int numTries =
          Utils.getInt(storm_conf.get(Config.MASTER_NIMBUS_RETRY_NUM), 10);
      int tries = 0;
      LOG.info(Config.MASTER_NIMBUS_RETRY_NUM.toString() + " = " + numTries);
      while (!nimbusReady && tries < numTries) {
        String host =
            (String) storm_conf.get(backtype.storm.Config.NIMBUS_HOST);
        int port =
            Utils.getInt(storm_conf
                .get(backtype.storm.Config.NIMBUS_THRIFT_PORT));
        if (port == -1) {
          port = (int) nimbusPort;
        }
        try {
          Socket socket = new Socket(host, port);
          socket.close();
          LOG.info("Nimbus now available: " + host + ":" + port);
          nimbusReady = true;
        } catch (Exception ex) {
          LOG.warn("Failed to connect to nimbus at " + host + ":" + port
              + ", retrying");
          Thread.sleep(1000);
          tries = tries + 1;

        }
      }
      if (!nimbusReady) {
        return false;
      }
      int sleepSec =
          Utils.getInt(storm_conf.get(Config.MASTER_SUBMIT_SLEEP_SECS), 30);
      LOG.info("Sleep " + sleepSec + " sec, before summit topology");
      Thread.sleep(sleepSec * 1000);
      String topologyMainClass = (String) storm_conf.get("topologyMainClass");
      String userArg = (String) storm_conf.get("userArgs");
      List<String> toRet =
          Util.buildUserTopologyCommands(storm_conf, topologyMainClass, userArg);
      // toRet.add("1>" + System.getenv("STORM_LOG_DIR") +
      // "/UserTopology_stderr");
      // toRet.add("2>" + System.getenv("STORM_LOG_DIR") +
      // "/UserTopology_stdout");
      LOG.info("start UserTopologyClass args: " + toRet);
      StringBuffer sb = new StringBuffer();
      for (String arg : toRet) {
        sb.append(arg).append(" ");
      }
      ProcessBuilder builder = new ProcessBuilder(toRet);
      _process = builder.start();
      Util.redirectStreamAsync(_process.getErrorStream(), System.out);
      Util.redirectStreamAsync(_process.getInputStream(), System.out);
      int exitCode = _process.waitFor();
      if (exitCode != 0) {
        LOG.error("startUserTopologyClass  failed, exitCode= " + exitCode);
        return false;
      }
    } catch (Exception ex) {
      LOG.error("startUserTopologyClass  failed:", ex);
    } finally {
      if (_process != null) {
        _process.destroy();
      }
    }
    return true;
  }

  @SuppressWarnings("rawtypes")
  public void submitTopologyJar(Map storm_conf, YarnConfiguration hadoopConf,
      ApplicationAttemptId appAttemptId) throws IOException {
    LOG.info("submitTopologyJar " + appAttemptId.toString());
    File stormDist =
        new File((String) storm_conf.get("storm.local.dir")
            + "/nimbus/stormdist/");
    FileSystem fs = FileSystem.get(hadoopConf);
    String appHome =
        Util.getApplicationHomeForId(appAttemptId.getApplicationId().toString());
    Path stormHdfs =
        new Path(fs.getHomeDirectory(), appHome + Path.SEPARATOR + "stormdist");
    if (stormDist.exists()) {
      if (!fs.exists(stormHdfs)) {
        fs.mkdirs(stormHdfs);
      }
      File[] files = stormDist.listFiles();
      for (int i = 0; i < files.length; i++) {
        File topologyPath = files[i];
        if (topologyPath.isDirectory()) {
          Path topologyHdfs = new Path(stormHdfs, topologyPath.getName());
          if (!fs.exists(topologyHdfs)) {
            String topologyPathName = topologyPath.toString();
            Path src = new Path(topologyPathName);
            fs.copyFromLocalFile(false, true, src, topologyHdfs);
            LOG.info("Copy topology jar from " + topologyPathName
                + " and add to " + topologyHdfs.toString());
          }
        }
      }
    } else {
      throw new IOException("path " + stormDist.toString() + "is not exist.");
    }
  }

  @SuppressWarnings("rawtypes")
  public boolean isSubmitTopology(Map storm_conf, YarnConfiguration hadoopConf,
      ApplicationAttemptId appAttemptId) throws IOException {
    LOG.info("isSubmitTopology " + appAttemptId.toString());
    FileSystem fs = FileSystem.get(hadoopConf);
    String appHome =
        Util.getApplicationHomeForId(appAttemptId.getApplicationId().toString());
    Path stormHdfs =
        new Path(fs.getHomeDirectory(), appHome + Path.SEPARATOR + "stormdist");
    if (fs.exists(stormHdfs)) {
      return true;
    } else {
      return false;
    }
  }

  @SuppressWarnings("rawtypes")
  public void downloadTopologyInfo(Map storm_conf,
      YarnConfiguration hadoopConf, ApplicationAttemptId appAttemptId)
      throws IOException {
    LOG.info("downloadTopologyInfo appid: " + appAttemptId.toString());
    File stormDist =
        new File((String) storm_conf.get("storm.local.dir")
            + "/nimbus/stormdist/");
    FileSystem fs = FileSystem.get(hadoopConf);
    String appHome =
        Util.getApplicationHomeForId(appAttemptId.getApplicationId().toString());
    Path stormHdfs =
        new Path(fs.getHomeDirectory(), appHome + Path.SEPARATOR + "stormdist");
    if (fs.exists(stormHdfs)) {
      if (!stormDist.exists()) {
        stormDist.mkdirs();
      }
      FileStatus[] topologyPaths = fs.listStatus(stormHdfs);
      for (int i = 0; i < topologyPaths.length; i++) {
        FileStatus topology = topologyPaths[i];
        String topologyLocal = stormDist + "/" + topology.getPath().getName();
        File topLocalFile = new File(topologyLocal);
        Util.copyToLocalFile(fs, topology.getPath(), topLocalFile);
        LOG.info("Copy topology jar from " + topology.getPath().toString()
            + " and add to " + topologyLocal);
      }
    }
  }

  public MasterServer(@SuppressWarnings("rawtypes") Map storm_conf,
      StormAMRMClient client) {
    this(storm_conf, new StormMasterServerHandler(storm_conf, client), null);
  }

  public MasterServer(@SuppressWarnings("rawtypes") Map storm_conf,
      StormAMRMClient client, ApplicationAttemptId appAttemptID, int port) {
    this(storm_conf, new StormMasterServerHandler(storm_conf, client),
        appAttemptID, port);
  }

  private MasterServer(@SuppressWarnings("rawtypes") Map storm_conf,
      StormMasterServerHandler handler, ApplicationAttemptId appAttemptID) {
    this(storm_conf, handler, appAttemptID, Utils.getInt(storm_conf
        .get(Config.MASTER_THRIFT_PORT)));
  }

  @SuppressWarnings({ "rawtypes" })
  private MasterServer(Map storm_conf, StormMasterServerHandler handler,
      ApplicationAttemptId appAttemptID, int port) {
    super(storm_conf, new Processor<StormMaster.Iface>(handler),
        ThriftConnectionType.NIMBUS);
    this.appAttemptID = appAttemptID;
    try {
      this.host = InetAddress.getLocalHost().getHostName();
      boolean firstAttempt = false;
      if (appAttemptID != null) {
        firstAttempt = appAttemptID.toString().endsWith("000001");
        if (!firstAttempt) {// before restart nimbus, need to download topology
                            // infos
          downloadTopologyInfo(storm_conf, new YarnConfiguration(),
              appAttemptID);
        }
      }

      _handler = handler;
      _handler.init(this);

      LOG.info("launch nimbus");
      _handler.startNimbus();

      LOG.info("launch ui");
      _handler.startUI();

      boolean drpcEnable =
          ServerUtils.parseBoolean(
              storm_conf.get(Config.MASTER_STORM_DRPC_SERVICE_ENABLE), false);
      if (drpcEnable) {
        LOG.info("launch drpc service");
        _handler.startDrpc();
      }

      getPortInfo(storm_conf);

      final Map _conf = storm_conf;
      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          scheduleLogic(_conf);
        }
      }, delay, period);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("rawtypes")
  private void getPortInfo(Map storm_conf) throws KeeperException,
      InterruptedException, IOException {
    ServerInfo serverInfo = null;
    ServerInfo UIServerInfo = null;

    while (true) {
      try {
        StormZkClusterState clusterState = new StormZkClusterState(storm_conf);
        serverInfo = clusterState.getLeaderHost();
        UIServerInfo = clusterState.getUIServerInfo();
        if (serverInfo == null || UIServerInfo == null) {
          throw new Exception("Nimbus or UI  ServerInfo is null");
        }
        break;
      } catch (Exception e) {
        LOG.info(e.getMessage() + " Wait 500ms ");
        Thread.sleep(500);
        continue;
      }
    }
    LOG.info("serverInfo: " + serverInfo.toString());
    this.nimbusPort = serverInfo.getPort();
    this.uiPort = UIServerInfo.getPort();
  }

  /**
   * 
   * @param _conf
   */
  @SuppressWarnings("rawtypes")
  private void scheduleLogic(Map storm_conf) {
    // logic 1
    watchNimbus(storm_conf);
    // TODO logic 2, worker disaster recovery solutions through dynamic
    // allocation
    // workerLack(storm_conf);
  }

  public static Map<String, Integer> topology2WokerNum =
      new HashMap<String, Integer>();

  public long getNimbusPort() {
    return nimbusPort;
  }

  public void setNimbusPort(long nimbusPort) {
    this.nimbusPort = nimbusPort;
  }

  /**
   * 定期调度nimbus的进程存在与否
   * 
   * @return
   */
  @SuppressWarnings("rawtypes")
  private boolean watchNimbus(Map storm_conf) {
    try {
      // nimbus and ui must be start together, because ui must to be nimbus's
      // port
      if (!_handler.getNimbusProcess().isAlive()) {
        _handler.startNimbus();
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return false;
  }

  public void killTopology() throws NotAliveException, TException {
    NimbusClient client =
        NimbusClient.getConfiguredClient(_handler.get_storm_conf());
    File stormDist =
        new File((String) _handler.get_storm_conf().get("storm.local.dir")
            + "/nimbus/stormdist/");
    if (stormDist.exists()) {
      File[] files = stormDist.listFiles();
      for (int i = 0; i < files.length; i++) {
        File topologyPath = files[i];
        if (topologyPath.isDirectory()) {
          String topologyId = topologyPath.getName();
          TopologyInfo topologyInfo =
              client.getClient().getTopologyInfo(topologyId);
          client.getClient().killTopology(topologyInfo.get_name());
        }
      }
    }

  }

  public void stop() {
    super.stop();
    if (_handler != null) {
      _handler.stop();
      _handler = null;
    }
    if (timer != null) {
      timer.cancel();
    }
  }
}
