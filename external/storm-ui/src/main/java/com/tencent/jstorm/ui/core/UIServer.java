package com.tencent.jstorm.ui.core;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.storm.Config;
import org.apache.storm.cluster.ClusterStateContext;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.cluster.DaemonType;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.apache.zookeeper.data.ACL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.ui.core.callback.CheckLogViewerChanged;
import com.tencent.jstorm.ui.core.callback.CheckNimbusesChanged;
import com.tencent.jstorm.ui.core.callback.CheckSupervisorsChanged;
import com.tencent.jstorm.ui.jetty.JettyEmbeddedServer;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.NetWorkUtils;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
@ClojureClass(className = "org.apache.storm.ui.core#-main")
public class UIServer {
  private static final Logger LOG = LoggerFactory.getLogger(UIServer.class);

  private InetSocketAddress httpAddress = null;
  private JettyEmbeddedServer httpServer;
  @SuppressWarnings("rawtypes")
  private static Map conf = ConfigUtils.readStormConfig();
  private List<ACL> acls;
  private IStormClusterState stormClusterState;
  private static ConcurrentHashMap<String, Map<Long, String>> hostToRealPortsToLvPort =
      new ConcurrentHashMap<String, Map<Long, String>>();
  private static ConcurrentHashMap<String, Long> supervisorIdToFirstSlot =
      new ConcurrentHashMap<String, Long>();

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "org.apache.storm.ui.core#start-server!")
  public void startServer() throws IOException {
    httpAddress = getHttpServerAddress();
    String infoHost = httpAddress.getHostName();
    int infoPort = httpAddress.getPort();

    httpServer = new JettyEmbeddedServer("storm-ui", infoHost, infoPort,
        infoPort == 0, conf);
    httpServer.setAttribute("ui.port", infoPort);
    try {
      if (Utils.isZkAuthenticationConfiguredStormServer(conf)) {
        this.acls = NimbusUtils.nimbusZKAcls();
      }
      stormClusterState = ClusterUtils.mkStormClusterState(conf, acls,
          new ClusterStateContext(DaemonType.UNKNOWN));
      // added by JStorm developer, For docker
      Map<Integer, Integer> virtualToRealPort =
          (Map<Integer, Integer>) conf.get("storm.virtual.real.ports");
      if (virtualToRealPort != null
          && virtualToRealPort.containsKey(infoPort)) {
        LOG.info("Use {} ---> {}", infoPort, virtualToRealPort.get(infoPort));
        infoPort = virtualToRealPort.get(infoPort);
      }
      stormClusterState.registerUIServerInfo(
          new NodeInfo(infoHost, Sets.newHashSet(Long.valueOf(infoPort))));
    } catch (Exception e) {
      throw new IOException(e);
    }

    // check logviewer for log-links on UI
    CheckLogViewerChanged checkLogViewerChanged =
        new CheckLogViewerChanged(this);
    // in case of UIServer cache miss some data, #54
    checkLogViewerChanged.establishLogviewerCallback();
    checkLogViewerChanged.run();
    // check supervisor for log-links on UI
    CheckSupervisorsChanged checkSupervisorsChanged =
        new CheckSupervisorsChanged(this);
    checkSupervisorsChanged.establishSupervisorsCallback();
    checkSupervisorsChanged.run();
    // check nimbuses for NimbusClient seeds
    CheckNimbusesChanged checkNimbusesChanged = new CheckNimbusesChanged(this);
    checkNimbusesChanged.establishNimbusesCallback();
    checkNimbusesChanged.run();

    httpServer.start();
    LOG.info(httpServer.toString());
  }

  @SuppressWarnings("unchecked")
  private static InetSocketAddress getHttpServerAddress() throws IOException {
    String uiHost = Utils.localHostname();
    int uiPort = Utils.getInt(conf.get(Config.UI_PORT), 8081);

    uiPort = NetWorkUtils.assignServerPort(uiPort);
    conf.put(Config.UI_PORT, uiPort);

    return NetWorkUtils.createSocketAddr(uiHost + ":" + String.valueOf(uiPort));
  }

  public static void main(String[] args) {
    LOG.info("Starting ui server for storm version '" + VersionInfo.getVersion()
        + "'");
    UIServer server = new UIServer();
    try {
      server.startServer();
    } catch (IOException e) {
      LOG.error(CoreUtil.stringifyError(e));
      e.printStackTrace();
    }
  }

  public IStormClusterState getStormClusterState() {
    return stormClusterState;
  }

  public static ConcurrentHashMap<String, Map<Long, String>> getHostToRealPortsToLvPort() {
    return hostToRealPortsToLvPort;
  }

  public static void resetHostToRealPortsToLvPort(
      Map<String, Map<Long, String>> hostToRealPortsToLvP) {
    hostToRealPortsToLvPort.clear();
    hostToRealPortsToLvPort.putAll(hostToRealPortsToLvP);
  }

  public static ConcurrentHashMap<String, Long> getSupervisorIdToFirstSlot() {
    return supervisorIdToFirstSlot;
  }

  public static void resetSupervisorIdToFirstSlot(
      Map<String, Long> sIdToSlots) {
    supervisorIdToFirstSlot.clear();
    supervisorIdToFirstSlot.putAll(sIdToSlots);
  }

}
