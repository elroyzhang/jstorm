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
package com.tencent.jstorm.daemon.logviewer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

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

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.logviewer.jetty.JettyEmbeddedServer;
import com.tencent.jstorm.daemon.nimbus.NimbusUtils;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.NetWorkUtils;

/**
 * LogViewer
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 */
@ClojureClass(className = "org.apache.storm.daemon.logviewer")
public class LogViewer {
  private static Logger LOG = LoggerFactory.getLogger(LogViewer.class);
  /** added by JStorm developer */
  private List<ACL> acls = null;
  private IStormClusterState stormClusterState;
  private static String proxyPath = null;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.logviewer#start-logviewer")
  private void startLogViewer(Map conf, String logRootDir,
      String daemonlogRootDir) throws IOException {
    Integer logviewerPort = Utils.getInt(conf.get(Config.LOGVIEWER_PORT), 8000);
    String logViewHost = InetAddress.getLocalHost().getHostName().toString();
    int logViewPort = NetWorkUtils.assignServerPort(logviewerPort);
    InetSocketAddress logViewAddress = NetWorkUtils
        .createSocketAddr(logViewHost + ":" + String.valueOf(logViewPort));

    /** Added by JStorm developer begin */
    setVirtualRealPort(conf, logviewerPort);
    /** Added by JStorm developer end */
    // don't commit before it can run
//    try {
//      int headerBufferSize = (int) (conf.get(Config.UI_HEADER_BUFFER_BYTES));
//      String filterClass = (String) (conf.get(Config.UI_FILTER));
//      Map<String, String> filterParams =
//          (Map<String, String>) (conf.get(Config.UI_FILTER_PARAMS)) == null
//              ? new HashMap<String, String>()
//              : (Map<String, String>) (conf.get(Config.UI_FILTER_PARAMS));
//      List<FilterConfiguration> filterConfs =
//          new ArrayList<FilterConfiguration>();
//      if (!StringUtils.isEmpty(filterClass)) {
//        filterConfs.add(new FilterConfiguration(filterParams, filterClass));
//      }
//      final LogviewerServlet logRoutesServlet = LogViewerUtils
//          .confMiddleware(new LogRoutesServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet dumpsServlet = LogViewerUtils
//          .confMiddleware(new DumpsServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet daemonLogServlet = LogViewerUtils
//          .confMiddleware(new DaemonLogServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet downloadServlet = LogViewerUtils
//          .confMiddleware(new DownloadServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet daemonDownloadServlet =
//          LogViewerUtils.confMiddleware(new DaemonDownloadServlet(), logRootDir,
//              daemonlogRootDir);
//      final LogviewerServlet searchServlet = LogViewerUtils
//          .confMiddleware(new SearchServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet deepSearchServlet = LogViewerUtils.confMiddleware(
//          new DeepSearchServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet searchLogsServlet = LogViewerUtils.confMiddleware(
//          new SearchLogsServlet(), logRootDir, daemonlogRootDir);
//      final LogviewerServlet listLogsServlet = LogViewerUtils
//          .confMiddleware(new ListLogsServlet(), logRootDir, daemonlogRootDir);
//      filterConfs
//          .add(new FilterConfiguration("org.eclipse.jetty.servlets.GzipFilter",
//              "Gzipper", new HashMap<String, String>()));
//      final Integer httpsPort =
//          Utils.getInt(conf.get(Config.LOGVIEWER_HTTPS_PORT), 0);
//      final String httpsKsPath =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_KEYSTORE_PATH));
//      final String httpsKsPassword =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_KEYSTORE_PASSWORD));
//      final String httpsKsType =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_KEYSTORE_TYPE));
//      final String httpsKeyPassword =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_KEY_PASSWORD));
//      final String httpsTsPath =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_TRUSTSTORE_PATH));
//      final String httpsTsPassword =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_TRUSTSTORE_PASSWORD));
//      final String httpsTsType =
//          (String) (conf.get(Config.LOGVIEWER_HTTPS_TRUSTSTORE_TYPE));
//      final Boolean httpsWantClientAuth =
//          (Boolean) (conf.get(Config.LOGVIEWER_HTTPS_WANT_CLIENT_AUTH));
//      final Boolean httpsNeedClientAuth =
//          (Boolean) (conf.get(Config.LOGVIEWER_HTTPS_NEED_CLIENT_AUTH));
//
//      UIHelpers.stormRunJetty(logviewerPort, new IConfigurator() {
//        @Override
//        public void execute(Server server) {
//          UIHelpers.configSsl(server, httpsPort, httpsKsPath, httpsKsPassword,
//              httpsKsType, httpsKeyPassword, httpsTsPath, httpsTsPassword,
//              httpsTsType, httpsNeedClientAuth, httpsWantClientAuth);
//          UIHelpers.configFilter(server, new DefaultServlet(), filterConfs);
//          ServletContextHandler context =
//              (ServletContextHandler) server.getHandlers()[0];
//          ServletHolder route = new ServletHolder(logRoutesServlet);
//          ServletHolder dumps = new ServletHolder(dumpsServlet);
//          ServletHolder daemonlog = new ServletHolder(daemonLogServlet);
//          ServletHolder download = new ServletHolder(downloadServlet);
//          ServletHolder daemondownload =
//              new ServletHolder(daemonDownloadServlet);
//          ServletHolder search = new ServletHolder(searchServlet);
//          ServletHolder deepSearch = new ServletHolder(deepSearchServlet);
//          ServletHolder searchLogs = new ServletHolder(searchLogsServlet);
//          ServletHolder listLogs = new ServletHolder(listLogsServlet);
//          context.addServlet(route, "/log");
//          context.addServlet(dumps, "/dumps");
//          context.addServlet(daemonlog, "/daemonlog");
//          context.addServlet(download, "/download");
//          context.addServlet(daemondownload, "/daemondownload");
//          context.addServlet(search, "/search");
//          context.addServlet(deepSearch, "/deepSearch");
//          context.addServlet(searchLogs, "/searchLogs");
//          context.addServlet(listLogs, "/listLogs");
//
//          ResourceHandler resourceHandler = new ResourceHandler();
//          resourceHandler.setDirectoriesListed(true);
//          String stormHome = System.getProperty("storm.home");
//          resourceHandler.setResourceBase(
//              stormHome + Utils.FILE_PATH_SEPARATOR + "public");
//          resourceHandler.setStylesheet("");
//          HandlerList handlers = new HandlerList();
//          handlers.setHandlers(new Handler[] { context, resourceHandler });
//          server.setHandler(handlers);
//        }
//      });
//    } catch (Exception e) {
//      LOG.error(CoreUtil.stringifyError(e));
//    }

    JettyEmbeddedServer server =
        new JettyEmbeddedServer("logviewer", logViewAddress.getHostName(),
            logViewAddress.getPort(), logViewAddress.getPort() == 0, conf);
    server.start();
    LOG.info("LogViewer {}", server.toString());
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.logviewer#main")
  public static void main(String[] args) {
    Utils.setupDefaultUncaughtExceptionHandler();
    Map conf = Utils.readStormConfig();
    String logRoot = ConfigUtils.workerArtifactsRoot(conf);
    String daemonLogRoot = LogViewerUtils
        .logRootDir((String) conf.get(Config.LOGVIEWER_APPENDER_NAME));
    LogViewer viewer = new LogViewer();
    try {
      viewer.startLogViewer(conf, logRoot, daemonLogRoot);
      LOG.info("Starting logviewer server for storm version '"
          + VersionInfo.getVersion() + "'");
    } catch (IOException e) {
      LOG.error(CoreUtil.stringifyError(e));
      e.printStackTrace();
    }
  }

  public static String getProxyPath() {
    return proxyPath;
  }

  public static void setProxyPath(String proxyPath) {
    LogViewer.proxyPath = proxyPath;
  }

  private void setVirtualRealPort(Map conf, int infoPort) {
    int realLvPort = -1;
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
        realLvPort = virtualToRealPort.get(infoPort);

        NodeInfo realHostPorts = new NodeInfo();
        String lvHost = InetAddress.getLocalHost().getCanonicalHostName();
        if (conf.containsKey(Config.STORM_LOCAL_HOSTNAME)) {
          lvHost = conf.get(Config.STORM_LOCAL_HOSTNAME).toString();
          LOG.info("Overriding logviewer host to storm.local.hostname -> {}",
              lvHost);
        }
        realHostPorts.set_node(lvHost);
        for (Integer realPort : virtualToRealPort.values()) {
          realHostPorts.add_to_port(realPort.longValue());
        }
        LOG.info("logviewer {} {}", lvHost + ":" + realLvPort, realHostPorts);
        stormClusterState.addLogviewer(lvHost + ":" + realLvPort,
            realHostPorts);

        /** FOR logviewer in docker Gaia */
        setProxyPath("port_" + realLvPort);
      }
    } catch (Exception e) {
      LOG.error("Start logviewer failed! {}", CoreUtil.stringifyError(e));
      throw new RuntimeException(e);
    }
  }
}
