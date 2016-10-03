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
package com.tencent.jstorm.daemon.nimbus;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.cluster.IStormClusterState;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.Nimbus.Iface;
import org.apache.storm.scheduler.INimbus;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.storm.security.auth.ThriftServer;
import org.apache.storm.utils.ConfigUtils;
import org.apache.storm.utils.Utils;
import org.apache.storm.utils.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.CoreUtil;

@ClojureClass(className = "backtype.storm.daemon.nimbus")
public class NimbusServer {
  private static final Logger LOG = LoggerFactory.getLogger(NimbusServer.class);
  private NimbusData data;
  private ServiceHandler serviceHandler;
  private ThriftServer server;

  @ClojureClass(className = "backtype.storm.daemon.nimbus#main")
  public static void main(String[] args) {
    Utils.setupDefaultUncaughtExceptionHandler();
    NimbusServer instance = new NimbusServer();
    INimbus iNimbus = new StandaloneNimbus();
    try {
      instance.launch(iNimbus);
    } catch (Exception e) {
      LOG.error(CoreUtil.stringifyError(e));
      e.printStackTrace();
      System.exit(-1);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#-launch")
  private void launch(INimbus inimbus) throws Exception {
    Map conf = ConfigUtils.readStormConfig();
    Map authConf = ConfigUtils.readYamlConfig("storm-cluster-auth.yaml", false);
    if (authConf != null) {
      conf.putAll(authConf);
    }
    launchServer(conf, inimbus);
  }

  @SuppressWarnings({ "rawtypes" })
  @ClojureClass(className = "backtype.storm.daemon.nimbus#launch-server!")
  public ServiceHandler launchServer(final Map conf, final INimbus inimbus)
      throws Exception {
    if (ConfigUtils.isLocalMode(conf)) {
      throw new IllegalArgumentException("Cannot start server in local mode!");
    }
    validatePortAvailable(conf);
    Utils.addShutdownHookWithForceKillIn1Sec(new Runnable() {
      @Override
      public void run() {
        if (serviceHandler != null) {
          serviceHandler.shutdown();
        }
        if (server != null) {
          server.stop();
        }
      }

    });
    LOG.info(
        "Starting nimbus server for storm version " + VersionInfo.getVersion());
    serviceHandler = new ServiceHandler(conf, inimbus);
    server = new ThriftServer(conf, new Nimbus.Processor<Iface>(serviceHandler),
        ThriftConnectionType.NIMBUS);
    server.serve();
    return serviceHandler;
  }

  @SuppressWarnings("rawtypes")
  @ClojureClass(className = "backtype.storm.daemon.nimbus#validate-port-available")
  public static void validatePortAvailable(Map conf) {
    int port = Utils.getInt(conf.get(Config.NIMBUS_THRIFT_PORT), -1);
    try {
      ServerSocket socket = new ServerSocket(port);
      socket.close();
    } catch (IOException e) {
      if (e instanceof BindException) {
        LOG.error(
            port + " is not available. Check if another process is already listening on "
                + port);
      } else {
        LOG.error(port + " is not available.", e);
      }

      System.exit(0);
    }
  }

  public IStormClusterState getStormClusterState() {
    return data.getStormClusterState();
  }

  public ServiceHandler getServiceHandler() {
    return serviceHandler;
  }

  public void cleanUpWhenMasterChanged() {
    try {
      if (serviceHandler != null) {
        serviceHandler.shutdown();
        serviceHandler = null;
      }
      if (server != null) {
        server.stop();
        server = null;
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      System.exit(-1);
    }
  }

}
