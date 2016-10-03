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
package org.apache.storm.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.storm.Config;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.DRPCExecutionException;
import org.apache.storm.generated.DistributedRPC;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.daemon.drpc.DRPCServerInfo;
import com.tencent.jstorm.utils.CoreUtil;

public class DRPCClient extends ThriftClient implements DistributedRPC.Iface {
    private static final Logger LOG = LoggerFactory.getLogger(DRPCClient.class);

    private TTransport conn;
    private DistributedRPC.Client client;
    private String host;
    private int port;
    private Integer timeout;

    public DRPCClient(Map conf, String host, int port) throws TTransportException {
        this(conf, host, port, null);
        _retryForever = true;
    }

    @ClojureClass(className="add for method getConfiguredClientAs, choose if retry for ever")
    public DRPCClient(Map conf, String host, int port, boolean retryForever) throws TTransportException {
        this(conf, host, port, null);
        _retryForever = retryForever;
    }

    public DRPCClient(Map conf, String host, int port, Integer timeout) throws TTransportException {
        super(conf, ThriftConnectionType.DRPC, host, port, timeout, null);
        this.host = host;
        this.port = port;
        this.client = new DistributedRPC.Client(_protocol);
        _retryForever = true;
    }
        
    public String getHost() {
        return host;
    }
    
    public int getPort() {
        return port;
    }
    
    public String execute(String func, String args) throws TException, DRPCExecutionException, AuthorizationException {
        return client.execute(func, args);
    }

    public DistributedRPC.Client getClient() {
        return client;
    }

    @ClojureClass(className = "added by JStorm developer, get drpc server ids from zk")
    public static DRPCClient getConfiguredClientAs(Map conf) {
      List<String> drpcServerIds = getDrpcServerHostportsFromZK(conf);

      for (String hostport : drpcServerIds) {
        String host = hostport.split(":")[0];
        int port = Integer.parseInt(hostport.split(":")[1]);
        DRPCClient client = null;
        try {
          client = new DRPCClient(conf, host, port, false);
          return client;
        } catch (Exception e) {
          LOG.warn(
              "Ignoring exception while trying to connect drpc server from "
                  + hostport + ". will retry with a different drpc server id.",
              e);
          continue;
        }
      }
      throw new RuntimeException(
          "Failed to create a drpc client for the drpc server ids " + drpcServerIds);
    }

    @SuppressWarnings("unchecked")
    @ClojureClass(className = "get drpc server ids hostport from zookeeper.")
    public static List<String> getDrpcServerHostportsFromZK(Map conf) {
        LOG.debug("Get drpc server ids hostports from zookeeper");
        List<String> rets = new ArrayList<String>();

        List<String> zkServers =
            (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
        Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
        ZookeeperAuthInfo zkAuthInfo = new ZookeeperAuthInfo(conf);
        CuratorFramework zkClient = Utils.newCurator(conf, zkServers, port,
            (String) conf.get(Config.STORM_ZOOKEEPER_ROOT), zkAuthInfo);
        zkClient.start();

        try {

          if (zkClient.checkExists()
              .forPath(ClusterUtils.DRPC_SERVER_SUBTREE) == null) {
            throw new RuntimeException("Could not find "
                + ClusterUtils.DRPC_SERVER_SUBTREE + " from zookeeper :" + zkServers);
          }
          rets = zkClient.getChildren().forPath(ClusterUtils.DRPC_SERVER_SUBTREE);
        } catch (Throwable t) {
          LOG.error("Get drpc server ids hostports from zookeeper failed : {}",
              CoreUtil.stringifyError(t));
        } finally {
          if (zkClient != null) {
            zkClient.close();
          }
        }
        return rets;
    }

    @ClojureClass(className = "get drpc server info from zookeeper.")
    public static List<DRPCServerInfo> getDrpcServerInfosFromZK(Map conf) {
      List<DRPCServerInfo> rets = new ArrayList<DRPCServerInfo>();
      List<String> drpcServerIds = getDrpcServerHostportsFromZK(conf);
      for(String id :drpcServerIds)
      {
        rets.add(DRPCServerInfo.parse(id));
      }
      return rets;
    }
}
