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

import java.security.Principal;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.curator.framework.CuratorFramework;
import org.apache.storm.Config;
import org.apache.storm.cluster.ClusterUtils;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.NimbusSummary;
import org.apache.storm.security.auth.ReqContext;
import org.apache.storm.security.auth.ThriftClient;
import org.apache.storm.security.auth.ThriftConnectionType;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.CoreUtil;

public class NimbusClient extends ThriftClient implements AutoCloseable {
  private Nimbus.Client _client;
  private static final Logger LOG = LoggerFactory.getLogger(NimbusClient.class);

  @ClojureClass(className = "add by JStorm developer, cache seeds for UI")
  private static ConcurrentSkipListSet<String> nimbusHostPorts =
      new ConcurrentSkipListSet<String>();
  
  public interface WithNimbus {
    public void run(Nimbus.Client client) throws Exception;
  }

  public static void withConfiguredClient(WithNimbus cb) throws Exception {
    withConfiguredClient(cb, ConfigUtils.readStormConfig());
  }

  public static void withConfiguredClient(WithNimbus cb, Map conf)
      throws Exception {
    ReqContext context = ReqContext.context();
    Principal principal = context.principal();
    String user = principal == null ? null : principal.getName();
    try (NimbusClient client = getConfiguredClientAs(conf, user);) {
      cb.run(client.getClient());
    }
  }

  public static NimbusClient getConfiguredClient(Map conf) {
    return getConfiguredClientAs(conf, null);
  }

  @ClojureClass(className = "added by JStorm developer, 1 don't use getCLusterInfo ,cost too much, 2 get nimbus hosts from zk")
  public static NimbusClient getConfiguredClientAs(Map conf, String asUser) {
    if (conf.containsKey(Config.STORM_DO_AS_USER)) {
      if (asUser != null && !asUser.isEmpty()) {
        LOG.warn(
            "You have specified a doAsUser as param {} and a doAsParam as config, config will take precedence.",
            asUser, conf.get(Config.STORM_DO_AS_USER));
      }
      asUser = (String) conf.get(Config.STORM_DO_AS_USER);
    }

    Set<String> seeds = new HashSet<String>();
    seeds.addAll(getNimbusHostports(conf));
    // if (conf.containsKey(Config.NIMBUS_HOST)) {
    // LOG.warn(
    // "Using deprecated config {} for backward compatibility. Please update
    // your storm.yaml so it only has config {}",
    // Config.NIMBUS_HOST, Config.NIMBUS_SEEDS);
    // seeds = Lists.newArrayList(conf.get(Config.NIMBUS_HOST).toString());
    // } else {
    // seeds = (List<String>) conf.get(Config.NIMBUS_SEEDS);
    // }

    /** added by JStorm developer */
    for (String hostport : seeds) {
      String host = hostport.split(":")[0];
      int port = Integer.parseInt(hostport.split(":")[1]);
      NimbusSummary leader;
      NimbusClient client = null;
      try {
        client = new NimbusClient(conf, host, port, null, asUser);
        leader = client.getClient().getNimbusLeader();
      } catch (Exception e) {
        LOG.warn(
            "Ignoring exception while trying to get leader nimbus info from "
                + host + ". will retry with a different seed host.",
            e);
        nimbusHostPorts.clear();
        continue;
      } finally {
        if (client != null) {
          client.close();
        }
      }
      try {
        return new NimbusClient(conf, leader.get_host(), leader.get_port(),
            null, asUser);
      } catch (TTransportException e) {
        String leaderNimbus = leader.get_host() + ":" + leader.get_port();
        nimbusHostPorts.clear();
        throw new RuntimeException(
            "Failed to create a nimbus client for the leader " + leaderNimbus,
            e);
      }
    }

    /*
     * for (String host : seeds) { int port =
     * Integer.parseInt(conf.get(Config.NIMBUS_THRIFT_PORT).toString());
     * ClusterSummary clusterInfo; try { NimbusClient client = new
     * NimbusClient(conf, host, port, null, asUser); clusterInfo =
     * client.getClient().getClusterInfo(); } catch (Exception e) { LOG.warn(
     * "Ignoring exception while trying to get leader nimbus info from " + host
     * + ". will retry with a different seed host.", e); continue; }
     * List<NimbusSummary> nimbuses = clusterInfo.get_nimbuses(); if (nimbuses
     * != null) { for (NimbusSummary nimbusSummary : nimbuses) { if
     * (nimbusSummary.is_isLeader()) { try { return new NimbusClient(conf,
     * nimbusSummary.get_host(), nimbusSummary.get_port(), null, asUser); }
     * catch (TTransportException e) { String leaderNimbus =
     * nimbusSummary.get_host() + ":" + nimbusSummary.get_port(); throw new
     * RuntimeException("Failed to create a nimbus client for the leader " +
     * leaderNimbus, e); } } } throw new NimbusLeaderNotFoundException(
     * "Found nimbuses " + nimbuses +
     * " none of which is elected as leader, please try " +
     * "again after some time."); } }
     */
    throw new NimbusLeaderNotFoundException(
        "Could not find leader nimbus from zookeeper nimbus hosts " + seeds
            + ". " + "Did you start a nimbus already?");
  }

  public NimbusClient(Map conf, String host, int port)
      throws TTransportException {
    this(conf, host, port, null, null);
  }

  public NimbusClient(Map conf, String host, int port, Integer timeout)
      throws TTransportException {
    super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, null);
    _client = new Nimbus.Client(_protocol);
  }

  public NimbusClient(Map conf, String host, Integer port, Integer timeout,
      String asUser) throws TTransportException {
    super(conf, ThriftConnectionType.NIMBUS, host, port, timeout, asUser);
    _client = new Nimbus.Client(_protocol);
  }

  public NimbusClient(Map conf, String host) throws TTransportException {
    super(conf, ThriftConnectionType.NIMBUS, host, null, null, null);
    _client = new Nimbus.Client(_protocol);
  }

  public Nimbus.Client getClient() {
    return _client;
  }

  @SuppressWarnings("unchecked")
  @ClojureClass(className = "get nimbuses hostport from zookeeper.")
  public static Set<String> getNimbusHostportsFromZK(Map conf) {
    LOG.debug("Get nimbus hostports from zookeeper");
    Set<String> seeds = new HashSet<String>();

    List<String> zkServers =
        (List<String>) conf.get(Config.STORM_ZOOKEEPER_SERVERS);
    Object port = conf.get(Config.STORM_ZOOKEEPER_PORT);
    ZookeeperAuthInfo zkAuthInfo = new ZookeeperAuthInfo(conf);
    CuratorFramework zkClient = Utils.newCurator(conf, zkServers, port,
        (String) conf.get(Config.STORM_ZOOKEEPER_ROOT), zkAuthInfo);
    zkClient.start();

    try {

      if (zkClient.checkExists()
          .forPath(ClusterUtils.NIMBUSES_SUBTREE) == null) {
        throw new NimbusLeaderNotFoundException("Could not find "
            + ClusterUtils.NIMBUSES_SUBTREE + " from zookeeper :" + zkServers);
      }
      List<String> nimbusIds =
          zkClient.getChildren().forPath(ClusterUtils.NIMBUSES_SUBTREE);

      for (String nimbusId : nimbusIds) {
        seeds.add(nimbusId);
      }
    } catch (Throwable t) {
      LOG.error("Get nimbus hostports from zookeeper failed : {}",
          CoreUtil.stringifyError(t));
    } finally {
      if (zkClient != null) {
        zkClient.close();
      }
    }
    return seeds;
  }

  @SuppressWarnings("rawtypes")
  public static Set<String> getNimbusHostports(Map conf) {
    if(nimbusHostPorts.size() == 0) {
      nimbusHostPorts.addAll(getNimbusHostportsFromZK(conf));
      return nimbusHostPorts;
    } else {
      LOG.debug("Get nimbus hostports from Cache");
      return nimbusHostPorts;
    }
  }

  public static void resetNimbusHostPorts(Collection<String> hostPorts) {
    nimbusHostPorts.clear();
    nimbusHostPorts.addAll(hostPorts);
  }

}
