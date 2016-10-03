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
package com.tencent.jstorm.zk;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.storm.Config;
import org.apache.storm.callback.DefaultWatcherCallBack;
import org.apache.storm.cluster.IStateStorage;
import org.apache.storm.generated.NodeInfo;
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.NetWorkUtils;

import junit.framework.Assert;
import junit.framework.TestCase;

public class InprocessZookeeperTest extends TestCase {
  private static Logger LOG =
      LoggerFactory.getLogger(InprocessZookeeperTest.class);
  private static NIOServerCnxnFactory zkServer;
  private int zkPort;
  @SuppressWarnings("rawtypes")
  private static Map stormConf;
  private static CuratorFramework client;

  @SuppressWarnings("unchecked")
  @BeforeClass
  public void setUp() {
    try {
      stormConf = Utils.readStormConfig();
      int zkport =
          Utils.getInt(stormConf.get(Config.STORM_ZOOKEEPER_PORT), 2182);
      String stormLocalDir =
          Utils.getString(stormConf.get(Config.STORM_LOCAL_DIR), "storm-local");
      List list = Zookeeper.mkInprocessZookeeper(stormLocalDir, zkport);
      zkServer = (NIOServerCnxnFactory) list.get(1);
      this.zkPort = ((Long)list.get(0)).intValue();

      ArrayList<String> zkServers = new ArrayList<String>();
      zkServers.add(InetAddress.getLocalHost().getHostName());
      stormConf.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);
      stormConf.put(Config.NIMBUS_HOST, "localhost");
      stormConf.put(Config.NIMBUS_THRIFT_PORT,
          NetWorkUtils.assignServerPort(-1));
      stormConf.put(Config.NIMBUS_HOST, "localhost");
      sleep(2000);

      client = Zookeeper.mkClient(stormConf,
          (List<String>) stormConf.get(Config.STORM_ZOOKEEPER_SERVERS),
          stormConf.get(Config.STORM_ZOOKEEPER_PORT), "",
          new DefaultWatcherCallBack(), null);
    } catch (Exception ex) {
      LOG.error("setup failure", CoreUtil.stringifyError(ex));
      Assert.assertEquals(null, ex);
    }
  }

  @Test
  public void testMkDirs() throws Exception {
    String path = String.valueOf(stormConf.get(Config.STORM_ZOOKEEPER_ROOT));
    try {
      Zookeeper.mkdirs(client, path, ZooDefs.Ids.OPEN_ACL_UNSAFE);
      Assert.assertEquals(true, Zookeeper.existsNode(client, path, false));
    } catch (Exception e) {
      LOG.error("testCreateNode failure", e);
      Assert.assertEquals(null, e);
    }

  }

  @Test
  public void testCreateNode() throws Exception {
    String path = String.valueOf(stormConf.get(Config.STORM_ZOOKEEPER_ROOT));
    try {
      NodeInfo server = new NodeInfo("localhost", Sets.newHashSet(8081l));
      Zookeeper.createNode(client, path + "/nimbus", Utils.serialize(server),
          ZooDefs.Ids.OPEN_ACL_UNSAFE);

      byte[] info = Zookeeper.getData(client, path + "/nimbus", false);
      Assert.assertNotNull(info);

      NodeInfo result = Utils.deserialize(info, NodeInfo.class);
      Assert.assertEquals("localhost", result.get_node());
      Assert.assertEquals(8081, result.get_port());

      Map dataInfo =
          Zookeeper.getDataWithVersion(client, path + "/nimbus", false);
      Assert.assertNotNull(dataInfo);
      byte[] data = (byte[]) dataInfo.get(IStateStorage.DATA);
      int version = (int) dataInfo.get(IStateStorage.VERSION);
      result = Utils.deserialize(data, NodeInfo.class);
      Assert.assertEquals("localhost", result.get_node());
      Assert.assertEquals(8081, result.get_port());
      Assert.assertEquals(0, version);

      List<String> children = Zookeeper.getChildren(client, path, false);
      Assert.assertEquals(1, children.size());
    } catch (Exception e) {
      LOG.error("testCreateNode failure", e);
      Assert.assertEquals(null, e);
    }
  }

  @Test
  public void testDeleteNode() {
    String path = String.valueOf(stormConf.get(Config.STORM_ZOOKEEPER_ROOT));
    try {
      Zookeeper.deleteNode(client, path + "/nimbus");
      boolean flag = Zookeeper.exists(client, path + "/nimbus", false);
      Assert.assertEquals(false, flag);
    } catch (Exception e) {
      LOG.error("testCreateNode failure", e);
      Assert.assertEquals(null, e);
    }
  }

  @AfterClass
  public static void cleanup() throws Exception {
    Zookeeper.shutdownInprocessZookeeper(zkServer);
    client.close();
  }

  /**
   * for debug
   */
  static void sleep() {
    try {
      Thread.sleep(30 * 60 * 1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void sleep(int i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

}
