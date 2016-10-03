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
import org.apache.storm.utils.Utils;
import org.apache.storm.zookeeper.Zookeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.jstorm.utils.NetWorkUtils;

import junit.framework.Assert;
import junit.framework.TestCase;

public class SessionTimeoutRecoverTest extends TestCase {

  private static Logger LOG =
      LoggerFactory.getLogger(SessionTimeoutRecoverTest.class);
  private static NIOServerCnxnFactory zkServer;
  private static int zkPort;
  private static Map stormConf;
  private static CuratorFramework client;

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
      sleep(10000);
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
  public void testA() {

  }

  @AfterClass
  public void cleanUp() {
    Zookeeper.shutdownInprocessZookeeper(zkServer);
    client.close();
  }

  private static void sleep(int i) {
    try {
      Thread.sleep(i);
    } catch (InterruptedException e) {
      LOG.info("setup thread sleep interrupted. message=" + e.getMessage());
    }
  }

}
