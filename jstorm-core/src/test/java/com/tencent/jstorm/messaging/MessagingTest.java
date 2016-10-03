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
package com.tencent.jstorm.messaging;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Thrift;
import org.apache.storm.Thrift.BoltDetails;
import org.apache.storm.Thrift.SpoutDetails;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.StreamInfo;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.utils.Utils;
import org.junit.Before;
import org.junit.Test;

import com.tencent.jstorm.ClojureClass;

import backtype.storm.LocalCluster;
import backtype.storm.testing.Testing;
import junit.framework.Assert;

@ClojureClass(className = "backtype.storm.messaging-test")
public class MessagingTest {

  private int supervisors;
  private int portsPerSupervisor;
  private Map<Object, Object> daemonConf;
  private int supervisorSlotPortmin;

  @Before
  public void init() {
    supervisors = 1;
    portsPerSupervisor = 2;
    daemonConf = new HashMap<Object, Object>();
    supervisorSlotPortmin = 6701;
  }

  @Test
  @ClojureClass(className = "backtype.storm.messaging-test#test-local-transport")
  public void testLocalTransport() {
    checkLocalTransport(false);
    // checkLocalTransport(true);
  }

  @Test
  @ClojureClass(className = "backtype.storm.messaging-test#test-receiver-message-order")
  public void testReceiverMessageOrder() {

  }

  @SuppressWarnings("unchecked")
  private void checkLocalTransport(boolean transportOn) {
    daemonConf.put(Config.TOPOLOGY_WORKERS, 2);
    daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, transportOn);
    daemonConf.put(Config.STORM_MESSAGING_TRANSPORT,
        "backtype.storm.messaging.netty.Context");

    LocalCluster cluster = null;
    try {
      cluster = new LocalCluster(supervisors, portsPerSupervisor, daemonConf,
          null, supervisorSlotPortmin);

      Map<String, SpoutDetails> spouts = new HashMap<String, SpoutDetails>();
      Map<String, StreamInfo> outputs = new HashMap<String, StreamInfo>();
      spouts.put("1", new SpoutDetails(new TestWordSpout(true), 2, null));
      Map<String, BoltDetails> bolts = new HashMap<String, BoltDetails>(1);
      Map<GlobalStreamId, Grouping> inputs1 =
          new HashMap<GlobalStreamId, Grouping>(1);
      inputs1.put(Utils.getGlobalStreamId("1", "c1"),
          Thrift.prepareShuffleGrouping());
      bolts.put("2", new BoltDetails(new TestGlobalCount(), null, 6, inputs1));

      StormTopology topology = Thrift.buildTopology(spouts, bolts);

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      mockSources.put("1",
          argsAsList("a", "b", "a", "b", "a", "b", "a", "b", "a", "b", "a", "b",
              "a", "b", "a", "b", "a", "b", "a", "b", "a", "b", "a", "b"));

      Map<String, List<FixedTuple>> result =
          Testing.completeTopology(cluster, topology, mockSources,
              new HashMap<Object, Object>(), true, null, 5000L);
      List<Object> mock = argsAsList(1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2,
          3, 4, 1, 2, 3, 4, 1, 2, 3, 4);
      Assert.assertTrue(
          Testing.multiSetEquals(mock, Testing.readTuples(result, "2")));
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      Testing.cleanupLocalCluster(cluster);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> List<Object> argsAsList(T... args) {
    if (null == args || args.length == 0) {
      return null;
    }
    List<Object> result = new ArrayList<Object>();
    for (T arg : args) {
      result.add(Arrays.asList(arg));
    }
    return result;
  }
}
