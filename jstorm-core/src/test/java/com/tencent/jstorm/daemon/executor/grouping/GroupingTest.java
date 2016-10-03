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
package com.tencent.jstorm.daemon.executor.grouping;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Thrift;
import org.apache.storm.Thrift.BoltDetails;
import org.apache.storm.Thrift.SpoutDetails;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.NGrouping;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.Time;
import org.apache.storm.utils.Utils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.CoreUtil;

import backtype.storm.LocalCluster;
import backtype.storm.testing.Testing;

@ClojureClass(className = "backtype.storm.grouping_test")
public class GroupingTest {
  private static final Logger LOG = LoggerFactory.getLogger(GroupingTest.class);

  @Test
  @ClojureClass(className = "backtype.storm.grouping_test#test-shuffle")
  public void testShuffle() {
    Time.startSimulating();
    LocalCluster cluster = new LocalCluster(4, 3, new HashMap(), null, 1024);

    try {

      Map<String, SpoutDetails> spouts = new HashMap<String, SpoutDetails>();
      spouts.put("1", new SpoutDetails(new TestWordSpout(true), 4, null));

      Map<String, BoltDetails> bolts = new HashMap<String, BoltDetails>();
      Map<GlobalStreamId, Grouping> inputs =
          new HashMap<GlobalStreamId, Grouping>();
      inputs.put(Utils.getGlobalStreamId("1", "cid"),
          Thrift.prepareShuffleGrouping());
      bolts.put("2", new BoltDetails(new TestGlobalCount(), null, 6, inputs));

      StormTopology topology = Thrift.buildTopology(spouts, bolts);

      // important for test that #tuples = multiple of 4 and 6
      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      for (int i = 0; i < 12; i++) {
        values.add(Arrays.asList("a"));
        values.add(Arrays.asList("b"));
      }
      mockSources.put("1", values);

      Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster,
          topology, mockSources, null, null, null, null);

      List<Object> expected = new ArrayList<Object>();
      for (int i = 0; i < 6; i++) {
        expected.add(Arrays.asList(1));
        expected.add(Arrays.asList(2));
        expected.add(Arrays.asList(3));
        expected.add(Arrays.asList(4));
      }
      List<Object> actual = Testing.readTuples(results, "2");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

    } catch (Throwable t) {
      LOG.error("Error in cluster", t);
      Assert.assertNull(t);
    } finally {
      /*
       * final AtomicBoolean keepWaiting = new AtomicBoolean(true); Thread
       * future = new Thread() {
       * 
       * @Override public void run() { while (keepWaiting.get()) { try {
       * Testing.simulateWait(cluster); } catch (InterruptedException e) {
       * e.printStackTrace(); } } } }; future.start();
       */

      cluster.shutdown();
      // keepWaiting.set(false);
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.grouping_test#test-custom-groupings")
  public void testCustomGroupings() {

    Time.startSimulating();

    LocalCluster cluster = new LocalCluster();
    try {

      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestWordSpout(true));
      builder.setBolt("2", new IdBolt(), 4).customGrouping("1",
          new NGrouping(2));
      builder.setBolt("3", new IdBolt(), 6).customGrouping("1",
          new NGrouping(3));

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList("a"));
      values.add(Arrays.asList("b"));
      mockSources.put("1", values);

      Map<String, List<FixedTuple>> results = Testing.completeTopology(cluster,
          builder.createTopology(), mockSources, null, null, null, null);

      List<Object> expected = new ArrayList<Object>();
      expected.add(Arrays.asList("a"));
      expected.add(Arrays.asList("a"));
      expected.add(Arrays.asList("b"));
      expected.add(Arrays.asList("b"));
      List<Object> actual = Testing.readTuples(results, "2");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

      expected.add(Arrays.asList("a"));
      expected.add(Arrays.asList("b"));
      actual = Testing.readTuples(results, "3");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

    } catch (Throwable t) {
      LOG.error("Error in cluster", t);
      Assert.assertNull(t);
    } finally {
      cluster.shutdown();
    }

    Time.stopSimulating();
  }

}

class IdBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("val"));
  }

}