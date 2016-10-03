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
package com.tencent.jstorm.daemon.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.storm.Config;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.generated.SubmitOptions;
import org.apache.storm.generated.TopologyInitialStatus;
import org.apache.storm.testing.AckFailMapTracker;
import org.apache.storm.testing.FeederSpout;
import org.apache.storm.testing.FixedTuple;
import org.apache.storm.testing.TestAggregatesCounter;
import org.apache.storm.testing.TestConfBolt;
import org.apache.storm.testing.TestGlobalCount;
import org.apache.storm.testing.TestPlannerSpout;
import org.apache.storm.testing.TestWordCounter;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Time;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import com.tencent.jstorm.utils.CoreUtil;

import backtype.storm.LocalCluster;
import backtype.storm.testing.Testing;
import junit.framework.Assert;

@ClojureClass(className = "backtype.storm.integration-test")
public class IntegrationTest implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory
      .getLogger(IntegrationTest.class);

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-basic-topology")
  public void testBasicTopology() {
    testBasicTopology(true);
    testBasicTopology(false);
  }

  private void testBasicTopology(boolean zmqOn) {
    Time.startSimulating();

    Map<String, Object> daemonConf = new HashMap<String, Object>();
    daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, zmqOn);
    LocalCluster cluster = new LocalCluster(4, 3, daemonConf, null, 1024);
    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestWordSpout(true), 3);
      builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping("1",
          new Fields("word"));
      builder.setBolt("3", new TestGlobalCount()).globalGrouping("1");
      builder.setBolt("4", new TestAggregatesCounter()).globalGrouping("2");
      StormTopology topology = builder.createTopology();

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList("nathan"));
      values.add(Arrays.asList("bob"));
      values.add(Arrays.asList("joey"));
      values.add(Arrays.asList("nathan"));
      mockSources.put("1", values);

      Map<String, Object> stormConf = new HashMap<String, Object>();
      stormConf.put(Config.TOPOLOGY_WORKERS, 2);

      Map<String, List<FixedTuple>> results =
          Testing.completeTopology(cluster, topology, mockSources, stormConf,
              null, null, null);

      List<Object> expected, actual;

      expected = new ArrayList<Object>();
      expected.add(Arrays.asList("nathan"));
      expected.add(Arrays.asList("bob"));
      expected.add(Arrays.asList("joey"));
      expected.add(Arrays.asList("nathan"));
      actual = Testing.readTuples(results, "1");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

      expected = new ArrayList<Object>();
      expected.add(Arrays.asList((Object) "nathan", (Object) 1));
      expected.add(Arrays.asList((Object) "nathan", (Object) 2));
      expected.add(Arrays.asList((Object) "bob", (Object) 1));
      expected.add(Arrays.asList((Object) "joey", (Object) 1));
      actual = Testing.readTuples(results, "2");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

      expected = new ArrayList<Object>();
      expected.add(Arrays.asList(1));
      expected.add(Arrays.asList(2));
      expected.add(Arrays.asList(3));
      expected.add(Arrays.asList(4));
      actual = Testing.readTuples(results, "3");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

      actual = Testing.readTuples(results, "4");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

    } catch (Throwable t) {
      Assert.assertNull("testBasicTopology failed when zmqOn is " + zmqOn, t);
    } finally {
      cluster.shutdown();
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-multi-tasks-per-executor")
  public void testMultiTasksPerExecutor() {
    Time.startSimulating();
    LocalCluster cluster = new LocalCluster(4, 3, new HashMap(), null, 1024);
    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestWordSpout(true));
      builder.setBolt("2", new EmitTaskIdBolt(), 3).shuffleGrouping("1")
          .setNumTasks(6);
      StormTopology topology = builder.createTopology();

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      for (int i = 0; i < 6; i++) {
        values.add(Arrays.asList("a"));
      }
      mockSources.put("1", values);

      Map<String, List<FixedTuple>> results =
          Testing.completeTopology(cluster, topology, mockSources,
              new HashMap(), true, null, Testing.TEST_TIMEOUT_MS);

      List<Object> expected, actual;

      expected = new ArrayList<Object>();
      for (int i = 0; i < 6; i++) {
        expected.add(Arrays.asList(i));
      }
      actual = Testing.readTuples(results, "2");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

    } catch (Exception t) {
      LOG.info("testMultiTasksPerExecutor failed:",
          CoreUtil.stringifyError(t));
      Assert.assertNull("testMultiTasksPerExecutor failed: " + t, t);
    } finally {
      cluster.shutdown();
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-timeout")
  public void testTimeout() {
    Time.startSimulating();

    Map<String, Object> daemonConf = new HashMap<String, Object>();
    daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
    LocalCluster cluster = new LocalCluster(4, 3, daemonConf, null, 1024);
    try {
      FeederSpout feeder = new FeederSpout(new Fields("field1"));
      AckFailMapTracker tracker = new AckFailMapTracker();
      feeder.setAckFailDelegate(tracker);

      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", feeder);
      builder.setBolt("2", new AckEveryOtherBolt()).globalGrouping("1");
      StormTopology topology = builder.createTopology();

      Map<String, Object> stormConf = new HashMap<String, Object>();
      stormConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
      stormConf.put(Config.TOPOLOGY_DEBUG, true);
      cluster.submitTopology("timeout-tester", stormConf, topology);

      Thread.sleep(10000); // wait 10s for JStorm to start all the components

      feeder.feed(Arrays.asList((Object) "a"), 1);
      feeder.feed(Arrays.asList((Object) "b"), 2);
      feeder.feed(Arrays.asList((Object) "c"), 3);

      Thread.sleep(9000);

      long startTime = System.currentTimeMillis();
      while (true) {
        boolean isEveryAcked = true;
        if (!tracker.isAcked(1) || !tracker.isAcked(3))
          isEveryAcked = false;

        if (isEveryAcked)
          break;

        Thread.sleep(1);
        Assert.assertTrue("wait too much time to ack",
            System.currentTimeMillis() - startTime < 2000);
      }

      Assert.assertTrue(!tracker.isFailed(2));

      Thread.sleep(12000);
      // tracker.fail(2);

      Assert.assertTrue("Tuple shoud be time out!", tracker.isFailed(2));

    } catch (Throwable t) {
      Assert.assertNull("testTimeout failed: " + t, t);
    } finally {
      cluster.shutdown();
    }
    Time.stopSimulating();
  }

  @ClojureClass(className = "backtype.storm.integration-test#mk-validate-topology-1")
  public StormTopology mkValidateTopology1() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("1", new TestWordSpout(true), 3);
    builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping("1",
        new Fields("word"));
    return builder.createTopology();
  }

  @ClojureClass(className = "backtype.storm.integration-test#mk-invalidate-topology-1")
  public StormTopology mkInValidateTopology1() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("1", new TestWordSpout(true), 3);
    builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping("3",
        new Fields("word"));
    return builder.createTopology();
  }

  @ClojureClass(className = "backtype.storm.integration-test#mk-invalidate-topology-2")
  public StormTopology mkInValidateTopology2() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("1", new TestWordSpout(true), 3);
    builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping("1",
        new Fields("non-exists-field"));
    return builder.createTopology();
  }

  @ClojureClass(className = "backtype.storm.integration-test#mk-invalidate-topology-3")
  public StormTopology mkInValidateTopology3() {
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("1", new TestWordSpout(true), 3);
    builder.setBolt("2", new TestWordCounter(), 4).fieldsGrouping("1",
        "non-exists-stream", new Fields("word"));
    return builder.createTopology();
  }

  @ClojureClass(className = "backtype.storm.integration-test#try-complete-wc-topology")
  public boolean tryCompleteWcTopology(LocalCluster cluster,
      StormTopology topology) {
    try {
      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList("nathan"));
      values.add(Arrays.asList("bob"));
      values.add(Arrays.asList("joey"));
      values.add(Arrays.asList("nathan"));
      mockSources.put("1", values);
      Map stormConf = new HashMap<String, Object>();
      stormConf.put(Config.TOPOLOGY_WORKERS, 2);

      Testing.completeTopology(cluster, topology, mockSources, stormConf, null,
          null, null);
      return false;
    } catch (InvalidTopologyException e) {
      return true;
    } catch (Exception e) {
      Assert.assertNull("Some other exception occurs: " + e, e);
      return true;
    }
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-validate-topology-structure")
  public void testValidateTopologyStructure() {
    Time.startSimulating();
    LocalCluster cluster = new LocalCluster(4, 3, new HashMap(), null, 1024);
    try {
      boolean anyError1 = tryCompleteWcTopology(cluster, mkValidateTopology1());
      boolean anyError2 =
          tryCompleteWcTopology(cluster, mkInValidateTopology1());
      boolean anyError3 =
          tryCompleteWcTopology(cluster, mkInValidateTopology2());
      boolean anyError4 =
          tryCompleteWcTopology(cluster, mkInValidateTopology3());

      Assert.assertFalse(anyError1);
      Assert.assertTrue(anyError2);
      Assert.assertTrue(anyError3);
      Assert.assertTrue(anyError4);

    } catch (Exception e) {
      Assert.assertNull("testValidateTopologyStructure failed: " + e, e);
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-system-stream")
  public void testSystemStream() {
    Time.startSimulating();
    LocalCluster cluster = new LocalCluster();

    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestWordSpout(true), 3);
      builder.setBolt("2", new IdentityBolt(), 1)
          .fieldsGrouping("1", new Fields("word"))
          .globalGrouping("1", "__system");
      StormTopology topology = builder.createTopology();

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList("a"));
      values.add(Arrays.asList("b"));
      values.add(Arrays.asList("c"));
      mockSources.put("1", values);
      Map stormConf = new HashMap<String, Object>();
      stormConf.put(Config.TOPOLOGY_WORKERS, 2);

      Map<String, List<FixedTuple>> results =
          Testing.completeTopology(cluster, topology, mockSources, null, null,
              null, null);

      List<Object> expected = new ArrayList<Object>();
      expected.add(Arrays.asList("a"));
      expected.add(Arrays.asList("b"));
      expected.add(Arrays.asList("c"));
      expected.add(Arrays.asList("startup"));
      expected.add(Arrays.asList("startup"));
      expected.add(Arrays.asList("startup"));
      List<Object> actual = Testing.readTuples(results, "2");
      Assert.assertEquals(CoreUtil.multiSet(expected),
          CoreUtil.multiSet(actual));

    } catch (Exception e) {
      Assert.assertNull("testSystemStream failed: " + e, e);
    } finally {
      cluster.shutdown();
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-acking")
  public void testAcking() {
    // TODO: Design our own test. The translation of Clojure codes need
    // massive Java codes.
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-ack-branching")
  public void testAckBranching() {
    // TODO: Design our own test. The translation of Clojure codes need
    // massive Java codes.
  }

  public static AtomicBoolean isBoltPrepared = new AtomicBoolean(false);
  public static AtomicBoolean isSpoutOpened = new AtomicBoolean(false);

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-submit-inactive-topology")
  public void testSubmitInactiveTopology() {
    Time.startSimulating();

    Map<String, Object> daemonConf = new HashMap<String, Object>();
    daemonConf.put(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS, true);
    LocalCluster cluster = new LocalCluster(2, 3, daemonConf, null, 1024);

    try {
      FeederSpout feeder = new FeederSpout(new Fields("field1"));
      AckFailMapTracker tracker = new AckFailMapTracker();
      feeder.setAckFailDelegate(tracker);

      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", feeder);
      builder.setSpout("2", new OpenTrackedSpout());
      builder.setBolt("3", new PrepareTrackedBolt()).globalGrouping("1");
      StormTopology topology = builder.createTopology();

      isBoltPrepared.set(false);
      isSpoutOpened.set(false);

      Map<String, Object> stormConf = new HashMap<String, Object>();
      stormConf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 10);
      SubmitOptions options = new SubmitOptions(TopologyInitialStatus.INACTIVE);
      cluster.submitTopologyWithOpts("test", stormConf, topology, options);

      feeder.feed(Arrays.asList((Object) "a"), 1);
      // Testing.advanceClusterTime(cluster, 9);
      Thread.sleep(9000);
      Assert.assertFalse(isBoltPrepared.get());
      Assert.assertFalse(isSpoutOpened.get());
      cluster.getNimbus().activate("test");

      // Testing.advanceClusterTime(cluster, 12);
      long startTime = System.currentTimeMillis();
      while (!tracker.isAcked(1)) {
        Thread.sleep(1);
        Assert.assertTrue("wait too much time to ack",
            System.currentTimeMillis() - startTime < 10000);
      }
      Assert.assertTrue(isBoltPrepared.get());
      Assert.assertTrue(isSpoutOpened.get());

    } catch (Exception e) {
      Assert.assertNull("testSubmitInactiveTopology failed: " + e, e);
    } finally {
      cluster.shutdown();
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-acking-self-anchor")
  public void testAckingSelfAnchor() {
    // TODO: Design our own test. The translation of Clojure codes need
    // massive Java codes.
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-kryo-decorators-config")
  public void testKryoDecoratorsConfig() {
    Time.startSimulating();
    Map<String, Object> daemonConf = new HashMap<String, Object>();
    daemonConf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
    daemonConf.put(Config.TOPOLOGY_KRYO_DECORATORS,
        Arrays.asList("this-is-overriden"));
    LocalCluster cluster = new LocalCluster(2, 3, daemonConf, null, 1024);

    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestPlannerSpout(new Fields("conf")));
      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put(Config.TOPOLOGY_KRYO_DECORATORS, Arrays.asList("one", "two"));
      builder.setBolt("2", new TestConfBolt(conf)).shuffleGrouping("1");

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList(Config.TOPOLOGY_KRYO_DECORATORS));
      mockSources.put("1", values);

      Map<String, Object> stormConf = new HashMap<String, Object>();
      stormConf.put(Config.TOPOLOGY_KRYO_DECORATORS,
          Arrays.asList("one", "three"));
      Map<String, List<FixedTuple>> results =
          Testing.completeTopology(cluster, builder.createTopology(),
              mockSources, stormConf, true, null, null);

      List<Object> res = Testing.readTuples(results, "2");
      Assert.assertTrue(res.size() == 1);
      List<Object> tuple = (List<Object>) res.get(0);
      Assert.assertEquals("topology.kryo.decorators", tuple.get(0));
      Assert.assertEquals(Arrays.asList("one", "two", "three"), tuple.get(1));

    } catch (Exception e) {
      Assert.assertNull("testKryoDecoratorsConfig failed: " + e, e);
    } finally {
      cluster.shutdown();
    }
    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test# test-component-specific-config")
  public void testComponentSpecificConfig() {
    Time.startSimulating();

    Map<String, Object> daemonConf = new HashMap<String, Object>();
    daemonConf.put(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, true);
    LocalCluster cluster = new LocalCluster(2, 3, daemonConf, null, 1024);

    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestPlannerSpout(new Fields("conf")));

      Map<String, Object> conf = new HashMap<String, Object>();
      conf.put("fake.config", 123);
      conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 20);
      conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 30);
      Map<String, String> register1 = new HashMap<String, String>();
      Map<String, String> register2 = new HashMap<String, String>();
      register1.put("fake.type", "bad.serializer");
      register2.put("fake.type2", "a.serializer");
      conf.put(Config.TOPOLOGY_KRYO_REGISTER,
          Arrays.asList(register1, register2));
      builder.setBolt("2", new TestConfBolt(conf)).shuffleGrouping("1")
          .setMaxTaskParallelism(2).addConfiguration("fake.config2", 987);

      Map<String, Object> stormConf = new HashMap<String, Object>();
      Map<String, String> register3 = new HashMap<String, String>();
      register3.put("fake.type", "good.serializer");
      register3.put("fake.type3", "a.serializer3");
      stormConf.put(Config.TOPOLOGY_KRYO_REGISTER, Arrays.asList(register3));

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList("fake.config"));
      values.add(Arrays.asList(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
      values.add(Arrays.asList(Config.TOPOLOGY_MAX_SPOUT_PENDING));
      values.add(Arrays.asList("fake.config2"));
      values.add(Arrays.asList(Config.TOPOLOGY_KRYO_REGISTER));
      mockSources.put("1", values);

      Map<String, List<FixedTuple>> results =
          Testing.completeTopology(cluster, builder.createTopology(),
              mockSources, stormConf, true, null, null);

      List<Object> res = Testing.readTuples(results, "2");
      Assert.assertEquals(5, res.size());
      Map<String, Object> m = new HashMap<String, Object>();
      for (Object e : res) {
        List<Object> list = (List<Object>) e;
        m.put((String) list.get(0), list.get(1));
      }

      Assert.assertEquals(123L, m.get("fake.config"));
      Assert.assertEquals(987L, m.get("fake.config2"));
      Assert.assertEquals(2L, m.get(Config.TOPOLOGY_MAX_TASK_PARALLELISM));
      Assert.assertEquals(30L, m.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));

      Map<String, String> register = new HashMap<String, String>();
      register.putAll(register1);
      register.putAll(register2);
      register.putAll(register3);
      Assert.assertEquals(register, m.get(Config.TOPOLOGY_KRYO_REGISTER));

    } catch (Exception e) {
      Assert.assertNull("testComponentSpecificConfig failed: " + e, e);
    } finally {
      cluster.shutdown();
    }

    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-hooks")
  public void testHooks() {
    Time.startSimulating();
    LocalCluster cluster = new LocalCluster();
    try {
      TopologyBuilder builder = new TopologyBuilder();
      builder.setSpout("1", new TestPlannerSpout(new Fields("conf")));
      builder.setBolt("2", new HooksBolt()).shuffleGrouping("1");
      StormTopology topology = builder.createTopology();

      Map<String, List<Object>> mockSources =
          new HashMap<String, List<Object>>();
      List<Object> values = new ArrayList<Object>();
      values.add(Arrays.asList(1));
      values.add(Arrays.asList(1));
      values.add(Arrays.asList(1));
      values.add(Arrays.asList(1));
      mockSources.put("1", values);

      Map<String, List<FixedTuple>> results =
          Testing.completeTopology(cluster, topology, mockSources, null, null,
              null, null);

      List<Object> expected = new ArrayList<Object>();
      expected.add(Arrays.asList(0, 0, 0, 0));
      expected.add(Arrays.asList(2, 1, 0, 1));
      expected.add(Arrays.asList(4, 1, 1, 2));
      expected.add(Arrays.asList(6, 2, 1, 3));
      List<Object> actual = Testing.readTuples(results, "2");
      Assert.assertEquals(expected, actual);

    } catch (Exception e) {
      Assert.assertNull("testHooks failed: " + e, e);
    } finally {
      cluster.shutdown();
    }

    Time.stopSimulating();
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-throttled-errors")
  public void testThrottledErrors() {
    // TODO: Design our own test. The translation of Clojure codes need
    // massive Java codes.
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-acking-branching-complex")
  public void testAckingBranchingComplex() {
    // test acking with branching in the topology
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-fields-grouping")
  public void testFieldsGrouping() {
    // 1. put a shitload of random tuples through it and test that counts are
    // right
    // 2. test that different spouts with different phints group the same way
  }

  @Test
  @ClojureClass(className = "backtype.storm.integration-test#test-all-grouping")
  public void testAllGrouping() {

  }
}