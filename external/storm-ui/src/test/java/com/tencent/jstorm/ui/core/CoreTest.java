package com.tencent.jstorm.ui.core;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.storm.Config;
import org.apache.storm.Thrift;
import org.apache.storm.Thrift.BoltDetails;
import org.apache.storm.Thrift.SpoutDetails;
import org.apache.storm.generated.ComponentType;
import org.apache.storm.generated.ExecutorStats;
import org.apache.storm.generated.ExecutorSummary;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestPlannerBolt;
import org.apache.storm.testing.TestPlannerSpout;
import org.apache.storm.utils.Utils;
import org.apache.thrift.TException;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.stats.Pair;
import com.tencent.jstorm.stats.Stats;
import com.tencent.jstorm.utils.CoreUtil;

import junit.framework.Assert;

/**
 * 
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * @author <a href="mailto:gulele2003@qq.com">gulele</a>
 * @author <a href="mailto:darezhong@qq.com">liuyuzhong</a>
 * @author <a href="mailto:fuangguang@126.com">wangfangguang</a>
 * @ModifiedBy kuncao
 * @ModifiedTime 11:39:14 AM Feb 26, 2016
 */
public class CoreTest {
  private final static Logger LOG = LoggerFactory.getLogger(CoreTest.class);

  private static String localTmpPath = null;

  @BeforeClass
  public static void SetUp() {
    localTmpPath = CoreUtil.localTempPath();
    try {
      FileUtils.forceMkdir(new File(localTmpPath));
    } catch (IOException e) {
      LOG.error("set up failed.", e);
    }
  }

  @Test
  public void testGetFilledStats() {
    List<ExecutorSummary> inputs = new ArrayList<ExecutorSummary>();
    ExecutorSummary summary1 = new ExecutorSummary();
    summary1.set_component_id("summary1");
    summary1.set_stats(null);
    inputs.add(summary1);
    ExecutorSummary summary2 = new ExecutorSummary();
    summary2.set_component_id("summary2");
    summary2.set_stats(null);
    inputs.add(summary2);
    Assert.assertEquals(0, Stats.getFilledStats(inputs).size());

    ExecutorStats stats = new ExecutorStats();
    HashMap<String, Long> emitted = new HashMap<String, Long>();
    emitted.put("s1", 100L);
    emitted.put("s2", 200L);
    stats.put_to_emitted("600", emitted);
    HashMap<String, Long> transferred = new HashMap<String, Long>();
    transferred.put("s1", 300L);
    transferred.put("s2", 400L);
    stats.put_to_transferred("600", transferred);
    ExecutorSummary summary3 = new ExecutorSummary();
    summary3.set_component_id("summary1");
    summary3.set_stats(stats);

    inputs.add(summary3);

    List<ExecutorStats> expected = new ArrayList<ExecutorStats>();
    expected.add(stats.deepCopy());
    Assert.assertEquals(expected, Stats.getFilledStats(inputs));

  }

  @Test
  public void testComponentType() {
    Map<String, SpoutDetails> spouts = new HashMap<String, SpoutDetails>(1);
    spouts.put("1", new SpoutDetails(new TestPlannerSpout(false), 3, null));
    Map<String, BoltDetails> bolts = new HashMap<String, BoltDetails>(2);
    Map<GlobalStreamId, Grouping> inputs1 =
        new HashMap<GlobalStreamId, Grouping>(1);
    inputs1.put(Utils.getGlobalStreamId("1", "cd1"),
        Thrift.prepareNoneGrouping());
    bolts.put("2", new BoltDetails(new TestPlannerBolt(), null, 4, inputs1));
    Map<GlobalStreamId, Grouping> inputs2 =
        new HashMap<GlobalStreamId, Grouping>(1);
    inputs2.put(Utils.getGlobalStreamId("2", "cd1"),
        Thrift.prepareNoneGrouping());
    bolts.put("3", new BoltDetails(new TestPlannerBolt(), null, null, inputs2));
    StormTopology topology = Thrift.buildTopology(spouts, bolts);

    Assert.assertEquals(ComponentType.SPOUT, Core.componentType(topology, "1"));
    Assert.assertEquals(ComponentType.BOLT, Core.componentType(topology, "2"));
    Assert.assertEquals(ComponentType.BOLT, Core.componentType(topology, "3"));
  }

  @Test
  public void testAddPairs() {
    Pair<Object, Object> pairs1 = new Pair<Object, Object>(1, 3);
    Pair<Object, Object> pairs2 = new Pair<Object, Object>(4, 5);
    Pair<Object, Object> result = Core.addPairs(pairs1, pairs2);
    Assert.assertEquals(5, result.getFirst());
    Assert.assertEquals(8, result.getSecond());

  }

  @Test
  public void testWorkerLogLink() {
    Assert.assertEquals("http://localhost:8000/log?file=worker-6701.log",
        Core.workerLogLink("localhost", 6701, "topolog-id"));
  }

  @Test
  public void testTopologyConf() {
    Map<String, Object> stormConf = new HashMap<String, Object>();
    stormConf.put(Config.STORM_ZOOKEEPER_PORT, 1234);
    stormConf.put(Config.STORM_ZOOKEEPER_ROOT, "/jstorm/test");

    String resultFile =
        localTmpPath + Utils.FILE_PATH_SEPARATOR + "topology-conf.test";
    try {
      OutputStreamWriter out;
      CoreUtil.touch(resultFile);
      out = new OutputStreamWriter(new FileOutputStream(new File(resultFile)));
      JsonFactory dumpFactory = new JsonFactory();
      JsonGenerator dumpGenerator = dumpFactory.createJsonGenerator(out);

      dumpGenerator.writeStartObject();
      Core.topologyConf(stormConf, dumpGenerator);
      dumpGenerator.writeEndObject();
      dumpGenerator.flush();
      String outFile = System.getProperty("user.dir")
          + "/src/test/results/topology-conf.json";
      int res = QTestUtils.executeDiffCommand(outFile, resultFile, true);
      Assert.assertEquals(0, res);

    } catch (Exception e) {
      LOG.error("test topology configuration failed", e);
    }
  }

  @Test
  public void testCheckIncludeSys() {
    Assert.assertTrue(Core.checkIncludeSys("true"));
    Assert.assertFalse(Core.checkIncludeSys("false"));
    Assert.assertFalse(Core.checkIncludeSys(null));
    Assert.assertFalse(Core.checkIncludeSys("123"));

  }

  @Test
  public void testExceptionToJson() {
    Exception ex = new IOException("test exception");
    String resultFile =
        localTmpPath + Utils.FILE_PATH_SEPARATOR + "exception-to-json.test";
    OutputStreamWriter out;
    try {
      CoreUtil.touch(resultFile);
      out = new OutputStreamWriter(new FileOutputStream(new File(resultFile)));
      try {
        Core.exceptionToJson(ex, out);
        String outFile = System.getProperty("user.dir")
            + "/src/test/results/exception-to-json.json";
        int res = QTestUtils.executeDiffCommand(outFile, resultFile, true);
        Assert.assertEquals(0, res);
      } catch (TException e) {
        e.printStackTrace();
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void CleanUp() {
    if (localTmpPath != null) {
      try {
        Utils.forceDelete(localTmpPath);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
