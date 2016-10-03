package com.tencent.jstorm.ui.helpers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.generated.ExecutorSummary;

import com.tencent.jstorm.stats.StatsFields;

/**
 * @author caokun {@link caofangkun@gmail.com}
 * @author zhangxun {@link xunzhang555@gmail.com}
 * 
 */
public class HelpersTest {

  @Test
  public void testUrlFormat() {
    String rest =
        Helpers.urlFormat("http://%s:%s/log?file=worker-%s.log", "test", 1234,
            6701);
    Assert.assertEquals("http://test:1234/log?file=worker-6701.log", rest);
  }

  @Test
  public void testPrettyUptimeSec() {
    Assert.assertEquals("0d0h1m40s", Helpers.prettyUptimeSec(100));
  }

  @Test
  public void testPrettyUptimeMs() {
    Assert.assertEquals("0d0h0m0s100ms", Helpers.prettyUptimeMs(100));
  }

  @Test
  public void testToTasks() {
    Assert.assertEquals(Arrays.asList(1, 2, 3),
        Helpers.toTasks(new ExecutorInfo(1, 3)));
  }

  @Test
  public void testSumTasks() {
    ExecutorSummary es1 = Mockito.mock(ExecutorSummary.class);
    Mockito.when(es1.get_executor_info()).thenReturn(new ExecutorInfo(1, 3));
    ExecutorSummary es2 = Mockito.mock(ExecutorSummary.class);
    Mockito.when(es2.get_executor_info()).thenReturn(new ExecutorInfo(4, 8));

    List<ExecutorSummary> executors = new ArrayList<ExecutorSummary>();
    executors.add(es1);
    executors.add(es2);
    Assert.assertEquals(8, Helpers.sumTasks(executors));

  }

  @Test
  public void testPrettyExecutorInfo() {
    Assert.assertEquals("[1-3]",
        Helpers.prettyExecutorInfo(new ExecutorInfo(1, 3)));

  }

  @Test
  public void testFloatStr() {
    Float n = null;
    Assert.assertEquals("0", Helpers.floatStr(n));
    n = 123.12345678f;
    Assert.assertEquals("123.123", Helpers.floatStr(n));
  }

  @Test
  public void testSwapMapOrder() {
    Map<StatsFields, Map<Object, Object>> test =
        new HashMap<StatsFields, Map<Object, Object>>();
    Map<Object, Object> streamToCnt1 = new HashMap<Object, Object>();
    streamToCnt1.put("stream1", 100);
    Map<Object, Object> streamToCnt2 = new HashMap<Object, Object>();
    streamToCnt2.put("stream2", 120);
    test.put(StatsFields.emitted, streamToCnt1);
    test.put(StatsFields.transferred, streamToCnt2);

    Map<Object, Map<StatsFields, Object>> result = Helpers.swapMapOrder(test);
    Assert.assertEquals(2, result.size());
    Assert.assertEquals(100, result.get("stream1").get(StatsFields.emitted));
    Assert
        .assertEquals(120, result.get("stream2").get(StatsFields.transferred));

  }

}
