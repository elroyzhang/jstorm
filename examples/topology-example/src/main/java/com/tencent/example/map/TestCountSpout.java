package com.tencent.example.map;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.storm.topology.UpdateRichSpout;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class TestCountSpout extends UpdateRichSpout {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCountSpout.class);

  boolean _isDistributed;
  SpoutOutputCollector _collector;
  long limit = 50000;
  private AtomicInteger cnt = new AtomicInteger(0);

  public TestCountSpout() {
    this(true);
  }

  public TestCountSpout(long limit) {
    this(true);
    this.limit = limit;
    LOG.info("Limit is " + limit);
  }

  public TestCountSpout(boolean isDistributed) {
    _isDistributed = isDistributed;
  }

  @SuppressWarnings("rawtypes")
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    _collector = collector;
  }

  public void close() {

  }

  public static void sleepMics(int micSecs) {
    try {
      TimeUnit.MICROSECONDS.sleep(micSecs);
    } catch (InterruptedException e) {
    }
  }

  public static void sleepNs(int ns) {
    try {
      Thread.sleep(0, ns);
    } catch (InterruptedException e) {

    }
  }

  public void nextTuple() {
    final String url = getDbUrl();
    if (url != null) {
      _collector.emit("STRAEM_ID_COUNT", new Values(url), url);
      LOG.info("spout-emit:" + url);
    } else {
      sleepMics(30 * 1000);
    }
  }

  public void update(Object msgId) {
    LOG.info("spout-update:" + msgId);
  }

  public void ack(Object msgId) {
    LOG.info("spout-ack:" + msgId);
  }

  public void fail(Object msgId) {
    LOG.info("spout-fail:" + msgId);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("STRAEM_ID_COUNT", new Fields("count"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    if (!_isDistributed) {
      Map<String, Object> ret = new HashMap<String, Object>();
      ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
      return ret;
    } else {
      return null;
    }
  }

  private String getDbUrl() {
    int row_num = cnt.incrementAndGet();
    if (row_num <= limit) {
      final String[] urls =
          new String[] { "http://www.qq.com", "http://www.sina.com",
              "http://www.tmall.com", "http://weibo.com", "http://36kr.com" };
      final Random rand = new Random();
      final String url = urls[rand.nextInt(urls.length)];
      return row_num + "-" + url;
    }
    return null;
  }
}