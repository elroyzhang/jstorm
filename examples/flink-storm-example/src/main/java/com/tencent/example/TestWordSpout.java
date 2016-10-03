package com.tencent.example;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TestWordSpout extends BaseRichSpout {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(TestWordSpout.class);

  boolean _isDistributed;
  SpoutOutputCollector _collector;
  long limit = 50000;
  private AtomicInteger cnt = new AtomicInteger(0);

  public TestWordSpout() {
    this(true);
  }

  public TestWordSpout(long limit) {
    this(true);
    this.limit = limit;
    LOG.info("Limit is " + limit);
  }

  public TestWordSpout(boolean isDistributed) {
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
      _collector.emit("STRAEM_ID_WORD", new Values(url), url);
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
    declarer.declareStream("STRAEM_ID_WORD", new Fields("word"));
  }

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