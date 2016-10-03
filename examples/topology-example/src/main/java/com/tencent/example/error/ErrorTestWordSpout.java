package com.tencent.example.error;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.storm.topology.UpdateRichSpout;

public class ErrorTestWordSpout extends UpdateRichSpout {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(ErrorTestWordSpout.class);

  boolean _isDistributed;
  SpoutOutputCollector _collector;
  long limit = 50000;
  private AtomicLong cnt = new AtomicLong(0);

  public ErrorTestWordSpout() {
    this(true);
  }

  public ErrorTestWordSpout(long limit) {
    this(true);
    this.limit = limit;
    LOG.info("Limit is " + limit);
  }

  public ErrorTestWordSpout(boolean isDistributed) {
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
    long row_num = cnt.incrementAndGet();
    if (row_num % 1000 == 0) {
      _collector.reportError(new Exception("ErrorTestWordSpout " + row_num));
    }
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