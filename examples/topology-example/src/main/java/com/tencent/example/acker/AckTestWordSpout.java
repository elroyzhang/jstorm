package com.tencent.example.acker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.utils.CoreUtil;
import com.tencent.storm.topology.UpdateRichSpout;

public class AckTestWordSpout extends UpdateRichSpout {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(AckTestWordSpout.class);

  boolean _isDistributed;
  private int _intervalSecs = 0;
  SpoutOutputCollector _collector;
  long limit = 50000;
  List<String> sendList = new ArrayList<String>();

  public AckTestWordSpout() {
    this(true);
  }

  public AckTestWordSpout(long limit, int intervalSecs) {
    this(true);
    this.limit = limit;
    this._intervalSecs = intervalSecs;
    LOG.info("Limit is " + limit);
  }

  public AckTestWordSpout(boolean isDistributed) {
    _isDistributed = isDistributed;
  }

  @SuppressWarnings("rawtypes")
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    _collector = collector;
    for (int i = 0; i < limit; i++) {
      sendList.add(i + " - msg");
    }
    Collections.synchronizedCollection(sendList);
    LOG.info("prepare finsh, list size: " + sendList.size());
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
    try {
      CoreUtil.sleepSecs(_intervalSecs);
    } catch (InterruptedException e) {
    }
    final String sendValue = getSendValue();
    if (sendValue != null) {
      _collector.emit("STRAEM_ID_WORD", new Values(sendValue), sendValue);
      LOG.info("spout-emit:" + sendValue);
    } else {
      LOG.info("send list finish, you can kill topology now");
      sleepMics(30 * 1000);
    }
  }

  public void update(Object msgId) {
    LOG.info("spout-update:" + msgId);
  }

  public void ack(Object msgId) {
    sendList.remove(msgId);
    LOG.info("spout-ack:" + msgId);
  }

  public void fail(Object msgId) {
    sendList.add(String.valueOf(msgId));
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

  private String getSendValue() {
    if (sendList.size() > 0) {
      String sendValue = sendList.remove(0);
      return sendValue;
    }
    return null;
  }
}