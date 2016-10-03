package com.tencent.jstorm.daemon.common;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

@ClojureClass(className = "backtype.storm.integration-test#ack-every-other")
public class AckEveryOtherBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory
      .getLogger(AckEveryOtherBolt.class);
  private static final long serialVersionUID = 1L;
  private AtomicInteger state;
  private OutputCollector _collector;

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    state = new AtomicInteger(-1);
    _collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    Integer val;
    synchronized (state) {
      val = state.get();
      state.set(-val);
      val = state.get();
    }
    if (val > 0) {
      LOG.info("AckEveryOther acking: " + input);
      _collector.ack(input);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
