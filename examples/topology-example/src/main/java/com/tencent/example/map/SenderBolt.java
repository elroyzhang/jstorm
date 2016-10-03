package com.tencent.example.map;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

public class SenderBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(NewExclamationBolt.class);
  OutputCollector _collector;
  private final static int tickFreqSecs = 40;

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf, TopologyContext context,
      OutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void execute(Tuple tuple) {
    if (TupleUtils.isTick(tuple)) {
      LOG.info("tick tuple every " + tickFreqSecs + " secs ...");
    } else {
      _collector.ack(tuple);
      LOG.info("updater-bolt:" + tuple.getString(0));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("STRAEM_ID_COUNT", new Fields("sender"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqSecs);
    return ret;
  }
}
