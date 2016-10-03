package com.tencent.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclamationBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(ExclamationBolt.class);
  OutputCollector _collector;
  private final static int tickFreqSecs = 20;

  @SuppressWarnings("rawtypes")
  public void prepare(Map conf, TopologyContext context,
      OutputCollector collector) {
    _collector = collector;
  }

  public void execute(Tuple tuple) {
    if (TupleUtils.isTick(tuple)) {
      LOG.info("tick tuple every " + tickFreqSecs + " secs ...");
    } else {
      _collector.emit("STRAEM_ID_WORD", tuple,
          new Values(tuple.getString(0) + "!!!"));
      _collector.ack(tuple);
      Values values = new Values();
      for (int i = 0; i < 3; i++) {
        values.add(tuple.getString(0) + "/" + i + ".exclam");
      }
      LOG.info("exclam-bolt:" + tuple.getString(0));
    }
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("STRAEM_ID_WORD", new Fields("exclam"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqSecs);
    return ret;
  }
}