package com.tencent.example.bp;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BPUpdaterBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(BPExclamationBolt.class);
  OutputCollector _collector;
  private final static int tickFreqSecs = 40;
  private int interval = 0;
  private boolean hasAck = true;

  public BPUpdaterBolt(int _interval3, boolean _hasAck) {
    this.interval = _interval3;
    this.hasAck = _hasAck;
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
      if (this.hasAck) {
        _collector.ack(tuple);
      }
      sleepNs(interval);
      LOG.debug("updater-bolt:" + tuple.getString(0));
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("STRAEM_ID_WORD", new Fields("updater"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> ret = new HashMap<String, Object>();
    ret.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFreqSecs);
    return ret;
  }
}
