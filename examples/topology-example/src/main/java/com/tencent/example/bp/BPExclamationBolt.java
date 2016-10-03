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
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BPExclamationBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(BPExclamationBolt.class);
  OutputCollector _collector;
  private final static int tickFreqSecs = 20;
  private int interval = 0;
  private boolean hasAck = true;
  private boolean update = false;

  public BPExclamationBolt(int _interval2, boolean _hasAck, boolean _update) {
    this.interval = _interval2;
    this.hasAck = _hasAck;
    this.update = _update;
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
      _collector.emit("STRAEM_ID_WORD", tuple,
          new Values(tuple.getString(0) + "!!!"));
      if (this.hasAck) {
        _collector.ack(tuple);
      }
      sleepNs(interval);
      if (this.update) {
        Values values = new Values();
        for (int i = 0; i < 3; i++) {
          values.add(tuple.getString(0) + "/" + i + ".exclam");
        }
        _collector.update(tuple, values);
      }
      LOG.debug("exclam-bolt:" + tuple.getString(0));
    }
  }

  @Override
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