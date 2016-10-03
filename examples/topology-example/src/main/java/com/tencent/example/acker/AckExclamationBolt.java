package com.tencent.example.acker;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

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

public class AckExclamationBolt extends BaseRichBolt {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG =
      LoggerFactory.getLogger(AckExclamationBolt.class);
  OutputCollector _collector;
  private final static int tickFreqSecs = 20;

  int failFrequncey = 100;
  boolean isTimeoutTest = false;
  Callable<Boolean> failSampler = null;

  public AckExclamationBolt(int failFrequncey, boolean isTimeoutTest) {
    this.failFrequncey = failFrequncey;
    this.isTimeoutTest = isTimeoutTest;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map conf, TopologyContext context,
      OutputCollector collector) {
    _collector = collector;
    final Random random = new Random();
    failSampler = new Callable<Boolean>() {
      private int curr = -1;
      private int target = random.nextInt(failFrequncey);

      @Override
      public Boolean call() throws Exception {
        curr++;
        if (curr >= failFrequncey) {
          curr = 0;
          target = random.nextInt(failFrequncey);
        }
        return (curr == target);
      }
    };
  }

  @Override
  public void execute(Tuple tuple) {
    if (TupleUtils.isTick(tuple)) {
      LOG.info("tick tuple every " + tickFreqSecs + " secs ...");
    } else {
      try {
        if (failSampler.call()) {
          if (isTimeoutTest) {
            LOG.info("do nothing exclam-bolt:" + tuple.getString(0));
          } else {
            _collector.fail(tuple);
            LOG.info("fail exclam-bolt:" + tuple.getString(0));
          }
        } else  {
          _collector.emit("STRAEM_ID_WORD", tuple,
              new Values(tuple.getString(0) + "!!!"));
          _collector.ack(tuple);
          Values values = new Values();
          for (int i = 0; i < 3; i++) {
            values.add(tuple.getString(0) + "/" + i + ".exclam");
          }
          _collector.update(tuple, values);
          LOG.info("exclam-bolt:" + tuple.getString(0));
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
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