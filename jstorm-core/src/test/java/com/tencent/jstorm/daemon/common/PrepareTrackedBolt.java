package com.tencent.jstorm.daemon.common;

import java.util.Map;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

@ClojureClass(className = "backtype.storm.integration-test#prepare-tracked-bolt")
public class PrepareTrackedBolt extends BaseRichBolt {

  private static final long serialVersionUID = 1L;
  private OutputCollector _collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {

    IntegrationTest.isBoltPrepared.set(true);
    _collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    _collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

  }

}
