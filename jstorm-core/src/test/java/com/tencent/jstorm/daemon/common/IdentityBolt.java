package com.tencent.jstorm.daemon.common;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

@ClojureClass(className = "backtype.storm.integration-test#identity-bolt")
public class IdentityBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    collector.emit(input.getValues());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("num"));
  }

}
