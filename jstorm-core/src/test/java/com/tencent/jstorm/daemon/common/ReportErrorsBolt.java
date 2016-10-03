package com.tencent.jstorm.daemon.common;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

@ClojureClass(className = "backtype.storm.integration-test#report-errors-bolt")
public class ReportErrorsBolt extends BaseBasicBolt {

  private static final long serialVersionUID = 1L;

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    int num = input.getInteger(0);
    for (int i = 0; i < num; i++) {
      collector.reportError(new RuntimeException());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

}
