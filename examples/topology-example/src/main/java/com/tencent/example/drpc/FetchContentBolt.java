package com.tencent.example.drpc;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetchContentBolt extends BaseRichBolt {

  private static Logger LOG = LoggerFactory.getLogger(FetchContentBolt.class);

  OutputCollector _collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this._collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    String param = input.getString(0);
    LOG.info("param: " + input.getString(0));
    String result = "drpc test, param value is: " + param;
    _collector.emit(new Values(result, input.getString(1)));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("id", "result"));
  }

}
