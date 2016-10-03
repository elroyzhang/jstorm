package com.tencent.jstorm.daemon.common;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@ClojureClass(className = "backtype.storm.integration-test#emit-task-id")
public class EmitTaskIdBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(EmitTaskIdBolt.class);

  private static final long serialVersionUID = 1L;
  private int tid;
  private OutputCollector _collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    tid = context.getThisTaskIndex();
    _collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    _collector.emit(input, new Values(tid));
    _collector.ack(input);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("tid"));
  }

}