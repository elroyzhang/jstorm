package com.tencent.jstorm.daemon.common;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.tencent.jstorm.ClojureClass;
import org.apache.storm.hooks.ITaskHook;
import org.apache.storm.hooks.info.BoltAckInfo;
import org.apache.storm.hooks.info.BoltExecuteInfo;
import org.apache.storm.hooks.info.BoltFailInfo;
import org.apache.storm.hooks.info.EmitInfo;
import org.apache.storm.hooks.info.SpoutAckInfo;
import org.apache.storm.hooks.info.SpoutFailInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@ClojureClass(className = "backtype.storm.integration-test#hooks-bolt")
public class HooksBolt extends BaseRichBolt {

  private static final long serialVersionUID = 1L;
  private AtomicInteger acked = new AtomicInteger(0);
  private AtomicInteger failed = new AtomicInteger(0);
  private AtomicInteger executed = new AtomicInteger(0);
  private AtomicInteger emitted = new AtomicInteger(0);

  private OutputCollector _collector;

  @Override
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {

    _collector = collector;
    context.addTaskHook(new MyTaskHook());
  }

  @Override
  public void execute(Tuple input) {
    _collector.emit(new Values(emitted.get(), acked.get(), failed.get(),
        executed.get()));
    if (acked.get() == failed.get()) {
      _collector.ack(input);
    } else {
      _collector.fail(input);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("emit", "ack", "fail", "executed"));
  }

  class MyTaskHook implements ITaskHook {
    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map conf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void emit(EmitInfo info) {
      emitted.incrementAndGet();
    }

    @Override
    public void spoutAck(SpoutAckInfo info) {
    }

    @Override
    public void spoutFail(SpoutFailInfo info) {
    }

    @Override
    public void boltExecute(BoltExecuteInfo info) {
      executed.incrementAndGet();
    }

    @Override
    public void boltAck(BoltAckInfo info) {
      acked.incrementAndGet();
    }

    @Override
    public void boltFail(BoltFailInfo info) {
      failed.incrementAndGet();
    }

  }
}
