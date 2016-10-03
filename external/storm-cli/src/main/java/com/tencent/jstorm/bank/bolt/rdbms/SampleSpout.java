package com.tencent.jstorm.bank.bolt.rdbms;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Time;

/**
 * Class implementing a test spout emitting a stream of tuples, each having a
 * String and an integer
 */
public class SampleSpout implements IRichSpout {
  private static final long serialVersionUID = 1L;
  SpoutOutputCollector _collector;
  boolean _isDistributed;
  Random _rand;
  int count = 0;

  public SampleSpout() {
    this(true);
  }

  public SampleSpout(boolean isDistributed) {
    _isDistributed = isDistributed;
  }

  public boolean isDistributed() {
    return true;
  }

  @Override
  public void nextTuple() {
    try {
      Time.sleep(1000);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String[] words =
        new String[] { "indore", "india", "impetus", "tiwari", "jayati" };
    Integer[] numbers = new Integer[] { 11, 22, 33, 44, 55 };

    if (count == numbers.length - 1) {
      count = 0;
    }
    count++;
    int number = numbers[count];
    String word = words[count];
    _collector.emit(new Values(word, number));

  }

  @Override
  public void open(Map conf, TopologyContext context,
      SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
  }

  @Override
  public void close() {
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word", "number"));
  }

  public void activate() {
    // TODO Auto-generated method stub

  }

  public void deactivate() {
    // TODO Auto-generated method stub

  }

  public Map<String, Object> getComponentConfiguration() {
    // TODO Auto-generated method stub
    return null;
  }
}
