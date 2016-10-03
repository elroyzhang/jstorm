package com.tencent.example.bp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.SpoutDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mortbay.log.Log;

public class BPLimitCountTopology {
  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = {
      "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "test";

  @Option(name = "--limit", aliases = {
      "--limit" }, metaVar = "NAME", usage = "default limit is Long.MAX_VALUE")
  private Long _limit = Long.MAX_VALUE;

  @Option(name = "--interval1", aliases = {
      "--i1" }, usage = "interval for word default is 0 ns")
  private int _interval1 = 0;

  @Option(name = "--interval2", aliases = {
      "--i2" }, usage = "interval for exclaim default is 0 ns")
  private int _interval2 = 0;

  @Option(name = "--interval3", aliases = {
      "--i3" }, usage = "interval for update default is 0 ns")
  private int _interval3 = 0;

  @Option(name = "--para1", aliases = {
      "--p1" }, usage = "paramllism for word default is 1")
  private int _para1 = 1;

  @Option(name = "--para2", aliases = {
      "--p2" }, usage = "paramllism for exclaim default is 1")
  private int _para2 = 1;

  @Option(name = "--para3", aliases = {
      "--p3" }, usage = "paramllism for update default is 1")
  private int _para3 = 1;

  @Option(name = "--task1", aliases = {
      "--t1" }, usage = "tasknum for word default is 1")
  private int _task1 = -1;

  @Option(name = "--task2", aliases = {
      "--t2" }, usage = "tasknum for exclaim default is 1")
  private int _task2 = -1;

  @Option(name = "--task3", aliases = {
      "--t3" }, usage = "tasknum for update default is 1")
  private int _task3 = -1;

  @Option(name = "--worker-num", aliases = {
      "--w" }, usage = "worker num default is 1")
  private int _workerNum = 1;

  @Option(name = "--max-pending", aliases = {
      "--maxp" }, usage = "topology.max.spout.pending default is null")
  private int _maxPending = -1;

  @Option(name = "--backpress", aliases = {
      "--bp" }, usage = "ABP default is 1 means true, 0 is false")
  private int _abp = 1;

  @Option(name = "--high", aliases = {
      "--high" }, usage = "backpressure.disruptor.high.watermark default is 0.9")
  private double _high = 0.9;

  @Option(name = "--low", aliases = {
      "--low" }, usage = "backpressure.disruptor.low.watermark default is 0.4")
  private double _low = 0.4;

  @Option(name = "--has-ack", aliases = {
      "--ack" }, usage = "has ack default is 1 means true, 0 is false")
  private int _hasAck = 1;

  @Option(name = "--debug-on", aliases = {
      "--d" }, usage = "topology.debug default is 0 means false, 1 meas true")
  private int _debugOn = 0;

  @Option(name = "--update", aliases = {
      "--u" }, usage = "update default is 0 is false, 1 means true")
  private int _update = 0;

  @Option(name = "--metrics", aliases = {
      "--m" }, usage = "metrics default is 0 means false, 1 is true")
  private int _metrics = 0;

  public static void main(String[] args) throws Exception {
    new BPLimitCountTopology().realMain(args);
  }

  public void realMain(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parser.setUsageWidth(80);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      _help = true;
    }
    if (_help) {
      parser.printUsage(System.err);
      System.err.println();
      return;
    }
    if (_name == null || _name.isEmpty()) {
      throw new IllegalArgumentException("privide topology name please");
    }

    TopologyBuilder builder = new TopologyBuilder();
    Config conf = new Config();
    Log.info("_interval1 " + _interval1);
    Log.info("_interval2 " + _interval2);
    Log.info("_interval3 " + _interval3);
    Log.info("_para1 " + _para1);
    Log.info("_para2 " + _para2);
    Log.info("_para3 " + _para3);
    Log.info("_task1 " + _task1);
    Log.info("_task2 " + _task2);
    Log.info("_task3 " + _task3);
    Log.info("_workerNum " + _workerNum);
    Log.info("_maxPending " + _maxPending);
    Log.info("_abp " + _abp);
    Log.info("_high " + _high);
    Log.info("_low " + _low);
    Log.info("_hasAck " + _hasAck);
    Log.info("_debugOn " + _debugOn);
    Log.info("_update " + _update);

    conf.setDebug(_debugOn == 1);
    conf.put(Config.TOPOLOGY_BACKPRESSURE_ENABLE, _abp == 1);
    conf.put(Config.BACKPRESSURE_DISRUPTOR_HIGH_WATERMARK, _high);
    conf.put(Config.BACKPRESSURE_DISRUPTOR_LOW_WATERMARK, _low);
    if (_maxPending > 0) {
      conf.setMaxSpoutPending(_maxPending);
    }
    if (_workerNum > 0) {
      conf.setNumWorkers(_workerNum);
    }
    if (_metrics > 0) {
      List<Map<String, Object>> registerInfo =
          new ArrayList<Map<String, Object>>();
      Map map1 = new HashMap<String, Object>();
      map1.put("class", "org.apache.storm.metric.LoggingMetricsConsumer");
      map1.put("parallelism.hint", 1);
      Map map2 = new HashMap<String, Object>();
      map2.put("class",
          "com.tencent.jstorm.metric.SystemHealthMetricsConsumer");
      map2.put("parallelism.hint", 1);
      registerInfo.add(map1);
      registerInfo.add(map2);
      conf.put(Config.TOPOLOGY_METRICS_CONSUMER_REGISTER, registerInfo);
    }

    SpoutDeclarer sd1 = builder.setSpout("word",
        new BPTestWordSpout(_limit, _interval1, _hasAck == 1), _para1);
    if (_task1 != -1) {
      sd1.setNumTasks(_task1);
    }
    BoltDeclarer bd1 = builder.setBolt("exclaim",
        new BPExclamationBolt(_interval2, _hasAck == 1, _update == 1), _para2)
        .shuffleGrouping("word", "STRAEM_ID_WORD");
    if (_task2 != -1) {
      bd1.setNumTasks(_task2);
    }
    BoltDeclarer bd2 = builder
        .setBolt("updater", new BPUpdaterBolt(_interval3, _hasAck == 1), _para3)
        .shuffleGrouping("exclaim", "STRAEM_ID_WORD");
    if (_task3 != -1) {
      bd2.setNumTasks(_task3);
    }
    StormSubmitter.submitTopology(_name, conf, builder.createTopology());
  }
}
