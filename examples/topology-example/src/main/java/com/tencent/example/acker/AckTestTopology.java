package com.tencent.example.acker;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mortbay.log.Log;

public class AckTestTopology {

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = {
      "--tn" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "test";

  @Option(name = "--worker-num", aliases = {
      "--wn" }, usage = "worker num default is 1")
  private int _worker_num = 1;

  @Option(name = "--ack-num", aliases = {
      "--an" }, metaVar = "NAME", usage = "ack num of topology, default value is work num")
  private int _ack_num = -1;

  @Option(name = "--limit", aliases = {
      "--limit" }, metaVar = "NAME", usage = "default limit is Long.MAX_VALUE")
  private Long _limit = 10000l;

  @Option(name = "--interval-secs", aliases = {
      "--is" }, usage = "interval for word default is 0")
  private int _interval_secs = 0;

  @Option(name = "--exclaim-fail-frequency", aliases = {
      "--eff" }, usage = "fail frequency for exclaim default is 100")
  private int _exclaim_fail_frequency = 100;

  @Option(name = "--updater-fail-frequency", aliases = {
      "--uff" }, usage = "fail frequency for updater default is 100")
  private int _updater_fail_frequency = 100;

  @Option(name = "--is-timeout-test", aliases = {
      "--itt" }, usage = "is timeout test for updater default is false: 1 for true, other for false")
  private int _is_timeout_test = 0;

  @Option(name = "--para1", aliases = {
      "--p1" }, usage = "paramllism for word default is 1")
  private int _para1 = 1;

  @Option(name = "--para2", aliases = {
      "--p2" }, usage = "paramllism for exclaim default is 1")
  private int _para2 = 1;

  @Option(name = "--para3", aliases = {
      "--p3" }, usage = "paramllism for update default is 1")
  private int _para3 = 1;

  public static void main(String[] args) throws Exception {
    new AckTestTopology().realMain(args);
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
    Log.info("_name " + _name);
    Log.info("_worker_num " + _worker_num);
    Log.info("_ack_num " + _ack_num);
    Log.info("_limit " + _limit);
    Log.info("_interval_secs " + _interval_secs);
    Log.info("_updater_fail_frequency " + _updater_fail_frequency);
    Log.info("_updater_fail_frequency " + _updater_fail_frequency);
    Log.info("_is_timeout_test " + _is_timeout_test);
    Log.info("_para1 " + _para1);
    Log.info("_para2 " + _para2);
    Log.info("_para3 " + _para3);
    conf.setDebug(false);
    conf.setNumWorkers(_worker_num);
    if (_ack_num != -1) {
      conf.setNumAckers(_ack_num); 
    }
    builder.setSpout("word", new AckTestWordSpout(_limit, _interval_secs),
        _para1);
    builder.setBolt("exclaim", new AckExclamationBolt(_exclaim_fail_frequency, _is_timeout_test == 1), _para2)
        .shuffleGrouping("word", "STRAEM_ID_WORD");
    builder
        .setBolt("updater",
            new AckUpdaterBolt(_updater_fail_frequency, _is_timeout_test == 1), _para3)
        .shuffleGrouping("exclaim", "STRAEM_ID_WORD");
    StormSubmitter.submitTopology(_name, conf, builder.createTopology());
  }
}
