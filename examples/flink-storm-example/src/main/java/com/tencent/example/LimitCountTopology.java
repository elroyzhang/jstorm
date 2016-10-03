package com.tencent.example;

import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tencent.flink.config.FlinkStormConfig;

import backtype.storm.topology.TopologyBuilder;

public class LimitCountTopology {
  public static final Logger LOG =
      LoggerFactory.getLogger(LimitCountTopology.class);

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = {
      "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "test";

  @Option(name = "--limit", aliases = {
      "--limitCount" }, usage = "the limit count of spout send")
  private int _limit = 2000;

  @Option(name = "--num-workers", aliases = {
      "-n" }, usage = "num of workers default is 1 ")
  private int _numWorkers = 1;

  @Option(name = "--jobmanager-host", aliases = {
      "-jh" }, usage = "flink on yarn jobmanager hostname ")
  private String jobManager_host = "localhost";

  @Option(name = "--jobmanager-port", aliases = {
      "-jp" }, usage = "flink on yarn jobmanager port")
  private int jobManager_port = 6123;

  public static void main(String[] args) throws Exception {
    new LimitCountTopology().realMain(args);
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
    FlinkStormConfig conf = new FlinkStormConfig();
    conf.setDebug(false);
    conf.setNumWorkers(_numWorkers);
    conf.setNimibusHost(jobManager_host);
    conf.setNimibusThriftPort(jobManager_port);
    builder.setSpout("word", new TestWordSpout(_limit), 1);
    builder.setBolt("exclaim", new ExclamationBolt(), 2).shuffleGrouping("word",
        "STRAEM_ID_WORD");
    builder.setBolt("updater", new UpdaterBolt(), 3).shuffleGrouping("exclaim",
        "STRAEM_ID_WORD");
    FlinkSubmitter.submitTopology(_name, conf,
        FlinkTopology.createTopology(builder));
  }
}
