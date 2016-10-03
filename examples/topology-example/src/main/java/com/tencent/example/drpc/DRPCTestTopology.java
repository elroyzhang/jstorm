package com.tencent.example.drpc;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.TopologyBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

public class DRPCTestTopology {

  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = {
      "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "test";

  @Option(name = "--func-name", aliases = {
      "--func-name" }, metaVar = "NAME", usage = "func name of the drpc topology")
  private String _func_name = "drpc-test";

  @Option(name = "--drpc-paramllism", aliases = {
      "--dp" }, usage = "paramllism for drpc default is 1")
  private int _dp = 1;

  @Option(name = "--fetch-paramllism", aliases = {
      "--fp" }, usage = "paramllism for fetch default is 1")
  private int _fp = 1;

  @Option(name = "--return-paramllism", aliases = {
      "--rp" }, usage = "paramllism for return default is 1")
  private int _rp = 1;

  @Option(name = "--worker-num", aliases = {
      "--w" }, usage = "worker num default is 1")
  private int _workerNum = 1;

  public static void main(String[] args) throws Exception {
    new DRPCTestTopology().realMain(args);
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
    DRPCSpout spout = new DRPCSpout(_func_name);
    builder.setSpout("drpc", spout, _dp);
    builder.setBolt("fetch", new FetchContentBolt(), _fp)
        .shuffleGrouping("drpc");
    builder.setBolt("return", new ReturnResults(), _rp)
        .shuffleGrouping("fetch");
    Config conf = new Config();
    conf.setNumWorkers(_workerNum);
    StormSubmitter.submitTopology(_name, conf, builder.createTopology());
  }
}
