package com.tencent.example.ras;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.mortbay.log.Log;

public class RASLimitCountTopology {
  @Option(name = "--help", aliases = { "-h" }, usage = "print help message")
  private boolean _help = false;

  @Option(name = "--name", aliases = {
      "--topologyName" }, metaVar = "NAME", usage = "name of the topology")
  private String _name = "test";

  @Option(name = "--limit", aliases = {
      "--limit" }, metaVar = "NAME", usage = "default limit is Long.MAX_VALUE")
  private Long _limit = Long.MAX_VALUE;

  @Option(name = "--worker-num", aliases = {
      "--wn" }, usage = "worker num default is 1")
  private int _worker_num = 1;

  @Option(name = "--ack-num", aliases = {
      "--an" }, metaVar = "NAME", usage = "ack num of topology, default value is work num")
  private int _ack_num = -1;

  @Option(name = "--onheap", aliases = {
      "-onheap" }, usage = "topology.component.resources.onheap.memory.mb default is 128.0")
  private double _onheap = 128.0;

  @Option(name = "--offheap", aliases = {
      "-offheap" }, usage = "topology.component.resources.offheap.memory.mb default is 0.0")
  private double _offheap = 0.0;

  @Option(name = "--cpu", aliases = {
      "-cpu" }, usage = "topology.component.cpu.pcore.percent default is 10.0")
  private double _cpu = 10.0;

  @Option(name = "--priority", aliases = {
      "--p" }, usage = "topology.priority default is 29")
  private int _priority = 29;

  @Option(name = "--max-worker-heap", aliases = {
      "--max" }, usage = "topology.worker.max.heap.size.mb default is 768.0")
  private double _maxWorkerHeap = 768.0;

  @Option(name = "--word-paramllism", aliases = {
      "--wp" }, usage = "word paramllism default is 1")
  private int _word_paramllism = 1;

  @Option(name = "--exclaim-paramllism", aliases = {
      "--ep" }, usage = "exclaim paramllism default is 1")
  private int _exclaim_paramllism = 1;

  @Option(name = "--updater-paramllism", aliases = {
      "--up" }, usage = "updater paramllism default is 1")
  private int _updater_paramllism = 1;

  @Option(name = "--word-task-num", aliases = {
      "--wtn" }, usage = "word task num default is 1")
  private int _word_task_num = 1;

  @Option(name = "--exclaim-task-num", aliases = {
      "--etn" }, usage = "exclaim task num default is 1")
  private int _exclaim_task_num = 1;

  @Option(name = "--updater-task-num", aliases = {
      "--utn" }, usage = "updater task num default is 1")
  private int _updater_task_num = 1;

  public static void main(String[] args) throws Exception {
    new RASLimitCountTopology().realMain(args);
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
    conf.setDebug(false);
    conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_ONHEAP_MEMORY_MB, _onheap);
    conf.put(Config.TOPOLOGY_COMPONENT_RESOURCES_OFFHEAP_MEMORY_MB, _offheap);
    conf.put(Config.TOPOLOGY_COMPONENT_CPU_PCORE_PERCENT, _cpu);
    conf.setTopologyPriority(_priority);
    conf.setTopologyWorkerMaxHeapSize(_maxWorkerHeap);
    conf.setNumWorkers(_worker_num);
    if (_ack_num != -1) {
      conf.setNumAckers(_ack_num);
    } else {
      _ack_num = _worker_num;
    }
    Log.info("_worker_num " + _worker_num);
    Log.info("_ack_num " + _ack_num);
    Log.info("_onheap " + _onheap);
    Log.info("_offheap " + _offheap);
    Log.info("_cpu " + _cpu);
    Log.info("_priority " + _priority);
    Log.info("_maxWorkerHeap " + _maxWorkerHeap);
    Log.info("_word_paramllism " + _word_paramllism);
    Log.info("_exclaim_paramllism " + _exclaim_paramllism);
    Log.info("_updater_paramllism " + _updater_paramllism);
    Log.info("_word_task_num " + _word_task_num);
    Log.info("_exclaim_task_num " + _exclaim_task_num);
    Log.info("_updater_task_num " + _updater_task_num);

    builder.setSpout("word", new RASTestWordSpout(_limit), _word_paramllism)
        .setNumTasks(_word_task_num);
    builder.setBolt("exclaim", new RASExclamationBolt(), _exclaim_paramllism)
        .shuffleGrouping("word", "STRAEM_ID_WORD")
        .setNumTasks(_exclaim_task_num);
    builder.setBolt("updater", new RASUpdaterBolt(), _updater_paramllism)
        .shuffleGrouping("exclaim", "STRAEM_ID_WORD")
        .setNumTasks(_updater_task_num);
    StormSubmitter.submitTopology(_name, conf, builder.createTopology());
  }
}
