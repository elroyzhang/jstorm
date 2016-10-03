package com.tencent.example.error;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class ErrorLimitCountTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    Config conf = new Config();
    conf.setDebug(false);
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      String topologyName = args[0];
      long limit = Long.parseLong(args[1]);
      builder.setSpout("word", new ErrorTestWordSpout(limit), 1);
      builder.setBolt("exclaim", new ErrorExclamationBolt(), 1)
          .shuffleGrouping("word", "STRAEM_ID_WORD");
      builder.setBolt("updater", new ErrorUpdaterBolt(), 1)
          .shuffleGrouping("exclaim", "STRAEM_ID_WORD");
      StormSubmitter.submitTopology(topologyName, conf,
          builder.createTopology());
    }
  }
}
