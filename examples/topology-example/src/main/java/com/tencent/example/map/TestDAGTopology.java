package com.tencent.example.map;

import com.tencent.example.TestWordSpout;
import com.tencent.example.UpdaterBolt;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

public class TestDAGTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();
    Config conf = new Config();
    conf.setDebug(false);
    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);
      String topologyName = args[0];
      long limit = Long.parseLong(args[1]);

      builder.setSpout("word", new TestWordSpout(limit), 1);
      builder.setSpout("count", new TestCountSpout(limit), 1);

      builder.setBolt("exclaim", new NewExclamationBolt(), 2)
          .shuffleGrouping("word", "STRAEM_ID_WORD")
          .shuffleGrouping("count", "STRAEM_ID_COUNT");

      builder.setBolt("updater", new UpdaterBolt(), 1)
          .shuffleGrouping("exclaim", "STRAEM_ID_WORD");

      builder.setBolt("sender", new SenderBolt(), 1).shuffleGrouping("exclaim",
          "STRAEM_ID_COUNT");

      StormSubmitter.submitTopology(topologyName, conf,
          builder.createTopology());
    }
  }
}
