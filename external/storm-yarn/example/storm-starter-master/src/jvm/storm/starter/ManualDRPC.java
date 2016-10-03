package storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.DRPCSpout;
import org.apache.storm.drpc.ReturnResults;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class ManualDRPC {
  public static class ExclamationBolt extends BaseBasicBolt {

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("result", "return-info"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String arg = tuple.getString(0);
      Object retInfo = tuple.getValue(1);
      collector.emit(new Values(arg + "!!!", retInfo));
    }

  }

  public static void main(String[] args) {
    TopologyBuilder builder = new TopologyBuilder();
    LocalDRPC drpc = new LocalDRPC();

    DRPCSpout spout = new DRPCSpout("exclamation", drpc);
    builder.setSpout("drpc", spout);
    builder.setBolt("exclaim", new ExclamationBolt(), 3).shuffleGrouping("drpc");
    builder.setBolt("return", new ReturnResults(), 3).shuffleGrouping("exclaim");

    LocalCluster cluster = new LocalCluster();
    Config conf = new Config();
    cluster.submitTopology("exclaim", conf, builder.createTopology());

    System.out.println(drpc.execute("exclamation", "aaa"));
    System.out.println(drpc.execute("exclamation", "bbb"));

  }
}
