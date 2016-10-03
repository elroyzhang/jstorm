package com.tencent.example.trident;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

public class TridentApp {
  public static void main(String[] args)
      throws FileNotFoundException, IOException, AlreadyAliveException,
      InvalidTopologyException, AuthorizationException {
    @SuppressWarnings("unchecked")
    FixedBatchSpout spout =
        new FixedBatchSpout(new Fields(new String[] { "sentence" }), 3,
            new List[] {
                new Values(new Object[] { "the cow jumped over the moon" }),
                new Values(new Object[] {
                    "the man went to the store and bought some candy" }),
            new Values(new Object[] { "four score and seven years ago" }),
            new Values(new Object[] { "how many apples can you eat" }) });

    spout.setCycle(true);

    TridentTopology topology = new TridentTopology();

    topology.newStream("spout1", spout)
        .each(new Fields(new String[] { "sentence" }), new Split(),
            new Fields(new String[] { "word" }))

        .parallelismHint(1).partitionBy(new Fields(new String[] { "word" }))
        .partitionAggregate(new Fields(new String[] { "word" }), new TAgg(),
            new Fields(new String[] { "sum" }))

        .parallelismHint(1);

    Config conf = new Config();

    int pvPending = Utils
        .getInt(conf.get("topology.max.pv.spout.pending"), Integer.valueOf(10))
        .intValue();
    conf.put("topology.max.spout.pending", new Integer(pvPending));

    int msgTimeout = Utils
        .getInt(conf.get("topology.message.timeout.secs"), Integer.valueOf(10))
        .intValue();
    conf.put("topology.message.timeout.secs", new Integer(msgTimeout));
    conf.setNumAckers(1);

    conf.setNumWorkers(Integer.parseInt(args[0]));

    StormSubmitter.submitTopology("wc1-topology", conf, topology.build());
  }
}