package com.tencent.jstorm.command;

import java.util.Map;

import org.apache.storm.command.CLI;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.utils.NimbusClient;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopologyDebug {
  private static final Logger LOG =
      LoggerFactory.getLogger(TopologyDebug.class);

  public static void main(String[] args) throws Exception {
    Map<String, Object> cl = CLI.opt("s", "sampling", 0, CLI.AS_INT)
        .opt("c", "component", "", CLI.AS_STRING)
        .opt("e", "enable", false, CLI.AS_STRING).arg("name", CLI.FIRST_WINS)
        .parse(args);
    final String name = (String) cl.get("name");
    final String component = (String) cl.get("c");
    final boolean enable = Boolean.valueOf((String) cl.get("e"));
    final double samplingPercentage =
        Utils.getInt(cl.get("s"), 0).doubleValue();
    NimbusClient.withConfiguredClient(new NimbusClient.WithNimbus() {
      @Override
      public void run(Nimbus.Client nimbus) throws Exception {
        nimbus.debug(name, component, enable, samplingPercentage);
        LOG.info("debug topology: {}, compnent{}, enable{}, sampling{}", name,
            component, enable, samplingPercentage);
      }
    });
  }
}
