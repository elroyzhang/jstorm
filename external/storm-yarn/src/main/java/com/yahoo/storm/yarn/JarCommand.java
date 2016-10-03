package com.yahoo.storm.yarn;

import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.utils.Utils;

import com.yahoo.storm.yarn.Client.ClientCommand;

public class JarCommand implements ClientCommand {
  private static final Logger LOG = LoggerFactory.getLogger(JarCommand.class);

  @Override
  public Options getOpts() {
    Options opts = new Options();
    opts.addOption("topologyJar", true, "jar of topology");
    opts.addOption("topologyMainClass", true, "jar main class of topology");
    opts.addOption(
        "args",
        true,
        "args of topologyMainClass separated by \",\"  eg: -args ./1.conf,./2.conf,./3.conf");
    opts.addOption("topologyName", true, "name of topology");
    opts.addOption("numWorkers", true, "number of workers");
    opts.addOption(
        "files",
        true,
        "config files of topology separated by \",\"  eg: -args /path/to/1.conf,/path/to/2.conf,/path/to/3.conf");

    opts.addOption("appname", true,
        "Application Name. Default value - Storm-on-Yarn");
    opts.addOption("queue", true,
        "RM Queue in which this application is to be submitted");
    opts.addOption("stormZip", true, "file path of storm.zip");
    opts.addOption("output", true, "Output file");
    return opts;
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public void process(CommandLine cl, Map stormConf) throws Exception {
    String appName = cl.getOptionValue("appname", "Storm-on-Yarn");
    String queue = cl.getOptionValue("queue", "default");

    String storm_zip_location = cl.getOptionValue("stormZip");
    Integer amSize = Utils.getInt(stormConf.get(Config.MASTER_SIZE_MB), 512);

    int numWorkers =
        Integer.valueOf((String) cl.getOptionValue("numWorkers", "1"));
    stormConf.put("topology.workers", numWorkers);

    String topologyJar = (String) cl.getOptionValue("topologyJar");
    if (topologyJar == null) {
      throw new IllegalArgumentException("-topologyJar is required");
    }
    stormConf.put("topologyJar", topologyJar);

    String topologyMainClass = (String) cl.getOptionValue("topologyMainClass");
    if (topologyMainClass == null) {
      throw new IllegalArgumentException("-topologyMainClass is required");
    }
    stormConf.put("topologyMainClass", topologyMainClass);

    String userArgs = (String) cl.getOptionValue("args");
    if (userArgs != null) {
      stormConf.put("userArgs", userArgs);
    }

    String userFiles = (String) cl.getOptionValue("files");
    if (userFiles != null) {
      stormConf.put("userFiles", userFiles);
    }

    StormOnYarn storm = null;
    try {
      storm =
          StormOnYarn.launchApplication(appName, queue, amSize, stormConf,
              storm_zip_location, topologyJar);
      LOG.info("Submitted application's ID:" + storm.getAppId());

      String output = cl.getOptionValue("output");
      if (output != null) {
        PrintStream os = new PrintStream(output);
        os.println(storm.getAppId());
        os.flush();
        os.close();
      }
    } finally {
      if (storm != null) {
        storm.stop();
      }
    }
  }

  @Override
  public String getHeaderDescription() {
    return "storm-yarn jar";
  }

}
