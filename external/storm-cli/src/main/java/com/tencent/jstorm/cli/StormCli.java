package com.tencent.jstorm.cli;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jline.ConsoleReader;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.misc.Signal;
import sun.misc.SignalHandler;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.TopologyBuilder;

import com.tencent.jstorm.ql.CommandNeedRetryException;
import com.tencent.jstorm.ql.Driver;
import com.tencent.jstorm.ql.exec.Utils;
import com.tencent.jstorm.ql.exec.Utils.StreamPrinter;
import com.tencent.jstorm.ql.parse.BoltDesc;
import com.tencent.jstorm.ql.parse.Context;
import com.tencent.jstorm.ql.parse.SpoutDesc;
import com.tencent.jstorm.ql.parse.VariableSubstitution;
import com.tencent.jstorm.ql.processors.CommandProcessor;
import com.tencent.jstorm.ql.processors.CommandProcessorFactory;
import com.tencent.jstorm.ql.processors.CommandProcessorResponse;
import com.tencent.jstorm.ql.session.SessionState;
import com.tencent.jstorm.ql.session.SessionState.LogHelper;

public class StormCli {
  public static String prompt = null;
  public static String prompt2 = null; // when ';' is not yet seen
  private final LogHelper console;
  private Config conf;
  private Context context;

  public StormCli() {
    SessionState ss = SessionState.get();
    Log LOG = LogFactory.getLog("StormCli");
    console = new LogHelper(LOG);
    conf = (ss != null) ? ss.getConf() : new Config();
    context = new Context();
  }

  public static void main(String[] args) throws Exception {
    int ret = new StormCli().run(args);
    System.exit(ret);
  }

  private int run(String[] args) throws Exception {
    OptionsProcessor oproc = new OptionsProcessor();
    if (!oproc.process_stage1(args)) {
      return 1;
    }
    CliSessionState ss = new CliSessionState(new Config(SessionState.class));
    ss.in = System.in;

    try {
      ss.out = new PrintStream(System.out, true, "UTF-8");
      ss.info = new PrintStream(System.err, true, "UTF-8");
      ss.err = new CachingPrintStream(System.err, true, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return 3;
    }

    if (!oproc.process_stage2(ss)) {
      return 2;
    }
    Config conf = ss.getConf();
    for (Map.Entry<Object, Object> item : ss.cmdProperties.entrySet()) {
      conf.put((String) item.getKey(), (String) item.getValue());
      ss.getOverriddenConfigurations().put((String) item.getKey(),
          (String) item.getValue());
    }
    SessionState.start(ss);
    int ret = 0;
    try {
      ret = executeDriver(ss, conf, oproc);
    } catch (Exception e) {
      ss.close();
      throw e;
    }

    ss.close();
    return ret;
  }

  private int executeDriver(CliSessionState ss, Config conf,
      OptionsProcessor oproc) throws Exception {

    ClassLoader loader = conf.getClassLoader();
    // String auxJars = Config.getVar(conf, Config.ConfVars.StormAUXJARS);
    // if (StringUtils.isNotBlank(auxJars)) {
    // loader =
    // Utils.addToClassPath(loader, StringUtils.split(auxJars, ","));
    // }
    // conf.setClassLoader(loader);
    Thread.currentThread().setContextClassLoader(loader);
    StormCli cli = new StormCli();
    cli.setStormVariables(oproc.getStormVariables());
    if (ss.execString != null) {
      int cmdProcessStatus = cli.processLine(ss.execString, context, false);
      return cmdProcessStatus;
    }

    try {
      if (ss.fileName != null) {
        return cli.processFile(ss.fileName);
      }
    } catch (FileNotFoundException e) {
      System.err.println("Could not open input file for reading. ("
          + e.getMessage() + ")");
      return 3;
    }

    ConsoleReader reader = getConsoleReader();
    reader.setBellEnabled(false);
    String line;
    int ret = 0;

    String prefix = "";

    while ((line = reader.readLine("> ")) != null) {
      if (!prefix.equals("")) {
        prefix += '\n';
      }
      if (line.trim().endsWith(";") && !line.trim().endsWith("\\;")) {
        line = prefix + line;
        ret = cli.processLine(line, context, true);
        prefix = "";
      } else {
        prefix = prefix + line;
        continue;
      }
    }
    return ret;
  }

  public void setStormVariables(Map<String, String> hiveVariables) {
    SessionState.get().setHiveVariables(hiveVariables);
  }

  protected ConsoleReader getConsoleReader() throws IOException {
    return new ConsoleReader();
  }

  private int processFile(String fileName) throws FileNotFoundException {
    // TODO Auto-generated method stub
    return 0;
  }

  private int processLine(String line, Context context, boolean allowInterupting)
      throws Exception {
    SignalHandler oldSignal = null;
    Signal interupSignal = null;
    if (allowInterupting) {
      // Remember all threads that were running at the time we started line
      // processing.
      // Hook up the custom Ctrl+C handler while processing this line
      interupSignal = new Signal("INT");
      oldSignal = Signal.handle(interupSignal, new SignalHandler() {
        private final Thread cliThread = Thread.currentThread();
        private boolean interruptRequested;

        @Override
        public void handle(Signal signal) {
          boolean initialRequest = !interruptRequested;
          interruptRequested = true;

          // Kill the VM on second ctrl+c
          if (!initialRequest) {
            console.printInfo("Exiting the JVM");
            System.exit(127);
          }

          // Interrupt the CLI thread to stop the current statement and return
          // to prompt
          console
              .printInfo("Interrupting... Be patient, this might take some time.");
          console.printInfo("Press Ctrl+C again to kill JVM");
        }
      });
    }
    try {
      int lastRet = 0, ret = 0;

      String command = "";
      for (String oneCmd : line.split(";")) {

        if (StringUtils.endsWith(oneCmd, "\\")) {
          command += StringUtils.chop(oneCmd) + ";";
          continue;
        } else {
          command += oneCmd;
        }
        if (StringUtils.isBlank(command)) {
          continue;
        }

        ret = processCmd(command, context);
        // wipe cli query state
        SessionState ss = SessionState.get();
        command = "";
        lastRet = ret;
      }
      return lastRet;
    } finally {
      // Once we are done processing the line, restore the old handler
      if (oldSignal != null && interupSignal != null) {
        Signal.handle(interupSignal, oldSignal);
      }
    }
  }

  private String getFirstCmd(String cmd, int length) {
    return cmd.substring(length).trim();
  }

  private String[] tokenizeCmd(String cmd) {
    return cmd.split("\\s+");
  }

  public int processCmd(String cmd, Context context) throws Exception {
    CliSessionState ss = (CliSessionState) SessionState.get();
    ss.setLastCommand(cmd);
    ss.err.flush();
    String cmd_trimmed = cmd.trim();
    String[] tokens = tokenizeCmd(cmd_trimmed);
    int ret = 0;

    if (cmd_trimmed.toLowerCase().equals("quit")
        || cmd_trimmed.toLowerCase().equals("exit")) {

      ss.close();
      System.exit(0);

    } else if (cmd_trimmed.toLowerCase().equals("submit")) {
      // TODO
      TopologyBuilder builder = new TopologyBuilder();
      for (Map.Entry<String, SpoutDesc> s : context.getSpoutDescs().entrySet()) {
        String spoutAlias = s.getKey();
        SpoutDesc spout = s.getValue();
        builder.setSpout(spoutAlias, (IRichSpout) spout.getSpout(),
            spout.getParallelismHint());
      }
      for (Map.Entry<String, BoltDesc> b : context.getBoltDescs().entrySet()) {
        String boltAlias = b.getKey();
        BoltDesc bolt = b.getValue();
        String type = bolt.getGroupingType();
        if (type.equals("SHUFFLE")) {
          builder.setBolt(boltAlias, (IRichBolt) bolt.getBolt(),
              bolt.getParallelismHint()).shuffleGrouping(bolt.getComponentId());
        }
        if (type.equals("FIELDS")) {
          builder.setBolt(boltAlias, (IBasicBolt) bolt.getBolt(),
              bolt.getParallelismHint()).fieldsGrouping(bolt.getComponentId(),
              bolt.getFields());
        }
      }
      String topologyName =
          backtype.storm.utils.Utils.parseString(
              conf.get(Config.TOPOLOGY_NAME), "test");
      Integer workerNums =
          backtype.storm.utils.Utils.parseInt(
              conf.get(Config.TOPOLOGY_WORKERS), 1);
      conf.put(Config.TOPOLOGY_WORKERS, workerNums);
      StormSubmitter.submitTopologyWithProgressBar(topologyName, conf,
          builder.createTopology());
      ss.close();
      System.exit(0);

    } else if (tokens[0].equalsIgnoreCase("source")) {
      String cmd_1 = getFirstCmd(cmd_trimmed, tokens[0].length());
      cmd_1 = new VariableSubstitution().substitute(ss.getConf(), cmd_1);

      File sourceFile = new File(cmd_1);
      if (!sourceFile.isFile()) {
        console.printError("File: " + cmd_1 + " is not a file.");
        ret = 1;
      } else {
        try {
          this.processFile(cmd_1);
        } catch (IOException e) {
          console
              .printError(
                  "Failed processing file " + cmd_1 + " "
                      + e.getLocalizedMessage(),
                  com.tencent.jstorm.utils.StringUtils.stringifyException(e));
          ret = 1;
        }
      }
    } else if (cmd_trimmed.startsWith("!")) {

      String shell_cmd = cmd_trimmed.substring(1);
      shell_cmd =
          new VariableSubstitution().substitute(ss.getConf(), shell_cmd);

      // shell_cmd = "/bin/bash -c \'" + shell_cmd + "\'";
      try {
        Process executor = Runtime.getRuntime().exec(shell_cmd);
        Utils.StreamPrinter outPrinter =
            new StreamPrinter(executor.getInputStream(), null, ss.out);
        StreamPrinter errPrinter =
            new StreamPrinter(executor.getErrorStream(), null, ss.err);

        outPrinter.start();
        errPrinter.start();

        ret = executor.waitFor();
        if (ret != 0) {
          console.printError("Command failed with exit code = " + ret);
        }
      } catch (Exception e) {
        console.printError(
            "Exception raised from Shell command " + e.getLocalizedMessage(),
            com.tencent.jstorm.utils.StringUtils.stringifyException(e));
        ret = 1;
      }

    } else if (tokens[0].toLowerCase().equals("list")) {
      SessionState.ResourceType t;
      if (tokens.length < 2
          || (t = SessionState.find_resource_type(tokens[1])) == null) {
        console.printError("Usage: list ["
            + StringUtils.join(SessionState.ResourceType.values(), "|")
            + "] [<value> [<value>]*]");
        ret = 1;
      } else {
        List<String> filter = null;
        if (tokens.length >= 3) {
          System.arraycopy(tokens, 2, tokens, 0, tokens.length - 2);
          filter = Arrays.asList(tokens);
        }
        Set<String> s = ss.list_resource(t, filter);
        if (s != null && !s.isEmpty()) {
          ss.out.println(StringUtils.join(s, "\n"));
        }
      }
    } else { // local mode
      try {
        CommandProcessor proc = CommandProcessorFactory.get(tokens, conf);
        ret = processLocalCmd(cmd, proc, ss, context);
      } catch (SQLException e) {
        console.printError(
            "Failed processing command " + tokens[0] + " "
                + e.getLocalizedMessage(),
            com.tencent.jstorm.utils.StringUtils.stringifyException(e));
        ret = 1;
      }
    }

    return ret;
  }

  int processLocalCmd(String cmd, CommandProcessor proc, CliSessionState ss,
      Context context) throws Exception {
    int tryCount = 0;
    boolean needRetry;
    int ret = 0;

    do {
      try {
        needRetry = false;
        if (proc != null) {
          if (proc instanceof Driver) {
            Driver qp = (Driver) proc;
            PrintStream out = ss.out;
            long start = System.currentTimeMillis();
            if (ss.getIsVerbose()) {
              out.println(cmd);
            }

            qp.setTryCount(tryCount);
            ret = qp.run(cmd, context).getResponseCode();
            if (ret != 0) {
              qp.close();
              return ret;
            }

            // query has run capture the time
            long end = System.currentTimeMillis();
            double timeTaken = (end - start) / 1000.0;

            ArrayList<String> res = new ArrayList<String>();

            // print the results
            int counter = 0;

            int cret = qp.close();
            if (ret == 0) {
              ret = cret;
            }

            console.printInfo("Time taken: " + timeTaken + " seconds");
          } else {
            String firstToken = tokenizeCmd(cmd.trim())[0];
            String cmd_1 = getFirstCmd(cmd.trim(), firstToken.length());

            if (ss.getIsVerbose()) {
              ss.out.println(firstToken + " " + cmd_1);
            }
            CommandProcessorResponse res = proc.run(cmd_1, context);
            if (res.getResponseCode() != 0) {
              ss.out
                  .println("Query returned non-zero code: "
                      + res.getResponseCode() + ", cause: "
                      + res.getErrorMessage());
            }
            ret = res.getResponseCode();
          }
        }
      } catch (CommandNeedRetryException e) {
        console.printInfo("Retry query with a different approach...");
        tryCount++;
        needRetry = true;
      }
    } while (needRetry);

    return ret;
  }
}
