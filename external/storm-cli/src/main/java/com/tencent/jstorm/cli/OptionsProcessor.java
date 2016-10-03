package com.tencent.jstorm.cli;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OptionsProcessor {
  protected static final Log l4j = LogFactory.getLog(OptionsProcessor.class
      .getName());
  private final Options options = new Options();
  private org.apache.commons.cli.CommandLine commandLine;
  Map<String, String> stormVariables = new HashMap<String, String>();

  @SuppressWarnings("static-access")
  public OptionsProcessor() {

    // -e 'quoted-query-string'
    options.addOption(OptionBuilder.hasArg().withArgName("quoted-query-string")
        .withDescription("SQL from command line").create('e'));

    // -f <query-file>
    options.addOption(OptionBuilder.hasArg().withArgName("filename")
        .withDescription("SQL from files").create('f'));

    // -i <init-query-file>
    options.addOption(OptionBuilder.hasArg().withArgName("filename")
        .withDescription("Initialization SQL file").create('i'));

    // -hiveconf x=y
    options.addOption(OptionBuilder.withValueSeparator().hasArgs(2)
        .withArgName("property=value").withLongOpt("hiveconf")
        .withDescription("Use value for given property").create());

    // -h hostname/ippaddress
    options.addOption(OptionBuilder.hasArg().withArgName("hostname")
        .withDescription("connecting to Hive Server on remote host")
        .create('h'));

    // -p port
    options.addOption(OptionBuilder.hasArg().withArgName("port")
        .withDescription("connecting to Hive Server on port number")
        .create('p'));

    // Substitution option -d, --define
    options
        .addOption(OptionBuilder
            .withValueSeparator()
            .hasArgs(2)
            .withArgName("key=value")
            .withLongOpt("define")
            .withDescription(
                "Variable subsitution to apply to hive commands. e.g. -d A=B or --define A=B")
            .create('d'));

    // Substitution option --hivevar
    options
        .addOption(OptionBuilder
            .withValueSeparator()
            .hasArgs(2)
            .withArgName("key=value")
            .withLongOpt("hivevar")
            .withDescription(
                "Variable subsitution to apply to hive commands. e.g. --hivevar A=B")
            .create());

    // [-S|--silent]
    options.addOption(new Option("S", "silent", false,
        "Silent mode in interactive shell"));

    // [-v|--verbose]
    options.addOption(new Option("v", "verbose", false,
        "Verbose mode (echo executed SQL to the console)"));

    // [-H|--help]
    options.addOption(new Option("H", "help", false, "Print help information"));

  }

  public boolean process_stage1(String[] argv) {
    try {
      commandLine = new GnuParser().parse(options, argv);
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        System.setProperty(propKey, confProps.getProperty(propKey));
      }

      Properties hiveVars = commandLine.getOptionProperties("define");
      for (String propKey : hiveVars.stringPropertyNames()) {
        stormVariables.put(propKey, hiveVars.getProperty(propKey));
      }

      Properties hiveVars2 = commandLine.getOptionProperties("hivevar");
      for (String propKey : hiveVars2.stringPropertyNames()) {
        stormVariables.put(propKey, hiveVars2.getProperty(propKey));
      }
    } catch (ParseException e) {
      System.err.println(e.getMessage());
      printUsage();
      return false;
    }
    return true;
  }

  public boolean process_stage2(CliSessionState ss) {
    ss.getConf();

    if (commandLine.hasOption('H')) {
      printUsage();
      return false;
    }

    ss.setIsSilent(commandLine.hasOption('S'));

    ss.execString = commandLine.getOptionValue('e');

    ss.fileName = commandLine.getOptionValue('f');

    ss.setIsVerbose(commandLine.hasOption('v'));

    ss.host = (String) commandLine.getOptionValue('h');

    ss.port =
        Integer.parseInt((String) commandLine.getOptionValue('p', "10000"));

    String[] initFiles = commandLine.getOptionValues('i');
    if (null != initFiles) {
      ss.initFiles = Arrays.asList(initFiles);
    }

    if (ss.execString != null && ss.fileName != null) {
      System.err
          .println("The '-e' and '-f' options cannot be specified simultaneously");
      printUsage();
      return false;
    }

    if (commandLine.hasOption("hiveconf")) {
      Properties confProps = commandLine.getOptionProperties("hiveconf");
      for (String propKey : confProps.stringPropertyNames()) {
        ss.cmdProperties.setProperty(propKey, confProps.getProperty(propKey));
      }
    }

    return true;
  }

  private void printUsage() {
    new HelpFormatter().printHelp("hive", options);
  }

  public Map<String, String> getStormVariables() {
    return stormVariables;
  }
}
