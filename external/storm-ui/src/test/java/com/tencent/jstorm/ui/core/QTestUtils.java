package com.tencent.jstorm.ui.core;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author caokun {@link caofangkun@gmail.com}
 * @author zhangxun {@link xunzhang555@gmail.com}
 * 
 */
public class QTestUtils {

  public static int executeDiffCommand(String inFileName, String outFileName,
      boolean ignoreWhiteSpace) throws Exception {

    int result = 0;

    ArrayList<String> diffCommandArgs = new ArrayList<String>();
    diffCommandArgs.add("diff");

    // Text file comparison
    diffCommandArgs.add("-a");

    // Ignore changes in the amount of white space
    if (ignoreWhiteSpace) {
      diffCommandArgs.add("-b");
    }

    diffCommandArgs.add(inFileName);
    diffCommandArgs.add(outFileName);

    result = executeCmd(diffCommandArgs);
    return result;
  }

  private static int executeCmd(Collection<String> args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static int executeCmd(String[] args) throws Exception {
    return executeCmd(args, null, null);
  }

  private static int executeCmd(Collection<String> args, String outFile,
      String errFile) throws Exception {
    String[] cmdArray = args.toArray(new String[args.size()]);
    return executeCmd(cmdArray, outFile, errFile);
  }

  private static int executeCmd(String[] args, String outFile, String errFile)
      throws Exception {
    System.out.println("Running: "
        + org.apache.commons.lang.StringUtils.join(args, ' '));

    Process executor = Runtime.getRuntime().exec(args);

    int result = executor.waitFor();

    return result;
  }

}
