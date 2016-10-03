package com.tencent.jstorm.ql;

import ParseUtils.ParseUtils;

import com.tencent.jstorm.ql.parse.ASTNode;
import com.tencent.jstorm.ql.parse.BaseSemanticAnalyzer;
import com.tencent.jstorm.ql.parse.Context;
import com.tencent.jstorm.ql.parse.ParseDriver;
import com.tencent.jstorm.ql.parse.SemanticAnalyzerFactory;
import com.tencent.jstorm.ql.processors.CommandProcessor;
import com.tencent.jstorm.ql.processors.CommandProcessorResponse;

public class Driver implements CommandProcessor {

  public void setTryCount(int tryCount) {
    // TODO Auto-generated method stub

  }

  public int close() {
    return 0;
    // TODO Auto-generated method stub

  }

  public CommandProcessorResponse run(String command, Context context)
      throws Exception {
    return run(command, false, context);
  }

  private CommandProcessorResponse run(String command, boolean alreadyCompiled,
      Context context) throws Exception {
    CommandProcessorResponse cpr =
        runInternal(command, alreadyCompiled, context);
    return cpr;
  }

  private CommandProcessorResponse runInternal(String command,
      boolean alreadyCompiled, Context context) throws Exception {
    int ret = compile(command, context);
    return new CommandProcessorResponse(ret);
  }

  public int compile(String command, Context context) throws Exception {
    ParseDriver pd = new ParseDriver();
    ASTNode tree = pd.parse(command, false);
    tree = ParseUtils.findRootNonNullToken(tree);
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(null, tree);
    sem.analyze(tree, context);

    return 0;

  }

  @Override
  public void init() {
    // TODO Auto-generated method stub

  }

  public void destroy() {
    // TODO Auto-generated method stub

  }

  public static void main(String[] args) throws Exception {
    Driver d = new Driver();
    String cmd =
        "REGISTER spout=SPOUT(\"storm.starter.spout.RandomSentenceSpout\")";
    d.compile(cmd, new Context());
  }
}
