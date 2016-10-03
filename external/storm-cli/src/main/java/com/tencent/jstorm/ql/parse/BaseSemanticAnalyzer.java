package com.tencent.jstorm.ql.parse;

import org.apache.storm.topology.TopologyBuilder;

public class BaseSemanticAnalyzer {
  private TopologyBuilder builder;
  private Context context;

  public BaseSemanticAnalyzer(TopologyBuilder builder) {
    builder = new TopologyBuilder();
    context = new Context();
  }

  public void analyzeInternal(ASTNode tree, Context context) throws Exception {

  }

  public void analyze(ASTNode tree, Context context) throws Exception {
    analyzeInternal(tree, context);
  }

  public TopologyBuilder getBuilder() {
    return builder;
  }

  public void setBuilder(TopologyBuilder builder) {
    this.builder = builder;
  }

  public Context getContext() {
    return context;
  }

  public void setContext(Context context) {
    this.context = context;
  }

}
