package com.tencent.jstorm.ql.parse;

import java.util.Map;

import antlr.SemanticException;

public class SemanticAnalyzerFactory {

  public static BaseSemanticAnalyzer get(Map conf, ASTNode tree)
      throws SemanticException {
    if (tree.getToken() == null) {
      throw new RuntimeException("Empty Syntax Tree");
    } else {
      switch (tree.getToken().getType()) {
      case StormParser.TOK_REGISTER:
        return new RegisterSemanticAnalyzer(null);
      default:
        return new SemanticAnalyzer(null);
      }
    }
  }
}
