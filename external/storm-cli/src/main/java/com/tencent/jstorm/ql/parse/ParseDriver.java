package com.tencent.jstorm.ql.parse;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.Token;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.TokenStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeAdaptor;
import org.antlr.runtime.tree.TreeAdaptor;

public class ParseDriver {

  /**
   * Tree adaptor for making antlr return ASTNodes instead of CommonTree nodes
   * so that the graph walking algorithms and the rules framework defined in
   * ql.lib can be used with the AST Nodes.
   */
  static final TreeAdaptor adaptor = new CommonTreeAdaptor() {
    /**
     * Creates an ASTNode for the given token. The ASTNode is a wrapper around
     * antlr's CommonTree class that implements the Node interface.
     * 
     * @param payload The token.
     * @return Object (which is actually an ASTNode) for the token.
     */
    @Override
    public Object create(Token payload) {
      return new ASTNode(payload);
    }

    @Override
    public Object dupNode(Object t) {

      return create(((CommonTree) t).token);
    };

    @Override
    public Object errorNode(TokenStream input, Token start, Token stop,
        RecognitionException e) {
      return new ASTErrorNode(input, start, stop, e);
    };
  };

  public ASTNode parse(String command, boolean setTokenRewriteStream)
      throws RecognitionException {
    ANTLRStringStream in = new ANTLRStringStream(command);
    StormLexer lexer = new StormLexer(in);
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    StormParser parser = new StormParser(tokens);
    parser.setTreeAdaptor(adaptor);
    StormParser.statement_return r = parser.statement();
    ASTNode tree = (ASTNode) r.getTree();
    return tree;
  }

}
