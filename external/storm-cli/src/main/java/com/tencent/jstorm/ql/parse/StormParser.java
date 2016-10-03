// $ANTLR 3.4 /home/ablecao/workspace/Storm.g 2014-05-19 20:04:40

package com.tencent.jstorm.ql.parse;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

import org.antlr.runtime.tree.*;


@SuppressWarnings({"all", "warnings", "unchecked"})
public class StormParser extends Parser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "AMPERSAND", "BITWISEOR", "BITWISEXOR", "COLON", "COMMA", "COMMENT", "CharSetLiteral", "CharSetName", "DIV", "DIVIDE", "DOLLAR", "DOT", "Digit", "EQUAL", "Exponent", "GREATERTHAN", "GREATERTHANOREQUALTO", "HexDigit", "Identifier", "KW_ADD", "KW_ALL", "KW_AND", "KW_ARRAY", "KW_BOLT", "KW_DISTINCT", "KW_DOT", "KW_EXISTS", "KW_EXPLAIN", "KW_FIELDS", "KW_FILE", "KW_FOR", "KW_IF", "KW_JAR", "KW_MAP", "KW_NONE", "KW_NOT", "KW_OR", "KW_REGISTER", "KW_SET", "KW_SHUFFLE", "KW_SPOUT", "LCURLY", "LESSTHAN", "LESSTHANOREQUALTO", "LPAREN", "LSQUARE", "Letter", "MINUS", "MOD", "NOTEQUAL", "Number", "PLUS", "QUESTION", "RCURLY", "RPAREN", "RSQUARE", "RegexComponent", "SEMICOLON", "STAR", "StringLiteral", "TILDE", "TOK_BOLT", "TOK_DOT", "TOK_EXPLAIN", "TOK_FOR", "TOK_FUNCTION", "TOK_OP_EQ", "TOK_PROPERTIES", "TOK_PROPERTY", "TOK_PROPLIST", "TOK_REGISTER", "TOK_SPOUT", "WS"
    };

    public static final int EOF=-1;
    public static final int AMPERSAND=4;
    public static final int BITWISEOR=5;
    public static final int BITWISEXOR=6;
    public static final int COLON=7;
    public static final int COMMA=8;
    public static final int COMMENT=9;
    public static final int CharSetLiteral=10;
    public static final int CharSetName=11;
    public static final int DIV=12;
    public static final int DIVIDE=13;
    public static final int DOLLAR=14;
    public static final int DOT=15;
    public static final int Digit=16;
    public static final int EQUAL=17;
    public static final int Exponent=18;
    public static final int GREATERTHAN=19;
    public static final int GREATERTHANOREQUALTO=20;
    public static final int HexDigit=21;
    public static final int Identifier=22;
    public static final int KW_ADD=23;
    public static final int KW_ALL=24;
    public static final int KW_AND=25;
    public static final int KW_ARRAY=26;
    public static final int KW_BOLT=27;
    public static final int KW_DISTINCT=28;
    public static final int KW_DOT=29;
    public static final int KW_EXISTS=30;
    public static final int KW_EXPLAIN=31;
    public static final int KW_FIELDS=32;
    public static final int KW_FILE=33;
    public static final int KW_FOR=34;
    public static final int KW_IF=35;
    public static final int KW_JAR=36;
    public static final int KW_MAP=37;
    public static final int KW_NONE=38;
    public static final int KW_NOT=39;
    public static final int KW_OR=40;
    public static final int KW_REGISTER=41;
    public static final int KW_SET=42;
    public static final int KW_SHUFFLE=43;
    public static final int KW_SPOUT=44;
    public static final int LCURLY=45;
    public static final int LESSTHAN=46;
    public static final int LESSTHANOREQUALTO=47;
    public static final int LPAREN=48;
    public static final int LSQUARE=49;
    public static final int Letter=50;
    public static final int MINUS=51;
    public static final int MOD=52;
    public static final int NOTEQUAL=53;
    public static final int Number=54;
    public static final int PLUS=55;
    public static final int QUESTION=56;
    public static final int RCURLY=57;
    public static final int RPAREN=58;
    public static final int RSQUARE=59;
    public static final int RegexComponent=60;
    public static final int SEMICOLON=61;
    public static final int STAR=62;
    public static final int StringLiteral=63;
    public static final int TILDE=64;
    public static final int TOK_BOLT=65;
    public static final int TOK_DOT=66;
    public static final int TOK_EXPLAIN=67;
    public static final int TOK_FOR=68;
    public static final int TOK_FUNCTION=69;
    public static final int TOK_OP_EQ=70;
    public static final int TOK_PROPERTIES=71;
    public static final int TOK_PROPERTY=72;
    public static final int TOK_PROPLIST=73;
    public static final int TOK_REGISTER=74;
    public static final int TOK_SPOUT=75;
    public static final int WS=76;

    // delegates
    public Parser[] getDelegates() {
        return new Parser[] {};
    }

    // delegators


    public StormParser(TokenStream input) {
        this(input, new RecognizerSharedState());
    }
    public StormParser(TokenStream input, RecognizerSharedState state) {
        super(input, state);
    }

protected TreeAdaptor adaptor = new CommonTreeAdaptor();

public void setTreeAdaptor(TreeAdaptor adaptor) {
    this.adaptor = adaptor;
}
public TreeAdaptor getTreeAdaptor() {
    return adaptor;
}
    public String[] getTokenNames() { return StormParser.tokenNames; }
    public String getGrammarFileName() { return "/home/ablecao/workspace/Storm.g"; }


      Stack msgs = new Stack<String>();


    public static class prog_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "prog"
    // /home/ablecao/workspace/Storm.g:72:1: prog : ( forstatement EOF | statement EOF );
    public final StormParser.prog_return prog() throws RecognitionException {
        StormParser.prog_return retval = new StormParser.prog_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token EOF2=null;
        Token EOF4=null;
        StormParser.forstatement_return forstatement1 =null;

        StormParser.statement_return statement3 =null;


        CommonTree EOF2_tree=null;
        CommonTree EOF4_tree=null;

        try {
            // /home/ablecao/workspace/Storm.g:72:5: ( forstatement EOF | statement EOF )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==KW_FOR) ) {
                alt1=1;
            }
            else if ( (LA1_0==KW_REGISTER) ) {
                alt1=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;

            }
            switch (alt1) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:72:7: forstatement EOF
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_forstatement_in_prog244);
                    forstatement1=forstatement();

                    state._fsp--;

                    adaptor.addChild(root_0, forstatement1.getTree());

                    EOF2=(Token)match(input,EOF,FOLLOW_EOF_in_prog246); 
                    EOF2_tree = 
                    (CommonTree)adaptor.create(EOF2)
                    ;
                    adaptor.addChild(root_0, EOF2_tree);


                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:73:5: statement EOF
                    {
                    root_0 = (CommonTree)adaptor.nil();


                    pushFollow(FOLLOW_statement_in_prog252);
                    statement3=statement();

                    state._fsp--;

                    adaptor.addChild(root_0, statement3.getTree());

                    EOF4=(Token)match(input,EOF,FOLLOW_EOF_in_prog254); 
                    EOF4_tree = 
                    (CommonTree)adaptor.create(EOF4)
                    ;
                    adaptor.addChild(root_0, EOF4_tree);


                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "prog"


    public static class forstatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "forstatement"
    // /home/ablecao/workspace/Storm.g:76:1: forstatement : KW_FOR LPAREN RPAREN block -> ^( TOK_FOR block ) ;
    public final StormParser.forstatement_return forstatement() throws RecognitionException {
        StormParser.forstatement_return retval = new StormParser.forstatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_FOR5=null;
        Token LPAREN6=null;
        Token RPAREN7=null;
        StormParser.block_return block8 =null;


        CommonTree KW_FOR5_tree=null;
        CommonTree LPAREN6_tree=null;
        CommonTree RPAREN7_tree=null;
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_KW_FOR=new RewriteRuleTokenStream(adaptor,"token KW_FOR");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_block=new RewriteRuleSubtreeStream(adaptor,"rule block");
        try {
            // /home/ablecao/workspace/Storm.g:76:13: ( KW_FOR LPAREN RPAREN block -> ^( TOK_FOR block ) )
            // /home/ablecao/workspace/Storm.g:76:15: KW_FOR LPAREN RPAREN block
            {
            KW_FOR5=(Token)match(input,KW_FOR,FOLLOW_KW_FOR_in_forstatement266);  
            stream_KW_FOR.add(KW_FOR5);


            LPAREN6=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_forstatement268);  
            stream_LPAREN.add(LPAREN6);


            RPAREN7=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_forstatement270);  
            stream_RPAREN.add(RPAREN7);


            pushFollow(FOLLOW_block_in_forstatement272);
            block8=block();

            state._fsp--;

            stream_block.add(block8.getTree());

            // AST REWRITE
            // elements: block
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 76:42: -> ^( TOK_FOR block )
            {
                // /home/ablecao/workspace/Storm.g:76:45: ^( TOK_FOR block )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_FOR, "TOK_FOR")
                , root_1);

                adaptor.addChild(root_1, stream_block.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "forstatement"


    public static class block_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "block"
    // /home/ablecao/workspace/Storm.g:79:1: block : LCURLY ( sentence )+ RCURLY ;
    public final StormParser.block_return block() throws RecognitionException {
        StormParser.block_return retval = new StormParser.block_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token LCURLY9=null;
        Token RCURLY11=null;
        StormParser.sentence_return sentence10 =null;


        CommonTree LCURLY9_tree=null;
        CommonTree RCURLY11_tree=null;

        try {
            // /home/ablecao/workspace/Storm.g:79:6: ( LCURLY ( sentence )+ RCURLY )
            // /home/ablecao/workspace/Storm.g:79:8: LCURLY ( sentence )+ RCURLY
            {
            root_0 = (CommonTree)adaptor.nil();


            LCURLY9=(Token)match(input,LCURLY,FOLLOW_LCURLY_in_block288); 
            LCURLY9_tree = 
            (CommonTree)adaptor.create(LCURLY9)
            ;
            adaptor.addChild(root_0, LCURLY9_tree);


            // /home/ablecao/workspace/Storm.g:79:15: ( sentence )+
            int cnt2=0;
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0==KW_REGISTER) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // /home/ablecao/workspace/Storm.g:79:15: sentence
            	    {
            	    pushFollow(FOLLOW_sentence_in_block290);
            	    sentence10=sentence();

            	    state._fsp--;

            	    adaptor.addChild(root_0, sentence10.getTree());

            	    }
            	    break;

            	default :
            	    if ( cnt2 >= 1 ) break loop2;
                        EarlyExitException eee =
                            new EarlyExitException(2, input);
                        throw eee;
                }
                cnt2++;
            } while (true);


            RCURLY11=(Token)match(input,RCURLY,FOLLOW_RCURLY_in_block293); 
            RCURLY11_tree = 
            (CommonTree)adaptor.create(RCURLY11)
            ;
            adaptor.addChild(root_0, RCURLY11_tree);


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "block"


    public static class sentence_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "sentence"
    // /home/ablecao/workspace/Storm.g:81:1: sentence : statement SEMICOLON ;
    public final StormParser.sentence_return sentence() throws RecognitionException {
        StormParser.sentence_return retval = new StormParser.sentence_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token SEMICOLON13=null;
        StormParser.statement_return statement12 =null;


        CommonTree SEMICOLON13_tree=null;

        try {
            // /home/ablecao/workspace/Storm.g:81:9: ( statement SEMICOLON )
            // /home/ablecao/workspace/Storm.g:81:11: statement SEMICOLON
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_statement_in_sentence300);
            statement12=statement();

            state._fsp--;

            adaptor.addChild(root_0, statement12.getTree());

            SEMICOLON13=(Token)match(input,SEMICOLON,FOLLOW_SEMICOLON_in_sentence302); 
            SEMICOLON13_tree = 
            (CommonTree)adaptor.create(SEMICOLON13)
            ;
            adaptor.addChild(root_0, SEMICOLON13_tree);


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "sentence"


    public static class statement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "statement"
    // /home/ablecao/workspace/Storm.g:83:1: statement : execStatement ;
    public final StormParser.statement_return statement() throws RecognitionException {
        StormParser.statement_return retval = new StormParser.statement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        StormParser.execStatement_return execStatement14 =null;



        try {
            // /home/ablecao/workspace/Storm.g:84:2: ( execStatement )
            // /home/ablecao/workspace/Storm.g:85:2: execStatement
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_execStatement_in_statement313);
            execStatement14=execStatement();

            state._fsp--;

            adaptor.addChild(root_0, execStatement14.getTree());

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "statement"


    public static class execStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "execStatement"
    // /home/ablecao/workspace/Storm.g:89:1: execStatement : registerStatement ;
    public final StormParser.execStatement_return execStatement() throws RecognitionException {
        StormParser.execStatement_return retval = new StormParser.execStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        StormParser.registerStatement_return registerStatement15 =null;



         msgs.push("statement"); 
        try {
            // /home/ablecao/workspace/Storm.g:92:5: ( registerStatement )
            // /home/ablecao/workspace/Storm.g:94:10: registerStatement
            {
            root_0 = (CommonTree)adaptor.nil();


            pushFollow(FOLLOW_registerStatement_in_execStatement352);
            registerStatement15=registerStatement();

            state._fsp--;

            adaptor.addChild(root_0, registerStatement15.getTree());

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "execStatement"


    public static class registerStatement_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "registerStatement"
    // /home/ablecao/workspace/Storm.g:98:1: registerStatement : KW_REGISTER keyFunctionProperty -> ^( TOK_REGISTER keyFunctionProperty ) ;
    public final StormParser.registerStatement_return registerStatement() throws RecognitionException {
        StormParser.registerStatement_return retval = new StormParser.registerStatement_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_REGISTER16=null;
        StormParser.keyFunctionProperty_return keyFunctionProperty17 =null;


        CommonTree KW_REGISTER16_tree=null;
        RewriteRuleTokenStream stream_KW_REGISTER=new RewriteRuleTokenStream(adaptor,"token KW_REGISTER");
        RewriteRuleSubtreeStream stream_keyFunctionProperty=new RewriteRuleSubtreeStream(adaptor,"rule keyFunctionProperty");
         msgs.push("register clause"); 
        try {
            // /home/ablecao/workspace/Storm.g:101:5: ( KW_REGISTER keyFunctionProperty -> ^( TOK_REGISTER keyFunctionProperty ) )
            // /home/ablecao/workspace/Storm.g:102:5: KW_REGISTER keyFunctionProperty
            {
            KW_REGISTER16=(Token)match(input,KW_REGISTER,FOLLOW_KW_REGISTER_in_registerStatement387);  
            stream_KW_REGISTER.add(KW_REGISTER16);


            pushFollow(FOLLOW_keyFunctionProperty_in_registerStatement389);
            keyFunctionProperty17=keyFunctionProperty();

            state._fsp--;

            stream_keyFunctionProperty.add(keyFunctionProperty17.getTree());

            // AST REWRITE
            // elements: keyFunctionProperty
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 102:37: -> ^( TOK_REGISTER keyFunctionProperty )
            {
                // /home/ablecao/workspace/Storm.g:102:40: ^( TOK_REGISTER keyFunctionProperty )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_REGISTER, "TOK_REGISTER")
                , root_1);

                adaptor.addChild(root_1, stream_keyFunctionProperty.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "registerStatement"


    public static class keyFunctionProperty_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "keyFunctionProperty"
    // /home/ablecao/workspace/Storm.g:106:2: keyFunctionProperty : key= Identifier EQUAL value= function -> ^( TOK_PROPERTY $key $value) ;
    public final StormParser.keyFunctionProperty_return keyFunctionProperty() throws RecognitionException {
        StormParser.keyFunctionProperty_return retval = new StormParser.keyFunctionProperty_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token key=null;
        Token EQUAL18=null;
        StormParser.function_return value =null;


        CommonTree key_tree=null;
        CommonTree EQUAL18_tree=null;
        RewriteRuleTokenStream stream_Identifier=new RewriteRuleTokenStream(adaptor,"token Identifier");
        RewriteRuleTokenStream stream_EQUAL=new RewriteRuleTokenStream(adaptor,"token EQUAL");
        RewriteRuleSubtreeStream stream_function=new RewriteRuleSubtreeStream(adaptor,"rule function");
         msgs.push("specifying key/function property"); 
        try {
            // /home/ablecao/workspace/Storm.g:109:5: (key= Identifier EQUAL value= function -> ^( TOK_PROPERTY $key $value) )
            // /home/ablecao/workspace/Storm.g:110:7: key= Identifier EQUAL value= function
            {
            key=(Token)match(input,Identifier,FOLLOW_Identifier_in_keyFunctionProperty439);  
            stream_Identifier.add(key);


            EQUAL18=(Token)match(input,EQUAL,FOLLOW_EQUAL_in_keyFunctionProperty441);  
            stream_EQUAL.add(EQUAL18);


            pushFollow(FOLLOW_function_in_keyFunctionProperty446);
            value=function();

            state._fsp--;

            stream_function.add(value.getTree());

            // AST REWRITE
            // elements: key, value
            // token labels: key
            // rule labels: retval, value
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleTokenStream stream_key=new RewriteRuleTokenStream(adaptor,"token key",key);
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
            RewriteRuleSubtreeStream stream_value=new RewriteRuleSubtreeStream(adaptor,"rule value",value!=null?value.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 110:44: -> ^( TOK_PROPERTY $key $value)
            {
                // /home/ablecao/workspace/Storm.g:110:47: ^( TOK_PROPERTY $key $value)
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_PROPERTY, "TOK_PROPERTY")
                , root_1);

                adaptor.addChild(root_1, stream_key.nextNode());

                adaptor.addChild(root_1, stream_value.nextTree());

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "keyFunctionProperty"


    public static class function_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "function"
    // /home/ablecao/workspace/Storm.g:114:1: function : ( KW_BOLT LPAREN ( ( expression COMMA Number )? ) RPAREN gp= groupingProperty -> ^( TOK_BOLT ( ( expression )+ )? Number $gp) | KW_SPOUT LPAREN ( ( expression COMMA Number )? ) RPAREN -> ^( TOK_SPOUT ( ( expression )+ )? Number ) );
    public final StormParser.function_return function() throws RecognitionException {
        StormParser.function_return retval = new StormParser.function_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token KW_BOLT19=null;
        Token LPAREN20=null;
        Token COMMA22=null;
        Token Number23=null;
        Token RPAREN24=null;
        Token KW_SPOUT25=null;
        Token LPAREN26=null;
        Token COMMA28=null;
        Token Number29=null;
        Token RPAREN30=null;
        StormParser.groupingProperty_return gp =null;

        StormParser.expression_return expression21 =null;

        StormParser.expression_return expression27 =null;


        CommonTree KW_BOLT19_tree=null;
        CommonTree LPAREN20_tree=null;
        CommonTree COMMA22_tree=null;
        CommonTree Number23_tree=null;
        CommonTree RPAREN24_tree=null;
        CommonTree KW_SPOUT25_tree=null;
        CommonTree LPAREN26_tree=null;
        CommonTree COMMA28_tree=null;
        CommonTree Number29_tree=null;
        CommonTree RPAREN30_tree=null;
        RewriteRuleTokenStream stream_KW_BOLT=new RewriteRuleTokenStream(adaptor,"token KW_BOLT");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_Number=new RewriteRuleTokenStream(adaptor,"token Number");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleTokenStream stream_KW_SPOUT=new RewriteRuleTokenStream(adaptor,"token KW_SPOUT");
        RewriteRuleSubtreeStream stream_expression=new RewriteRuleSubtreeStream(adaptor,"rule expression");
        RewriteRuleSubtreeStream stream_groupingProperty=new RewriteRuleSubtreeStream(adaptor,"rule groupingProperty");
         msgs.push("function specififunctioncation"); 
        try {
            // /home/ablecao/workspace/Storm.g:117:5: ( KW_BOLT LPAREN ( ( expression COMMA Number )? ) RPAREN gp= groupingProperty -> ^( TOK_BOLT ( ( expression )+ )? Number $gp) | KW_SPOUT LPAREN ( ( expression COMMA Number )? ) RPAREN -> ^( TOK_SPOUT ( ( expression )+ )? Number ) )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==KW_BOLT) ) {
                alt5=1;
            }
            else if ( (LA5_0==KW_SPOUT) ) {
                alt5=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;

            }
            switch (alt5) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:118:3: KW_BOLT LPAREN ( ( expression COMMA Number )? ) RPAREN gp= groupingProperty
                    {
                    KW_BOLT19=(Token)match(input,KW_BOLT,FOLLOW_KW_BOLT_in_function492);  
                    stream_KW_BOLT.add(KW_BOLT19);


                    LPAREN20=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_function500);  
                    stream_LPAREN.add(LPAREN20);


                    // /home/ablecao/workspace/Storm.g:120:5: ( ( expression COMMA Number )? )
                    // /home/ablecao/workspace/Storm.g:121:8: ( expression COMMA Number )?
                    {
                    // /home/ablecao/workspace/Storm.g:121:8: ( expression COMMA Number )?
                    int alt3=2;
                    int LA3_0 = input.LA(1);

                    if ( (LA3_0==Identifier||LA3_0==StringLiteral) ) {
                        alt3=1;
                    }
                    switch (alt3) {
                        case 1 :
                            // /home/ablecao/workspace/Storm.g:121:9: expression COMMA Number
                            {
                            pushFollow(FOLLOW_expression_in_function516);
                            expression21=expression();

                            state._fsp--;

                            stream_expression.add(expression21.getTree());

                            COMMA22=(Token)match(input,COMMA,FOLLOW_COMMA_in_function518);  
                            stream_COMMA.add(COMMA22);


                            Number23=(Token)match(input,Number,FOLLOW_Number_in_function520);  
                            stream_Number.add(Number23);


                            }
                            break;

                    }


                    }


                    RPAREN24=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_function539);  
                    stream_RPAREN.add(RPAREN24);


                    pushFollow(FOLLOW_groupingProperty_in_function548);
                    gp=groupingProperty();

                    state._fsp--;

                    stream_groupingProperty.add(gp.getTree());

                    // AST REWRITE
                    // elements: gp, Number, expression
                    // token labels: 
                    // rule labels: retval, gp
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);
                    RewriteRuleSubtreeStream stream_gp=new RewriteRuleSubtreeStream(adaptor,"rule gp",gp!=null?gp.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 125:11: -> ^( TOK_BOLT ( ( expression )+ )? Number $gp)
                    {
                        // /home/ablecao/workspace/Storm.g:125:14: ^( TOK_BOLT ( ( expression )+ )? Number $gp)
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_BOLT, "TOK_BOLT")
                        , root_1);

                        // /home/ablecao/workspace/Storm.g:125:25: ( ( expression )+ )?
                        if ( stream_expression.hasNext() ) {
                            if ( !(stream_expression.hasNext()) ) {
                                throw new RewriteEarlyExitException();
                            }
                            while ( stream_expression.hasNext() ) {
                                adaptor.addChild(root_1, stream_expression.nextTree());

                            }
                            stream_expression.reset();

                        }
                        stream_expression.reset();

                        adaptor.addChild(root_1, 
                        stream_Number.nextNode()
                        );

                        adaptor.addChild(root_1, stream_gp.nextTree());

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:126:5: KW_SPOUT LPAREN ( ( expression COMMA Number )? ) RPAREN
                    {
                    KW_SPOUT25=(Token)match(input,KW_SPOUT,FOLLOW_KW_SPOUT_in_function583);  
                    stream_KW_SPOUT.add(KW_SPOUT25);


                    LPAREN26=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_function593);  
                    stream_LPAREN.add(LPAREN26);


                    // /home/ablecao/workspace/Storm.g:128:5: ( ( expression COMMA Number )? )
                    // /home/ablecao/workspace/Storm.g:129:8: ( expression COMMA Number )?
                    {
                    // /home/ablecao/workspace/Storm.g:129:8: ( expression COMMA Number )?
                    int alt4=2;
                    int LA4_0 = input.LA(1);

                    if ( (LA4_0==Identifier||LA4_0==StringLiteral) ) {
                        alt4=1;
                    }
                    switch (alt4) {
                        case 1 :
                            // /home/ablecao/workspace/Storm.g:129:9: expression COMMA Number
                            {
                            pushFollow(FOLLOW_expression_in_function609);
                            expression27=expression();

                            state._fsp--;

                            stream_expression.add(expression27.getTree());

                            COMMA28=(Token)match(input,COMMA,FOLLOW_COMMA_in_function611);  
                            stream_COMMA.add(COMMA28);


                            Number29=(Token)match(input,Number,FOLLOW_Number_in_function613);  
                            stream_Number.add(Number29);


                            }
                            break;

                    }


                    }


                    RPAREN30=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_function631);  
                    stream_RPAREN.add(RPAREN30);


                    // AST REWRITE
                    // elements: expression, Number
                    // token labels: 
                    // rule labels: retval
                    // token list labels: 
                    // rule list labels: 
                    // wildcard labels: 
                    retval.tree = root_0;
                    RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

                    root_0 = (CommonTree)adaptor.nil();
                    // 132:7: -> ^( TOK_SPOUT ( ( expression )+ )? Number )
                    {
                        // /home/ablecao/workspace/Storm.g:132:10: ^( TOK_SPOUT ( ( expression )+ )? Number )
                        {
                        CommonTree root_1 = (CommonTree)adaptor.nil();
                        root_1 = (CommonTree)adaptor.becomeRoot(
                        (CommonTree)adaptor.create(TOK_SPOUT, "TOK_SPOUT")
                        , root_1);

                        // /home/ablecao/workspace/Storm.g:132:23: ( ( expression )+ )?
                        if ( stream_expression.hasNext() ) {
                            if ( !(stream_expression.hasNext()) ) {
                                throw new RewriteEarlyExitException();
                            }
                            while ( stream_expression.hasNext() ) {
                                adaptor.addChild(root_1, stream_expression.nextTree());

                            }
                            stream_expression.reset();

                        }
                        stream_expression.reset();

                        adaptor.addChild(root_1, 
                        stream_Number.nextNode()
                        );

                        adaptor.addChild(root_0, root_1);
                        }

                    }


                    retval.tree = root_0;

                    }
                    break;

            }
            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "function"


    public static class groupingProperty_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "groupingProperty"
    // /home/ablecao/workspace/Storm.g:135:2: groupingProperty : ( DOT groupingType LPAREN ( StringLiteral ( COMMA StringLiteral )* ) RPAREN )? -> ^( TOK_DOT groupingType ( ( StringLiteral )+ )? ) ;
    public final StormParser.groupingProperty_return groupingProperty() throws RecognitionException {
        StormParser.groupingProperty_return retval = new StormParser.groupingProperty_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token DOT31=null;
        Token LPAREN33=null;
        Token StringLiteral34=null;
        Token COMMA35=null;
        Token StringLiteral36=null;
        Token RPAREN37=null;
        StormParser.groupingType_return groupingType32 =null;


        CommonTree DOT31_tree=null;
        CommonTree LPAREN33_tree=null;
        CommonTree StringLiteral34_tree=null;
        CommonTree COMMA35_tree=null;
        CommonTree StringLiteral36_tree=null;
        CommonTree RPAREN37_tree=null;
        RewriteRuleTokenStream stream_StringLiteral=new RewriteRuleTokenStream(adaptor,"token StringLiteral");
        RewriteRuleTokenStream stream_RPAREN=new RewriteRuleTokenStream(adaptor,"token RPAREN");
        RewriteRuleTokenStream stream_DOT=new RewriteRuleTokenStream(adaptor,"token DOT");
        RewriteRuleTokenStream stream_COMMA=new RewriteRuleTokenStream(adaptor,"token COMMA");
        RewriteRuleTokenStream stream_LPAREN=new RewriteRuleTokenStream(adaptor,"token LPAREN");
        RewriteRuleSubtreeStream stream_groupingType=new RewriteRuleSubtreeStream(adaptor,"rule groupingType");
         msgs.push("specifying grouping property"); 
        try {
            // /home/ablecao/workspace/Storm.g:138:3: ( ( DOT groupingType LPAREN ( StringLiteral ( COMMA StringLiteral )* ) RPAREN )? -> ^( TOK_DOT groupingType ( ( StringLiteral )+ )? ) )
            // /home/ablecao/workspace/Storm.g:139:3: ( DOT groupingType LPAREN ( StringLiteral ( COMMA StringLiteral )* ) RPAREN )?
            {
            // /home/ablecao/workspace/Storm.g:139:3: ( DOT groupingType LPAREN ( StringLiteral ( COMMA StringLiteral )* ) RPAREN )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==DOT) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:139:4: DOT groupingType LPAREN ( StringLiteral ( COMMA StringLiteral )* ) RPAREN
                    {
                    DOT31=(Token)match(input,DOT,FOLLOW_DOT_in_groupingProperty689);  
                    stream_DOT.add(DOT31);


                    pushFollow(FOLLOW_groupingType_in_groupingProperty692);
                    groupingType32=groupingType();

                    state._fsp--;

                    stream_groupingType.add(groupingType32.getTree());

                    LPAREN33=(Token)match(input,LPAREN,FOLLOW_LPAREN_in_groupingProperty695);  
                    stream_LPAREN.add(LPAREN33);


                    // /home/ablecao/workspace/Storm.g:139:30: ( StringLiteral ( COMMA StringLiteral )* )
                    // /home/ablecao/workspace/Storm.g:139:31: StringLiteral ( COMMA StringLiteral )*
                    {
                    StringLiteral34=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_groupingProperty698);  
                    stream_StringLiteral.add(StringLiteral34);


                    // /home/ablecao/workspace/Storm.g:139:45: ( COMMA StringLiteral )*
                    loop6:
                    do {
                        int alt6=2;
                        int LA6_0 = input.LA(1);

                        if ( (LA6_0==COMMA) ) {
                            alt6=1;
                        }


                        switch (alt6) {
                    	case 1 :
                    	    // /home/ablecao/workspace/Storm.g:139:46: COMMA StringLiteral
                    	    {
                    	    COMMA35=(Token)match(input,COMMA,FOLLOW_COMMA_in_groupingProperty701);  
                    	    stream_COMMA.add(COMMA35);


                    	    StringLiteral36=(Token)match(input,StringLiteral,FOLLOW_StringLiteral_in_groupingProperty703);  
                    	    stream_StringLiteral.add(StringLiteral36);


                    	    }
                    	    break;

                    	default :
                    	    break loop6;
                        }
                    } while (true);


                    }


                    RPAREN37=(Token)match(input,RPAREN,FOLLOW_RPAREN_in_groupingProperty709);  
                    stream_RPAREN.add(RPAREN37);


                    }
                    break;

            }


            // AST REWRITE
            // elements: groupingType, StringLiteral
            // token labels: 
            // rule labels: retval
            // token list labels: 
            // rule list labels: 
            // wildcard labels: 
            retval.tree = root_0;
            RewriteRuleSubtreeStream stream_retval=new RewriteRuleSubtreeStream(adaptor,"rule retval",retval!=null?retval.tree:null);

            root_0 = (CommonTree)adaptor.nil();
            // 140:3: -> ^( TOK_DOT groupingType ( ( StringLiteral )+ )? )
            {
                // /home/ablecao/workspace/Storm.g:140:6: ^( TOK_DOT groupingType ( ( StringLiteral )+ )? )
                {
                CommonTree root_1 = (CommonTree)adaptor.nil();
                root_1 = (CommonTree)adaptor.becomeRoot(
                (CommonTree)adaptor.create(TOK_DOT, "TOK_DOT")
                , root_1);

                adaptor.addChild(root_1, stream_groupingType.nextTree());

                // /home/ablecao/workspace/Storm.g:140:29: ( ( StringLiteral )+ )?
                if ( stream_StringLiteral.hasNext() ) {
                    if ( !(stream_StringLiteral.hasNext()) ) {
                        throw new RewriteEarlyExitException();
                    }
                    while ( stream_StringLiteral.hasNext() ) {
                        adaptor.addChild(root_1, 
                        stream_StringLiteral.nextNode()
                        );

                    }
                    stream_StringLiteral.reset();

                }
                stream_StringLiteral.reset();

                adaptor.addChild(root_0, root_1);
                }

            }


            retval.tree = root_0;

            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "groupingProperty"


    public static class expression_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "expression"
    // /home/ablecao/workspace/Storm.g:144:1: expression : ( Identifier | StringLiteral );
    public final StormParser.expression_return expression() throws RecognitionException {
        StormParser.expression_return retval = new StormParser.expression_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set38=null;

        CommonTree set38_tree=null;

        try {
            // /home/ablecao/workspace/Storm.g:145:2: ( Identifier | StringLiteral )
            // /home/ablecao/workspace/Storm.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set38=(Token)input.LT(1);

            if ( input.LA(1)==Identifier||input.LA(1)==StringLiteral ) {
                input.consume();
                adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set38)
                );
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "expression"


    public static class functionName_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "functionName"
    // /home/ablecao/workspace/Storm.g:151:1: functionName : ( KW_BOLT | KW_SPOUT );
    public final StormParser.functionName_return functionName() throws RecognitionException {
        StormParser.functionName_return retval = new StormParser.functionName_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set39=null;

        CommonTree set39_tree=null;

         msgs.push("function name"); 
        try {
            // /home/ablecao/workspace/Storm.g:154:5: ( KW_BOLT | KW_SPOUT )
            // /home/ablecao/workspace/Storm.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set39=(Token)input.LT(1);

            if ( input.LA(1)==KW_BOLT||input.LA(1)==KW_SPOUT ) {
                input.consume();
                adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set39)
                );
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "functionName"


    public static class groupingType_return extends ParserRuleReturnScope {
        CommonTree tree;
        public Object getTree() { return tree; }
    };


    // $ANTLR start "groupingType"
    // /home/ablecao/workspace/Storm.g:158:1: groupingType : ( KW_ALL | KW_NONE | KW_SHUFFLE | KW_FIELDS );
    public final StormParser.groupingType_return groupingType() throws RecognitionException {
        StormParser.groupingType_return retval = new StormParser.groupingType_return();
        retval.start = input.LT(1);


        CommonTree root_0 = null;

        Token set40=null;

        CommonTree set40_tree=null;

         msgs.push("grouping type"); 
        try {
            // /home/ablecao/workspace/Storm.g:161:5: ( KW_ALL | KW_NONE | KW_SHUFFLE | KW_FIELDS )
            // /home/ablecao/workspace/Storm.g:
            {
            root_0 = (CommonTree)adaptor.nil();


            set40=(Token)input.LT(1);

            if ( input.LA(1)==KW_ALL||input.LA(1)==KW_FIELDS||input.LA(1)==KW_NONE||input.LA(1)==KW_SHUFFLE ) {
                input.consume();
                adaptor.addChild(root_0, 
                (CommonTree)adaptor.create(set40)
                );
                state.errorRecovery=false;
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                throw mse;
            }


            }

            retval.stop = input.LT(-1);


            retval.tree = (CommonTree)adaptor.rulePostProcessing(root_0);
            adaptor.setTokenBoundaries(retval.tree, retval.start, retval.stop);

             msgs.pop(); 
        }

        catch (RecognitionException e) {
         reportError(e);
          throw e;
        }

        finally {
        	// do for sure before leaving
        }
        return retval;
    }
    // $ANTLR end "groupingType"

    // Delegated rules


 

    public static final BitSet FOLLOW_forstatement_in_prog244 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_prog246 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statement_in_prog252 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_prog254 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_FOR_in_forstatement266 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_LPAREN_in_forstatement268 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_forstatement270 = new BitSet(new long[]{0x0000200000000000L});
    public static final BitSet FOLLOW_block_in_forstatement272 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_LCURLY_in_block288 = new BitSet(new long[]{0x0000020000000000L});
    public static final BitSet FOLLOW_sentence_in_block290 = new BitSet(new long[]{0x0200020000000000L});
    public static final BitSet FOLLOW_RCURLY_in_block293 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_statement_in_sentence300 = new BitSet(new long[]{0x2000000000000000L});
    public static final BitSet FOLLOW_SEMICOLON_in_sentence302 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_execStatement_in_statement313 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_registerStatement_in_execStatement352 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_REGISTER_in_registerStatement387 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_keyFunctionProperty_in_registerStatement389 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_Identifier_in_keyFunctionProperty439 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_EQUAL_in_keyFunctionProperty441 = new BitSet(new long[]{0x0000100008000000L});
    public static final BitSet FOLLOW_function_in_keyFunctionProperty446 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_BOLT_in_function492 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_LPAREN_in_function500 = new BitSet(new long[]{0x8400000000400000L});
    public static final BitSet FOLLOW_expression_in_function516 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_COMMA_in_function518 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_Number_in_function520 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_function539 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_groupingProperty_in_function548 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_KW_SPOUT_in_function583 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_LPAREN_in_function593 = new BitSet(new long[]{0x8400000000400000L});
    public static final BitSet FOLLOW_expression_in_function609 = new BitSet(new long[]{0x0000000000000100L});
    public static final BitSet FOLLOW_COMMA_in_function611 = new BitSet(new long[]{0x0040000000000000L});
    public static final BitSet FOLLOW_Number_in_function613 = new BitSet(new long[]{0x0400000000000000L});
    public static final BitSet FOLLOW_RPAREN_in_function631 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_DOT_in_groupingProperty689 = new BitSet(new long[]{0x0000084101000000L});
    public static final BitSet FOLLOW_groupingType_in_groupingProperty692 = new BitSet(new long[]{0x0001000000000000L});
    public static final BitSet FOLLOW_LPAREN_in_groupingProperty695 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_StringLiteral_in_groupingProperty698 = new BitSet(new long[]{0x0400000000000100L});
    public static final BitSet FOLLOW_COMMA_in_groupingProperty701 = new BitSet(new long[]{0x8000000000000000L});
    public static final BitSet FOLLOW_StringLiteral_in_groupingProperty703 = new BitSet(new long[]{0x0400000000000100L});
    public static final BitSet FOLLOW_RPAREN_in_groupingProperty709 = new BitSet(new long[]{0x0000000000000002L});

}