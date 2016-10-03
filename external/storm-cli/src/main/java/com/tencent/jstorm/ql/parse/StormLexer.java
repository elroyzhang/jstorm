// $ANTLR 3.4 /home/ablecao/workspace/Storm.g 2014-05-19 20:04:40
package com.tencent.jstorm.ql.parse;

import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked"})
public class StormLexer extends Lexer {
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
    // delegators
    public Lexer[] getDelegates() {
        return new Lexer[] {};
    }

    public StormLexer() {} 
    public StormLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public StormLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);
    }
    public String getGrammarFileName() { return "/home/ablecao/workspace/Storm.g"; }

    // $ANTLR start "KW_EXPLAIN"
    public final void mKW_EXPLAIN() throws RecognitionException {
        try {
            int _type = KW_EXPLAIN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:44:11: ( 'EXPLAIN' )
            // /home/ablecao/workspace/Storm.g:44:13: 'EXPLAIN'
            {
            match("EXPLAIN"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_EXPLAIN"

    // $ANTLR start "KW_ADD"
    public final void mKW_ADD() throws RecognitionException {
        try {
            int _type = KW_ADD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:45:7: ( 'ADD' )
            // /home/ablecao/workspace/Storm.g:45:9: 'ADD'
            {
            match("ADD"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_ADD"

    // $ANTLR start "KW_FILE"
    public final void mKW_FILE() throws RecognitionException {
        try {
            int _type = KW_FILE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:46:8: ( 'FILE' )
            // /home/ablecao/workspace/Storm.g:46:10: 'FILE'
            {
            match("FILE"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_FILE"

    // $ANTLR start "KW_JAR"
    public final void mKW_JAR() throws RecognitionException {
        try {
            int _type = KW_JAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:47:7: ( 'JAR' )
            // /home/ablecao/workspace/Storm.g:47:9: 'JAR'
            {
            match("JAR"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_JAR"

    // $ANTLR start "KW_EXISTS"
    public final void mKW_EXISTS() throws RecognitionException {
        try {
            int _type = KW_EXISTS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:48:10: ( 'EXISTS' )
            // /home/ablecao/workspace/Storm.g:48:12: 'EXISTS'
            {
            match("EXISTS"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_EXISTS"

    // $ANTLR start "KW_SET"
    public final void mKW_SET() throws RecognitionException {
        try {
            int _type = KW_SET;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:49:7: ( 'SET' )
            // /home/ablecao/workspace/Storm.g:49:9: 'SET'
            {
            match("SET"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_SET"

    // $ANTLR start "KW_NOT"
    public final void mKW_NOT() throws RecognitionException {
        try {
            int _type = KW_NOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:50:7: ( 'NOI' )
            // /home/ablecao/workspace/Storm.g:50:9: 'NOI'
            {
            match("NOI"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_NOT"

    // $ANTLR start "KW_AND"
    public final void mKW_AND() throws RecognitionException {
        try {
            int _type = KW_AND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:51:7: ( 'AND' )
            // /home/ablecao/workspace/Storm.g:51:9: 'AND'
            {
            match("AND"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_AND"

    // $ANTLR start "KW_OR"
    public final void mKW_OR() throws RecognitionException {
        try {
            int _type = KW_OR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:52:6: ( 'OR' )
            // /home/ablecao/workspace/Storm.g:52:8: 'OR'
            {
            match("OR"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_OR"

    // $ANTLR start "KW_DISTINCT"
    public final void mKW_DISTINCT() throws RecognitionException {
        try {
            int _type = KW_DISTINCT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:53:12: ( 'DISTINCT' )
            // /home/ablecao/workspace/Storm.g:53:14: 'DISTINCT'
            {
            match("DISTINCT"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_DISTINCT"

    // $ANTLR start "KW_IF"
    public final void mKW_IF() throws RecognitionException {
        try {
            int _type = KW_IF;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:54:6: ( 'IF' )
            // /home/ablecao/workspace/Storm.g:54:8: 'IF'
            {
            match("IF"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_IF"

    // $ANTLR start "KW_ARRAY"
    public final void mKW_ARRAY() throws RecognitionException {
        try {
            int _type = KW_ARRAY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:55:9: ( 'ARRAY' )
            // /home/ablecao/workspace/Storm.g:55:11: 'ARRAY'
            {
            match("ARRAY"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_ARRAY"

    // $ANTLR start "KW_MAP"
    public final void mKW_MAP() throws RecognitionException {
        try {
            int _type = KW_MAP;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:56:7: ( 'MAP' )
            // /home/ablecao/workspace/Storm.g:56:9: 'MAP'
            {
            match("MAP"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_MAP"

    // $ANTLR start "KW_REGISTER"
    public final void mKW_REGISTER() throws RecognitionException {
        try {
            int _type = KW_REGISTER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:57:12: ( 'REGISTER' )
            // /home/ablecao/workspace/Storm.g:57:14: 'REGISTER'
            {
            match("REGISTER"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_REGISTER"

    // $ANTLR start "KW_FOR"
    public final void mKW_FOR() throws RecognitionException {
        try {
            int _type = KW_FOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:58:8: ( 'FOR' )
            // /home/ablecao/workspace/Storm.g:58:10: 'FOR'
            {
            match("FOR"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_FOR"

    // $ANTLR start "KW_SPOUT"
    public final void mKW_SPOUT() throws RecognitionException {
        try {
            int _type = KW_SPOUT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:59:9: ( 'SPOUT' )
            // /home/ablecao/workspace/Storm.g:59:11: 'SPOUT'
            {
            match("SPOUT"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_SPOUT"

    // $ANTLR start "KW_BOLT"
    public final void mKW_BOLT() throws RecognitionException {
        try {
            int _type = KW_BOLT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:60:9: ( 'BOLT' )
            // /home/ablecao/workspace/Storm.g:60:10: 'BOLT'
            {
            match("BOLT"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_BOLT"

    // $ANTLR start "KW_ALL"
    public final void mKW_ALL() throws RecognitionException {
        try {
            int _type = KW_ALL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:61:8: ( 'ALL' )
            // /home/ablecao/workspace/Storm.g:61:9: 'ALL'
            {
            match("ALL"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_ALL"

    // $ANTLR start "KW_NONE"
    public final void mKW_NONE() throws RecognitionException {
        try {
            int _type = KW_NONE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:62:9: ( 'NONE' )
            // /home/ablecao/workspace/Storm.g:62:10: 'NONE'
            {
            match("NONE"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_NONE"

    // $ANTLR start "KW_DOT"
    public final void mKW_DOT() throws RecognitionException {
        try {
            int _type = KW_DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:63:8: ( 'DOT' )
            // /home/ablecao/workspace/Storm.g:63:9: 'DOT'
            {
            match("DOT"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_DOT"

    // $ANTLR start "KW_FIELDS"
    public final void mKW_FIELDS() throws RecognitionException {
        try {
            int _type = KW_FIELDS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:64:11: ( 'FIELDS' )
            // /home/ablecao/workspace/Storm.g:64:12: 'FIELDS'
            {
            match("FIELDS"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_FIELDS"

    // $ANTLR start "KW_SHUFFLE"
    public final void mKW_SHUFFLE() throws RecognitionException {
        try {
            int _type = KW_SHUFFLE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:65:12: ( 'SHUFFLE' )
            // /home/ablecao/workspace/Storm.g:65:13: 'SHUFFLE'
            {
            match("SHUFFLE"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "KW_SHUFFLE"

    // $ANTLR start "DOT"
    public final void mDOT() throws RecognitionException {
        try {
            int _type = DOT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:166:5: ( '.' )
            // /home/ablecao/workspace/Storm.g:166:7: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DOT"

    // $ANTLR start "COLON"
    public final void mCOLON() throws RecognitionException {
        try {
            int _type = COLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:167:7: ( ':' )
            // /home/ablecao/workspace/Storm.g:167:9: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COLON"

    // $ANTLR start "COMMA"
    public final void mCOMMA() throws RecognitionException {
        try {
            int _type = COMMA;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:168:7: ( ',' )
            // /home/ablecao/workspace/Storm.g:168:9: ','
            {
            match(','); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COMMA"

    // $ANTLR start "SEMICOLON"
    public final void mSEMICOLON() throws RecognitionException {
        try {
            int _type = SEMICOLON;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:169:11: ( ';' )
            // /home/ablecao/workspace/Storm.g:169:13: ';'
            {
            match(';'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "SEMICOLON"

    // $ANTLR start "LPAREN"
    public final void mLPAREN() throws RecognitionException {
        try {
            int _type = LPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:171:8: ( '(' )
            // /home/ablecao/workspace/Storm.g:171:10: '('
            {
            match('('); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LPAREN"

    // $ANTLR start "RPAREN"
    public final void mRPAREN() throws RecognitionException {
        try {
            int _type = RPAREN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:172:8: ( ')' )
            // /home/ablecao/workspace/Storm.g:172:10: ')'
            {
            match(')'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "RPAREN"

    // $ANTLR start "LSQUARE"
    public final void mLSQUARE() throws RecognitionException {
        try {
            int _type = LSQUARE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:173:9: ( '[' )
            // /home/ablecao/workspace/Storm.g:173:11: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LSQUARE"

    // $ANTLR start "RSQUARE"
    public final void mRSQUARE() throws RecognitionException {
        try {
            int _type = RSQUARE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:174:9: ( ']' )
            // /home/ablecao/workspace/Storm.g:174:11: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "RSQUARE"

    // $ANTLR start "LCURLY"
    public final void mLCURLY() throws RecognitionException {
        try {
            int _type = LCURLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:175:8: ( '{' )
            // /home/ablecao/workspace/Storm.g:175:10: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LCURLY"

    // $ANTLR start "RCURLY"
    public final void mRCURLY() throws RecognitionException {
        try {
            int _type = RCURLY;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:176:8: ( '}' )
            // /home/ablecao/workspace/Storm.g:176:10: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "RCURLY"

    // $ANTLR start "EQUAL"
    public final void mEQUAL() throws RecognitionException {
        try {
            int _type = EQUAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:178:7: ( '=' | '==' )
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0=='=') ) {
                int LA1_1 = input.LA(2);

                if ( (LA1_1=='=') ) {
                    alt1=2;
                }
                else {
                    alt1=1;
                }
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;

            }
            switch (alt1) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:178:9: '='
                    {
                    match('='); 

                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:178:15: '=='
                    {
                    match("=="); 



                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "EQUAL"

    // $ANTLR start "NOTEQUAL"
    public final void mNOTEQUAL() throws RecognitionException {
        try {
            int _type = NOTEQUAL;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:179:10: ( '<>' | '!=' )
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='<') ) {
                alt2=1;
            }
            else if ( (LA2_0=='!') ) {
                alt2=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 2, 0, input);

                throw nvae;

            }
            switch (alt2) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:179:12: '<>'
                    {
                    match("<>"); 



                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:179:19: '!='
                    {
                    match("!="); 



                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "NOTEQUAL"

    // $ANTLR start "LESSTHANOREQUALTO"
    public final void mLESSTHANOREQUALTO() throws RecognitionException {
        try {
            int _type = LESSTHANOREQUALTO;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:180:19: ( '<=' )
            // /home/ablecao/workspace/Storm.g:180:21: '<='
            {
            match("<="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LESSTHANOREQUALTO"

    // $ANTLR start "LESSTHAN"
    public final void mLESSTHAN() throws RecognitionException {
        try {
            int _type = LESSTHAN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:181:10: ( '<' )
            // /home/ablecao/workspace/Storm.g:181:12: '<'
            {
            match('<'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "LESSTHAN"

    // $ANTLR start "GREATERTHANOREQUALTO"
    public final void mGREATERTHANOREQUALTO() throws RecognitionException {
        try {
            int _type = GREATERTHANOREQUALTO;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:182:22: ( '>=' )
            // /home/ablecao/workspace/Storm.g:182:24: '>='
            {
            match(">="); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "GREATERTHANOREQUALTO"

    // $ANTLR start "GREATERTHAN"
    public final void mGREATERTHAN() throws RecognitionException {
        try {
            int _type = GREATERTHAN;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:183:13: ( '>' )
            // /home/ablecao/workspace/Storm.g:183:15: '>'
            {
            match('>'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "GREATERTHAN"

    // $ANTLR start "DIVIDE"
    public final void mDIVIDE() throws RecognitionException {
        try {
            int _type = DIVIDE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:185:8: ( '/' )
            // /home/ablecao/workspace/Storm.g:185:10: '/'
            {
            match('/'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DIVIDE"

    // $ANTLR start "PLUS"
    public final void mPLUS() throws RecognitionException {
        try {
            int _type = PLUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:186:6: ( '+' )
            // /home/ablecao/workspace/Storm.g:186:8: '+'
            {
            match('+'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "PLUS"

    // $ANTLR start "MINUS"
    public final void mMINUS() throws RecognitionException {
        try {
            int _type = MINUS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:187:7: ( '-' )
            // /home/ablecao/workspace/Storm.g:187:9: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "MINUS"

    // $ANTLR start "STAR"
    public final void mSTAR() throws RecognitionException {
        try {
            int _type = STAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:188:6: ( '*' )
            // /home/ablecao/workspace/Storm.g:188:8: '*'
            {
            match('*'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "STAR"

    // $ANTLR start "MOD"
    public final void mMOD() throws RecognitionException {
        try {
            int _type = MOD;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:189:5: ( '%' )
            // /home/ablecao/workspace/Storm.g:189:7: '%'
            {
            match('%'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "MOD"

    // $ANTLR start "DIV"
    public final void mDIV() throws RecognitionException {
        try {
            int _type = DIV;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:190:5: ( 'DIV' )
            // /home/ablecao/workspace/Storm.g:190:7: 'DIV'
            {
            match("DIV"); 



            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DIV"

    // $ANTLR start "AMPERSAND"
    public final void mAMPERSAND() throws RecognitionException {
        try {
            int _type = AMPERSAND;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:192:11: ( '&' )
            // /home/ablecao/workspace/Storm.g:192:13: '&'
            {
            match('&'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "AMPERSAND"

    // $ANTLR start "TILDE"
    public final void mTILDE() throws RecognitionException {
        try {
            int _type = TILDE;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:193:7: ( '~' )
            // /home/ablecao/workspace/Storm.g:193:9: '~'
            {
            match('~'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "TILDE"

    // $ANTLR start "BITWISEOR"
    public final void mBITWISEOR() throws RecognitionException {
        try {
            int _type = BITWISEOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:194:11: ( '|' )
            // /home/ablecao/workspace/Storm.g:194:13: '|'
            {
            match('|'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "BITWISEOR"

    // $ANTLR start "BITWISEXOR"
    public final void mBITWISEXOR() throws RecognitionException {
        try {
            int _type = BITWISEXOR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:195:12: ( '^' )
            // /home/ablecao/workspace/Storm.g:195:14: '^'
            {
            match('^'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "BITWISEXOR"

    // $ANTLR start "QUESTION"
    public final void mQUESTION() throws RecognitionException {
        try {
            int _type = QUESTION;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:196:10: ( '?' )
            // /home/ablecao/workspace/Storm.g:196:12: '?'
            {
            match('?'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "QUESTION"

    // $ANTLR start "DOLLAR"
    public final void mDOLLAR() throws RecognitionException {
        try {
            int _type = DOLLAR;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:197:8: ( '$' )
            // /home/ablecao/workspace/Storm.g:197:10: '$'
            {
            match('$'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "DOLLAR"

    // $ANTLR start "Letter"
    public final void mLetter() throws RecognitionException {
        try {
            // /home/ablecao/workspace/Storm.g:203:5: ( 'a' .. 'z' | 'A' .. 'Z' )
            // /home/ablecao/workspace/Storm.g:
            {
            if ( (input.LA(1) >= 'A' && input.LA(1) <= 'Z')||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Letter"

    // $ANTLR start "HexDigit"
    public final void mHexDigit() throws RecognitionException {
        try {
            // /home/ablecao/workspace/Storm.g:208:5: ( 'a' .. 'f' | 'A' .. 'F' )
            // /home/ablecao/workspace/Storm.g:
            {
            if ( (input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "HexDigit"

    // $ANTLR start "Digit"
    public final void mDigit() throws RecognitionException {
        try {
            // /home/ablecao/workspace/Storm.g:213:5: ( '0' .. '9' )
            // /home/ablecao/workspace/Storm.g:
            {
            if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Digit"

    // $ANTLR start "Exponent"
    public final void mExponent() throws RecognitionException {
        try {
            // /home/ablecao/workspace/Storm.g:219:5: ( 'e' ( PLUS | MINUS )? ( Digit )+ )
            // /home/ablecao/workspace/Storm.g:220:5: 'e' ( PLUS | MINUS )? ( Digit )+
            {
            match('e'); 

            // /home/ablecao/workspace/Storm.g:220:9: ( PLUS | MINUS )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0=='+'||LA3_0=='-') ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:
                    {
                    if ( input.LA(1)=='+'||input.LA(1)=='-' ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    }
                    break;

            }


            // /home/ablecao/workspace/Storm.g:220:25: ( Digit )+
            int cnt4=0;
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( ((LA4_0 >= '0' && LA4_0 <= '9')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // /home/ablecao/workspace/Storm.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt4 >= 1 ) break loop4;
                        EarlyExitException eee =
                            new EarlyExitException(4, input);
                        throw eee;
                }
                cnt4++;
            } while (true);


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Exponent"

    // $ANTLR start "RegexComponent"
    public final void mRegexComponent() throws RecognitionException {
        try {
            // /home/ablecao/workspace/Storm.g:225:5: ( 'a' .. 'z' | 'A' .. 'Z' | '0' .. '9' | '_' | PLUS | STAR | QUESTION | MINUS | DOT | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY | BITWISEXOR | BITWISEOR | DOLLAR )
            // /home/ablecao/workspace/Storm.g:
            {
            if ( input.LA(1)=='$'||(input.LA(1) >= '(' && input.LA(1) <= '+')||(input.LA(1) >= '-' && input.LA(1) <= '.')||(input.LA(1) >= '0' && input.LA(1) <= '9')||input.LA(1)=='?'||(input.LA(1) >= 'A' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '_')||(input.LA(1) >= 'a' && input.LA(1) <= '}') ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            }


        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "RegexComponent"

    // $ANTLR start "StringLiteral"
    public final void mStringLiteral() throws RecognitionException {
        try {
            int _type = StringLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:231:5: ( ( '\\'' (~ ( '\\'' | '\\\\' ) | ( '\\\\' . ) )* '\\'' | '\\\"' (~ ( '\\\"' | '\\\\' ) | ( '\\\\' . ) )* '\\\"' )+ )
            // /home/ablecao/workspace/Storm.g:232:5: ( '\\'' (~ ( '\\'' | '\\\\' ) | ( '\\\\' . ) )* '\\'' | '\\\"' (~ ( '\\\"' | '\\\\' ) | ( '\\\\' . ) )* '\\\"' )+
            {
            // /home/ablecao/workspace/Storm.g:232:5: ( '\\'' (~ ( '\\'' | '\\\\' ) | ( '\\\\' . ) )* '\\'' | '\\\"' (~ ( '\\\"' | '\\\\' ) | ( '\\\\' . ) )* '\\\"' )+
            int cnt7=0;
            loop7:
            do {
                int alt7=3;
                int LA7_0 = input.LA(1);

                if ( (LA7_0=='\'') ) {
                    alt7=1;
                }
                else if ( (LA7_0=='\"') ) {
                    alt7=2;
                }


                switch (alt7) {
            	case 1 :
            	    // /home/ablecao/workspace/Storm.g:232:7: '\\'' (~ ( '\\'' | '\\\\' ) | ( '\\\\' . ) )* '\\''
            	    {
            	    match('\''); 

            	    // /home/ablecao/workspace/Storm.g:232:12: (~ ( '\\'' | '\\\\' ) | ( '\\\\' . ) )*
            	    loop5:
            	    do {
            	        int alt5=3;
            	        int LA5_0 = input.LA(1);

            	        if ( ((LA5_0 >= '\u0000' && LA5_0 <= '&')||(LA5_0 >= '(' && LA5_0 <= '[')||(LA5_0 >= ']' && LA5_0 <= '\uFFFF')) ) {
            	            alt5=1;
            	        }
            	        else if ( (LA5_0=='\\') ) {
            	            alt5=2;
            	        }


            	        switch (alt5) {
            	    	case 1 :
            	    	    // /home/ablecao/workspace/Storm.g:232:14: ~ ( '\\'' | '\\\\' )
            	    	    {
            	    	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '&')||(input.LA(1) >= '(' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '\uFFFF') ) {
            	    	        input.consume();
            	    	    }
            	    	    else {
            	    	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	    	        recover(mse);
            	    	        throw mse;
            	    	    }


            	    	    }
            	    	    break;
            	    	case 2 :
            	    	    // /home/ablecao/workspace/Storm.g:232:29: ( '\\\\' . )
            	    	    {
            	    	    // /home/ablecao/workspace/Storm.g:232:29: ( '\\\\' . )
            	    	    // /home/ablecao/workspace/Storm.g:232:30: '\\\\' .
            	    	    {
            	    	    match('\\'); 

            	    	    matchAny(); 

            	    	    }


            	    	    }
            	    	    break;

            	    	default :
            	    	    break loop5;
            	        }
            	    } while (true);


            	    match('\''); 

            	    }
            	    break;
            	case 2 :
            	    // /home/ablecao/workspace/Storm.g:233:7: '\\\"' (~ ( '\\\"' | '\\\\' ) | ( '\\\\' . ) )* '\\\"'
            	    {
            	    match('\"'); 

            	    // /home/ablecao/workspace/Storm.g:233:12: (~ ( '\\\"' | '\\\\' ) | ( '\\\\' . ) )*
            	    loop6:
            	    do {
            	        int alt6=3;
            	        int LA6_0 = input.LA(1);

            	        if ( ((LA6_0 >= '\u0000' && LA6_0 <= '!')||(LA6_0 >= '#' && LA6_0 <= '[')||(LA6_0 >= ']' && LA6_0 <= '\uFFFF')) ) {
            	            alt6=1;
            	        }
            	        else if ( (LA6_0=='\\') ) {
            	            alt6=2;
            	        }


            	        switch (alt6) {
            	    	case 1 :
            	    	    // /home/ablecao/workspace/Storm.g:233:14: ~ ( '\\\"' | '\\\\' )
            	    	    {
            	    	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '!')||(input.LA(1) >= '#' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '\uFFFF') ) {
            	    	        input.consume();
            	    	    }
            	    	    else {
            	    	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	    	        recover(mse);
            	    	        throw mse;
            	    	    }


            	    	    }
            	    	    break;
            	    	case 2 :
            	    	    // /home/ablecao/workspace/Storm.g:233:29: ( '\\\\' . )
            	    	    {
            	    	    // /home/ablecao/workspace/Storm.g:233:29: ( '\\\\' . )
            	    	    // /home/ablecao/workspace/Storm.g:233:30: '\\\\' .
            	    	    {
            	    	    match('\\'); 

            	    	    matchAny(); 

            	    	    }


            	    	    }
            	    	    break;

            	    	default :
            	    	    break loop6;
            	        }
            	    } while (true);


            	    match('\"'); 

            	    }
            	    break;

            	default :
            	    if ( cnt7 >= 1 ) break loop7;
                        EarlyExitException eee =
                            new EarlyExitException(7, input);
                        throw eee;
                }
                cnt7++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "StringLiteral"

    // $ANTLR start "CharSetLiteral"
    public final void mCharSetLiteral() throws RecognitionException {
        try {
            int _type = CharSetLiteral;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:238:5: ( StringLiteral | '0' 'X' ( HexDigit | Digit )+ )
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0=='\"'||LA9_0=='\'') ) {
                alt9=1;
            }
            else if ( (LA9_0=='0') ) {
                alt9=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 9, 0, input);

                throw nvae;

            }
            switch (alt9) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:239:5: StringLiteral
                    {
                    mStringLiteral(); 


                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:240:7: '0' 'X' ( HexDigit | Digit )+
                    {
                    match('0'); 

                    match('X'); 

                    // /home/ablecao/workspace/Storm.g:240:15: ( HexDigit | Digit )+
                    int cnt8=0;
                    loop8:
                    do {
                        int alt8=2;
                        int LA8_0 = input.LA(1);

                        if ( ((LA8_0 >= '0' && LA8_0 <= '9')||(LA8_0 >= 'A' && LA8_0 <= 'F')||(LA8_0 >= 'a' && LA8_0 <= 'f')) ) {
                            alt8=1;
                        }


                        switch (alt8) {
                    	case 1 :
                    	    // /home/ablecao/workspace/Storm.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'F')||(input.LA(1) >= 'a' && input.LA(1) <= 'f') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt8 >= 1 ) break loop8;
                                EarlyExitException eee =
                                    new EarlyExitException(8, input);
                                throw eee;
                        }
                        cnt8++;
                    } while (true);


                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "CharSetLiteral"

    // $ANTLR start "Number"
    public final void mNumber() throws RecognitionException {
        try {
            int _type = Number;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:244:5: ( ( Digit )+ ( DOT ( Digit )* ( Exponent )? | Exponent )? )
            // /home/ablecao/workspace/Storm.g:245:5: ( Digit )+ ( DOT ( Digit )* ( Exponent )? | Exponent )?
            {
            // /home/ablecao/workspace/Storm.g:245:5: ( Digit )+
            int cnt10=0;
            loop10:
            do {
                int alt10=2;
                int LA10_0 = input.LA(1);

                if ( ((LA10_0 >= '0' && LA10_0 <= '9')) ) {
                    alt10=1;
                }


                switch (alt10) {
            	case 1 :
            	    // /home/ablecao/workspace/Storm.g:
            	    {
            	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt10 >= 1 ) break loop10;
                        EarlyExitException eee =
                            new EarlyExitException(10, input);
                        throw eee;
                }
                cnt10++;
            } while (true);


            // /home/ablecao/workspace/Storm.g:245:14: ( DOT ( Digit )* ( Exponent )? | Exponent )?
            int alt13=3;
            int LA13_0 = input.LA(1);

            if ( (LA13_0=='.') ) {
                alt13=1;
            }
            else if ( (LA13_0=='e') ) {
                alt13=2;
            }
            switch (alt13) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:245:16: DOT ( Digit )* ( Exponent )?
                    {
                    mDOT(); 


                    // /home/ablecao/workspace/Storm.g:245:20: ( Digit )*
                    loop11:
                    do {
                        int alt11=2;
                        int LA11_0 = input.LA(1);

                        if ( ((LA11_0 >= '0' && LA11_0 <= '9')) ) {
                            alt11=1;
                        }


                        switch (alt11) {
                    	case 1 :
                    	    // /home/ablecao/workspace/Storm.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop11;
                        }
                    } while (true);


                    // /home/ablecao/workspace/Storm.g:245:29: ( Exponent )?
                    int alt12=2;
                    int LA12_0 = input.LA(1);

                    if ( (LA12_0=='e') ) {
                        alt12=1;
                    }
                    switch (alt12) {
                        case 1 :
                            // /home/ablecao/workspace/Storm.g:245:30: Exponent
                            {
                            mExponent(); 


                            }
                            break;

                    }


                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:245:43: Exponent
                    {
                    mExponent(); 


                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Number"

    // $ANTLR start "Identifier"
    public final void mIdentifier() throws RecognitionException {
        try {
            int _type = Identifier;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:249:5: ( ( Letter | Digit ) ( Letter | Digit | '_' )* | '`' ( RegexComponent )+ '`' )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( ((LA16_0 >= '0' && LA16_0 <= '9')||(LA16_0 >= 'A' && LA16_0 <= 'Z')||(LA16_0 >= 'a' && LA16_0 <= 'z')) ) {
                alt16=1;
            }
            else if ( (LA16_0=='`') ) {
                alt16=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;

            }
            switch (alt16) {
                case 1 :
                    // /home/ablecao/workspace/Storm.g:250:5: ( Letter | Digit ) ( Letter | Digit | '_' )*
                    {
                    if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                        input.consume();
                    }
                    else {
                        MismatchedSetException mse = new MismatchedSetException(null,input);
                        recover(mse);
                        throw mse;
                    }


                    // /home/ablecao/workspace/Storm.g:250:22: ( Letter | Digit | '_' )*
                    loop14:
                    do {
                        int alt14=2;
                        int LA14_0 = input.LA(1);

                        if ( ((LA14_0 >= '0' && LA14_0 <= '9')||(LA14_0 >= 'A' && LA14_0 <= 'Z')||LA14_0=='_'||(LA14_0 >= 'a' && LA14_0 <= 'z')) ) {
                            alt14=1;
                        }


                        switch (alt14) {
                    	case 1 :
                    	    // /home/ablecao/workspace/Storm.g:
                    	    {
                    	    if ( (input.LA(1) >= '0' && input.LA(1) <= '9')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop14;
                        }
                    } while (true);


                    }
                    break;
                case 2 :
                    // /home/ablecao/workspace/Storm.g:251:7: '`' ( RegexComponent )+ '`'
                    {
                    match('`'); 

                    // /home/ablecao/workspace/Storm.g:251:11: ( RegexComponent )+
                    int cnt15=0;
                    loop15:
                    do {
                        int alt15=2;
                        int LA15_0 = input.LA(1);

                        if ( (LA15_0=='$'||(LA15_0 >= '(' && LA15_0 <= '+')||(LA15_0 >= '-' && LA15_0 <= '.')||(LA15_0 >= '0' && LA15_0 <= '9')||LA15_0=='?'||(LA15_0 >= 'A' && LA15_0 <= '[')||(LA15_0 >= ']' && LA15_0 <= '_')||(LA15_0 >= 'a' && LA15_0 <= '}')) ) {
                            alt15=1;
                        }


                        switch (alt15) {
                    	case 1 :
                    	    // /home/ablecao/workspace/Storm.g:
                    	    {
                    	    if ( input.LA(1)=='$'||(input.LA(1) >= '(' && input.LA(1) <= '+')||(input.LA(1) >= '-' && input.LA(1) <= '.')||(input.LA(1) >= '0' && input.LA(1) <= '9')||input.LA(1)=='?'||(input.LA(1) >= 'A' && input.LA(1) <= '[')||(input.LA(1) >= ']' && input.LA(1) <= '_')||(input.LA(1) >= 'a' && input.LA(1) <= '}') ) {
                    	        input.consume();
                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;
                    	    }


                    	    }
                    	    break;

                    	default :
                    	    if ( cnt15 >= 1 ) break loop15;
                                EarlyExitException eee =
                                    new EarlyExitException(15, input);
                                throw eee;
                        }
                        cnt15++;
                    } while (true);


                    match('`'); 

                    }
                    break;

            }
            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "Identifier"

    // $ANTLR start "CharSetName"
    public final void mCharSetName() throws RecognitionException {
        try {
            int _type = CharSetName;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:255:5: ( '_' ( Letter | Digit | '_' | '-' | '.' | ':' )+ )
            // /home/ablecao/workspace/Storm.g:256:5: '_' ( Letter | Digit | '_' | '-' | '.' | ':' )+
            {
            match('_'); 

            // /home/ablecao/workspace/Storm.g:256:9: ( Letter | Digit | '_' | '-' | '.' | ':' )+
            int cnt17=0;
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( ((LA17_0 >= '-' && LA17_0 <= '.')||(LA17_0 >= '0' && LA17_0 <= ':')||(LA17_0 >= 'A' && LA17_0 <= 'Z')||LA17_0=='_'||(LA17_0 >= 'a' && LA17_0 <= 'z')) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // /home/ablecao/workspace/Storm.g:
            	    {
            	    if ( (input.LA(1) >= '-' && input.LA(1) <= '.')||(input.LA(1) >= '0' && input.LA(1) <= ':')||(input.LA(1) >= 'A' && input.LA(1) <= 'Z')||input.LA(1)=='_'||(input.LA(1) >= 'a' && input.LA(1) <= 'z') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    if ( cnt17 >= 1 ) break loop17;
                        EarlyExitException eee =
                            new EarlyExitException(17, input);
                        throw eee;
                }
                cnt17++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "CharSetName"

    // $ANTLR start "WS"
    public final void mWS() throws RecognitionException {
        try {
            int _type = WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:259:5: ( ( ' ' | '\\r' | '\\t' | '\\n' ) )
            // /home/ablecao/workspace/Storm.g:259:8: ( ' ' | '\\r' | '\\t' | '\\n' )
            {
            if ( (input.LA(1) >= '\t' && input.LA(1) <= '\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
                input.consume();
            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;
            }


            _channel=HIDDEN;

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "WS"

    // $ANTLR start "COMMENT"
    public final void mCOMMENT() throws RecognitionException {
        try {
            int _type = COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // /home/ablecao/workspace/Storm.g:263:3: ( '--' (~ ( '\\n' | '\\r' ) )* )
            // /home/ablecao/workspace/Storm.g:263:5: '--' (~ ( '\\n' | '\\r' ) )*
            {
            match("--"); 



            // /home/ablecao/workspace/Storm.g:263:10: (~ ( '\\n' | '\\r' ) )*
            loop18:
            do {
                int alt18=2;
                int LA18_0 = input.LA(1);

                if ( ((LA18_0 >= '\u0000' && LA18_0 <= '\t')||(LA18_0 >= '\u000B' && LA18_0 <= '\f')||(LA18_0 >= '\u000E' && LA18_0 <= '\uFFFF')) ) {
                    alt18=1;
                }


                switch (alt18) {
            	case 1 :
            	    // /home/ablecao/workspace/Storm.g:
            	    {
            	    if ( (input.LA(1) >= '\u0000' && input.LA(1) <= '\t')||(input.LA(1) >= '\u000B' && input.LA(1) <= '\f')||(input.LA(1) >= '\u000E' && input.LA(1) <= '\uFFFF') ) {
            	        input.consume();
            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;
            	    }


            	    }
            	    break;

            	default :
            	    break loop18;
                }
            } while (true);


             _channel=HIDDEN; 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        	// do for sure before leaving
        }
    }
    // $ANTLR end "COMMENT"

    public void mTokens() throws RecognitionException {
        // /home/ablecao/workspace/Storm.g:1:8: ( KW_EXPLAIN | KW_ADD | KW_FILE | KW_JAR | KW_EXISTS | KW_SET | KW_NOT | KW_AND | KW_OR | KW_DISTINCT | KW_IF | KW_ARRAY | KW_MAP | KW_REGISTER | KW_FOR | KW_SPOUT | KW_BOLT | KW_ALL | KW_NONE | KW_DOT | KW_FIELDS | KW_SHUFFLE | DOT | COLON | COMMA | SEMICOLON | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY | EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN | DIVIDE | PLUS | MINUS | STAR | MOD | DIV | AMPERSAND | TILDE | BITWISEOR | BITWISEXOR | QUESTION | DOLLAR | StringLiteral | CharSetLiteral | Number | Identifier | CharSetName | WS | COMMENT )
        int alt19=57;
        alt19 = dfa19.predict(input);
        switch (alt19) {
            case 1 :
                // /home/ablecao/workspace/Storm.g:1:10: KW_EXPLAIN
                {
                mKW_EXPLAIN(); 


                }
                break;
            case 2 :
                // /home/ablecao/workspace/Storm.g:1:21: KW_ADD
                {
                mKW_ADD(); 


                }
                break;
            case 3 :
                // /home/ablecao/workspace/Storm.g:1:28: KW_FILE
                {
                mKW_FILE(); 


                }
                break;
            case 4 :
                // /home/ablecao/workspace/Storm.g:1:36: KW_JAR
                {
                mKW_JAR(); 


                }
                break;
            case 5 :
                // /home/ablecao/workspace/Storm.g:1:43: KW_EXISTS
                {
                mKW_EXISTS(); 


                }
                break;
            case 6 :
                // /home/ablecao/workspace/Storm.g:1:53: KW_SET
                {
                mKW_SET(); 


                }
                break;
            case 7 :
                // /home/ablecao/workspace/Storm.g:1:60: KW_NOT
                {
                mKW_NOT(); 


                }
                break;
            case 8 :
                // /home/ablecao/workspace/Storm.g:1:67: KW_AND
                {
                mKW_AND(); 


                }
                break;
            case 9 :
                // /home/ablecao/workspace/Storm.g:1:74: KW_OR
                {
                mKW_OR(); 


                }
                break;
            case 10 :
                // /home/ablecao/workspace/Storm.g:1:80: KW_DISTINCT
                {
                mKW_DISTINCT(); 


                }
                break;
            case 11 :
                // /home/ablecao/workspace/Storm.g:1:92: KW_IF
                {
                mKW_IF(); 


                }
                break;
            case 12 :
                // /home/ablecao/workspace/Storm.g:1:98: KW_ARRAY
                {
                mKW_ARRAY(); 


                }
                break;
            case 13 :
                // /home/ablecao/workspace/Storm.g:1:107: KW_MAP
                {
                mKW_MAP(); 


                }
                break;
            case 14 :
                // /home/ablecao/workspace/Storm.g:1:114: KW_REGISTER
                {
                mKW_REGISTER(); 


                }
                break;
            case 15 :
                // /home/ablecao/workspace/Storm.g:1:126: KW_FOR
                {
                mKW_FOR(); 


                }
                break;
            case 16 :
                // /home/ablecao/workspace/Storm.g:1:133: KW_SPOUT
                {
                mKW_SPOUT(); 


                }
                break;
            case 17 :
                // /home/ablecao/workspace/Storm.g:1:142: KW_BOLT
                {
                mKW_BOLT(); 


                }
                break;
            case 18 :
                // /home/ablecao/workspace/Storm.g:1:150: KW_ALL
                {
                mKW_ALL(); 


                }
                break;
            case 19 :
                // /home/ablecao/workspace/Storm.g:1:157: KW_NONE
                {
                mKW_NONE(); 


                }
                break;
            case 20 :
                // /home/ablecao/workspace/Storm.g:1:165: KW_DOT
                {
                mKW_DOT(); 


                }
                break;
            case 21 :
                // /home/ablecao/workspace/Storm.g:1:172: KW_FIELDS
                {
                mKW_FIELDS(); 


                }
                break;
            case 22 :
                // /home/ablecao/workspace/Storm.g:1:182: KW_SHUFFLE
                {
                mKW_SHUFFLE(); 


                }
                break;
            case 23 :
                // /home/ablecao/workspace/Storm.g:1:193: DOT
                {
                mDOT(); 


                }
                break;
            case 24 :
                // /home/ablecao/workspace/Storm.g:1:197: COLON
                {
                mCOLON(); 


                }
                break;
            case 25 :
                // /home/ablecao/workspace/Storm.g:1:203: COMMA
                {
                mCOMMA(); 


                }
                break;
            case 26 :
                // /home/ablecao/workspace/Storm.g:1:209: SEMICOLON
                {
                mSEMICOLON(); 


                }
                break;
            case 27 :
                // /home/ablecao/workspace/Storm.g:1:219: LPAREN
                {
                mLPAREN(); 


                }
                break;
            case 28 :
                // /home/ablecao/workspace/Storm.g:1:226: RPAREN
                {
                mRPAREN(); 


                }
                break;
            case 29 :
                // /home/ablecao/workspace/Storm.g:1:233: LSQUARE
                {
                mLSQUARE(); 


                }
                break;
            case 30 :
                // /home/ablecao/workspace/Storm.g:1:241: RSQUARE
                {
                mRSQUARE(); 


                }
                break;
            case 31 :
                // /home/ablecao/workspace/Storm.g:1:249: LCURLY
                {
                mLCURLY(); 


                }
                break;
            case 32 :
                // /home/ablecao/workspace/Storm.g:1:256: RCURLY
                {
                mRCURLY(); 


                }
                break;
            case 33 :
                // /home/ablecao/workspace/Storm.g:1:263: EQUAL
                {
                mEQUAL(); 


                }
                break;
            case 34 :
                // /home/ablecao/workspace/Storm.g:1:269: NOTEQUAL
                {
                mNOTEQUAL(); 


                }
                break;
            case 35 :
                // /home/ablecao/workspace/Storm.g:1:278: LESSTHANOREQUALTO
                {
                mLESSTHANOREQUALTO(); 


                }
                break;
            case 36 :
                // /home/ablecao/workspace/Storm.g:1:296: LESSTHAN
                {
                mLESSTHAN(); 


                }
                break;
            case 37 :
                // /home/ablecao/workspace/Storm.g:1:305: GREATERTHANOREQUALTO
                {
                mGREATERTHANOREQUALTO(); 


                }
                break;
            case 38 :
                // /home/ablecao/workspace/Storm.g:1:326: GREATERTHAN
                {
                mGREATERTHAN(); 


                }
                break;
            case 39 :
                // /home/ablecao/workspace/Storm.g:1:338: DIVIDE
                {
                mDIVIDE(); 


                }
                break;
            case 40 :
                // /home/ablecao/workspace/Storm.g:1:345: PLUS
                {
                mPLUS(); 


                }
                break;
            case 41 :
                // /home/ablecao/workspace/Storm.g:1:350: MINUS
                {
                mMINUS(); 


                }
                break;
            case 42 :
                // /home/ablecao/workspace/Storm.g:1:356: STAR
                {
                mSTAR(); 


                }
                break;
            case 43 :
                // /home/ablecao/workspace/Storm.g:1:361: MOD
                {
                mMOD(); 


                }
                break;
            case 44 :
                // /home/ablecao/workspace/Storm.g:1:365: DIV
                {
                mDIV(); 


                }
                break;
            case 45 :
                // /home/ablecao/workspace/Storm.g:1:369: AMPERSAND
                {
                mAMPERSAND(); 


                }
                break;
            case 46 :
                // /home/ablecao/workspace/Storm.g:1:379: TILDE
                {
                mTILDE(); 


                }
                break;
            case 47 :
                // /home/ablecao/workspace/Storm.g:1:385: BITWISEOR
                {
                mBITWISEOR(); 


                }
                break;
            case 48 :
                // /home/ablecao/workspace/Storm.g:1:395: BITWISEXOR
                {
                mBITWISEXOR(); 


                }
                break;
            case 49 :
                // /home/ablecao/workspace/Storm.g:1:406: QUESTION
                {
                mQUESTION(); 


                }
                break;
            case 50 :
                // /home/ablecao/workspace/Storm.g:1:415: DOLLAR
                {
                mDOLLAR(); 


                }
                break;
            case 51 :
                // /home/ablecao/workspace/Storm.g:1:422: StringLiteral
                {
                mStringLiteral(); 


                }
                break;
            case 52 :
                // /home/ablecao/workspace/Storm.g:1:436: CharSetLiteral
                {
                mCharSetLiteral(); 


                }
                break;
            case 53 :
                // /home/ablecao/workspace/Storm.g:1:451: Number
                {
                mNumber(); 


                }
                break;
            case 54 :
                // /home/ablecao/workspace/Storm.g:1:458: Identifier
                {
                mIdentifier(); 


                }
                break;
            case 55 :
                // /home/ablecao/workspace/Storm.g:1:469: CharSetName
                {
                mCharSetName(); 


                }
                break;
            case 56 :
                // /home/ablecao/workspace/Storm.g:1:481: WS
                {
                mWS(); 


                }
                break;
            case 57 :
                // /home/ablecao/workspace/Storm.g:1:484: COMMENT
                {
                mCOMMENT(); 


                }
                break;

        }

    }


    protected DFA19 dfa19 = new DFA19(this);
    static final String DFA19_eotS =
        "\1\uffff\14\52\13\uffff\1\101\1\uffff\1\103\2\uffff\1\105\12\uffff"+
        "\2\115\3\uffff\14\52\1\137\2\52\1\143\3\52\10\uffff\1\150\2\uffff"+
        "\1\150\1\52\1\uffff\1\52\1\115\2\52\1\156\1\157\1\52\1\161\2\52"+
        "\1\164\1\165\1\166\2\52\1\171\1\52\1\uffff\1\52\1\174\1\175\1\uffff"+
        "\1\176\2\52\3\uffff\1\u0081\1\115\2\52\2\uffff\1\52\1\uffff\1\u0085"+
        "\1\52\3\uffff\2\52\1\uffff\1\u0089\1\52\3\uffff\1\52\1\u008c\1\uffff"+
        "\2\52\1\u008f\1\uffff\1\52\1\u0091\1\52\1\uffff\2\52\1\uffff\1\52"+
        "\1\u0096\1\uffff\1\u0097\1\uffff\3\52\1\u009b\2\uffff\1\u009c\2"+
        "\52\2\uffff\1\u009f\1\u00a0\2\uffff";
    static final String DFA19_eofS =
        "\u00a1\uffff";
    static final String DFA19_minS =
        "\1\11\1\130\1\104\1\111\1\101\1\105\1\117\1\122\1\111\1\106\1\101"+
        "\1\105\1\117\13\uffff\1\75\1\uffff\1\75\2\uffff\1\55\10\uffff\2"+
        "\0\2\60\3\uffff\1\111\2\104\1\122\1\114\1\105\2\122\1\124\1\117"+
        "\1\125\1\111\1\60\1\123\1\124\1\60\1\120\1\107\1\114\6\uffff\2\0"+
        "\1\42\2\0\1\42\1\60\1\uffff\1\53\1\60\1\114\1\123\2\60\1\101\1\60"+
        "\1\105\1\114\3\60\1\125\1\106\1\60\1\105\1\uffff\1\124\2\60\1\uffff"+
        "\1\60\1\111\1\124\1\0\1\uffff\1\0\2\60\1\101\1\124\2\uffff\1\131"+
        "\1\uffff\1\60\1\104\3\uffff\1\124\1\106\1\uffff\1\60\1\111\3\uffff"+
        "\1\123\1\60\1\uffff\1\111\1\123\1\60\1\uffff\1\123\1\60\1\114\1"+
        "\uffff\1\116\1\124\1\uffff\1\116\1\60\1\uffff\1\60\1\uffff\1\105"+
        "\1\103\1\105\1\60\2\uffff\1\60\1\124\1\122\2\uffff\2\60\2\uffff";
    static final String DFA19_maxS =
        "\1\176\1\130\1\122\1\117\1\101\1\120\1\117\1\122\1\117\1\106\1\101"+
        "\1\105\1\117\13\uffff\1\76\1\uffff\1\75\2\uffff\1\55\10\uffff\2"+
        "\uffff\2\172\3\uffff\1\120\2\104\1\122\2\114\2\122\1\124\1\117\1"+
        "\125\1\116\1\172\1\126\1\124\1\172\1\120\1\107\1\114\6\uffff\2\uffff"+
        "\1\47\2\uffff\1\47\1\146\1\uffff\1\71\1\172\1\114\1\123\2\172\1"+
        "\101\1\172\1\105\1\114\3\172\1\125\1\106\1\172\1\105\1\uffff\1\124"+
        "\2\172\1\uffff\1\172\1\111\1\124\1\uffff\1\uffff\1\uffff\2\172\1"+
        "\101\1\124\2\uffff\1\131\1\uffff\1\172\1\104\3\uffff\1\124\1\106"+
        "\1\uffff\1\172\1\111\3\uffff\1\123\1\172\1\uffff\1\111\1\123\1\172"+
        "\1\uffff\1\123\1\172\1\114\1\uffff\1\116\1\124\1\uffff\1\116\1\172"+
        "\1\uffff\1\172\1\uffff\1\105\1\103\1\105\1\172\2\uffff\1\172\1\124"+
        "\1\122\2\uffff\2\172\2\uffff";
    static final String DFA19_acceptS =
        "\15\uffff\1\27\1\30\1\31\1\32\1\33\1\34\1\35\1\36\1\37\1\40\1\41"+
        "\1\uffff\1\42\1\uffff\1\47\1\50\1\uffff\1\52\1\53\1\55\1\56\1\57"+
        "\1\60\1\61\1\62\4\uffff\1\66\1\67\1\70\23\uffff\1\43\1\44\1\45\1"+
        "\46\1\71\1\51\7\uffff\1\65\21\uffff\1\11\3\uffff\1\13\4\uffff\1"+
        "\63\5\uffff\1\2\1\10\1\uffff\1\22\2\uffff\1\17\1\4\1\6\2\uffff\1"+
        "\7\2\uffff\1\54\1\24\1\15\2\uffff\1\64\3\uffff\1\3\3\uffff\1\23"+
        "\2\uffff\1\21\2\uffff\1\14\1\uffff\1\20\4\uffff\1\5\1\25\3\uffff"+
        "\1\1\1\26\2\uffff\1\12\1\16";
    static final String DFA19_specialS =
        "\46\uffff\1\7\1\5\36\uffff\1\3\1\4\1\uffff\1\0\1\6\34\uffff\1\1"+
        "\1\uffff\1\2\67\uffff}>";
    static final String[] DFA19_transitionS = {
            "\2\54\2\uffff\1\54\22\uffff\1\54\1\31\1\47\1\uffff\1\45\1\37"+
            "\1\40\1\46\1\21\1\22\1\36\1\34\1\17\1\35\1\15\1\33\1\50\11\51"+
            "\1\16\1\20\1\30\1\27\1\32\1\44\1\uffff\1\2\1\14\1\52\1\10\1"+
            "\1\1\3\2\52\1\11\1\4\2\52\1\12\1\6\1\7\2\52\1\13\1\5\7\52\1"+
            "\23\1\uffff\1\24\1\43\1\53\33\52\1\25\1\42\1\26\1\41",
            "\1\55",
            "\1\56\7\uffff\1\61\1\uffff\1\57\3\uffff\1\60",
            "\1\62\5\uffff\1\63",
            "\1\64",
            "\1\65\2\uffff\1\67\7\uffff\1\66",
            "\1\70",
            "\1\71",
            "\1\72\5\uffff\1\73",
            "\1\74",
            "\1\75",
            "\1\76",
            "\1\77",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\100\1\31",
            "",
            "\1\102",
            "",
            "",
            "\1\104",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\47\106\1\110\64\106\1\107\uffa3\106",
            "\42\111\1\113\71\111\1\112\uffa3\111",
            "\12\117\7\uffff\27\52\1\114\2\52\4\uffff\1\52\1\uffff\4\52"+
            "\1\116\25\52",
            "\12\117\7\uffff\32\52\4\uffff\1\52\1\uffff\4\52\1\116\25\52",
            "",
            "",
            "",
            "\1\121\6\uffff\1\120",
            "\1\122",
            "\1\123",
            "\1\124",
            "\1\125",
            "\1\127\6\uffff\1\126",
            "\1\130",
            "\1\131",
            "\1\132",
            "\1\133",
            "\1\134",
            "\1\135\4\uffff\1\136",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\140\2\uffff\1\141",
            "\1\142",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\144",
            "\1\145",
            "\1\146",
            "",
            "",
            "",
            "",
            "",
            "",
            "\47\106\1\110\64\106\1\107\uffa3\106",
            "\0\147",
            "\1\47\4\uffff\1\46",
            "\42\111\1\113\71\111\1\112\uffa3\111",
            "\0\151",
            "\1\47\4\uffff\1\46",
            "\12\152\7\uffff\6\152\32\uffff\6\152",
            "",
            "\1\115\1\uffff\1\115\2\uffff\12\153",
            "\12\117\7\uffff\32\52\4\uffff\1\52\1\uffff\4\52\1\116\25\52",
            "\1\154",
            "\1\155",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\160",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\162",
            "\1\163",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\167",
            "\1\170",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\172",
            "",
            "\1\173",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\177",
            "\1\u0080",
            "\47\106\1\110\64\106\1\107\uffa3\106",
            "",
            "\42\111\1\113\71\111\1\112\uffa3\111",
            "\12\152\7\uffff\6\152\24\52\4\uffff\1\52\1\uffff\6\152\24\52",
            "\12\153\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0082",
            "\1\u0083",
            "",
            "",
            "\1\u0084",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0086",
            "",
            "",
            "",
            "\1\u0087",
            "\1\u0088",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u008a",
            "",
            "",
            "",
            "\1\u008b",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u008d",
            "\1\u008e",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u0090",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u0092",
            "",
            "\1\u0093",
            "\1\u0094",
            "",
            "\1\u0095",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "\1\u0098",
            "\1\u0099",
            "\1\u009a",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\1\u009d",
            "\1\u009e",
            "",
            "",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "\12\52\7\uffff\32\52\4\uffff\1\52\1\uffff\32\52",
            "",
            ""
    };

    static final short[] DFA19_eot = DFA.unpackEncodedString(DFA19_eotS);
    static final short[] DFA19_eof = DFA.unpackEncodedString(DFA19_eofS);
    static final char[] DFA19_min = DFA.unpackEncodedStringToUnsignedChars(DFA19_minS);
    static final char[] DFA19_max = DFA.unpackEncodedStringToUnsignedChars(DFA19_maxS);
    static final short[] DFA19_accept = DFA.unpackEncodedString(DFA19_acceptS);
    static final short[] DFA19_special = DFA.unpackEncodedString(DFA19_specialS);
    static final short[][] DFA19_transition;

    static {
        int numStates = DFA19_transitionS.length;
        DFA19_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA19_transition[i] = DFA.unpackEncodedString(DFA19_transitionS[i]);
        }
    }

    class DFA19 extends DFA {

        public DFA19(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 19;
            this.eot = DFA19_eot;
            this.eof = DFA19_eof;
            this.min = DFA19_min;
            this.max = DFA19_max;
            this.accept = DFA19_accept;
            this.special = DFA19_special;
            this.transition = DFA19_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( KW_EXPLAIN | KW_ADD | KW_FILE | KW_JAR | KW_EXISTS | KW_SET | KW_NOT | KW_AND | KW_OR | KW_DISTINCT | KW_IF | KW_ARRAY | KW_MAP | KW_REGISTER | KW_FOR | KW_SPOUT | KW_BOLT | KW_ALL | KW_NONE | KW_DOT | KW_FIELDS | KW_SHUFFLE | DOT | COLON | COMMA | SEMICOLON | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY | EQUAL | NOTEQUAL | LESSTHANOREQUALTO | LESSTHAN | GREATERTHANOREQUALTO | GREATERTHAN | DIVIDE | PLUS | MINUS | STAR | MOD | DIV | AMPERSAND | TILDE | BITWISEOR | BITWISEXOR | QUESTION | DOLLAR | StringLiteral | CharSetLiteral | Number | Identifier | CharSetName | WS | COMMENT );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            IntStream input = _input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA19_73 = input.LA(1);

                        s = -1;
                        if ( (LA19_73=='\"') ) {s = 75;}

                        else if ( ((LA19_73 >= '\u0000' && LA19_73 <= '!')||(LA19_73 >= '#' && LA19_73 <= '[')||(LA19_73 >= ']' && LA19_73 <= '\uFFFF')) ) {s = 73;}

                        else if ( (LA19_73=='\\') ) {s = 74;}

                        if ( s>=0 ) return s;
                        break;

                    case 1 : 
                        int LA19_103 = input.LA(1);

                        s = -1;
                        if ( (LA19_103=='\'') ) {s = 72;}

                        else if ( ((LA19_103 >= '\u0000' && LA19_103 <= '&')||(LA19_103 >= '(' && LA19_103 <= '[')||(LA19_103 >= ']' && LA19_103 <= '\uFFFF')) ) {s = 70;}

                        else if ( (LA19_103=='\\') ) {s = 71;}

                        if ( s>=0 ) return s;
                        break;

                    case 2 : 
                        int LA19_105 = input.LA(1);

                        s = -1;
                        if ( (LA19_105=='\"') ) {s = 75;}

                        else if ( ((LA19_105 >= '\u0000' && LA19_105 <= '!')||(LA19_105 >= '#' && LA19_105 <= '[')||(LA19_105 >= ']' && LA19_105 <= '\uFFFF')) ) {s = 73;}

                        else if ( (LA19_105=='\\') ) {s = 74;}

                        if ( s>=0 ) return s;
                        break;

                    case 3 : 
                        int LA19_70 = input.LA(1);

                        s = -1;
                        if ( (LA19_70=='\'') ) {s = 72;}

                        else if ( ((LA19_70 >= '\u0000' && LA19_70 <= '&')||(LA19_70 >= '(' && LA19_70 <= '[')||(LA19_70 >= ']' && LA19_70 <= '\uFFFF')) ) {s = 70;}

                        else if ( (LA19_70=='\\') ) {s = 71;}

                        if ( s>=0 ) return s;
                        break;

                    case 4 : 
                        int LA19_71 = input.LA(1);

                        s = -1;
                        if ( ((LA19_71 >= '\u0000' && LA19_71 <= '\uFFFF')) ) {s = 103;}

                        if ( s>=0 ) return s;
                        break;

                    case 5 : 
                        int LA19_39 = input.LA(1);

                        s = -1;
                        if ( ((LA19_39 >= '\u0000' && LA19_39 <= '!')||(LA19_39 >= '#' && LA19_39 <= '[')||(LA19_39 >= ']' && LA19_39 <= '\uFFFF')) ) {s = 73;}

                        else if ( (LA19_39=='\\') ) {s = 74;}

                        else if ( (LA19_39=='\"') ) {s = 75;}

                        if ( s>=0 ) return s;
                        break;

                    case 6 : 
                        int LA19_74 = input.LA(1);

                        s = -1;
                        if ( ((LA19_74 >= '\u0000' && LA19_74 <= '\uFFFF')) ) {s = 105;}

                        if ( s>=0 ) return s;
                        break;

                    case 7 : 
                        int LA19_38 = input.LA(1);

                        s = -1;
                        if ( ((LA19_38 >= '\u0000' && LA19_38 <= '&')||(LA19_38 >= '(' && LA19_38 <= '[')||(LA19_38 >= ']' && LA19_38 <= '\uFFFF')) ) {s = 70;}

                        else if ( (LA19_38=='\\') ) {s = 71;}

                        else if ( (LA19_38=='\'') ) {s = 72;}

                        if ( s>=0 ) return s;
                        break;
            }
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 19, _s, input);
            error(nvae);
            throw nvae;
        }

    }
 

}