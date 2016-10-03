grammar Storm;

options
{
output=AST;
ASTLabelType=CommonTree;
backtrack=false;
k=3;
}

tokens {
TOK_BOLT;
TOK_DOT;
TOK_FUNCTION;
TOK_OP_EQ;
TOK_PROPERTY;
TOK_PROPERTIES;
TOK_FUNCTION;
TOK_REGISTER;
TOK_PROPLIST;
TOK_FOR;
TOK_EXPLAIN;
TOK_SPOUT;
}


@header {
package com.tencent.jstorm.ql.parse;
}
@lexer::header {package com.tencent.jstorm.ql.parse;}


@members {
  Stack msgs = new Stack<String>();
}

@rulecatch {
catch (RecognitionException e) {
 reportError(e);
  throw e;
}
}

KW_EXPLAIN: 'EXPLAIN';
KW_ADD: 'ADD';
KW_FILE: 'FILE';
KW_JAR: 'JAR';
KW_EXISTS: 'EXISTS';
KW_SET: 'SET';
KW_NOT: 'NOI';
KW_AND: 'AND';
KW_OR: 'OR';
KW_DISTINCT: 'DISTINCT';
KW_IF: 'IF';
KW_ARRAY: 'ARRAY';
KW_MAP: 'MAP';
KW_REGISTER: 'REGISTER';
KW_FOR	: 'FOR';
KW_SPOUT:	'SPOUT';
KW_BOLT	:'BOLT';
KW_ALL	:'ALL';
KW_NONE	:'NONE';
KW_DOT	:'DOT';
KW_FIELDS	:'FIELDS';
KW_SHUFFLE :'SHUFFLE';
		




// starting rule 
prog: forstatement EOF
		| statement EOF
		;
		
forstatement: KW_FOR LPAREN RPAREN block -> ^(TOK_FOR block);


block: LCURLY sentence+ RCURLY;

sentence: statement SEMICOLON;

statement
	: 
	execStatement
	;


execStatement
@init { msgs.push("statement"); }
@after { msgs.pop(); }
    : 
   
         registerStatement
    ;
   
// register a=fun();
registerStatement
@init { msgs.push("register clause"); }
@after { msgs.pop(); }
    :
    KW_REGISTER keyFunctionProperty -> ^(TOK_REGISTER keyFunctionProperty)
    ;   
 
 
 keyFunctionProperty
@init { msgs.push("specifying key/function property"); }
@after { msgs.pop(); }
    :
      key=Identifier EQUAL value= function -> ^(TOK_PROPERTY $key $value)
    ;    

// fun(par1, par2, par3)
function
@init { msgs.push("function specififunctioncation"); }
@after { msgs.pop(); }
    :
  KW_BOLT
      LPAREN
    (
       (expression COMMA Number )?
      )
      RPAREN
     gp=groupingProperty
          -> ^(TOK_BOLT (expression+)?  Number $gp) 
  | KW_SPOUT
        LPAREN
    (
       (expression COMMA Number)?
      )
      RPAREN
      -> ^( TOK_SPOUT (expression+)? Number) 
        ;
  
 groupingProperty
 @init { msgs.push("specifying grouping property"); }
@after { msgs.pop(); }
 	:
 	(DOT  groupingType  LPAREN (StringLiteral (COMMA StringLiteral)*)  RPAREN)?
 	-> ^(TOK_DOT groupingType ( StringLiteral+)? )	
 	; 
   
    
expression
	:	
	Identifier|StringLiteral
	;    
	
	

functionName
@init { msgs.push("function name"); }
@after { msgs.pop(); }
    : // Keyword IF is also a function name
   KW_BOLT | KW_SPOUT
    ;    
    
groupingType
@init { msgs.push("grouping type"); }
@after { msgs.pop(); }
    : // Keyword IF is also a function name
   KW_ALL | KW_NONE | KW_SHUFFLE | KW_FIELDS
    ;    
    
    
DOT : '.'; // generated as a part of Number rule
COLON : ':' ;
COMMA : ',' ;
SEMICOLON : ';' ;

LPAREN : '(' ;
RPAREN : ')' ;
LSQUARE : '[' ;
RSQUARE : ']' ;
LCURLY : '{';
RCURLY : '}';

EQUAL : '=' | '==';
NOTEQUAL : '<>' | '!=';
LESSTHANOREQUALTO : '<=';
LESSTHAN : '<';
GREATERTHANOREQUALTO : '>=';
GREATERTHAN : '>';

DIVIDE : '/';
PLUS : '+';
MINUS : '-';
STAR : '*';
MOD : '%';
DIV : 'DIV';

AMPERSAND : '&';
TILDE : '~';
BITWISEOR : '|';
BITWISEXOR : '^';
QUESTION : '?';
DOLLAR : '$';

// LITERALS
fragment
Letter
    : 'a'..'z' | 'A'..'Z'
    ;

fragment
HexDigit
    : 'a'..'f' | 'A'..'F'
    ;

fragment
Digit
    :
    '0'..'9'
    ;

fragment
Exponent
    :
    'e' ( PLUS|MINUS )? (Digit)+
    ;

fragment
RegexComponent
    : 'a'..'z' | 'A'..'Z' | '0'..'9' | '_'
    | PLUS | STAR | QUESTION | MINUS | DOT
    | LPAREN | RPAREN | LSQUARE | RSQUARE | LCURLY | RCURLY
    | BITWISEXOR | BITWISEOR | DOLLAR
    ;

StringLiteral
    :
    ( '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '\"' ( ~('\"'|'\\') | ('\\' .) )* '\"'
    )+
    ;

CharSetLiteral
    :
    StringLiteral
    | '0' 'X' (HexDigit|Digit)+
    ;

Number
    :
    (Digit)+ ( DOT (Digit)* (Exponent)? | Exponent)?
    ;

Identifier
    :
    (Letter | Digit) (Letter | Digit | '_')*
    | '`' RegexComponent+ '`'
    ;

CharSetName
    :
    '_' (Letter | Digit | '_' | '-' | '.' | ':' )+
    ;

WS  :  (' '|'\r'|'\t'|'\n') {$channel=HIDDEN;}
    ;

COMMENT
  : '--' (~('\n'|'\r'))*
    { $channel=HIDDEN; }
  ;   