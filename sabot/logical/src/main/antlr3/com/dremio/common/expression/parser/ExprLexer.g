//
// Copyright (C) 2017-2019 Dremio Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

lexer grammar ExprLexer;

options {
    language=Java;
}

@header {
package com.dremio.common.expression.parser;
}

If       : 'if';
Else     : 'else';
Return   : 'return';
Then     : 'then';
End      : 'end';
In       : 'in';
Case     : 'case';
When     : 'when';

Cast: 'cast';
Convert  : 'convert_' ('from' | 'to');
Nullable: 'nullable';
Repeat: 'repeat';
As: 'as';

NULL     : '__$INTERNAL_NULL$__';
BIT      : 'bit' | 'BIT';
INT      : 'int' | 'INT';
BIGINT   : 'bigint' | 'BIGINT';
FLOAT4   : 'float4' | 'FLOAT4';
FLOAT8   : 'float8' | 'FLOAT8';
VARCHAR  : 'varchar' | 'VARCHAR';
VARBINARY: 'varbinary' | 'VARBINARY';
DATE     : 'date' | 'DATE';
TIMESTAMP: 'timestamp' | 'TIMESTAMP';
TIME     : 'time' | 'TIME';
TIMESTAMPTZ: 'timestamptz' | 'TIMESTAMPTZ';
INTERVAL : 'interval' | 'INTERVAL';
INTERVALYEAR : 'intervalyear' | 'INTERVALYEAR';
INTERVALDAY : 'intervalday' | 'INTERVALDAY';
Period : '.';
DECIMAL : 'decimal' | 'DECIMAL';
DECIMAL9 : 'decimal9' | 'DECIMAL9';
DECIMAL18 : 'decimal18' | 'DECIMAL18';
DECIMAL28DENSE : 'decimal28dense' | 'DECIMAL28DENSE';
DECIMAL28SPARSE : 'decimal28sparse' | 'DECIMAL28SPARSE';
DECIMAL38DENSE : 'decimal38dense' | 'DECIMAL38DENSE';
DECIMAL38SPARSE : 'decimal38sparse' | 'DECIMAL38SPARSE';
Or       : 'or' | 'OR' | 'Or';
And      : 'and' | 'AND' ;
Equals   : '==' | '=';
NEquals  : '<>' | '!=';
GTEquals : '>=';
LTEquals : '<=';
Caret      : '^';
Excl     : '!';
GT       : '>';
LT       : '<';
Plus      : '+';
Minus : '-';
Asterisk : '*';
ForwardSlash   : '/';
Percent  : '%';
OBrace   : '{';
CBrace   : '}';
OBracket : '[';
CBracket : ']';
OParen   : '(';
CParen   : ')';
SColon   : ';';
Comma    : ',';
QMark    : '?';
Colon    : ':';
SingleQuote: '\'';
InputReference : 'INPUT_REFERENCE';

Bool
  :  'true' 
  |  'false'
  ;

Number
  :  Int ('.' Digit*)? (('e' | 'E') ('+' | '-')? Digit*)? ('d'|'l'|'f'|'i'|'m')?
  ;

//Float
//  :  Int ('.' Digit*)? ('e' ('+' | '-')? Digit*)?
//  ;
//
//Integer
//  :  Digit Digit*
//  ;
  
Identifier
  : ('a'..'z' | 'A'..'Z' | '_' | '$') ('a'..'z' | 'A'..'Z' | '_' | '$' | Digit)*
  ;

QuotedIdentifier
@after {
  setText(getText().substring(1, getText().length()-1).replaceAll("\\\\(.)", "$1"));
}
  :  '`'  (~('`' | '\\')  | '\\' ('\\' | '`'))* '`' 
  ;

String
@after {
  setText(getText().substring(1, getText().length()-1).replaceAll("\\\\(.)", "$1"));
}
  :  '\'' (~('\'' | '\\') | '\\' ('\\' | '\''))* '\''
  ;

Comment
  :  '//' ~('\r' | '\n')* {skip();}
  |  '/*' .* '*/'         {skip();}
  ;

Space
  :  (' ' | '\t' | '\r' | '\n' | '\u000C') {skip();}
  ;

fragment Int
  :  '1'..'9' Digit*
  |  '0'
  ;
  
fragment Digit 
  :  '0'..'9'
  ;

FallThrough
	@after{
	  throw new RuntimeException(java.lang.String.format(
	      "Encountered an illegal char on line \%d, column \%d: '\%s'", 
	      getLine(), getCharPositionInLine(), getText()
	    )
	  );
	}
  :
  ;
