<#--

    Copyright (C) 2017-2019 Dremio Corporation

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

-->
<#-- Copyright 2016 Dremio Corporation -->

  <#noparse>
<LUCENE_DEFAULT, LUCENE_BOOST, LUCENE_RANGE> TOKEN : {
  <#_NUM_CHAR:   ["0"-"9"] >
// every character that follows a backslash is considered as an escaped character
| <#_ESCAPED_CHAR: "\\" ~[] >
| <#_TERM_START_CHAR: ( ~[ " ", "\t", "\n", "\r", "\u3000", "+", "-", "!", "(", ")", ":", "^",
                           "[", "]", "\"", "{", "}", "~", "*", "?", "\\", "/" ]
                       | <_ESCAPED_CHAR> ) >
| <#_TERM_CHAR: ( <_TERM_START_CHAR> | <_ESCAPED_CHAR> | "-" | "+" ) >
| <#_WHITESPACE: ( " " | "\t" | "\n" | "\r" | "\u3000") >
| <#_QUOTED_CHAR: ( ~[ "\"", "\\" ] | <_ESCAPED_CHAR> ) >
}
</#noparse>

<LUCENE_DEFAULT, LUCENE_RANGE> SKIP : {
  < <_WHITESPACE>>
}

<LUCENE_DEFAULT> TOKEN : {
  <L_AND:       ("AND" | "and" | "&&") >
| <L_OR:        ("OR" | "or" | "||") >
| <L_NOT:       ("NOT" | "not" | "!") >
| <L_PLUS:      "+" >
| <L_MINUS:     "-" >
| <BAREOPER:    ("+"|"-"|"!") <_WHITESPACE> >
| <L_LPAREN:    "(" > { leftParen(); }
| <L_RPAREN:    ")" > { rightParen(); }
| <L_COLON:     ":" >
| <L_STAR:      "*" >
| <CARAT:     "^" > : LUCENE_BOOST
| <QUOTED:     "\"" (<_QUOTED_CHAR>)* "\"">
| <TERM:      <_TERM_START_CHAR> (<_TERM_CHAR>)*  >
| <FUZZY_SLOP:     "~" ((<_NUM_CHAR>)+ (( "." (<_NUM_CHAR>)+ )? (<_TERM_CHAR>)*) | (<_TERM_CHAR>)*)  >
| <PREFIXTERM:  ("*") | ( <_TERM_START_CHAR> (<_TERM_CHAR>)* "*" ) >
| <WILDTERM:  (<_TERM_START_CHAR> | [ "*", "?" ]) (<_TERM_CHAR> | ( [ "*", "?" ] ))* >
| <REGEXPTERM: "/" (~[ "/" ] | "\\/" )* "/" >
| <RANGEIN_START: "[" > : LUCENE_RANGE
| <RANGEEX_START: "{" > : LUCENE_RANGE
}

<LUCENE_BOOST> TOKEN : {
<L_NUMBER:    (<_NUM_CHAR>)+ ( "." (<_NUM_CHAR>)+ )? > : LUCENE_DEFAULT
}

<LUCENE_RANGE> TOKEN : {
<RANGE_TO: "TO">
| <RANGEIN_END: "]"> : LUCENE_DEFAULT
| <RANGEEX_END: "}"> : LUCENE_DEFAULT
| <RANGE_QUOTED: "\"" (~["\""] | "\\\"")+ "\"">
| <RANGE_GOOP: (~[ " ", "]", "}" ])+ >
}

// *   Query  ::= ( Clause )*
// *   Clause ::= ["+", "-"] [<TERM> ":"] ( <TERM> | "(" Query ")" )

void Conjunction(StringBuilder b) : {
}
{
  [
    <L_AND> { b.append(token.image + " "); }
    | <L_OR> { b.append(token.image + " "); }
  ]
}

void Modifiers(StringBuilder b) : {
}
{
  [
     <L_PLUS> { b.append(token.image); if (!token.image.equals("+")) b.append(" "); }
     | <L_MINUS> { b.append(token.image); if (!token.image.equals("-")) b.append(" "); }
     | <L_NOT> { b.append(token.image); if (!token.image.equals("!")) b.append(" "); }
  ]
}

// This makes sure that there is no garbage after the query string
SqlNode LuceneQuery() :
{
  StringBuilder b = new StringBuilder();
  Map<String,String> fieldMap = new HashMap();
}
{
  <L_CONTAINS> Query(b, fieldMap) <L_RPAREN>
  {
    return SqlContains.getNode(getPos(), fieldMap, b.toString().trim());
  }
}

void Query(StringBuilder b, Map<String,String> fieldMap) :
{
}
{
  Modifiers(b) Clause(b, fieldMap) { b.append(" ");}
  (
    Conjunction(b) Modifiers(b) Clause(b, fieldMap) { b.append(" ");}
  )*
}

void Clause(StringBuilder b, Map<String,String> fieldMap) : {
}
{
  [
    LOOKAHEAD(2)
    (
    <TERM> { b.append(SqlContains.getNewField(token.image, fieldMap) + " "); } <L_COLON> {b.append(token.image + " ");}
    | <L_STAR> { b.append(token.image + " " ); } <L_COLON> {b.append(token.image + " ");}
    )
  ]

  (
   Term(b)
   | <L_LPAREN> { b.append(token.image); } Query(b, fieldMap) <L_RPAREN> { b.append(token.image + " "); } (<CARAT> { b.append(token.image + " "); } <L_NUMBER> { b.append(token.image + " "); } )?

  )
    {   }
}


void Term(StringBuilder b) : {
}
{
  (
     (
       <TERM> {b.append(token.image + " ");}
       | <L_STAR> {b.append(token.image + " ");}
       | <PREFIXTERM> {b.append(token.image + " ");}
       | <WILDTERM> {b.append(token.image + " ");}
       | <REGEXPTERM> {b.append(token.image + " ");}
       | <L_NUMBER> {b.append(token.image + " ");}
       | <BAREOPER> {b.append(token.image + " ");}
     )
     [ <FUZZY_SLOP> {b.append(token.image + " ");} ]
     [ <CARAT> {b.append(token.image + " ");} <L_NUMBER> {b.append(token.image + " ");} [ <FUZZY_SLOP> {b.append(token.image + " ");} ] ]
     | ( ( <RANGEIN_START> {b.append(token.image + " ");} | <RANGEEX_START> {b.append(token.image + " ");} )
         ( <RANGE_GOOP> {b.append(token.image + " ");} | <RANGE_QUOTED> {b.append(token.image + " ");} )
         [ <RANGE_TO> {b.append(token.image + " ");} ]
         ( <RANGE_GOOP> {b.append(token.image + " ");} | <RANGE_QUOTED> {b.append(token.image + " ");} )
         ( <RANGEIN_END> {b.append(token.image + " ");} | <RANGEEX_END> {b.append(token.image + " ");}))
       [ <CARAT> {b.append(token.image + " ");} <L_NUMBER> {b.append(token.image + " ");} ]
     | <QUOTED> {b.append(token.image + " ");}
       [ <FUZZY_SLOP> {b.append(token.image + " ");} ]
       [ <CARAT> {b.append(token.image + " ");} <L_NUMBER> {b.append(token.image + " ");} ]
  )
}
