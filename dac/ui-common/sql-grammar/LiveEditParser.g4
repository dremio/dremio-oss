/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
parser grammar LiveEditParser;
import DremioParser;

options { tokenVocab=LiveEditLexer; }

/*
 This extends DremioParser with rules overriden to allow (some) invalid queries to be successfully parsed.
 Autocomplete normally requires the query up to the cursor position to be valid so that candidate tokens can be
 properly identified by accurately determining the parser context at which the caret lies. Antlr4-c3 requires this as
 it does not use the actual parse result (which utilizes error recovery to resynchronize the input) but performs its
 own traversal based on the ATN directly. Therefore, using this custom parser's ATN and parse tree serves two purposes:
   1) To correctly collect candidates when the caret is beyond the FROM in a SELECT statement - even if the select
      clause is empty or has a trailing comma.
   2) To correctly parse the FROM clause to determine the tables in scope to suggest tables/column names when
      constructing the SELECT clause, even when empty or incomplete.

 This allows the following to produce suggestions:
   - SELECT FROM ^ (case 1)
   - FROM ^ (case 1)
   - SELECT ^ FROM tbl (case 2)
   - SELECT col1, ^ FROM tbl (case 2)
 */

/*
 Rule overrides
 If adding any new ones, make sure to add a test case in LiveEditParser.test.ts
 */

// Overriden to allow suggestions in e.g. "SELECT FROM ^" and "FROM ^" by treating select clause as optional
sqlSelect :
  SELECT (HINT_BEG commaSepatatedSqlHints COMMENT_END)? STREAM? (DISTINCT | ALL)? (
    selectList (FROM fromClause whereOpt groupByOpt havingOpt windowOpt)?
    | invalidFrom fromClause whereOpt groupByOpt havingOpt windowOpt
  )
  | invalidFrom fromClause whereOpt groupByOpt havingOpt windowOpt  ;

// Overriden to allow suggestions in e.g. "SELECT col1, FROM ^"
selectList : selectItem (COMMA selectItem)* trailingSelectComma?  ;

// Overriden to allow suggestions in e.g. "SELECT tbl.^ FROM tbl"
// Otherwise the FROM table is not parseable in our error recovery implementation (is tbl the table name? alias?)
selectItem :
  selectExpression (
    trailingSelectItemDot (invalidAs simpleIdentifier)?
    | (AS? simpleIdentifier)?
  )  ;

// This is overriden to support suggestions after a trailing DOT e.g. select tbl.^ from tbl
// Otherwise, with rule expression2b : prefixRowOperator* expression3 (DOT identifier)*
// the . is not parsed as part of the compoundIdentifier in expression3 which makes determining
// the column suggestions challenging.
// This rule override should be removed once https://github.com/mike-lischke/antlr4-c3/issues/40#issuecomment-1732124123
// is done.
expression2b : prefixRowOperator* expression3  ;

/*
 Custom rules
 These are defined as rules and not embedded in sqlSelect/selectList to allow for referring to them
 in the autocomplete code, e.g. for ignoring tokens.
 */

invalidFrom: FROM  ;

invalidAs: AS  ;

trailingSelectComma: COMMA  ;

trailingSelectItemDot: DOT  ;
