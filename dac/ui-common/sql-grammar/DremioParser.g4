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
parser grammar DremioParser;

options { tokenVocab=DremioLexer; }

extendedTableRef : unusedExtension  ;

tableOverOpt :   ;

sqlSelectKeywords :   ;

sqlInsertKeywords :   ;

extendedBuiltinFunctionCall : unusedExtension  ;

floorCeilOptions : standardFloorCeilOptions  ;

exprOrJoinOrOrderedQuery : 
    query orderByLimitOpt
    | tableRef1 joinTable* addSetOpQuery*
	  ;

orderedQueryOrExpr : queryOrExpr orderByLimitOpt  ;

orderByLimitOpt : orderBy? (LIMIT (unsignedNumericLiteralOrParam COMMA unsignedNumericLiteralOrParam | unsignedNumericLiteralOrParam | ALL))? (OFFSET unsignedNumericLiteralOrParam (ROW | ROWS)?)? (FETCH (FIRST | NEXT) unsignedNumericLiteralOrParam (ROW | ROWS) ONLY)?  ;

leafQuery : 
    sqlSelect
    | tableConstructor
    | explicitTable
	  ;

parenthesizedExpression : LPAREN exprOrJoinOrOrderedQuery RPAREN  ;

parenthesizedQueryOrCommaList : LPAREN orderedQueryOrExpr (COMMA expression)* RPAREN  ;

parenthesizedQueryOrCommaListWithDefault : LPAREN (orderedQueryOrExpr | default) (COMMA (expression | default))* RPAREN  ;

functionParameterList : LPAREN (DISTINCT | ALL)? arg0 (COMMA arg)* RPAREN  ;

arg0 : (simpleIdentifier NAMED_ARGUMENT_ASSIGNMENT)? (default | orderedQueryOrExpr)  ;

arg : (simpleIdentifier NAMED_ARGUMENT_ASSIGNMENT)? (default | expression)  ;

default : DEFAULT_  ;

sqlQueryEof : orderedQueryOrExpr EOF  ;

sqlStmtList : sqlStmt (SEMICOLON sqlStmt?)* EOF  ;

sqlStmt : 
    sqlShowTables
    | sqlShowSchemas
    | sqlShowBranches
    | sqlShowTags
    | sqlShowLogs
    | sqlDescribeTable
    | sqlUseVersion
    | sqlUseSchema
    | sqlCopyInto
    | sqlCreateOrReplace
    | sqlDropView
    | sqlShowFiles
    | sqlCreateBranch
    | sqlCreateTable
    | sqlCreateTag
    | sqlInsertTable
    | sqlDeleteFromTable
    | sqlMergeIntoTable
    | sqlUpdateTable
    | sqlDropBranch
    | sqlDropTag
    | sqlDropTable
    | sqlRollbackTable
    | sqlTruncateTable
    | sqlAccel
    | sqlRefreshReflection
    | sqlLoadMaterialization
    | sqlCompactMaterialization
    | sqlExplainJson
    | sqlAlterClearPlanCache
    | sqlGrant
    | sqlRevoke
    | sqlCreateRole
    | sqlRevokeRole
    | sqlCreateUser
    | sqlAlterUser
    | sqlDropUser
    | sqlDropRole
    | sqlAnalyzeTableStatistics
    | sqlRefreshDataset
    | sqlAssignBranch
    | sqlAssignTag
    | sqlMergeBranch
    | sqlDropFunction
    | sqlShowViews
    | sqlDescribeFunction
    | sqlShowFunctions
    | sqlOptimize
    | sqlVacuum
    | sqlSetOption
    | sqlAlter
    | orderedQueryOrExpr
    | sqlExplain
    | sqlDescribe
    | sqlInsert
    | sqlDelete
    | sqlUpdate
    | sqlMerge
    | sqlProcedureCall
	  ;

sqlStmtEof : sqlStmt EOF  ;

sqlShowTables : SHOW TABLES (AT (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier)? ((FROM | IN) compoundIdentifier)? (LIKE stringLiteral)?  ;

sqlShowViews : SHOW VIEWS (AT (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier)? ((FROM | IN) compoundIdentifier)? (LIKE stringLiteral)?  ;

sqlShowFiles : SHOW FILES ((FROM | IN) compoundIdentifier)?  ;

sqlShowSchemas : SHOW (DATABASES | SCHEMAS) (LIKE stringLiteral)?  ;

sqlDescribeTable : (DESCRIBE | DESC) compoundIdentifier compoundIdentifier?  ;

sqlUseSchema : USE compoundIdentifier  ;

parseOptionalFieldList : parseRequiredFieldList?  ;

parseOptionalFieldListWithMasking : parseRequiredFieldListWithMasking?  ;

parseRequiredFieldList : LPAREN simpleIdentifierCommaList RPAREN  ;

parseRequiredFieldListWithMasking : LPAREN columnNamesWithMasking (COMMA columnNamesWithMasking)* RPAREN  ;

columnNamesWithMasking : simpleIdentifier (MASKING POLICY policy)?  ;

sqlCreateOrReplace : CREATE (OR REPLACE)? (FUNCTION (IF NOT EXISTS)? compoundIdentifier parseFunctionFieldList RETURNS (TABLE parseFunctionReturnFieldList | dataType nullableOptDefaultTrue) RETURN orderedQueryOrExpr | (VIEW | VDS) compoundIdentifier parseOptionalFieldListWithMasking (ROW ACCESS POLICY policy)? AS orderedQueryOrExpr)  ;

sqlDropFunction : DROP FUNCTION (IF EXISTS)? compoundIdentifier  ;

sqlDescribeFunction : (DESCRIBE | DESC) FUNCTION compoundIdentifier  ;

sqlShowFunctions : SHOW FUNCTIONS (LIKE stringLiteral)?  ;

parseFunctionReturnFieldList : LPAREN functionReturnTypeCommaList RPAREN  ;

functionReturnTypeCommaList : (returnKeyValuePair (COMMA returnKeyValuePair)*)?  ;

returnKeyValuePair : simpleIdentifier dataType nullableOptDefaultTrue  ;

parseFunctionFieldList : LPAREN fieldFunctionTypeCommaList RPAREN  ;

functionKeyValuePair : simpleIdentifier dataType nullableOptDefaultTrue (DEFAULT_ orderedQueryOrExpr)?  ;

fieldFunctionTypeCommaList : (functionKeyValuePair (COMMA functionKeyValuePair)*)?  ;

sqlDropView : DROP (VIEW | VDS) (IF EXISTS)? compoundIdentifier  ;

tableElementListWithMasking : LPAREN tableElementWithMasking (COMMA tableElementWithMasking)* RPAREN  ;

tableElementList : LPAREN tableElement (COMMA tableElement)* RPAREN  ;

tableElementWithMasking : 
    simpleIdentifier (dataType nullableOptDefaultTrue (MASKING POLICY policy)? | (MASKING POLICY policy)?)
    | simpleIdentifier (MASKING POLICY policy)?
	  ;

tableElement : 
    simpleIdentifier (dataType nullableOptDefaultTrue)?
    | simpleIdentifier
	  ;

parsePartitionTransform : 
    simpleIdentifier (LPAREN (literal COMMA)* simpleIdentifier RPAREN)?
    | (YEAR | MONTH | HOUR | DAY | TRUNCATE | IDENTITY) LPAREN (literal COMMA)* simpleIdentifier RPAREN
	  ;

parsePartitionTransformList : LPAREN parsePartitionTransform (COMMA parsePartitionTransform)* RPAREN  ;

sqlCopyInto : COPY INTO compoundIdentifier FROM stringLiteral (FILES LPAREN literal (COMMA literal)* RPAREN | REGEX stringLiteral)? (FILE_FORMAT literal)? (LPAREN parseCopyIntoOptions (COMMA parseCopyIntoOptions)* RPAREN)?  ;

parseCopyIntoOptions : 
    (DATE_FORMAT | TIME_FORMAT | TIMESTAMP_FORMAT | TRIM_SPACE | RECORD_DELIMITER | FIELD_DELIMITER | QUOTE_CHAR | ESCAPE_CHAR | EMPTY_AS_NULL | ON_ERROR) literal
    | NULL_IF LPAREN literal (COMMA literal)* RPAREN
	  ;

sqlCreateTable : CREATE TABLE (IF NOT EXISTS)? compoundIdentifier tableElementListWithMasking? ((STRIPED | HASH | ROUNDROBIN)? PARTITION BY parsePartitionTransformList)? (DISTRIBUTE BY parseRequiredFieldList)? (LOCALSORT BY parseRequiredFieldList)? (LOCATION stringLiteral)? (STORE AS LPAREN arg0 (COMMA arg)* RPAREN)? (WITH SINGLE WRITER)? (ROW ACCESS POLICY policy)? (AS orderedQueryOrExpr)?  ;

sqlInsertTable : INSERT INTO compoundIdentifier tableElementList? orderedQueryOrExpr  ;

sqlDeleteFromTable : DELETE FROM compoundIdentifier (AS? simpleIdentifier)? (USING fromClause)? whereOpt  ;

sqlUpdateTable : UPDATE compoundIdentifier (AS? simpleIdentifier)? SET simpleIdentifier EQ expression (COMMA simpleIdentifier EQ expression)* (FROM fromClause)? whereOpt  ;

sqlMergeIntoTable : MERGE INTO compoundIdentifier (AS? simpleIdentifier)? USING tableRef ON expression (dremioWhenMatchedClause dremioWhenNotMatchedClause? | dremioWhenNotMatchedClause)  ;

dremioWhenMatchedClause : WHEN MATCHED THEN UPDATE SET (STAR | simpleIdentifier EQ expression (COMMA simpleIdentifier EQ expression)*)  ;

dremioWhenNotMatchedClause : WHEN NOT MATCHED THEN INSERT sqlInsertKeywords (STAR | parenthesizedSimpleIdentifierList? LPAREN? VALUES rowConstructor RPAREN?)  ;

sqlDropTable : DROP TABLE (IF EXISTS)? compoundIdentifier  ;

sqlRollbackTable : ROLLBACK TABLE compoundIdentifier TO (SNAPSHOT stringLiteral | TIMESTAMP stringLiteral)  ;

sqlVacuum : VACUUM TABLE compoundIdentifier EXPIRE SNAPSHOTS (OLDER_THAN EQ stringLiteral)? (RETAIN_LAST EQ unsignedNumericLiteral)?  ;

sqlTruncateTable : TRUNCATE TABLE? (IF EXISTS)? compoundIdentifier  ;

sqlRefreshReflection : REFRESH REFLECTION stringLiteral AS stringLiteral  ;

sqlLoadMaterialization : LOAD MATERIALIZATION METADATA compoundIdentifier  ;

sqlCompactMaterialization : COMPACT MATERIALIZATION compoundIdentifier AS stringLiteral  ;

sqlAnalyzeTableStatistics : ANALYZE TABLE compoundIdentifier FOR (ALL COLUMNS | COLUMNS parseOptionalFieldList) (COMPUTE STATISTICS | DELETE STATISTICS)  ;

sqlRefreshDataset : REFRESH DATASET compoundIdentifier (FOR ALL (FILES | PARTITIONS) | FOR FILES parseRequiredFilesList | FOR PARTITIONS parseRequiredPartitionList)? (AUTO PROMOTION | AVOID PROMOTION)? (FORCE UPDATE | LAZY UPDATE)? (DELETE WHEN MISSING | MAINTAIN WHEN MISSING)?  ;

sqlOptimize : OPTIMIZE TABLE compoundIdentifier (REWRITE DATA)? (USING BIN_PACK)? (LPAREN parseOptimizeOptions (COMMA parseOptimizeOptions)* RPAREN)?  ;

parseOptimizeOptions : (MIN_INPUT_FILES | TARGET_FILE_SIZE_MB | MIN_FILE_SIZE_MB | MAX_FILE_SIZE_MB) EQ literal  ;

fieldNameStructTypeCommaList : simpleIdentifier COLON? dataType nullableOptDefaultTrue (COMMA simpleIdentifier COLON? dataType nullableOptDefaultTrue)*  ;

dremioRowTypeName : (ROW | STRUCT) (LPAREN | LT) fieldNameStructTypeCommaList (RPAREN | GT)  ;

arrayTypeName : (ARRAY | LIST) (LPAREN | LT) dataType nullableOptDefaultTrue (RPAREN | GT)  ;

sqlAccel : ALTER (SOURCE simpleIdentifier REFRESH STATUS | SPACE simpleIdentifier ROUTE (ALL REFLECTIONS | REFLECTIONS) sqlAlterDatasetReflectionRouting | FOLDER compoundIdentifier ROUTE (ALL REFLECTIONS | REFLECTIONS) sqlAlterDatasetReflectionRouting | (TABLE | VDS | VIEW | PDS | DATASET) compoundIdentifier (ADD ROW ACCESS POLICY policy | DROP ROW ACCESS POLICY policy | ADD PRIMARY KEY parseRequiredFieldList | DROP PRIMARY KEY | ROUTE (ALL REFLECTIONS | REFLECTIONS) sqlAlterDatasetReflectionRouting | ADD (COLUMNS tableElementList | PARTITION FIELD parsePartitionTransform) | (CHANGE | ALTER | MODIFY) COLUMN? simpleIdentifier (typedElement | SET MASKING POLICY policy | UNSET MASKING POLICY policyWithoutArgs | sqlSetOption) | DROP (REFLECTION sqlDropReflection | PARTITION FIELD parsePartitionTransform | COLUMN? simpleIdentifier) | CREATE (AGGREGATE REFLECTION simpleIdentifier sqlCreateAggReflection | RAW REFLECTION simpleIdentifier sqlCreateRawReflection | EXTERNAL REFLECTION simpleIdentifier sqlAddExternalReflection) | FORGET METADATA | REFRESH METADATA (FOR ALL (FILES | PARTITIONS) | FOR FILES parseRequiredFilesList | FOR PARTITIONS parseRequiredPartitionList)? (AUTO PROMOTION | AVOID PROMOTION)? (FORCE UPDATE | LAZY UPDATE)? (DELETE WHEN MISSING | MAINTAIN WHEN MISSING)? | ENABLE (SCHEMA LEARNING | APPROXIMATE STATS | (RAW | AGGREGATE) ACCELERATION) | DISABLE (SCHEMA LEARNING | APPROXIMATE STATS | (RAW | AGGREGATE) ACCELERATION) | sqlSetOption))  ;

parseRequiredFilesList : LPAREN stringLiteralCommaList RPAREN  ;

stringLiteralCommaList : stringLiteral (COMMA stringLiteral)*  ;

parseRequiredPartitionList : LPAREN keyValueCommaList RPAREN  ;

keyValueCommaList : keyValuePair (COMMA keyValuePair)*  ;

keyValuePair : simpleIdentifier EQ (NULL | stringLiteral)  ;

sqlCreateAggReflection : USING DIMENSIONS parseRequiredFieldListWithGranularity MEASURES parseRequiredFieldListWithMeasures (DISTRIBUTE BY parseRequiredFieldList)? ((STRIPED | CONSOLIDATED)? PARTITION BY parseRequiredFieldList)? (LOCALSORT BY parseRequiredFieldList)? (ARROW CACHE)?  ;

parseRequiredFieldListWithGranularity : LPAREN simpleIdentifierCommaListWithGranularity RPAREN  ;

simpleIdentifierCommaListWithGranularity : simpleIdentifier (BY DAY)? (COMMA simpleIdentifierCommaListWithGranularity)*  ;

parseRequiredFieldListWithMeasures : (LPAREN simpleIdentifierCommaListWithMeasures? RPAREN)?  ;

simpleIdentifierCommaListWithMeasures : simpleIdentifier (LPAREN measureList RPAREN)? (COMMA simpleIdentifierCommaListWithMeasures)*  ;

measureList : (MIN | MAX | COUNT | SUM | (APPROXIMATE | APPROX) COUNT DISTINCT) (COMMA measureList)*  ;

sqlCreateRawReflection : USING DISPLAY parseOptionalFieldList (DISTRIBUTE BY parseRequiredFieldList)? ((STRIPED | CONSOLIDATED)? PARTITION BY parseRequiredFieldList)? (LOCALSORT BY parseRequiredFieldList)? (ARROW CACHE)?  ;

sqlDropReflection : simpleIdentifier  ;

sqlAddExternalReflection : USING compoundIdentifier  ;

sqlAlterDatasetReflectionRouting : TO DEFAULT_? (QUEUE | ENGINE) simpleIdentifier  ;

policy : compoundIdentifier parseColumns  ;

policyWithoutArgs : compoundIdentifier  ;

parseColumns : LPAREN identifierCommaList RPAREN  ;

identifierCommaList : simpleIdentifier (COMMA simpleIdentifier)*  ;

typedElement : simpleIdentifier dataType nullableOptDefaultTrue  ;

sqlAlterClearPlanCache : ALTER scope (CLEAR PLAN CACHE | sqlSetOption)  ;

conjunction : (L_AND | L_OR)?  ;

modifiers : (L_PLUS | L_MINUS | L_NOT)?  ;

luceneQuery : L_CONTAINS dQuery L_RPAREN  ;

dQuery : modifiers clause (conjunction modifiers clause)*  ;

clause : (TERM L_COLON | L_STAR L_COLON)? (term | L_LPAREN dQuery L_RPAREN (CARAT L_NUMBER)?)  ;

term : 
    (TERM | L_STAR | PREFIXTERM | WILDTERM | REGEXPTERM | L_NUMBER | BAREOPER) FUZZY_SLOP? (CARAT L_NUMBER FUZZY_SLOP?)?
    | (RANGEIN_START | RANGEEX_START) (RANGE_GOOP | RANGE_QUOTED) RANGE_TO? (RANGE_GOOP | RANGE_QUOTED) (RANGEIN_END | RANGEEX_END) (CARAT L_NUMBER)?
    | QUOTED FUZZY_SLOP? (CARAT L_NUMBER)?
	  ;

sqlExplainJson : EXPLAIN JSON simpleIdentifier? FOR sqlQueryOrDml  ;

sqlGrant : GRANT (OWNERSHIP sqlGrantOwnership | ROLE sqlGrantRole | sqlGrantPrivilege)  ;

sqlGrantPrivilege : privilegeCommaList ON ((SYSTEM | PROJECT) | (PDS | TABLE) compoundIdentifier | (VDS | VIEW) compoundIdentifier | FUNCTION compoundIdentifier | (FOLDER | SCHEMA) compoundIdentifier | SOURCE simpleIdentifier | SPACE simpleIdentifier | ORG | CLOUD simpleIdentifier | ENGINE simpleIdentifier | IDENTITY PROVIDER simpleIdentifier | OAUTH APPLICATION simpleIdentifier | EXTERNAL TOKENS PROVIDER simpleIdentifier | SCRIPT simpleIdentifier | ALL DATASETS IN ((FOLDER | SCHEMA) compoundIdentifier | SOURCE simpleIdentifier | SPACE simpleIdentifier)) TO parseGranteeType simpleIdentifier  ;

parseGranteeType : 
    USER
    | ROLE
	  ;

privilegeCommaList : privilege (COMMA privilege)*  ;

privilege : 
    VIEW JOB HISTORY
    | ALTER REFLECTION
    | ALTER
    | SELECT
    | VIEW REFLECTION
    | VIEW
    | MODIFY
    | MANAGE GRANTS
    | CREATE TABLE
    | DROP
    | EXTERNAL QUERY
    | OWNERSHIP
    | MONITOR
    | OPERATE
    | USAGE
    | CREATE CLOUD
    | CREATE PROJECT
    | CREATE CATALOG
    | CONFIGURE SECURITY
    | INSERT
    | TRUNCATE
    | DELETE
    | UPDATE
    | CREATE USER
    | CREATE ROLE
    | EXECUTE
    | CREATE SOURCE
    | UPLOAD
    | ALL
	  ;

sqlRevoke : REVOKE privilegeCommaList ON ((SYSTEM | PROJECT) | (PDS | TABLE) compoundIdentifier | (VDS | VIEW) compoundIdentifier | FUNCTION compoundIdentifier | (FOLDER | SCHEMA) compoundIdentifier | SOURCE simpleIdentifier | SPACE simpleIdentifier | ORG | CLOUD simpleIdentifier | ENGINE simpleIdentifier | IDENTITY PROVIDER simpleIdentifier | OAUTH APPLICATION simpleIdentifier | EXTERNAL TOKENS PROVIDER simpleIdentifier | SCRIPT simpleIdentifier | ALL DATASETS IN ((FOLDER | SCHEMA) compoundIdentifier | SOURCE simpleIdentifier | SPACE simpleIdentifier)) FROM parseGranteeType simpleIdentifier  ;

sqlGrantOwnership : ON (USER simpleIdentifier | (PDS | TABLE) compoundIdentifier | (VDS | VIEW) compoundIdentifier | FUNCTION compoundIdentifier | (FOLDER | SCHEMA) compoundIdentifier | SPACE simpleIdentifier | SOURCE simpleIdentifier | PROJECT | ORG | CLOUD simpleIdentifier | CATALOG simpleIdentifier | ENGINE simpleIdentifier | ROLE simpleIdentifier | IDENTITY PROVIDER simpleIdentifier | OAUTH APPLICATION simpleIdentifier | EXTERNAL TOKENS PROVIDER simpleIdentifier) TO parseGranteeType simpleIdentifier  ;

sqlGrantRole : simpleIdentifier TO parseGranteeType simpleIdentifier  ;

sqlCreateRole : CREATE ROLE simpleIdentifier  ;

sqlRevokeRole : REVOKE ROLE simpleIdentifier FROM parseGranteeType simpleIdentifier  ;

sqlDropRole : DROP ROLE simpleIdentifier  ;

sqlCreateUser : CREATE USER simpleIdentifier (SET PASSWORD stringLiteral)?  ;

sqlDropUser : DROP USER simpleIdentifier  ;

sqlAlterUser : ALTER USER simpleIdentifier (SET PASSWORD stringLiteral | UNSET PASSWORD)  ;

sqlUseVersion : USE (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier (IN simpleIdentifier)?  ;

sqlShowBranches : SHOW BRANCHES (IN simpleIdentifier)?  ;

sqlShowTags : SHOW TAGS (IN simpleIdentifier)?  ;

sqlShowLogs : SHOW LOGS (AT (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier)? (IN simpleIdentifier)?  ;

sqlCreateBranch : CREATE BRANCH (IF NOT EXISTS)? simpleIdentifier (AT (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier)? (IN simpleIdentifier)?  ;

sqlCreateTag : CREATE TAG (IF NOT EXISTS)? simpleIdentifier (AT (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier)? (IN simpleIdentifier)?  ;

sqlDropBranch : DROP BRANCH (IF EXISTS)? simpleIdentifier (AT COMMIT simpleIdentifier | FORCE) (IN simpleIdentifier)?  ;

sqlDropTag : DROP TAG (IF EXISTS)? simpleIdentifier (AT COMMIT simpleIdentifier | FORCE) (IN simpleIdentifier)?  ;

sqlMergeBranch : MERGE BRANCH simpleIdentifier (INTO simpleIdentifier)? (IN simpleIdentifier)?  ;

sqlAssignBranch : ALTER BRANCH simpleIdentifier ASSIGN (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier (IN simpleIdentifier)?  ;

sqlAssignTag : ALTER TAG simpleIdentifier ASSIGN (REF | REFERENCE | BRANCH | TAG | COMMIT) simpleIdentifier (IN simpleIdentifier)?  ;

tableWithVersionContext : AT (SNAPSHOT stringLiteral | BRANCH simpleIdentifier | TAG simpleIdentifier | COMMIT simpleIdentifier | (REF | REFERENCE) simpleIdentifier | expression)  ;

sqlSelect : SELECT sqlSelectKeywords STREAM? (DISTINCT | ALL)? selectList (FROM fromClause whereOpt groupByOpt havingOpt windowOpt)?  ;

sqlExplain : EXPLAIN PLAN explainDetailLevel? explainDepth (AS XML | AS JSON)? FOR sqlQueryOrDml  ;

sqlQueryOrDml : 
    orderedQueryOrExpr
    | sqlInsert
    | sqlDelete
    | sqlUpdate
    | sqlMerge
	  ;

explainDepth : (WITH TYPE | WITH IMPLEMENTATION | WITHOUT IMPLEMENTATION)?  ;

explainDetailLevel : 
    EXCLUDING ATTRIBUTES
    | INCLUDING ALL? ATTRIBUTES
	  ;

sqlDescribe : DESCRIBE ((DATABASE | CATALOG | SCHEMA) compoundIdentifier | TABLE? compoundIdentifier simpleIdentifier? | STATEMENT? sqlQueryOrDml)  ;

sqlProcedureCall : CALL namedRoutineCall  ;

namedRoutineCall : compoundIdentifier LPAREN (arg0 (COMMA arg)*)? RPAREN  ;

sqlInsert : (INSERT | UPSERT) sqlInsertKeywords INTO compoundIdentifier (EXTEND? extendList)? parenthesizedCompoundIdentifierList? orderedQueryOrExpr  ;

sqlDelete : DELETE FROM compoundIdentifier (EXTEND? extendList)? (AS? simpleIdentifier)? whereOpt  ;

sqlUpdate : UPDATE compoundIdentifier (EXTEND? extendList)? (AS? simpleIdentifier)? SET simpleIdentifier EQ expression (COMMA simpleIdentifier EQ expression)* whereOpt  ;

sqlMerge : MERGE INTO compoundIdentifier (EXTEND? extendList)? (AS? simpleIdentifier)? USING tableRef ON expression (whenMatchedClause whenNotMatchedClause? | whenNotMatchedClause)  ;

whenMatchedClause : WHEN MATCHED THEN UPDATE SET simpleIdentifier EQ expression (COMMA simpleIdentifier EQ expression)*  ;

whenNotMatchedClause : WHEN NOT MATCHED THEN INSERT sqlInsertKeywords parenthesizedSimpleIdentifierList? LPAREN? VALUES rowConstructor RPAREN?  ;

selectList : selectItem (COMMA selectItem)*  ;

selectItem : selectExpression (AS? simpleIdentifier)?  ;

selectExpression : 
    STAR
    | expression
	  ;

natural : NATURAL?  ;

joinType : 
    JOIN
    | INNER JOIN
    | LEFT OUTER? JOIN
    | RIGHT OUTER? JOIN
    | FULL OUTER? JOIN
    | CROSS JOIN
	  ;

fromClause : join (COMMA join)*  ;

join : tableRef1 joinTable*  ;

joinTable : 
    natural joinType tableRef1 (ON expression | USING parenthesizedSimpleIdentifierList)?
    | CROSS APPLY tableRef2
    | OUTER APPLY tableRef2
	  ;

tableRef : tableRef3  ;

tableRef1 : tableRef3  ;

tableRef2 : tableRef3  ;

tableRef3 : (compoundIdentifier tableWithVersionContext? (EXTEND? extendList)? tableOverOpt matchRecognizeOpt? | LATERAL? parenthesizedExpression tableOverOpt matchRecognizeOpt? | UNNEST parenthesizedQueryOrCommaList (WITH ORDINALITY)? | LATERAL? tableFunctionCall tableWithVersionContext? | extendedTableRef) pivot? unpivot? (AS? simpleIdentifier parenthesizedSimpleIdentifierList?)? tablesample?  ;

tablesample : TABLESAMPLE (SUBSTITUTE LPAREN stringLiteral RPAREN | (BERNOULLI | SYSTEM) LPAREN unsignedNumericLiteral RPAREN (REPEATABLE LPAREN intLiteral RPAREN)?)  ;

extendList : LPAREN columnType (COMMA columnType)* RPAREN  ;

columnType : compoundIdentifier dataType (NOT NULL)?  ;

compoundIdentifierType : compoundIdentifier (dataType (NOT NULL)?)?  ;

tableFunctionCall : TABLE LPAREN SPECIFIC? namedRoutineCall RPAREN  ;

explicitTable : TABLE compoundIdentifier  ;

tableConstructor : VALUES rowConstructorList  ;

rowConstructorList : rowConstructor (COMMA rowConstructor)*  ;

rowConstructor : 
    LPAREN ROW parenthesizedQueryOrCommaListWithDefault RPAREN
    | ROW? parenthesizedQueryOrCommaListWithDefault
    | expression
	  ;

whereOpt : (WHERE expression)?  ;

groupByOpt : (GROUP BY groupingElementList)?  ;

groupingElementList : groupingElement (COMMA groupingElement)*  ;

groupingElement : 
    GROUPING SETS LPAREN groupingElementList RPAREN
    | ROLLUP LPAREN expressionCommaList RPAREN
    | CUBE LPAREN expressionCommaList RPAREN
    | LPAREN RPAREN
    | expression
	  ;

expressionCommaList : expressionCommaList2  ;

expressionCommaList2 : expression (COMMA expression)*  ;

havingOpt : (HAVING expression)?  ;

windowOpt : (WINDOW simpleIdentifier AS windowSpecification (COMMA simpleIdentifier AS windowSpecification)*)?  ;

windowSpecification : LPAREN simpleIdentifier? (PARTITION BY expressionCommaList)? orderBy? ((ROWS | RANGE) (BETWEEN windowRange AND windowRange | windowRange))? (ALLOW PARTIAL | DISALLOW PARTIAL)? RPAREN  ;

windowRange : 
    CURRENT ROW
    | UNBOUNDED (PRECEDING | FOLLOWING)
    | expression (PRECEDING | FOLLOWING)
	  ;

orderBy : ORDER BY orderItem (COMMA orderItem)*  ;

orderItem : expression (ASC | DESC)? (NULLS FIRST | NULLS LAST)?  ;

pivot : PIVOT LPAREN pivotAgg (COMMA pivotAgg)* FOR simpleIdentifierOrList IN LPAREN (pivotValue (COMMA pivotValue)*)? RPAREN RPAREN  ;

pivotAgg : namedFunctionCall (AS? simpleIdentifier)?  ;

pivotValue : rowConstructor (AS? simpleIdentifier)?  ;

unpivot : UNPIVOT (INCLUDE NULLS | EXCLUDE NULLS)? LPAREN simpleIdentifierOrList FOR simpleIdentifierOrList IN LPAREN unpivotValue (COMMA unpivotValue)* RPAREN RPAREN  ;

unpivotValue : simpleIdentifierOrList (AS rowConstructor)?  ;

matchRecognizeOpt : MATCH_RECOGNIZE LPAREN (PARTITION BY expressionCommaList)? orderBy? (MEASURES measureColumnCommaList)? (ONE ROW PER MATCH | ALL ROWS PER MATCH)? (AFTER MATCH SKIP_ (TO (NEXT ROW | FIRST simpleIdentifier | LAST? simpleIdentifier) | PAST LAST ROW))? PATTERN LPAREN CARET? patternExpression DOLLAR? RPAREN (WITHIN intervalLiteral)? (SUBSET subsetDefinitionCommaList)? DEFINE patternDefinitionCommaList RPAREN  ;

measureColumnCommaList : measureColumn (COMMA measureColumn)*  ;

measureColumn : expression AS simpleIdentifier  ;

patternExpression : patternTerm (VERTICAL_BAR patternTerm)*  ;

patternTerm : patternFactor patternFactor*  ;

patternFactor : patternPrimary ((STAR | PLUS | HOOK | LBRACE (unsignedNumericLiteral (COMMA unsignedNumericLiteral?)? RBRACE | COMMA unsignedNumericLiteral RBRACE | MINUS patternExpression MINUS RBRACE)) HOOK?)?  ;

patternPrimary : 
    simpleIdentifier
    | LPAREN patternExpression RPAREN
    | LBRACE MINUS patternExpression MINUS RBRACE
    | PERMUTE LPAREN patternExpression (COMMA patternExpression)* RPAREN
	  ;

subsetDefinitionCommaList : subsetDefinition (COMMA subsetDefinition)*  ;

subsetDefinition : simpleIdentifier EQ LPAREN expressionCommaList RPAREN  ;

patternDefinitionCommaList : patternDefinition (COMMA patternDefinition)*  ;

patternDefinition : simpleIdentifier AS expression  ;

sqlExpressionEof : expression EOF  ;

queryOrExpr : withList? leafQueryOrExpr (binaryQueryOperator leafQueryOrExpr)*  ;

query : withList? leafQuery (binaryQueryOperator (parenthesizedExpression | leafQuery))*  ;

addSetOpQuery : binaryQueryOperator leafQueryOrExpr  ;

withList : WITH withItem (COMMA withItem)*  ;

withItem : simpleIdentifier parenthesizedSimpleIdentifierList? AS parenthesizedExpression  ;

leafQueryOrExpr : 
    expression
    | leafQuery
	  ;

expression : expression2  ;

expression2b : prefixRowOperator* expression3  ;

expression2 : expression2b (((NOT IN | IN | comp (SOME | ANY | ALL)) parenthesizedQueryOrCommaList | (NOT BETWEEN (SYMMETRIC | ASYMMETRIC)? | BETWEEN (SYMMETRIC | ASYMMETRIC)?) expression3 | NOT? ((LIKE ALL | LIKE ANY | LIKE SOME) parenthesizedQueryOrCommaList | (LIKE | SIMILAR TO) expression2 (ESCAPE expression3)?) | binaryRowOperator expression2b | LBRACKET expression RBRACKET (DOT simpleIdentifier)* | postfixRowOperator)+)?  ;

comp : 
    LT
    | LE
    | GT
    | GE
    | EQ
    | NE
    | NE2
	  ;

expression3 : 
    atomicRowExpression
    | cursorExpression
    | ROW parenthesizedSimpleIdentifierList
    | ROW? parenthesizedQueryOrCommaList intervalQualifier?
	  ;

periodOperator : 
    OVERLAPS
    | IMMEDIATELY PRECEDES
    | PRECEDES
    | IMMEDIATELY SUCCEEDS
    | SUCCEEDS
    | EQUALS
	  ;

collateClause : COLLATE COLLATION_ID  ;

unsignedNumericLiteralOrParam : 
    unsignedNumericLiteral
    | dynamicParam
	  ;

atomicRowExpression : 
    literal
    | dynamicParam
    | builtinFunctionCall
    | jdbcFunctionCall
    | multisetConstructor
    | arrayConstructor
    | mapConstructor
    | periodConstructor
    | namedFunctionCall
    | contextVariable
    | compoundIdentifier
    | newSpecification
    | caseExpression
    | sequenceExpression
	  ;

caseExpression : CASE expression? (WHEN expressionCommaList THEN expression)+ (ELSE expression)? END  ;

sequenceExpression : (NEXT | CURRENT) VALUE FOR compoundIdentifier  ;

sqlSetOption : 
    SET compoundIdentifier EQ (literal | simpleIdentifier | ON)
    | RESET (compoundIdentifier | ALL)
	  ;

sqlAlter : ALTER scope sqlSetOption  ;

scope : 
    SYSTEM
    | SESSION
	  ;

literal : 
    numericLiteral
    | stringLiteral
    | specialLiteral
    | dateTimeLiteral
    | intervalLiteral
    | luceneQuery
	  ;

unsignedNumericLiteral : 
    UNSIGNED_INTEGER_LITERAL
    | DECIMAL_NUMERIC_LITERAL
    | APPROX_NUMERIC_LITERAL
	  ;

numericLiteral : 
    PLUS unsignedNumericLiteral
    | MINUS unsignedNumericLiteral
    | unsignedNumericLiteral
	  ;

specialLiteral : 
    TRUE
    | FALSE
    | UNKNOWN
    | NULL
	  ;

stringLiteral : 
    BINARY_STRING_LITERAL QUOTED_STRING*
    | (PREFIXED_STRING_LITERAL | QUOTED_STRING | UNICODE_STRING_LITERAL) QUOTED_STRING* (UESCAPE QUOTED_STRING)?
	  ;

dateTimeLiteral : 
    LBRACE_D QUOTED_STRING RBRACE
    | LBRACE_T QUOTED_STRING RBRACE
    | LBRACE_TS QUOTED_STRING RBRACE
    | DATE QUOTED_STRING
    | TIME QUOTED_STRING
    | TIMESTAMP QUOTED_STRING
	  ;

multisetConstructor : MULTISET (LPAREN leafQueryOrExpr RPAREN | LBRACKET expression (COMMA expression)* RBRACKET)  ;

arrayConstructor : ARRAY (LPAREN leafQueryOrExpr RPAREN | LBRACKET expressionCommaList? RBRACKET)  ;

mapConstructor : MAP (LPAREN leafQueryOrExpr RPAREN | LBRACKET expressionCommaList? RBRACKET)  ;

periodConstructor : PERIOD LPAREN expression COMMA expression RPAREN  ;

intervalLiteral : INTERVAL (MINUS | PLUS)? QUOTED_STRING intervalQualifier  ;

intervalQualifier : 
    YEAR (LPAREN unsignedIntLiteral RPAREN)? (TO MONTH)?
    | MONTH (LPAREN unsignedIntLiteral RPAREN)?
    | DAY (LPAREN unsignedIntLiteral RPAREN)? (TO (HOUR | MINUTE | SECOND (LPAREN unsignedIntLiteral RPAREN)?))?
    | HOUR (LPAREN unsignedIntLiteral RPAREN)? (TO (MINUTE | SECOND (LPAREN unsignedIntLiteral RPAREN)?))?
    | MINUTE (LPAREN unsignedIntLiteral RPAREN)? (TO SECOND (LPAREN unsignedIntLiteral RPAREN)?)?
    | SECOND (LPAREN unsignedIntLiteral (COMMA unsignedIntLiteral)? RPAREN)?
	  ;

timeUnit : 
    SECOND
    | MINUTE
    | HOUR
    | DAY
    | DOW
    | DOY
    | WEEK
    | MONTH
    | QUARTER
    | YEAR
    | EPOCH
    | DECADE
    | CENTURY
    | MILLENNIUM
	  ;

timestampInterval : 
    FRAC_SECOND
    | MICROSECOND
    | SQL_TSI_FRAC_SECOND
    | SQL_TSI_MICROSECOND
    | SECOND
    | SQL_TSI_SECOND
    | MINUTE
    | SQL_TSI_MINUTE
    | HOUR
    | SQL_TSI_HOUR
    | DAY
    | SQL_TSI_DAY
    | WEEK
    | SQL_TSI_WEEK
    | MONTH
    | SQL_TSI_MONTH
    | QUARTER
    | SQL_TSI_QUARTER
    | YEAR
    | SQL_TSI_YEAR
	  ;

dynamicParam : HOOK  ;

identifierSegment : 
    IDENTIFIER
    | QUOTED_IDENTIFIER
    | BACK_QUOTED_IDENTIFIER
    | BRACKET_QUOTED_IDENTIFIER
    | UNICODE_QUOTED_IDENTIFIER (UESCAPE QUOTED_STRING)?
    | nonReservedKeyWord
	  ;

identifier : identifierSegment  ;

simpleIdentifier : identifierSegment  ;

simpleIdentifierCommaList : simpleIdentifier (COMMA simpleIdentifier)*  ;

parenthesizedSimpleIdentifierList : LPAREN simpleIdentifierCommaList RPAREN  ;

simpleIdentifierOrList : 
    simpleIdentifier
    | parenthesizedSimpleIdentifierList
	  ;

compoundIdentifier : simpleIdentifier (DOT (simpleIdentifier | STAR) | LBRACKET unsignedIntLiteral RBRACKET)*  ;

compoundIdentifierTypeCommaList : compoundIdentifierType (COMMA compoundIdentifierType)*  ;

parenthesizedCompoundIdentifierList : LPAREN compoundIdentifierTypeCommaList RPAREN  ;

newSpecification : NEW namedRoutineCall  ;

unsignedIntLiteral : UNSIGNED_INTEGER_LITERAL  ;

intLiteral : 
    (UNSIGNED_INTEGER_LITERAL | PLUS UNSIGNED_INTEGER_LITERAL)
    | MINUS UNSIGNED_INTEGER_LITERAL
	  ;

dataType : typeName collectionsTypeName?  ;

typeName : 
    dremioRowTypeName
    | arrayTypeName
    | sqlTypeName
    | rowTypeName
    | compoundIdentifier
	  ;

sqlTypeName : 
    sqlTypeName1
    | sqlTypeName2
    | sqlTypeName3
    | characterTypeName
    | dateTimeTypeName
	  ;

sqlTypeName1 : 
    GEOMETRY
    | BOOLEAN
    | (INTEGER | INT)
    | TINYINT
    | SMALLINT
    | BIGINT
    | REAL
    | FLOAT
	  ;

sqlTypeName2 : (BINARY VARYING? | VARBINARY) precisionOpt  ;

sqlTypeName3 : 
    ((DECIMAL | DEC | NUMERIC) | ANY) (LPAREN unsignedIntLiteral (COMMA unsignedIntLiteral)? RPAREN)?
    | DOUBLE PRECISION?
	  ;

jdbcOdbcDataTypeName : 
    (SQL_CHAR | CHAR)
    | (SQL_VARCHAR | VARCHAR)
    | (SQL_DATE | DATE)
    | (SQL_TIME | TIME)
    | (SQL_TIMESTAMP | TIMESTAMP)
    | (SQL_DECIMAL | DECIMAL)
    | (SQL_NUMERIC | NUMERIC)
    | (SQL_BOOLEAN | BOOLEAN)
    | (SQL_INTEGER | INTEGER)
    | (SQL_BINARY | BINARY)
    | (SQL_VARBINARY | VARBINARY)
    | (SQL_TINYINT | TINYINT)
    | (SQL_SMALLINT | SMALLINT)
    | (SQL_BIGINT | BIGINT)
    | (SQL_REAL | REAL)
    | (SQL_DOUBLE | DOUBLE)
    | (SQL_FLOAT | FLOAT)
    | SQL_INTERVAL_YEAR
    | SQL_INTERVAL_YEAR_TO_MONTH
    | SQL_INTERVAL_MONTH
    | SQL_INTERVAL_DAY
    | SQL_INTERVAL_DAY_TO_HOUR
    | SQL_INTERVAL_DAY_TO_MINUTE
    | SQL_INTERVAL_DAY_TO_SECOND
    | SQL_INTERVAL_HOUR
    | SQL_INTERVAL_HOUR_TO_MINUTE
    | SQL_INTERVAL_HOUR_TO_SECOND
    | SQL_INTERVAL_MINUTE
    | SQL_INTERVAL_MINUTE_TO_SECOND
    | SQL_INTERVAL_SECOND
	  ;

jdbcOdbcDataType : jdbcOdbcDataTypeName  ;

collectionsTypeName : 
    MULTISET
    | ARRAY
	  ;

nullableOptDefaultTrue : (NULL | NOT NULL)?  ;

nullableOptDefaultFalse : (NULL | NOT NULL)?  ;

fieldNameTypeCommaList : simpleIdentifier dataType nullableOptDefaultFalse (COMMA simpleIdentifier dataType nullableOptDefaultFalse)*  ;

rowTypeName : ROW LPAREN fieldNameTypeCommaList RPAREN  ;

characterTypeName : ((CHARACTER | CHAR) VARYING? | VARCHAR) precisionOpt (CHARACTER SET identifier)?  ;

dateTimeTypeName : 
    DATE
    | TIME precisionOpt timeZoneOpt
    | TIMESTAMP precisionOpt timeZoneOpt
	  ;

precisionOpt : (LPAREN unsignedIntLiteral RPAREN)?  ;

timeZoneOpt : (WITHOUT TIME ZONE | WITH LOCAL TIME ZONE)?  ;

cursorExpression : CURSOR expression  ;

builtinFunctionCall : 
    CAST LPAREN expression AS (dataType | INTERVAL intervalQualifier) RPAREN
    | EXTRACT LPAREN (MICROSECOND | timeUnit) FROM expression RPAREN
    | POSITION LPAREN atomicRowExpression IN expression (FROM expression)? RPAREN
    | CONVERT LPAREN expression USING simpleIdentifier RPAREN
    | TRANSLATE LPAREN expression (USING simpleIdentifier RPAREN | (COMMA expression)* RPAREN)
    | OVERLAY LPAREN expression PLACING expression FROM expression (FOR expression)? RPAREN
    | FLOOR floorCeilOptions
    | (CEIL | CEILING) floorCeilOptions
    | SUBSTRING LPAREN expression (FROM | COMMA) expression ((FOR | COMMA) expression)? RPAREN
    | TRIM LPAREN ((BOTH | TRAILING | LEADING)? expression? (FROM | RPAREN))? expression RPAREN
    | timestampAddFunctionCall
    | timestampDiffFunctionCall
    | extendedBuiltinFunctionCall
    | matchRecognizeFunctionCall
	  ;

timestampAddFunctionCall : TIMESTAMPADD LPAREN timestampInterval COMMA expression COMMA expression RPAREN  ;

timestampDiffFunctionCall : TIMESTAMPDIFF LPAREN timestampInterval COMMA expression COMMA expression RPAREN  ;

matchRecognizeFunctionCall : 
    CLASSIFIER LPAREN RPAREN
    | MATCH_NUMBER LPAREN RPAREN
    | matchRecognizeNavigationLogical
    | matchRecognizeNavigationPhysical
    | matchRecognizeCallWithModifier
	  ;

matchRecognizeCallWithModifier : (RUNNING | FINAL) namedFunctionCall  ;

matchRecognizeNavigationLogical : (RUNNING | FINAL)? (FIRST | LAST) LPAREN expression (COMMA numericLiteral)? RPAREN  ;

matchRecognizeNavigationPhysical : (PREV | NEXT) LPAREN expression (COMMA numericLiteral)? RPAREN  ;

namedFunctionCall : SPECIFIC? functionName (LPAREN STAR RPAREN | LPAREN RPAREN | functionParameterList) (WITHIN GROUP LPAREN orderBy RPAREN)? (FILTER LPAREN WHERE expression RPAREN)? (OVER (simpleIdentifier | windowSpecification))?  ;

standardFloorCeilOptions : LPAREN expression (TO timeUnit)? RPAREN (OVER (simpleIdentifier | windowSpecification))?  ;

nonReservedJdbcFunctionName : SUBSTRING  ;

functionName : 
    compoundIdentifier
    | reservedFunctionName
	  ;

reservedFunctionName : 
    ABS
    | AVG
    | CARDINALITY
    | CEILING
    | CHAR_LENGTH
    | CHARACTER_LENGTH
    | COALESCE
    | COLLECT
    | COVAR_POP
    | COVAR_SAMP
    | CUME_DIST
    | COUNT
    | CURRENT_DATE
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | DENSE_RANK
    | ELEMENT
    | EXP
    | FIRST_VALUE
    | FLOOR
    | FUSION
    | GROUPING
    | HOUR
    | LAG
    | LEAD
    | LEFT
    | LAST_VALUE
    | LN
    | LOCALTIME
    | LOCALTIMESTAMP
    | LOWER
    | MAX
    | MIN
    | MINUTE
    | MOD
    | MONTH
    | NTH_VALUE
    | NTILE
    | NULLIF
    | OCTET_LENGTH
    | PERCENT_RANK
    | PERCENTILE_CONT
    | PERCENTILE_DISC
    | POWER
    | RANK
    | REGR_SXX
    | REGR_SYY
    | RIGHT
    | ROW_NUMBER
    | SECOND
    | SQRT
    | STDDEV_POP
    | STDDEV_SAMP
    | SUM
    | UPPER
    | TRUNCATE
    | USER
    | VAR_POP
    | VAR_SAMP
    | YEAR
	  ;

contextVariable : 
    CURRENT_CATALOG
    | CURRENT_DATE
    | CURRENT_DEFAULT_TRANSFORM_GROUP
    | CURRENT_PATH
    | CURRENT_ROLE
    | CURRENT_SCHEMA
    | CURRENT_TIME
    | CURRENT_TIMESTAMP
    | CURRENT_USER
    | LOCALTIME
    | LOCALTIMESTAMP
    | SESSION_USER
    | SYSTEM_USER
    | USER
	  ;

jdbcFunctionCall : LBRACE_FN (timestampAddFunctionCall | timestampDiffFunctionCall | CONVERT LPAREN expression COMMA jdbcOdbcDataType RPAREN | ((INSERT | LEFT | RIGHT | TRUNCATE) | reservedFunctionName | nonReservedJdbcFunctionName | identifier) (LPAREN STAR RPAREN | LPAREN RPAREN | parenthesizedQueryOrCommaList)) RBRACE  ;

binaryQueryOperator : 
    UNION (ALL | DISTINCT)?
    | INTERSECT (ALL | DISTINCT)?
    | (EXCEPT | SET_MINUS) (ALL | DISTINCT)?
	  ;

binaryMultisetOperator : MULTISET (UNION (ALL | DISTINCT)? | INTERSECT (ALL | DISTINCT)? | EXCEPT (ALL | DISTINCT)?)  ;

binaryRowOperator : 
    EQ
    | GT
    | LT
    | LE
    | GE
    | NE
    | NE2
    | PLUS
    | MINUS
    | STAR
    | SLASH
    | PERCENT_REMAINDER
    | CONCAT
    | AND
    | OR
    | IS DISTINCT FROM
    | IS NOT DISTINCT FROM
    | MEMBER OF
    | SUBMULTISET OF
    | NOT SUBMULTISET OF
    | CONTAINS
    | OVERLAPS
    | EQUALS
    | PRECEDES
    | SUCCEEDS
    | IMMEDIATELY PRECEDES
    | IMMEDIATELY SUCCEEDS
    | binaryMultisetOperator
	  ;

prefixRowOperator : 
    PLUS
    | MINUS
    | NOT
    | EXISTS
	  ;

postfixRowOperator : IS (A SET | NOT (NULL | TRUE | FALSE | UNKNOWN | A SET | EMPTY) | (NULL | TRUE | FALSE | UNKNOWN | EMPTY))  ;

nonReservedKeyWord : 
    nonReservedKeyWord0of3
    | nonReservedKeyWord1of3
    | nonReservedKeyWord2of3
	  ;

nonReservedKeyWord0of3 : 
    A
    | ACCESS
    | ADD
    | AGGREGATE
    | APPLICATION
    | APPROXIMATE
    | ASSIGN
    | ATTRIBUTE
    | AVOID
    | BIN_PACK
    | BREADTH
    | CASCADE
    | CENTURY
    | CHARACTERISTICS
    | CHARACTER_SET_NAME
    | CLEAR
    | COLLATION
    | COLLATION_SCHEMA
    | COLUMN_NAME
    | COMMIT
    | CONFIGURE
    | CONSOLIDATED
    | CONSTRAINT_NAME
    | CONTINUE
    | DATABASE
    | DATETIME_INTERVAL_CODE
    | DEFAULTS
    | DEFINED
    | DEPTH
    | DESCRIPTION
    | DIMENSIONS
    | DISPLAY
    | DOW
    | DYNAMIC_FUNCTION_CODE
    | EPOCH
    | EXCLUDING
    | FINAL
    | FOLLOWING
    | FORTRAN
    | FUNCTIONS
    | GENERATED
    | GOTO
    | HASH
    | IMMEDIATE
    | INCLUDING
    | INPUT
    | INTO
    | JAVA
    | K
    | KEY_TYPE
    | LAYOUT
    | LENGTH
    | LIST
    | LOCATOR
    | MAINTAIN
    | MAP
    | MATERIALIZATION
    | MEASURES
    | MESSAGE_OCTET_LENGTH
    | MILLENNIUM
    | MIN_INPUT_FILES
    | MONITOR
    | NAME
    | NORMALIZED
    | NUMBER
    | OCTETS
    | OPTIONS
    | ORG
    | OVERRIDING
    | PARAMETER_MODE
    | PARAMETER_SPECIFIC_CATALOG
    | PARTIAL
    | PASSWORD
    | PDS
    | PLI
    | PRESERVE
    | PROJECT
    | PUBLIC
    | QUEUE
    | REFERENCE
    | RELATIVE
    | RESTART
    | RETURNED_LENGTH
    | REWRITE
    | ROUTE
    | ROUTINE_NAME
    | SCALE
    | SCOPE_CATALOGS
    | SECTION
    | SEQUENCE
    | SERVER_NAME
    | SIMPLE
    | SNAPSHOT
    | SPACE
    | SQL_BINARY
    | SQL_BOOLEAN
    | SQL_DATE
    | SQL_FLOAT
    | SQL_INTERVAL_DAY_TO_HOUR
    | SQL_INTERVAL_HOUR
    | SQL_INTERVAL_MINUTE
    | SQL_INTERVAL_SECOND
    | SQL_LONGVARBINARY
    | SQL_NCHAR
    | SQL_NVARCHAR
    | SQL_TIME
    | SQL_TSI_DAY
    | SQL_TSI_MICROSECOND
    | SQL_TSI_QUARTER
    | SQL_TSI_YEAR
    | STATE
    | STATUS
    | STRUCT
    | SUBCLASS_ORIGIN
    | TAG
    | TEMPORARY
    | TIMESTAMPDIFF
    | TRANSACTION
    | TRANSACTIONS_ROLLED_BACK
    | TRIGGER_CATALOG
    | TYPE
    | UNDER
    | USAGE
    | USER_DEFINED_TYPE_NAME
    | VERSION
    | WEEK
    | WRITE
    | ZONE
	  ;

nonReservedKeyWord1of3 : 
    ABSOLUTE
    | ACTION
    | ADMIN
    | ALTER
    | APPLY
    | ARROW
    | ASSERTION
    | ATTRIBUTES
    | BEFORE
    | BRANCH
    | C
    | CATALOG
    | CHAIN
    | CHARACTERS
    | CHARACTER_SET_SCHEMA
    | CLOUD
    | COLLATION_CATALOG
    | COLUMN
    | COMMAND_FUNCTION
    | COMMITTED
    | CONNECTION
    | CONSTRAINTS
    | CONSTRAINT_SCHEMA
    | CURSOR_NAME
    | DATASET
    | DATETIME_INTERVAL_PRECISION
    | DEFERRABLE
    | DEFINER
    | DERIVED
    | DESCRIPTOR
    | DISABLE
    | DISTRIBUTE
    | DOY
    | ENABLE
    | EXCEPTION
    | EXECUTE
    | FIRST
    | FORCE
    | FOUND
    | G
    | GEOMETRY
    | GRANTED
    | HIERARCHY
    | IMMEDIATELY
    | INCREMENT
    | INSTANCE
    | INVOKER
    | JOB
    | KEY
    | LABEL
    | LAZY
    | LEVEL
    | LOCALSORT
    | LOGS
    | MANAGE
    | MASKING
    | MAXVALUE
    | MERGE
    | MESSAGE_TEXT
    | MINVALUE
    | MISSING
    | MORE_
    | NAMES
    | NULLABLE
    | OAUTH
    | OPERATE
    | ORDERING
    | OTHERS
    | OWNERSHIP
    | PARAMETER_NAME
    | PARAMETER_SPECIFIC_NAME
    | PASCAL
    | PAST
    | PLACING
    | POLICY
    | PRIOR
    | PROMOTION
    | QUARTER
    | RAW
    | REFLECTION
    | REPEATABLE
    | RESTRICT
    | RETURNED_OCTET_LENGTH
    | ROLE
    | ROUTINE
    | ROUTINE_SCHEMA
    | SCHEMA
    | SCOPE_NAME
    | SECURITY
    | SERIALIZABLE
    | SESSION
    | SINGLE
    | SORT
    | SPECIFIC_NAME
    | SQL_BIT
    | SQL_CHAR
    | SQL_DECIMAL
    | SQL_INTEGER
    | SQL_INTERVAL_DAY_TO_MINUTE
    | SQL_INTERVAL_HOUR_TO_MINUTE
    | SQL_INTERVAL_MINUTE_TO_SECOND
    | SQL_INTERVAL_YEAR
    | SQL_LONGVARCHAR
    | SQL_NCLOB
    | SQL_REAL
    | SQL_TIMESTAMP
    | SQL_TSI_FRAC_SECOND
    | SQL_TSI_MINUTE
    | SQL_TSI_SECOND
    | SQL_VARBINARY
    | STATEMENT
    | STORE
    | STRUCTURE
    | SUBSTITUTE
    | TAGS
    | TIES
    | TOP_LEVEL_COUNT
    | TRANSACTIONS_ACTIVE
    | TRANSFORM
    | TRIGGER_NAME
    | UNBOUNDED
    | UNNAMED
    | USER_DEFINED_TYPE_CATALOG
    | USER_DEFINED_TYPE_SCHEMA
    | VIEW
    | WORK
    | WRITER
	  ;

nonReservedKeyWord2of3 : 
    ACCELERATION
    | ADA
    | AFTER
    | ALWAYS
    | APPROX
    | ASC
    | ASSIGNMENT
    | AUTO
    | BERNOULLI
    | BRANCHES
    | CACHE
    | CATALOG_NAME
    | CHANGE
    | CHARACTER_SET_CATALOG
    | CLASS_ORIGIN
    | COBOL
    | COLLATION_NAME
    | COLUMNS
    | COMMAND_FUNCTION_CODE
    | CONDITION_NUMBER
    | CONNECTION_NAME
    | CONSTRAINT_CATALOG
    | CONSTRUCTOR
    | DATA
    | DATASETS
    | DECADE
    | DEFERRED
    | DEGREE
    | DESC
    | DIAGNOSTICS
    | DISPATCH
    | DOMAIN
    | DYNAMIC_FUNCTION
    | ENGINE
    | EXCLUDE
    | FIELD
    | FOLDER
    | FORGET
    | FRAC_SECOND
    | GENERAL
    | GO
    | GRANTS
    | HISTORY
    | IMPLEMENTATION
    | INITIALLY
    | INSTANTIABLE
    | ISOLATION
    | JSON
    | KEY_MEMBER
    | LAST
    | LEARNING
    | LIBRARY
    | LOCATION
    | M
    | MANIFESTS
    | MATCHED
    | MAX_FILE_SIZE_MB
    | MESSAGE_LENGTH
    | MICROSECOND
    | MIN_FILE_SIZE_MB
    | MODIFY
    | MUMPS
    | NESTING
    | NULLS
    | OBJECT
    | OPTION
    | ORDINALITY
    | OUTPUT
    | PAD
    | PARAMETER_ORDINAL_POSITION
    | PARAMETER_SPECIFIC_SCHEMA
    | PASSTHROUGH
    | PATH
    | PLAN
    | PRECEDING
    | PRIVILEGES
    | PROVIDER
    | QUERY
    | READ
    | REFLECTIONS
    | REPLACE
    | RETURNED_CARDINALITY
    | RETURNED_SQLSTATE
    | ROUNDROBIN
    | ROUTINE_CATALOG
    | ROW_COUNT
    | SCHEMA_NAME
    | SCOPE_SCHEMA
    | SELF
    | SERVER
    | SETS
    | SIZE
    | SOURCE
    | SQL_BIGINT
    | SQL_BLOB
    | SQL_CLOB
    | SQL_DOUBLE
    | SQL_INTERVAL_DAY
    | SQL_INTERVAL_DAY_TO_SECOND
    | SQL_INTERVAL_HOUR_TO_SECOND
    | SQL_INTERVAL_MONTH
    | SQL_INTERVAL_YEAR_TO_MONTH
    | SQL_LONGVARNCHAR
    | SQL_NUMERIC
    | SQL_SMALLINT
    | SQL_TINYINT
    | SQL_TSI_HOUR
    | SQL_TSI_MONTH
    | SQL_TSI_WEEK
    | SQL_VARCHAR
    | STATS
    | STRIPED
    | STYLE
    | TABLE_NAME
    | TARGET_FILE_SIZE_MB
    | TIMESTAMPADD
    | TOKENS
    | TRANSACTIONS_COMMITTED
    | TRANSFORMS
    | TRIGGER_SCHEMA
    | UNCOMMITTED
    | UPLOAD
    | USER_DEFINED_TYPE_CODE
    | VDS
    | VIEWS
    | WRAPPER
    | XML
	  ;

unusedExtension : ZONE  ;
