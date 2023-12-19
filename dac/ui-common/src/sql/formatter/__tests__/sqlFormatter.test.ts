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
/* eslint-disable */
import { RuleName } from "../abstractSqlGenerator";
import { formatQuery } from "../sqlFormatter";

describe("sqlFormatter", () => {
  type TestCase = [input: string, expectedIfDifferent?: string];
  const parserRuleTests: { [K in RuleName]: TestCase[] | null } = {
    // Commands
    sqlQueryEof: null, // covered by other cases
    sqlStmtList: [
      [
        `
SELECT *
FROM   tbl1;

INSERT INTO tbl1 (col1)
VALUES      (a)`,
      ],
    ],
    sqlStmt: null, // covered by other cases
    sqlStmtEof: null, // covered by other cases
    sqlShowTables: [
      [
        `
SHOW TABLES AT TAG abc123
FROM               def456
LIKE               '%vendor%'`,
      ],
    ],
    sqlShowViews: [
      [
        `
SHOW VIEWS AT BRANCH abc123
FROM                 def456
LIKE                 '%vendor%'`,
      ],
    ],
    sqlShowFiles: [
      [
        `
SHOW FILES IN abc123.def456`,
      ],
    ],
    sqlShowSchemas: [
      [
        `
SHOW SCHEMAS LIKE '%vendor%'`,
      ],
    ],
    sqlDescribeTable: [
      [
        `
DESCRIBE abc123.def456 tbl`,
      ],
    ],
    sqlUseSchema: [
      [
        `
USE abc123.def456`,
      ],
    ],
    sqlCreateOrReplace: [
      [
        `
CREATE OR REPLACE FUNCTION IF NOT EXISTS func(x int, y int)
RETURNS                                  int
RETURN SELECT                            x * y;`,
      ],
      [
        `
CREATE FUNCTION func(x int, y int)
RETURNS TABLE   (x int NULL,
                 y int NULL)
RETURN          (1,
                 1);`,
      ],
      [
        `
CREATE VDS        space.v1 (
                    col1 MASKING POLICY protect_ssn(col1, col2),
                    col2
                  ) AT BRANCH branch1
ROW ACCESS POLICY filterRows(col3)
AS                (SELECT "abc",
                          "def"
                   FROM   tbl)`,
      ],
    ],
    sqlDropFunction: [
      [
        `
DROP FUNCTION IF EXISTS abc123.def456`,
      ],
    ],
    sqlDescribeFunction: [
      [
        `
DESC FUNCTION abc123.def456`,
      ],
    ],
    sqlShowFunctions: [
      [
        `
SHOW FUNCTIONS LIKE '%vendor%'`,
      ],
    ],
    sqlDropView: [
      [
        `
DROP VIEW IF EXISTS abc123.def456 AT REFERENCE ref1`,
      ],
    ],
    sqlCreateTable: [
      [
        `
CREATE TABLE IF NOT EXISTS           "@user".tbl1 (abc MASKING POLICY policy(abc)) AT BRANCH branch1
ROUNDROBIN PARTITION BY              (col1,
                                      col2)
DISTRIBUTE BY                        (col3)
LOCALSORT BY                         (col4)
LOCATION                             'loc'
STORE AS                             (arg0)
WITH SINGLE WRITER ROW ACCESS POLICY filterRows(col5)
AS                                   (SELECT 1);`,
      ],
    ],
    sqlInsertTable: [
      [
        `
INSERT INTO tbl1 (
              col1,
              col2)
VALUES      (1,
             2)`,
      ],
      [
        `
INSERT INTO tbl1 AT BRANCH branch1 (
              col1,
              col2)
VALUES      (1,
             2)`,
      ],
    ],
    sqlDeleteFromTable: [
      [
        `
DELETE FROM  abc123.def456 AT BRANCH branch1 AS tbl
USING        tbl
NATURAL JOIN tbl2
ON           tbl.col1 = tbl2.col1
WHERE        tbl.col2 > tbl2.col2`,
      ],
    ],
    sqlUpdateTable: [
      [
        `
UPDATE tbl AT REF ref1
SET    col1 = (col1 + 1),
       col2 = (col2 - 1)
FROM   tbl
WHERE  col3 != 0`,
      ],
    ],
    sqlMergeIntoTable: [
      [
        `
MERGE INTO                   tbl AT REF ref1 AS target
USING                        "source"
ON                           "source".id = target.id
WHEN MATCHED THEN UPDATE SET "name" = "source"."name",
                             category = "source".category
WHEN NOT MATCHED THEN INSERT (id,
                              "name",
                              category)
VALUES                       ("source".id,
                              "source"."name",
                              "source".category)`,
      ],
    ],
    sqlDropTable: [
      [
        `
DROP TABLE IF EXISTS abc123.def456 AT BRANCH branch1`,
      ],
    ],
    sqlTruncateTable: [
      [
        `
TRUNCATE TABLE abc123.def456 AT BRANCH branch1`,
      ],
    ],
    sqlRefreshReflection: [
      [
        `
REFRESH REFLECTION 'reflectionBlah' AS 'rfl'`,
      ],
    ],
    sqlLoadMaterialization: [
      [
        `
LOAD MATERIALIZATION METADATA blah;`,
      ],
    ],
    sqlCompactMaterialization: [
      [
        `
COMPACT MATERIALIZATION abc.def AS 'mtr'`,
      ],
    ],
    sqlAnalyzeTableStatistics: [
      [
        `
ANALYZE TABLE      abc123.def456
FOR COLUMNS        (col1)
COMPUTE STATISTICS`,
      ],
      [
        `
ANALYZE TABLE                      abc123.def456
FOR ALL COLUMNS COMPUTE STATISTICS`,
      ],
    ],
    sqlRefreshDataset: [
      [
        `
REFRESH DATASET space1.vds1
FOR FILES       ('file1')
FORCE UPDATE   `,
      ],
    ],
    sqlAccel: [
      [
        `
ALTER SOURCE   s3
REFRESH STATUS`,
      ],
      [
        `
ALTER SPACE                myspace
ROUTE REFLECTIONS TO QUEUE runner1`,
      ],
      [
        `
ALTER TABLE     granite AT BRANCH branch1
ADD PRIMARY KEY (llave)`,
      ],
      [
        `
ALTER DATASET granite
CHANGE COLUMN pillar
SET           a = 1`,
      ],
      [
        `
ALTER VIEW             vds1
ENABLE SCHEMA LEARNING`,
      ],
    ],
    sqlCreateAggReflection: [
      [
        `
ALTER VIEW                  vv
CREATE AGGREGATE REFLECTION arf
USING DIMENSIONS            (dim1 BY DAY,
                             dim2)
MEASURES                    (me1 (
                               SUM,
                               MIN,
                               MAX))
DISTRIBUTE BY               (db1,
                             db2)
ARROW CACHE                `,
      ],
    ],
    sqlCreateRawReflection: [
      [
        `
ALTER VIEW            vv AT BRANCH branch1
CREATE RAW REFLECTION rrf
USING DISPLAY         (dim1,
                       dim2)
DISTRIBUTE BY         (db1,
                       db2)`,
      ],
    ],
    sqlAddExternalReflection: [
      [
        `
ALTER VIEW                 vv AT BRANCH branch1
CREATE EXTERNAL REFLECTION erf
USING                      a.b.c`,
      ],
    ],
    sqlAlterDatasetReflectionRouting: null, // covered by other cases
    sqlAlterClearPlanCache: [
      [
        `
ALTER SESSION SET abc = '123'`,
      ],
    ],
    sqlExplainJson: [
      [
        `
EXPLAIN JSON abc
FOR SELECT   *
FROM         tbl`,
      ],
    ],
    sqlGrant: [
      [
        `
GRANT OWNERSHIP ON TABLE tbl
TO USER                  usr`,
      ],
      [
        `
GRANT   SELECT,
        ALTER,
        MODIFY
ON VDS  view1
TO ROLE role1`,
      ],
    ],
    sqlGrantPrivilege: null, // covered by other cases
    parseGranteeType: null, // covered by other cases
    sqlRevoke: [
      [
        `
REVOKE    ALL
ON SOURCE s3
FROM USER hacker`,
      ],
      [
        `
REVOKE    WRITE
ON TABLE  tbl AT BRANCH branch1
FROM USER hacker`,
      ],
    ],
    sqlGrantOwnership: null, // covered by other cases
    sqlGrantRole: null, // covered by other cases
    sqlCreateRole: [
      [
        `
CREATE ROLE role1`,
      ],
    ],
    sqlRevokeRole: [
      [
        `
REVOKE ROLE role1
FROM USER   user1`,
      ],
    ],
    sqlDropRole: [
      [
        `
DROP ROLE role1`,
      ],
    ],
    sqlCreateUser: [
      [
        `
CREATE USER  user1
SET PASSWORD 'letmein'`,
      ],
    ],
    sqlDropUser: [
      [
        `
DROP USER user1`,
      ],
    ],
    sqlAlterUser: [
      [
        `
ALTER USER     user1
UNSET PASSWORD`,
      ],
    ],
    sqlUseVersion: [
      [
        `
USE COMMIT abc123
IN         def456`,
      ],
    ],
    sqlShowBranches: [
      [
        `
SHOW BRANCHES IN abc`,
      ],
    ],
    sqlShowTags: [
      [
        `
SHOW TAGS IN abc`,
      ],
    ],
    sqlShowLogs: [
      [
        `
SHOW LOGS AT TAG tag
IN               abc`,
      ],
      [
        `
SHOW LOG AT TAG tag
IN              abc`,
      ],
    ],
    sqlCreateBranch: [
      [
        `
CREATE BRANCH branch1 AT REFERENCE ref1`,
      ],
      [
        `
CREATE BRANCH branch2
FROM BRANCH   branch1`,
      ],
    ],
    sqlCreateTag: [
      [
        `
CREATE TAG IF NOT EXISTS tag1 AT BRANCH branch1`,
      ],
      [
        `
CREATE TAG tag2
FROM TAG   tag1`,
      ],
    ],
    sqlDropBranch: [
      [
        `
DROP BRANCH branch1 AT COMMIT abc123`,
      ],
      [
        `
DROP BRANCH branch1`,
      ],
    ],
    sqlDropTag: [
      [
        `
DROP TAG tag1
FORCE IN abc`,
      ],
    ],
    sqlMergeBranch: [
      [
        `
MERGE BRANCH branch1
INTO         branch2`,
      ],
    ],
    sqlAssignBranch: [
      [
        `
ALTER BRANCH branch1
ASSIGN TAG   tag1
IN           abc`,
      ],
    ],
    sqlAssignTag: [
      [
        `
ALTER TAG  tag1
ASSIGN REF ref1`,
      ],
    ],
    sqlSelect: [
      [
        `
SELECT STREAM DISTINCT *
FROM                   tbl
WHERE                  col1 > 10
GROUP BY               (),
                       ROLLUP (col2),
                       ROW (
                         col1,
                         col2),
                       col1
HAVING                 COUNT(col1) > 0
WINDOW                 col2 AS (
                         abc
                         ORDER BY col3)
QUALIFY                col2 = 0`,
      ],
    ],
    sqlExplainQueryDML: [
      [
        `
EXPLAIN PLAN EXCLUDING ATTRIBUTES WITH TYPE AS JSON FOR (SELECT 1);`,
      ],
    ],
    sqlQueryOrDml: null, // covered by other cases
    sqlDescribe: [
      [
        `
DESCRIBE CATALOG cat1`,
      ],
      [
        `
DESCRIBE STATEMENT (SELECT col1
                    FROM   tbl1)`,
      ],
    ],
    sqlProcedureCall: [
      [
        `
CALL abc.funcA(arg)`,
      ],
    ],
    natural: null, // covered by other cases
    joinType: null, // covered by other cases
    joinTable: [
      [
        `
SELECT          *
FROM            tbl1
FULL OUTER JOIN tbl2
ON              tbl1.id = tbl2.id`,
      ],
      [
        `
SELECT     *
FROM       tbl1
INNER JOIN tbl2
USING      (id)`,
      ],
      [
        `
SELECT      *
FROM        tbl1
CROSS APPLY tbl2`,
      ],
      [
        `
SELECT    *
FROM      tbl1 AT BRANCH branch1
LEFT JOIN tbl2 AT BRANCH branch1
ON        tbl1.id = tbl2.id`,
      ],
    ],
    explicitTable: null, // covered by other cases
    tableConstructor: null, // covered by other cases
    whereOpt: null, // covered by other cases
    groupByOpt: null, // covered by other cases
    havingOpt: [
      [
        `
SELECT *
FROM   tbl
HAVING col1 > 10`,
      ],
    ],
    windowOpt: null, // covered by other cases
    windowSpecification: [
      [
        `
SELECT *
FROM   tbl
WINDOW id AS (
         abc
         PARTITION BY  (col1,
                        col2)
         ORDER BY      col3
         ROWS BETWEEN  1 PRECEDING AND
                       2 FOLLOWING
         ALLOW PARTIAL)`,
      ],
    ],
    orderBy: null, // covered by other cases
    pivot: [
      [
        `
SELECT Cost AS Cost_Sorted_By_Production_Days
FROM   (SELECT DaysToManufacture,
               StandardCost
        FROM   Production.Product)
PIVOT  (AVG(StandardCost)
        FOR DaysToManufacture
        IN  (WeekDays)
       ) AS PivotTable`,
      ],
    ],
    unpivot: [
      [
        `
SELECT                Cost AS Cost_Sorted_By_Production_Days
FROM                  (SELECT DaysToManufacture,
                              StandardCost
                       FROM   Production.Product)
UNPIVOT INCLUDE NULLS (StandardCost
                       FOR DaysToManufacture
                       IN  (WeekDays,
                            Months)
                      ) AS UnpivotTable`,
      ],
    ],
    matchRecognizeOpt: [
      [
        `
SELECT          *
FROM            stock_price_history
MATCH_RECOGNIZE (PARTITION BY                               company
                 ORDER BY                                   price_date
                 MEASURES                                   match_number() as "match_number",
                                                            first(price_date) as start_date,
                                                            last(price_date) as end_date,
                                                            count(*) as rows_in_sequence,
                                                            count(row_with_price_decrease.*) as num_decreases,
                                                            count(row_with_price_increase.*) as num_increases
                 ONE ROW PER MATCH AFTER MATCH SKIP TO LAST row_with_price_increase
                 PATTERN                                    (row_before_decrease row_with_price_decrease + row_with_price_increase +)
                 DEFINE                                     row_with_price_decrease as price < lag(price),
                                                            row_with_price_increase as price > lag(price))
ORDER BY        company,
                "match_number"`,
      ],
    ],
    sqlExpressionEof: null, // covered by other cases
    sqlSetOption: null, // covered by other cases
    sqlAlter: [
      [
        `
ALTER SESSION RESET ALL`,
      ],
    ],
    sqlTypeName: null, // covered by other cases
    withList: [
      [
        `
WITH   temporaryTable (averageValue) AS (
         SELECT avg(Attr1)
         FROM   tbl)
SELECT Attr1
FROM   tbl,
       temporaryTable
WHERE  tbl.Attr1 > temporaryTable.averageValue;`,
      ],
    ],
    sqlRollbackTable: [
      [
        `
ROLLBACK TABLE tbl
TO SNAPSHOT    'abc123'`,
      ],
    ],
    sqlVacuum: [
      [
        `
VACUUM TABLE     tbl
EXPIRE SNAPSHOTS RETAIN_LAST = 10`,
      ],
      [
        `
VACUUM CATALOG cat1`,
      ],
    ],
    sqlOptimize: null,
    orderByLimitOpt: [
      [
        `
SELECT *
FROM   tbl
LIMIT  5,
       10`,
      ],
      [
        `
SELECT *
FROM   tbl
LIMIT  5 OFFSET 2 ROWS FETCH FIRST 1 ROW ONLY`,
      ],
    ],
    sqlCopyInto: [
      [
        `
COPY INTO myspace.tbl
FROM      'abc'
FILES     ('file1',
           'file2'
          ) (
            DATE_FORMAT 'mmddyyyy',
            NULL_IF (
              'abc',
              'abc'))`,
      ],
    ],
    dremioWhenMatchedClause: null, // covered by other cases
    dremioWhenNotMatchedClause: null, // covered by other cases
    explainDetailLevel: null, // covered by other cases
    explainDepth: null, // covered by other cases
    scope: null, // covered by other cases

    // Identifiers
    stringLiteral: null, // covered by other cases
    identifierSegment: null, // covered by other cases
    identifier: null, // covered by other cases
    simpleIdentifier: null, // covered by other cases
    compoundIdentifier: null, // covered by other cases
    nonReservedKeyWord: null, // covered by other cases
    nonReservedKeyWord0of3: null, // covered by other cases
    nonReservedKeyWord1of3: null, // covered by other cases
    nonReservedKeyWord2of3: null, // covered by other cases

    // Functions
    tableFunctionCall: [
      [
        `
SELECT *
FROM   table (specific func(arg0, arg1))`,
      ],
    ],
    extendedBuiltinFunctionCall: null, // covered by other cases
    builtinFunctionCall: [
      [
        `
SELECT trim(leading '...' FROM '...abc')`,
      ],
    ],
    timestampAddFunctionCall: [
      [
        `
SELECT timestampadd(month, 2, '2009-05-18')`,
      ],
    ],
    timestampDiffFunctionCall: [
      [
        `
SELECT timestampdiff(month, '2009-05-18', '2009-07-29');`,
      ],
    ],
    matchRecognizeFunctionCall: null, // covered by other cases
    namedFunctionCall: [[`SELECT func(arg0, arg1)`]],
    jdbcFunctionCall: [[`SELECT { fn funcy(*) }`]],

    // Other
    extendedTableRef: null, // covered by other cases
    floorCeilOptions: null, // covered by other cases
    orderedQueryOrExpr: null, // covered by other cases
    leafQuery: null, // covered by other cases
    parenthesizedExpression: null, // covered by other cases
    parenthesizedQueryOrCommaList: null, // covered by other cases
    parenthesizedQueryOrCommaListWithDefault: null, // covered by other cases
    functionParameterList: [
      [
        `
SELECT func(DISTINCT arg0, arg1)`,
      ],
    ],
    arg0: null, // covered by other cases
    arg: null, // covered by other cases
    default: null, // covered by other cases
    parseOptionalFieldList: null, // covered by other cases
    parseOptionalFieldListWithMasking: null, // covered by other cases
    parseRequiredFieldList: null, // covered by other cases
    parseRequiredFieldListWithMasking: null, // covered by other cases
    columnNamesWithMasking: null, // covered by other cases
    parseFunctionFieldList: null, // covered by other cases
    functionKeyValuePair: null, // covered by other cases
    fieldFunctionTypeCommaList: null, // covered by other cases
    tableElementListWithMasking: null, // covered by other cases
    tableElementList: null, // covered by other cases
    tableElementWithMasking: null, // covered by other cases
    tableElement: null, // covered by other cases
    parsePartitionTransform: null, // covered by other cases
    parsePartitionTransformList: null, // covered by other cases
    nullableOptDefaultTrue: null, // covered by other cases
    fieldNameStructTypeCommaList: null, // covered by other cases
    rowTypeName: null, // covered by other cases
    arrayTypeName: null, // covered by other cases
    parseRequiredFilesList: null, // covered by other cases
    stringLiteralCommaList: null, // covered by other cases
    parseRequiredPartitionList: null, // covered by other cases
    keyValueCommaList: null, // covered by other cases
    keyValuePair: null, // covered by other cases
    parseFieldListWithGranularity: null, // covered by other cases
    simpleIdentifierCommaListWithGranularity: null, // covered by other cases
    parseFieldListWithMeasures: null, // covered by other cases
    simpleIdentifierCommaListWithMeasures: null, // covered by other cases
    measureList: null, // covered by other cases
    policy: null, // covered by other cases
    policyWithoutArgs: null, // covered by other cases
    parseColumns: null, // covered by other cases
    identifierCommaList: null, // covered by other cases
    typedElement: null, // covered by other cases
    conjunction: null, // covered by other cases
    modifiers: null, // covered by other cases
    luceneQuery: [
      [
        `
SELECT location,
       type
FROM   elasticsearch."schema1"."table1"
WHERE  contains((location: San\ Francisco AND NOT age: (>=10 AND <20) OR age:>99) || *: [1000 TO *] || *: {0, 10} OR subject: *impact* | *service* | appointment OR name: /.*john.*/i OR action: jump~0.8 OR recent: true^10 OR state: (+green +"very beautiful") -rocky)`,
      ],
    ],
    query: null, // covered by other cases
    clause: null, // covered by other cases
    term: null, // covered by other cases
    privilegeCommaList: null, // covered by other cases
    privilege: null, // covered by other cases
    tableWithVersionContext: null, // covered by other cases
    namedRoutineCall: null, // covered by other cases
    whenMatchedClause: null, // covered by other cases
    whenNotMatchedClause: null, // covered by other cases
    selectList: null, // covered by other cases
    selectItem: null, // covered by other cases
    selectExpression: null, // covered by other cases
    fromClause: null, // covered by other cases
    tableRef: null, // covered by other cases
    tableRef2: null, // covered by other cases
    extendList: null, // covered by other cases
    columnType: null, // covered by other cases
    compoundIdentifierType: null, // covered by other cases
    rowConstructorList: null, // covered by other cases
    rowConstructor: null, // covered by other cases
    groupingElementList: null, // covered by other cases
    groupingElement: null, // covered by other cases
    expressionCommaList: null, // covered by other cases
    expressionCommaList2: null, // covered by other cases
    windowRange: null, // covered by other cases
    orderItem: [
      [
        `
SELECT   *
FROM     tbl
ORDER BY tbl.col ASC NULLS FIRST`,
      ],
    ],
    pivotAgg: null, // covered by other cases
    pivotValue: null, // covered by other cases
    unpivotValue: null, // covered by other cases
    measureColumnCommaList: null, // covered by other cases
    measureColumn: null, // covered by other cases
    patternExpression: null, // covered by other cases
    patternTerm: null, // covered by other cases
    patternFactor: null, // covered by other cases
    patternPrimary: null, // covered by other cases
    subsetDefinitionCommaList: null,
    subsetDefinition: null, // covered by other cases
    patternDefinitionCommaList: null, // covered by other cases
    patternDefinition: null, // covered by other cases
    queryOrExpr: null, // covered by other cases
    withItem: null, // covered by other cases
    leafQueryOrExpr: null, // covered by other cases
    expression: null, // covered by other cases
    expression2b: null, // covered by other cases
    expression2: null, // covered by other cases
    comp: null, // covered by other cases
    expression3: null, // covered by other cases
    periodOperator: null, // covered by other cases
    collateClause: null, // covered by other cases
    unsignedNumericLiteralOrParam: null, // covered by other cases
    atomicRowExpression: null, // covered by other cases
    caseExpression: [
      [
        `
SELECT (CASE WHEN a > 10 THEN "high" WHEN a > 5 THEN "medium" ELSE "low" END)
FROM   tbl`,
      ],
    ],
    sequenceExpression: null, // covered by other cases
    literal: null, // covered by other cases
    unsignedNumericLiteral: null, // covered by other cases
    numericLiteral: null, // covered by other cases
    specialLiteral: null, // covered by other cases
    dateTimeLiteral: null, // covered by other cases
    multisetConstructor: null, // covered by other cases
    arrayConstructor: null, // covered by other cases
    mapConstructor: null, // covered by other cases
    periodConstructor: null, // covered by other cases
    intervalLiteral: null, // covered by other cases
    intervalQualifier: null, // covered by other cases
    timeUnit: null, // covered by other cases
    timestampInterval: null, // covered by other cases
    dynamicParam: null, // covered by other cases
    simpleIdentifierCommaList: null, // covered by other cases
    parenthesizedSimpleIdentifierList: null, // covered by other cases
    simpleIdentifierOrList: null, // covered by other cases
    compoundIdentifierTypeCommaList: null, // covered by other cases
    parenthesizedCompoundIdentifierList: null, // covered by other cases
    newSpecification: null, // covered by other cases
    unsignedIntLiteral: null, // covered by other cases
    intLiteral: null, // covered by other cases
    dataType: null, // covered by other cases
    typeName: null, // covered by other cases
    jdbcOdbcDataTypeName: null, // covered by other cases
    jdbcOdbcDataType: null, // covered by other cases
    collectionsTypeName: null, // covered by other cases
    cursorExpression: null, // covered by other cases
    matchRecognizeCallWithModifier: null, // covered by other cases
    matchRecognizeNavigationLogical: null, // covered by other cases
    matchRecognizeNavigationPhysical: null, // covered by other cases
    standardFloorCeilOptions: null, // covered by other cases
    nonReservedJdbcFunctionName: null, // covered by other cases
    functionName: null, // covered by other cases
    reservedFunctionName: null, // covered by other cases
    contextVariable: null, // covered by other cases
    binaryQueryOperator: null, // covered by other cases
    binaryMultisetOperator: null, // covered by other cases
    binaryRowOperator: null, // covered by other cases
    prefixRowOperator: null, // covered by other cases
    postfixRowOperator: null, // covered by other cases
    unusedExtension: null, // covered by other cases
    tableOverOpt: null, // covered by other cases
    sqlSelectKeywords: null, // covered by other cases
    sqlInsertKeywords: null, // covered by other cases
    exprOrJoinOrOrderedQuery: [
      [
        `
SELECT *
FROM   tbl
LIMIT  10;`,
      ],
      [
        `
SELECT           col1
FROM             tbl1
LEFT JOIN        tbl2
ON               tbl1.col2 = tbl2.col2
WHERE            tbl1.col2 = 2
UNION ALL SELECT col3
FROM             tbl3`,
      ],
    ],
    parseFunctionReturnFieldList: null, // covered by other cases
    functionReturnTypeCommaList: null, // covered by other cases
    returnKeyValuePair: null, // covered by other cases
    parseCopyIntoOptions: null, // covered by other cases
    parseOptimizeOptions: null, // covered by other cases
    dremioRowTypeName: null, // covered by other cases
    sqlDropReflection: null, // covered by other cases
    dQuery: null, // covered by other cases
    join: null, // covered by other cases
    tableRef1: null, // covered by other cases
    tableRef3: null, // covered by other cases
    tablesample: null, // covered by other cases
    addSetOpQuery: null, // covered by other cases
    sqlTypeName1: null, // covered by other cases
    sqlTypeName2: null, // covered by other cases
    sqlTypeName3: null, // covered by other cases
    nullableOptDefaultFalse: null, // covered by other cases
    fieldNameTypeCommaList: null, // covered by other cases
    characterTypeName: null, // covered by other cases
    dateTimeTypeName: null, // covered by other cases
    precisionOpt: null, // covered by other cases
    timeZoneOpt: null, // covered by other cases
    parseTableProperty: null, // covered by other cases
    vacuumTableExpireSnapshotOptions: null, // covered by other cases
    vacuumTableRemoveOrphanFilesOptions: null, // covered by other cases
    sqlVacuumTable: null, // covered by other cases
    sqlVacuumCatalog: null, // covered by other cases
    sqlQueryOrTableDml: null, // covered by other cases
    sqlCreateFolder: [
      [
        `
CREATE FOLDER folder1 AT BRANCH branch1`,
      ],
    ],
    sqlDropFolder: [
      [
        `
DROP FOLDER folder1 AT BRANCH branch1`,
      ],
    ],
    sqlShowCreate: [
      [
        `
SHOW CREATE TABLE abc123.def456 AT BRANCH branch1`,
      ],
    ],
    sqlShowTableProperties: [
      [
        `
SHOW TBLPROPERTIES space1.tbl1`,
      ],
    ],
    sqlCreatePipe: [
      [
        `
CREATE PIPE                  pipe1
NOTIFICATION_PROVIDER        provider1
NOTIFICATION_QUEUE_REFERENCE ref1 AS
COPY INTO                    myspace.tbl
FROM                         'abc'
FILES                        ('file1',
                              'file2'
                             ) (
                               DATE_FORMAT 'mmddyyyy',
                               NULL_IF (
                                 'abc',
                                 'abc'))`,
      ],
    ],
    sqlDescribePipe: [
      [
        `
DESCRIBE PIPE pipe1`,
      ],
    ],
    sqlShowPipes: [
      [
        `
SHOW PIPES`,
      ],
    ],

    parseReferenceType: null, // covered by other cases
    aTVersionSpec: null, // covered by other cases
    aTBranchVersionOrReferenceSpec: null, // covered by other cases
    parenthesizedKeyValueOptionCommaList: null, // covered by other cases
    keyValueOption: null, // covered by other cases
    commaSepatatedSqlHints: null, // covered by other cases
    tableRefWithHintsOpt: null, // covered by other cases
    qualifyOpt: null, // covered by other cases
    jsonRepresentation: null, // covered by other cases
    jsonInputClause: null, // covered by other cases
    jsonReturningClause: null, // covered by other cases
    jsonOutputClause: null, // covered by other cases
    jsonValueExpression: null, // covered by other cases
    jsonPathSpec: null, // covered by other cases
    jsonApiCommonSyntax: null, // covered by other cases
    jsonExistsErrorBehavior: null, // covered by other cases
    jsonExistsFunctionCall: [
      [
        `
SELECT tbl.col1
FROM   tbl
WHERE  json_exists(tbl.col1, '$.Field1.Field2?(@.Field3 == $v1)' PASSING 'val1' AS "v1");`,
      ],
    ],
    jsonValueEmptyOrErrorBehavior: null, // covered by other cases
    jsonValueFunctionCall: [
      [
        `
SELECT json_value(col1, '$.Field1' RETURNING NUMBER ERROR ON ERROR)
FROM   tbl;`,
      ],
    ],
    jsonQueryEmptyOrErrorBehavior: null, // covered by other cases
    jsonQueryWrapperBehavior: null, // covered by other cases
    jsonQueryFunctionCall: [
      [
        `
SELECT json_query(col1, col2 WITH ARRAY WRAPPER EMPTY OBJECT ON ERROR)
FROM   tbl;`,
      ],
    ],
    jsonName: null, // covered by other cases
    jsonNameAndValue: null, // covered by other cases
    jsonConstructorNullClause: null, // covered by other cases
    jsonObjectFunctionCall: [
      [
        `
SELECT json_object(KEY '$.Field1' VALUE 'val1', '$.Field2' VALUE 'val2', '$.Field3' : 'val3' ABSENT ON NULL);`,
      ],
    ],
    jsonObjectAggFunctionCall: [
      [
        `
SELECT json_objectagg('$.Field1' : col1 NULL ON NULL)
FROM   tbl;`,
      ],
    ],
    jsonArrayFunctionCall: [
      [
        `
SELECT json_array('abc', 'def' NULL ON NULL);`,
      ],
    ],
    jsonArrayAggFunctionCall: [
      [
        `
SELECT json_arrayagg(col1 ABSENT ON NULL)
FROM   tbl;`,
      ],
    ],
  };

  test.each(
    (Object.keys(parserRuleTests) as RuleName[]).filter(
      (ruleName) => !!parserRuleTests[ruleName]
    )
  )("formatQuery should correctly format rule %s", (ruleName: RuleName) => {
    const testCases: TestCase[] = parserRuleTests[ruleName]!;
    for (const testCase of testCases) {
      const [input, expectedIfDifferent] = testCase;
      const expected = (expectedIfDifferent || input).replace("\n", "");
      const actual = formatQuery(input);
      if (expected != actual) {
        console.error(
          "Formatted query does not match expected. Actual:\n" + actual
        );
      }
      expect(formatQuery(input)).toEqual(expected);
    }
  });

  describe("error tests", () => {
    beforeAll(() => {
      jest.spyOn(console, "error").mockImplementation(() => {});
    });

    afterAll(() => {
      (console.error as jest.Mock).mockRestore();
    });

    afterEach(() => {
      (console.error as jest.Mock).mockClear();
    });

    it("should throw an error for lexing errors", () => {
      const input = "$][abc8^";
      expect(() => formatQuery(input)).toThrow();
    });

    it("should throw an exception for parsing errors", () => {
      const input = "SELECT * FROM VIEW JOB HISTORY";
      expect(() => formatQuery(input)).toThrow();
    });

    it("should not trivially succeed", () => {
      const input = "SELECT * FROM tbl";
      expect(formatQuery(input)).not.toEqual(input);
    });
  });

  describe("whitebox tests", () => {
    describe("identifiers", () => {
      it("should separate two sequential disparate identifiers", () => {
        const input = "SELECT col1 col";
        expect(formatQuery(input)).toEqual(input);
      });

      it("should not separate compound identifier parts", () => {
        const input = `
SELECT *
FROM   space1.table1`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });
    });

    describe("functions", () => {
      it("should not newline separate args, should not add unnecessary whitespace", () => {
        const input = "SELECT avg   (  arg0  ,  arg1  )";
        const expected = "SELECT avg(arg0, arg1)";
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });
    });

    describe("grammar", () => {
      it("should handle parenthesized statement", () => {
        const input = `
(SELECT 1)`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle non-parenthesized statement", () => {
        const input = `
SELECT 1`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle multiple statements", () => {
        const input = `
SELECT 1;;;;

SELECT 2`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle noncommand then parenthesized", () => {
        const input = `
INSERT INTO table_name (
              column1,
              column2)
VALUES      (value1,
             value2)`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle parenthesized then noncommand", () => {
        const input = `
SELECT (SELECT *
        FROM   tbl
       ) AS sel`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle conjunctions", () => {
        const input = `
SELECT *
FROM   tbl
WHERE  col1 = 0 AND
       col2 = 0 AND
       (col3 = 1 OR
        col4 = 1) AND
       col5 = 2`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle nested parens", () => {
        const input = `
INSERT INTO table_name (
              column1,
              column2,
              column3)
VALUES      (value1,
             ((1 + 1) + 3),
             8)`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should handle subqueries", () => {
        const input = `
DELETE FROM orders
WHERE       EXISTS (
              SELECT 1
              FROM   "returns"
              WHERE  (order_id = orders.order_id AND
                      order_total > 100 AND
                      (order_buyer LIKE '%john%' OR
                       order_buyer LIKE '%frank%')))`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });
    });

    describe("comments", () => {
      it("should preserve commented out list element positions", () => {
        const input = `
SELECT col1,
       /* col2, */
       col3
FROM   tbl`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve single line commented out list element positions", () => {
        const input = `
SELECT col1,
       -- col2,
       col3
FROM   tbl`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve trailing list elem comments", () => {
        const input = `
SELECT col1, /* we need this! */
       col2
FROM   tbl`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve trailing list elem single line comments", () => {
        const input = `
SELECT col1, -- we need this!
       col2
FROM   tbl`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve trailing param comments", () => {
        const input = `
SELECT col1
FROM   tbl /* my favorite table */
WHERE  col2 > 10`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve trailing param single line comments", () => {
        const input = `
SELECT col1
FROM   tbl -- my favorite table
WHERE  col2 > 10`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve trailing comments", () => {
        const input = `
SELECT col1
FROM   tbl /* my favorite table */`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve trailing comment on newline", () => {
        const input = `
SELECT col1
FROM   tbl
/* WHERE col2 > 10 */`;
        const expected = `
SELECT col1
FROM   tbl
       /* WHERE col2 > 10 */`; // future improvement: pull to left
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve trailing single line comment on newline", () => {
        const input = `
SELECT col1
FROM   tbl
-- WHERE col2 > 10`;
        const expected = `
SELECT col1
FROM   tbl
       -- WHERE col2 > 10`; // future improvement: pull to left
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve leading comment on newline", () => {
        const input = `
/* hello world */
SELECT col1
FROM   tbl`;
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve leading single line comments", () => {
        const input = `
-- This is my cool query
-- Hello world
DELETE FROM
tbl1
WHERE
col1 = null`;
        const expected = `
-- This is my cool query
-- Hello world
DELETE FROM tbl1
WHERE       col1 = null`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should pull leading comment to newline", () => {
        const input = "/* hello world */ SELECT col1 FROM tbl";
        const expected = `
/* hello world */
SELECT col1
FROM   tbl`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve comments within command", () => {
        const input = "DELETE /* hi */ FROM tbl1 WHERE col1 = null";
        const expected = `
DELETE /* hi */
FROM   tbl1
WHERE  col1 = null`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve single line comments within command", () => {
        const input = `
DELETE --test
FROM
tbl1
WHERE
col1 = null`;
        const expected = `
DELETE --test
FROM   tbl1
WHERE  col1 = null`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve comments after command", () => {
        const input = "DELETE FROM /* hi */ tbl1 WHERE col1 = null";
        const expected = `
DELETE FROM /* hi */
            tbl1
WHERE       col1 = null`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve single line comments after command", () => {
        const input = `
DELETE FROM --test
--helloworld
tbl1
WHERE
col1 = null`;
        const expected = `
DELETE FROM --test
            --helloworld
            tbl1
WHERE       col1 = null`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });

      it("should preserve trailing single line comment", () => {
        const input = `
SELECT col1
FROM   tbl --test123`; // future improvement: pull to left
        expect(formatQuery(input)).toEqual(input.replace("\n", ""));
      });

      it("should preserve mixed comments", () => {
        const input = `
-- hello
-- world
/* hello world */
SELECT col1 /* bye */ /* bye */ -- forever
/* clockman */
FROM tbl --test123 /* my favorite */
-- finito`;
        const expected = `
-- hello
-- world
/* hello world */
SELECT col1 /* bye */ /* bye */ -- forever
       /* clockman */
FROM   tbl --test123 /* my favorite */
       -- finito`;
        expect(formatQuery(input)).toEqual(expected.replace("\n", ""));
      });
    });
  });
});
