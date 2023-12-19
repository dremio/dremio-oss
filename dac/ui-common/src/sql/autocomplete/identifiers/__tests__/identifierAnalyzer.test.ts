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

import {
  getAutocompleteParseTree,
  prepareQuery,
} from "../../__tests__/__utils__/testUtils";
import { TokenSuggestions } from "../../engine/TokenSuggestions";
import {
  type AnalyzedIdentifier,
  analyzeIdentifier,
  IdentifierType,
} from "../identifierAnalyzer";

describe("identifierAnalyzer", () => {
  describe("analyzeIdentifier", () => {
    const runTest = (
      queryText: string,
      expected: AnalyzedIdentifier | undefined
    ): void => {
      const [query, caretPosition] = prepareQuery(queryText);
      const { parseTree, parser } = getAutocompleteParseTree(query);
      const suggestions = new TokenSuggestions(
        parseTree,
        parser,
        caretPosition,
        []
      ).get();
      const analyzedIdentifier = suggestions.identifiers
        ? analyzeIdentifier(suggestions.identifiers, suggestions.cursorInfo)
        : undefined;
      expect(analyzedIdentifier).toEqual(expected);
    };

    const COLUMN = (compoundAllowed?: boolean): AnalyzedIdentifier => ({
      type: IdentifierType.COLUMN,
      compoundAllowed: compoundAllowed ?? true,
    });
    const FOLDER = (): AnalyzedIdentifier => ({ type: IdentifierType.FOLDER });
    const GENERIC = (): AnalyzedIdentifier => ({
      type: IdentifierType.GENERIC_CONTAINER,
    });
    const SOURCE = (): AnalyzedIdentifier => ({ type: IdentifierType.SOURCE });
    const SPACE = (): AnalyzedIdentifier => ({ type: IdentifierType.SPACE });
    const TABLE = (): AnalyzedIdentifier => ({ type: IdentifierType.TABLE });
    const UDF = (): AnalyzedIdentifier => ({ type: IdentifierType.UDF });

    describe("columns", () => {
      it("SELECT ^ FROM tbl", () => runTest("SELECT ^ FROM tbl", COLUMN()));

      it('SELECT "^" FROM tbl', () => runTest('SELECT "^" FROM tbl', COLUMN()));

      it("SELECT * FROM tbl ORDER BY ^", () =>
        runTest("SELECT * FROM tbl ORDER BY ^", COLUMN()));

      it("SELECT * FROM tbl ORDER BY colA, ^", () =>
        runTest("SELECT * FROM tbl ORDER BY colA, ^", COLUMN()));

      it("SELECT col ^ FROM tbl", () =>
        runTest("SELECT col ^ FROM tbl", undefined));

      it("SELECT col AS ^ FROM tbl", () =>
        runTest("SELECT col AS ^ FROM tbl", undefined));

      it("SELECT tbl.^ FROM tbl", () =>
        runTest("SELECT tbl.^ FROM tbl", COLUMN()));

      it("SELECT col1, ^ FROM tbl", () =>
        runTest("SELECT col1, ^ FROM tbl", COLUMN()));

      it("SELECT func(^) FROM tbl", () =>
        runTest("SELECT func(^) FROM tbl", COLUMN()));

      // column prefix is reserved keyword
      it("SELECT TABLE^ FROM tbl", () =>
        runTest("SELECT TABLE^ FROM tbl", COLUMN()));

      it("SELECT FROM tbl JOIN tbl2 ON ^", () =>
        runTest("SELECT FROM tbl JOIN tbl2 ON ^", COLUMN()));

      it("SELECT FROM tbl JOIN tbl2 ON col = ^", () =>
        runTest("SELECT FROM tbl JOIN tbl2 ON col = ^", COLUMN()));

      it("SELECT FROM tbl MATCH_RECOGNIZE (PARTITION BY ^)", () =>
        runTest("SELECT FROM tbl MATCH_RECOGNIZE (PARTITION BY ^)", COLUMN()));

      it("SELECT FROM tbl MATCH_RECOGNIZE (ORDER BY ^)", () =>
        runTest("SELECT FROM tbl MATCH_RECOGNIZE (ORDER BY ^)", COLUMN()));

      it("SELECT FROM tbl MATCH_RECOGNIZE (MEASURES ^)", () =>
        runTest("SELECT FROM tbl MATCH_RECOGNIZE (MEASURES ^)", COLUMN()));

      it("SELECT FROM tbl MATCH_RECOGNIZE (MEASURES col AS ^)", () =>
        runTest(
          "SELECT FROM tbl MATCH_RECOGNIZE (MEASURES col AS ^)",
          undefined
        ));

      it("SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (^))", () =>
        runTest("SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (^))", undefined));

      it("SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) SUBSET ^)", () =>
        runTest(
          "SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) SUBSET ^)",
          undefined
        ));

      it("SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) SUBSET S1 = (^))", () =>
        runTest(
          "SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) SUBSET S1 = (^))",
          undefined
        ));

      it("SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) DEFINE ^)", () =>
        runTest(
          "SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) DEFINE ^)",
          undefined
        ));

      it("SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) DEFINE term AS ^)", () =>
        runTest(
          "SELECT FROM tbl MATCH_RECOGNIZE (PATTERN (term) DEFINE term AS ^)",
          COLUMN()
        ));

      it("DESCRIBE tbl ^", () => runTest("DESCRIBE tbl ^)", COLUMN()));

      it("DESCRIBE TABLE tbl ^", () =>
        runTest("DESCRIBE TABLE tbl ^)", COLUMN(false)));

      it("CREATE OR REPLACE VDS view1 ROW ACCESS POLICY func1(^)", () =>
        runTest(
          "CREATE OR REPLACE VDS view1 ROW ACCESS POLICY func1(^)",
          COLUMN(false)
        ));

      it("CREATE TABLE tbl (^)", () =>
        runTest("CREATE TABLE tbl (^)", undefined));

      it("SELECT * FROM tbl PIVOT(^)", () =>
        runTest("SELECT * FROM tbl PIVOT(^)", undefined));

      it("SELECT * FROM tbl PIVOT(SUM(^))", () =>
        runTest("SELECT * FROM tbl PIVOT(SUM(^))", COLUMN()));

      it("SELECT * FROM tbl PIVOT(SUM(colA) FOR ^)", () =>
        runTest("SELECT * FROM tbl PIVOT(SUM(colA) FOR ^)", COLUMN(false)));

      it("SELECT * FROM tbl PIVOT(SUM(colA) FOR colB IN (^))", () =>
        runTest(
          "SELECT * FROM tbl PIVOT(SUM(colA) FOR colB IN (^))",
          undefined
        ));

      it("VALUES(^)", () => runTest("VALUES(^)", undefined));

      it("SELECT * FROM tbl UNPIVOT(^)", () =>
        runTest("SELECT * FROM tbl UNPIVOT(^)", undefined));

      it("SELECT * FROM tbl UNPIVOT(colA FOR ^)", () =>
        runTest("SELECT * FROM tbl UNPIVOT(colA FOR ^)", undefined));

      it("SELECT * FROM tbl UNPIVOT(colA FOR colB IN (^))", () =>
        runTest(
          "SELECT * FROM tbl UNPIVOT(colA FOR colB IN (^))",
          COLUMN(false)
        ));

      it("SELECT CAST(^)", () => runTest("SELECT CAST(^)", COLUMN()));

      it("SELECT CAST(1 AS ^)", () =>
        runTest("SELECT CAST(1 AS ^)", undefined));

      it("SELECT EXTRACT(SECOND FROM ^)", () =>
        runTest("SELECT EXTRACT(SECOND FROM ^)", COLUMN()));

      it("SELECT POSITION(^)", () => runTest("SELECT POSITION(^)", COLUMN()));

      it("SELECT POSITION(colA IN ^)", () =>
        runTest("SELECT POSITION(colA IN ^)", COLUMN()));

      it("SELECT POSITION(colA IN colB FROM ^)", () =>
        runTest("SELECT POSITION(colA IN colB FROM ^)", COLUMN()));

      it("SELECT FLOOR(^)", () => runTest("SELECT FLOOR(^)", COLUMN()));

      it("SELECT CONVERT(colA USING ^)", () =>
        runTest("SELECT CONVERT(colA USING ^)", COLUMN(false)));

      it("INSERT INTO tbl (^)", () =>
        runTest("INSERT INTO tbl (^)", COLUMN(false)));

      it("SELECT * FROM tbl WHERE ABS(^)", () =>
        runTest("SELECT * FROM tbl WHERE ABS(^)", COLUMN()));

      it("DELETE FROM tbl WHERE ^", () =>
        runTest("DELETE FROM tbl WHERE ^", COLUMN()));

      it("UPDATE tbl SET ^", () => runTest("UPDATE tbl SET ^", COLUMN(false)));

      it("UPDATE tbl ^", () => runTest("UPDATE tbl ^", undefined));

      it("UPDATE tbl AS ^", () => runTest("UPDATE tbl AS ^", undefined));

      it("UPDATE tbl SET colA = ^", () =>
        runTest("UPDATE tbl SET colA = ^", COLUMN()));

      it("UPDATE tbl SET colA = colB WHERE ^", () =>
        runTest("UPDATE tbl SET colA = colB WHERE ^", COLUMN()));

      it("ANALYZE TABLE tbl FOR COLUMNS (^)", () =>
        runTest("ANALYZE TABLE tbl FOR COLUMNS (^)", COLUMN(false)));

      it("REFRESH DATASET tbl FOR PARTITIONS (^)", () =>
        runTest("REFRESH DATASET tbl FOR PARTITIONS (^)", COLUMN(false)));

      it("ALTER TABLE pds1 ADD ROW ACCESS POLICY policy1 (^)", () =>
        runTest(
          "ALTER TABLE pds1 ADD ROW ACCESS POLICY policy1 (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 DROP ROW ACCESS POLICY policy1 (^)", () =>
        runTest(
          "ALTER VDS vds1 DROP ROW ACCESS POLICY policy1 (^)",
          COLUMN(false)
        ));

      it("ALTER PDS pds1 ADD PRIMARY KEY (^)", () =>
        runTest("ALTER PDS pds1 ADD PRIMARY KEY (^)", COLUMN(false)));

      it("ALTER PDS pds1 ADD COLUMNS (^)", () =>
        runTest("ALTER PDS pds1 ADD COLUMNS (^)", undefined));

      it("ALTER TABLE pds1 ADD PARTITION FIELD ^", () =>
        runTest("ALTER TABLE pds1 ADD PARTITION FIELD ^", COLUMN(false)));

      it("ALTER DATASET pds1 CHANGE ^", () =>
        runTest("ALTER DATASET pds1 CHANGE ^", COLUMN(false)));

      it("ALTER DATASET pds1 ALTER ^", () =>
        runTest("ALTER DATASET pds1 ALTER ^", COLUMN(false)));

      it("ALTER DATASET pds1 MODIFY ^", () =>
        runTest("ALTER DATASET pds1 MODIFY ^", COLUMN(false)));

      it("ALTER DATASET pds1 MODIFY COLUMN ^", () =>
        runTest("ALTER DATASET pds1 MODIFY COLUMN ^", COLUMN(false)));

      it("ALTER DATASET pds1 MODIFY COLUMN colA ^", () =>
        runTest("ALTER DATASET pds1 MODIFY COLUMN colA ^", undefined));

      it("ALTER VIEW vds1 CHANGE colA SET MASKING POLICY policy1 (^)", () =>
        runTest(
          "ALTER VIEW vds1 CHANGE colA SET MASKING POLICY policy1 (^)",
          COLUMN(false)
        ));

      it("ALTER DATASET vds1 CHANGE colA SET MASKING POLICY policy1 (^)", () =>
        runTest(
          "ALTER DATASET vds1 CHANGE colA SET MASKING POLICY policy1 (^)",
          COLUMN(false)
        ));

      it("ALTER DATASET vds1 CHANGE colA SET ^", () =>
        runTest("ALTER DATASET vds1 CHANGE colA SET ^", undefined));

      it("ALTER DATASET vds1 CHANGE colA SET opt = ^", () =>
        runTest("ALTER DATASET vds1 CHANGE colA SET opt = ^", undefined));

      it("ALTER TABLE pds1 DROP PARTITION FIELD ^", () =>
        runTest("ALTER TABLE pds1 DROP PARTITION FIELD ^", COLUMN(false)));

      it("ALTER TABLE pds1 DROP COLUMN ^", () =>
        runTest("ALTER TABLE pds1 DROP COLUMN ^", COLUMN(false)));

      it("ALTER VDS vds1 REFRESH METADATA FOR PARTITIONS (^)", () =>
        runTest(
          "ALTER VDS vds1 REFRESH METADATA FOR PARTITIONS (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (colB) DISTRIBUTE BY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (colB) DISTRIBUTE BY (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (colB) PARTITION BY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (colB) PARTITION BY (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (colB) LOCALSORT BY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE AGGREGATE REFLECTION refl1 USING DIMENSIONS (colA) MEASURES (colB) LOCALSORT BY (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY DISTRIBUTE BY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY DISTRIBUTE BY (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY PARTITION BY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY PARTITION BY (^)",
          COLUMN(false)
        ));

      it("ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY LOCALSORT BY (^)", () =>
        runTest(
          "ALTER VDS vds1 CREATE RAW REFLECTION refl1 USING DISPLAY LOCALSORT BY (^)",
          COLUMN(false)
        ));

      it("SELECT * FROM tbl WHERE ^", () =>
        runTest("SELECT * FROM tbl WHERE ^", COLUMN()));

      it("SELECT * FROM tbl WHERE colA = ^", () =>
        runTest("SELECT * FROM tbl WHERE colA = ^", COLUMN()));

      it("SELECT * FROM tbl GROUP BY ^", () =>
        runTest("SELECT * FROM tbl GROUP BY ^", COLUMN()));

      it("SELECT * FROM tbl GROUP BY colA, ^", () =>
        runTest("SELECT * FROM tbl GROUP BY colA, ^", COLUMN()));

      it("SELECT * FROM tbl HAVING ^", () =>
        runTest("SELECT * FROM tbl HAVING ^", COLUMN()));

      it("SELECT * FROM tbl WINDOW ^", () =>
        runTest("SELECT * FROM tbl WINDOW ^", undefined));

      it("SELECT * FROM tbl WINDOW win1 AS (PARTITION BY ^)", () =>
        runTest("SELECT * FROM tbl WINDOW win1 AS (PARTITION BY ^)", COLUMN()));

      it("SELECT * FROM tbl WINDOW win1 AS (ORDER BY ^)", () =>
        runTest("SELECT * FROM tbl WINDOW win1 AS (ORDER BY ^)", COLUMN()));

      it("SELECT * FROM tbl WINDOW win1 AS (ROWS BETWEEN ^)", () =>
        runTest(
          "SELECT * FROM tbl WINDOW win1 AS (ROWS BETWEEN ^)",
          undefined
        ));

      it("SELECT * FROM tbl JOIN tbl2 USING (^)", () =>
        runTest("SELECT * FROM tbl JOIN tbl2 USING (^)", COLUMN(false)));

      it("WITH ^", () => runTest("WITH ^", undefined));

      it("SELECT { fn ^ }", () => runTest("SELECT { fn ^ }", undefined));

      it("SELECT { fn func1(^) }", () =>
        runTest("SELECT { fn func1(^) }", COLUMN()));

      it("DELETE FROM tbl WHERE ^", () =>
        runTest("DELETE FROM tbl WHERE ^", COLUMN()));

      it("UPDATE tbl SET ^", () => runTest("UPDATE tbl SET ^", COLUMN(false)));

      it("UPDATE tbl SET colA = 1 WHERE ^", () =>
        runTest("UPDATE tbl SET colA = 1 WHERE ^", COLUMN()));

      it("MERGE INTO tbl USING tbl2 ON ^", () =>
        runTest("MERGE INTO tbl USING tbl2 ON ^", COLUMN()));

      it("MERGE INTO tbl USING tbl2 ON colA = 1 WHEN MATCHED THEN UPDATE SET ^", () =>
        runTest(
          "MERGE INTO tbl USING tbl2 ON colA = 1 WHEN MATCHED THEN UPDATE SET ^",
          COLUMN(false)
        ));

      it("MERGE INTO tbl USING tbl2 ON colA = 1 WHEN NOT MATCHED THEN INSERT (^)", () =>
        runTest(
          "MERGE INTO tbl USING tbl2 ON colA = 1 WHEN NOT MATCHED THEN INSERT (^)",
          COLUMN(false)
        ));
    });

    describe("folders", () => {
      it("USE ^", () => runTest("USE ^", FOLDER()));

      it("ALTER FOLDER ^", () => runTest("ALTER FOLDER ^", FOLDER()));

      it("ALTER FOLDER space1.^", () =>
        runTest("ALTER FOLDER space1.^", FOLDER()));

      it("GRANT SELECT ON FOLDER ^ ^", () =>
        runTest("GRANT SELECT ON FOLDER ^", FOLDER()));

      it("GRANT SELECT ON SCHEMA ^", () =>
        runTest("GRANT SELECT ON SCHEMA ^", FOLDER()));

      it("GRANT SELECT ON ALL DATASETS IN FOLDER ^", () =>
        runTest("GRANT SELECT ON ALL DATASETS IN FOLDER ^", FOLDER()));

      it("GRANT SELECT ON ALL DATASETS IN SCHEMA ^", () =>
        runTest("GRANT SELECT ON ALL DATASETS IN SCHEMA ^", FOLDER()));

      it("REVOKE SELECT ON FOLDER ^ ^", () =>
        runTest("REVOKE SELECT ON FOLDER ^", FOLDER()));

      it("REVOKE SELECT ON SCHEMA ^", () =>
        runTest("REVOKE SELECT ON SCHEMA ^", FOLDER()));

      it("REVOKE SELECT ON ALL DATASETS IN FOLDER ^", () =>
        runTest("REVOKE SELECT ON ALL DATASETS IN FOLDER ^", FOLDER()));

      it("REVOKE SELECT ON ALL DATASETS IN SCHEMA ^", () =>
        runTest("REVOKE SELECT ON ALL DATASETS IN SCHEMA ^", FOLDER()));

      it("GRANT OWNERSHIP ON FOLDER ^", () =>
        runTest("GRANT OWNERSHIP ON FOLDER ^", FOLDER()));

      it("GRANT OWNERSHIP ON SCHEMA ^", () =>
        runTest("GRANT OWNERSHIP ON SCHEMA ^", FOLDER()));

      it("GRANT OWNERSHIP ON ALL DATASETS IN FOLDER ^", () =>
        runTest("GRANT OWNERSHIP ON ALL DATASETS IN FOLDER ^", FOLDER()));

      it("GRANT OWNERSHIP ON ALL DATASETS IN SCHEMA ^", () =>
        runTest("GRANT OWNERSHIP ON ALL DATASETS IN SCHEMA ^", FOLDER()));
    });

    describe("generic containers", () => {
      it("SHOW TABLES FROM ^", () => runTest("SHOW TABLES FROM ^", GENERIC()));

      it("SHOW TABLES IN ^", () => runTest("SHOW TABLES IN ^", GENERIC()));

      it("SHOW VIEWS FROM ^", () => runTest("SHOW VIEWS FROM ^", GENERIC()));

      it("SHOW VIEWS IN ^", () => runTest("SHOW VIEWS IN ^", GENERIC()));

      it("SHOW VIEWS IN space1.^", () =>
        runTest("SHOW VIEWS IN space1.^", GENERIC()));
    });

    describe("sources", () => {
      it("USE BRANCH branch1 IN ^", () =>
        runTest("USE BRANCH branch1 IN ^", SOURCE()));

      it("SHOW BRANCHES IN ^", () => runTest("SHOW BRANCHES IN ^", SOURCE()));

      it("SHOW TAGS IN ^", () => runTest("SHOW TAGS IN ^", SOURCE()));

      it("SHOW LOGS IN ^", () => runTest("SHOW LOGS IN ^", SOURCE()));

      it("CREATE BRANCH branch1 IN ^", () =>
        runTest("CREATE BRANCH branch1 IN ^", SOURCE()));

      it("CREATE TAG tag1 IN ^", () =>
        runTest("CREATE TAG tag1 IN ^", SOURCE()));

      it("DROP BRANCH branch1 AT COMMIT ^", () =>
        runTest("DROP BRANCH branch1 AT COMMIT ^", undefined));

      it("DROP BRANCH branch1 FORCE IN ^", () =>
        runTest("DROP BRANCH branch1 FORCE IN ^", SOURCE()));

      it("DROP TAG tag1 AT COMMIT ^", () =>
        runTest("DROP TAG tag1 AT COMMIT ^", undefined));

      it("DROP TAG tag1 FORCE IN ^", () =>
        runTest("DROP TAG tag1 FORCE IN ^", SOURCE()));

      it("MERGE BRANCH branch1 INTO ^", () =>
        runTest("MERGE BRANCH branch1 INTO ^", undefined));

      it("MERGE BRANCH branch1 IN ^", () =>
        runTest("MERGE BRANCH branch1 IN ^", SOURCE()));

      it("ALTER BRANCH branch1 ASSIGN BRANCH branch2 IN ^", () =>
        runTest("ALTER BRANCH branch1 ASSIGN BRANCH branch2 IN ^", SOURCE()));

      it("ALTER TAG tag1 ASSIGN TAG tag2 IN ^", () =>
        runTest("ALTER TAG tag1 ASSIGN TAG tag2 IN ^", SOURCE()));

      it("ALTER SOURCE ^", () => runTest("ALTER SOURCE ^", SOURCE()));

      it("GRANT SELECT ON SOURCE ^", () =>
        runTest("GRANT SELECT ON SOURCE ^", SOURCE()));

      it("GRANT SELECT ON ALL DATASETS IN SOURCE ^", () =>
        runTest("GRANT SELECT ON ALL DATASETS IN SOURCE ^", SOURCE()));

      it("REVOKE SELECT ON SOURCE ^", () =>
        runTest("REVOKE SELECT ON SOURCE ^", SOURCE()));

      it("REVOKE SELECT ON ALL DATASETS IN SOURCE ^", () =>
        runTest("REVOKE SELECT ON ALL DATASETS IN SOURCE ^", SOURCE()));

      it("GRANT OWNERSHIP ON SOURCE ^", () =>
        runTest("GRANT OWNERSHIP ON SOURCE ^", SOURCE()));

      it("REVOKE SELECT ON CATALOG ^", () =>
        runTest("REVOKE SELECT ON CATALOG ^", SOURCE()));

      it("GRANT OWNERSHIP ON CATALOG ^", () =>
        runTest("GRANT OWNERSHIP ON CATALOG ^", SOURCE()));

      it("VACUUM CATALOG ^", () => runTest("VACUUM CATALOG ^", SOURCE()));
    });

    describe("spaces", () => {
      it("ALTER SPACE ^", () => runTest("ALTER SPACE ^", SPACE()));

      it("GRANT SELECT ON SPACE ^", () =>
        runTest("GRANT SELECT ON SPACE ^", SPACE()));

      it("GRANT SELECT ON ALL DATASETS IN SPACE ^", () =>
        runTest("GRANT SELECT ON ALL DATASETS IN SPACE ^", SPACE()));

      it("REVOKE SELECT ON SPACE ^", () =>
        runTest("REVOKE SELECT ON SPACE ^", SPACE()));

      it("REVOKE SELECT ON ALL DATASETS IN SPACE ^", () =>
        runTest("REVOKE SELECT ON ALL DATASETS IN SPACE ^", SPACE()));

      it("GRANT OWNERSHIP ON SPACE ^", () =>
        runTest("GRANT OWNERSHIP ON SPACE ^", SPACE()));
    });

    describe("tables", () => {
      it("DESCRIBE ^", () => runTest("DESCRIBE ^", TABLE()));

      it("DESCRIBE TABLE ^", () => runTest("DESCRIBE TABLE ^", TABLE()));

      it('DESCRIBE TABLE "@home".^', () =>
        runTest('DESCRIBE TABLE "@home".^', TABLE()));

      it("DESC ^", () => runTest("DESC ^", TABLE()));

      it("DROP VIEW ^", () => runTest("DROP VIEW ^", TABLE()));

      it("DROP VDS ^", () => runTest("DROP VDS ^", TABLE()));

      it("DROP VDS IF EXISTS ^", () =>
        runTest("DROP VDS IF EXISTS ^", TABLE()));

      it("GRANT SELECT ON PDS ^", () =>
        runTest("GRANT SELECT ON PDS ^", TABLE()));

      it("GRANT SELECT ON TABLE ^", () =>
        runTest("GRANT SELECT ON TABLE ^", TABLE()));

      it("GRANT SELECT ON VDS ^", () =>
        runTest("GRANT SELECT ON VDS ^", TABLE()));

      it("GRANT SELECT ON VIEW ^", () =>
        runTest("GRANT SELECT ON VIEW ^", TABLE()));

      it("REVOKE SELECT ON PDS ^", () =>
        runTest("REVOKE SELECT ON PDS ^", TABLE()));

      it("REVOKE SELECT ON TABLE ^", () =>
        runTest("REVOKE SELECT ON TABLE ^", TABLE()));

      it("REVOKE SELECT ON VDS ^", () =>
        runTest("REVOKE SELECT ON VDS ^", TABLE()));

      it("REVOKE SELECT ON VIEW ^", () =>
        runTest("REVOKE SELECT ON VIEW ^", TABLE()));

      it("GRANT OWNERSHIP ON PDS ^", () =>
        runTest("GRANT OWNERSHIP ON PDS ^", TABLE()));

      it("GRANT OWNERSHIP ON TABLE ^", () =>
        runTest("GRANT OWNERSHIP ON TABLE ^", TABLE()));

      it("GRANT OWNERSHIP ON VDS ^", () =>
        runTest("GRANT OWNERSHIP ON VDS ^", TABLE()));

      it("GRANT OWNERSHIP ON VIEW ^", () =>
        runTest("GRANT OWNERSHIP ON VIEW ^", TABLE()));

      it("TRUNCATE TABLE ^", () => runTest("TRUNCATE TABLE ^", TABLE()));

      it("TRUNCATE TABLE IF EXISTS ^", () =>
        runTest("TRUNCATE TABLE IF EXISTS ^", TABLE()));

      it("INSERT INTO ^", () => runTest("INSERT INTO ^", TABLE()));

      it("UPDATE ^", () => runTest("UPDATE ^", TABLE()));

      it("UPDATE tbl SET colA = 1 FROM ^", () =>
        runTest("UPDATE tbl SET colA = 1 FROM ^", TABLE()));

      it("MERGE INTO ^", () => runTest("MERGE INTO ^", TABLE()));

      it("MERGE INTO tbl USING ^", () =>
        runTest("MERGE INTO tbl USING ^", TABLE()));

      it("DROP TABLE ^", () => runTest("DROP TABLE ^", TABLE()));

      it("DROP TABLE IF EXISTS ^", () =>
        runTest("DROP TABLE IF EXISTS ^", TABLE()));

      it("ROLLBACK TABLE ^", () => runTest("ROLLBACK TABLE ^", TABLE()));

      it("VACUUM TABLE ^", () => runTest("VACUUM TABLE ^", TABLE()));

      it("TRUNCATE TABLE ^", () => runTest("TRUNCATE TABLE ^", TABLE()));

      it("TRUNCATE TABLE IF EXISTS ^", () =>
        runTest("TRUNCATE TABLE IF EXISTS ^", TABLE()));

      it("ANALYZE TABLE ^", () => runTest("ANALYZE TABLE ^", TABLE()));

      it("REFRESH DATASET ^", () => runTest("REFRESH DATASET ^", TABLE()));

      it("OPTIMIZE TABLE ^", () => runTest("OPTIMIZE TABLE ^", TABLE()));

      it("ALTER TABLE ^", () => runTest("ALTER TABLE ^", TABLE()));

      it("ALTER VDS ^", () => runTest("ALTER VDS ^", TABLE()));

      it("ALTER VIEW ^", () => runTest("ALTER VIEW ^", TABLE()));

      it("ALTER PDS ^", () => runTest("ALTER PDS ^", TABLE()));

      it("ALTER DATASET ^", () => runTest("ALTER DATASET ^", TABLE()));

      it("DELETE FROM ^", () => runTest("DELETE FROM ^", TABLE()));

      it("DELETE FROM tbl USING ^", () =>
        runTest("DELETE FROM tbl USING ^", TABLE()));

      it("SELECT * FROM ^", () => runTest("SELECT * FROM ^", TABLE()));

      it("SELECT * FROM tbl, ^", () =>
        runTest("SELECT * FROM tbl, ^", TABLE()));

      it("SELECT * FROM tbl. ^", () =>
        runTest("SELECT * FROM tbl. ^", TABLE()));

      it("SELECT * FROM tbl JOIN ^", () =>
        runTest("SELECT * FROM tbl JOIN ^", TABLE()));

      it("SELECT * FROM tbl CROSS APPLY ^", () =>
        runTest("SELECT * FROM tbl CROSS APPLY ^", TABLE()));

      it("SELECT * FROM tbl OUTER APPLY ^", () =>
        runTest("SELECT * FROM tbl OUTER APPLY ^", TABLE()));

      it("SELECT * FROM tbl, LATERAL (SELECT * FROM ^)", () =>
        runTest("SELECT * FROM tbl, LATERAL (SELECT * FROM ^)", TABLE()));

      it("SELECT * FROM tbl JOIN (SELECT * FROM ^)", () =>
        runTest("SELECT * FROM tbl JOIN (SELECT * FROM ^)", TABLE()));

      it("TABLE ^", () => runTest("TABLE ^", TABLE()));

      it("SELECT * FROM tbl JOIN (TABLE ^)", () =>
        runTest("SELECT * FROM tbl JOIN (TABLE ^)", TABLE()));

      it("EXPLAIN JSON FOR SELECT * FROM ^", () =>
        runTest("EXPLAIN JSON FOR SELECT * FROM ^", TABLE()));

      it("DESCRIBE STATEMENT SELECT * FROM ^", () =>
        runTest("DESCRIBE STATEMENT SELECT * FROM ^", TABLE()));

      it("SHOW CREATE TABLE ^", () => runTest("SHOW CREATE TABLE ^", TABLE()));

      it("SHOW TBLPROPERTIES ^", () =>
        runTest("SHOW TBLPROPERTIES ^", TABLE()));
    });

    describe("udfs", () => {
      it("ALTER VIEW vds1 CHANGE colA SET MASKING POLICY ^", () =>
        runTest("ALTER VIEW vds1 CHANGE colA SET MASKING POLICY ^", UDF()));

      it("ALTER VIEW vds1 CHANGE colA UNSET MASKING POLICY ^", () =>
        runTest("ALTER VIEW vds1 CHANGE colA UNSET MASKING POLICY ^", UDF()));

      it("CREATE FUNCTION IF NOT EXISTS ^", () =>
        runTest("CREATE FUNCTION IF NOT EXISTS ^", undefined));

      it("CREATE OR REPLACE FUNCTION IF NOT EXISTS ^", () =>
        runTest("CREATE OR REPLACE FUNCTION IF NOT EXISTS ^", undefined));

      it("CREATE FUNCTION ^", () => runTest("CREATE FUNCTION ^", undefined));

      it("CREATE OR REPLACE FUNCTION ^", () =>
        runTest("CREATE OR REPLACE FUNCTION ^", UDF()));

      it("CREATE VDS vds1 ROW ACCESS POLICY ^", () =>
        runTest("CREATE VDS vds1 ROW ACCESS POLICY ^", UDF()));

      it("CREATE OR REPLACE VIEW vds1 ROW ACCESS POLICY ^", () =>
        runTest("CREATE OR REPLACE VIEW vds1 ROW ACCESS POLICY ^", UDF()));

      it("CREATE OR REPLACE VDS vds1 (colA MASKING POLICY ^)", () =>
        runTest("CREATE OR REPLACE VDS vds1 (colA MASKING POLICY ^)", UDF()));

      it("CREATE TABLE IF NOT EXISTS tbl (colA MASKING POLICY ^)", () =>
        runTest(
          "CREATE TABLE IF NOT EXISTS tbl (colA MASKING POLICY ^)",
          UDF()
        ));

      it("CREATE TABLE IF NOT EXISTS tbl ROW ACCESS POLICY ^", () =>
        runTest("CREATE TABLE IF NOT EXISTS tbl ROW ACCESS POLICY ^", UDF()));

      it("ALTER TABLE tbl ADD ROW ACCESS POLICY ^", () =>
        runTest("ALTER TABLE tbl ADD ROW ACCESS POLICY ^", UDF()));

      it("ALTER VDS vds1 DROP ROW ACCESS POLICY ^", () =>
        runTest("ALTER VDS vds1 DROP ROW ACCESS POLICY ^", UDF()));

      it("ALTER VIEW vds1 CHANGE colA SET MASKING POLICY ^", () =>
        runTest("ALTER VIEW vds1 CHANGE colA SET MASKING POLICY ^", UDF()));

      it("ALTER DATASET vds1 CHANGE colA UNSET MASKING POLICY ^", () =>
        runTest(
          "ALTER DATASET vds1 CHANGE colA UNSET MASKING POLICY ^",
          UDF()
        ));

      it("DROP FUNCTION ^", () => runTest("DROP FUNCTION ^", UDF()));

      it('DROP FUNCTION "my-space".^', () =>
        runTest('DROP FUNCTION "my-space".^', UDF()));

      it("DROP FUNCTION IF EXISTS ^", () =>
        runTest("DROP FUNCTION IF EXISTS ^", UDF()));

      it("DESCRIBE FUNCTION ^", () => runTest("DESCRIBE FUNCTION ^", UDF()));

      it("DESC FUNCTION ^", () => runTest("DESC FUNCTION ^", UDF()));

      it("GRANT SELECT ON FUNCTION ^", () =>
        runTest("GRANT SELECT ON FUNCTION ^", UDF()));

      it("REVOKE SELECT ON FUNCTION ^", () =>
        runTest("REVOKE SELECT ON FUNCTION ^", UDF()));

      it("GRANT OWNERSHIP ON FUNCTION ^", () =>
        runTest("GRANT OWNERSHIP ON FUNCTION ^", UDF()));
    });
  });
});
