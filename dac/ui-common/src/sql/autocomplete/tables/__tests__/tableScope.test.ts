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
import { computeCursorInfo } from "../../parsing/cursorInfo";
import type { IColumnFetcher } from "../../apiClient/columnFetcher";
import { type QueryPlan, QueryVisitor } from "../tableScope";
import {
  getAutocompleteParseTree,
  prepareQuery,
} from "../../__tests__/__utils__/testUtils";
import type { CatalogColumn } from "../../apiClient/autocompleteApi";
import { SimpleDataType } from "../../../../sonar/catalog/SimpleDataType.type";
import type { Column, Table } from "../../types/Table";
import { ContainerType } from "../../types/ContainerType";
import { getIdentifierInfo } from "../../identifiers/identifierUtils";

const getTestColumns = (parentPath: string[]): CatalogColumn[] => [
  { name: "col1", type: SimpleDataType.TEXT, parentPath },
  { name: "col2", type: SimpleDataType.INTEGER, parentPath },
];

function testGetQueryPlan(queryWithCaret: string): Promise<QueryPlan> {
  const [query, caretPosition] = prepareQuery(queryWithCaret);
  const { parseTree, commonTokenStream } = getAutocompleteParseTree(query);
  const cursorInfo = computeCursorInfo(parseTree, caretPosition)!;
  const identifierInfo = getIdentifierInfo(cursorInfo);
  const columnFetcher: IColumnFetcher = {
    getColumns: (tablePath: string[]) =>
      Promise.resolve(getTestColumns(tablePath)),
  };
  const queryVisitor = new QueryVisitor(
    cursorInfo,
    identifierInfo,
    commonTokenStream,
    columnFetcher,
    []
  );
  return queryVisitor.getTablesInScope();
}

// These tests are meant to validate that the table in scope can be identified for all supported queries.
// It is NOT meant to exercise all types of queries that can suggest a column at the caret position!
describe("tableScope", () => {
  describe("getTablesInScope", () => {
    const getExpectedColumns = (...tablePath: string[]): Column[] => [
      { ...getTestColumns(tablePath)[0], origin: "table" },
      { ...getTestColumns(tablePath)[1], origin: "table" },
    ];
    const expectedType: ContainerType = ContainerType.DIRECT;

    it("SELECT ^ FROM tbl", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM tbl");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM space1.tbl", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM space1.tbl");
      const expected: Table[] = [
        {
          derived: false,
          path: ["space1", "tbl"],
          columns: getExpectedColumns("space1", "tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl WHERE ^", async () => {
      const queryPlan = testGetQueryPlan("SELECT * FROM tbl WHERE ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM space1.tbl", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM space1.tbl");
      const expected: Table[] = [
        {
          derived: false,
          path: ["space1", "tbl"],
          columns: getExpectedColumns("space1", "tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ABS(^ FROM tbl", async () => {
      const queryPlan = testGetQueryPlan("SELECT ABS(^ FROM tbl");
      // Ideally this returned tbl but this is currently not working because we allow sqlSelect to start with FROM token
      // so FROM tbl is interpreted as an arg0 subquery of ABS and table scope resolution starts from the prior token (
      // Choosing not to prioritize this because in the sql editor, typing a ( always inserts the matching ) so this
      // should not be a common scenario
      expect((await queryPlan).tables).toEqual([]);
    });

    it("SELECT ABS(^) FROM tbl", async () => {
      const queryPlan = testGetQueryPlan("SELECT ABS(^) FROM tbl");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT CAST(^) FROM tbl", async () => {
      // This differs from the previous ABS test case because it's a built in function
      const queryPlan = testGetQueryPlan("SELECT CAST(^) FROM tbl");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT col1, ^ FROM tbl", async () => {
      const queryPlan = testGetQueryPlan("SELECT col1, ^ FROM tbl");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM tbl1 JOIN tbl2", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM tbl1 JOIN tbl2");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1 JOIN tbl2 ON ^", async () => {
      const queryPlan = testGetQueryPlan("SELECT * FROM tbl1 JOIN tbl2 ON ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM tbl1, tbl2", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM tbl1, tbl2");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM tbl1, tbl2", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM tbl1, tbl2");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT * FROM tbl)", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM (SELECT * FROM tbl)");
      const expected: Table[] = [
        { derived: true, columns: getExpectedColumns("tbl") },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT * FROM tbl) al", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT * FROM tbl) al"
      );
      const expected: Table[] = [
        { derived: true, columns: getExpectedColumns("tbl"), alias: "al" },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT * FROM tbl) al (colAl1, colAl2)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT * FROM tbl) al (colAl1, colAl2)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: getExpectedColumns("tbl"),
          alias: "al",
          columnAliases: ["colAl1", "colAl2"],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (TABLE tbl)", async () => {
      const queryPlan = testGetQueryPlan("SELECT ^ FROM (TABLE tbl)");
      const expected: Table[] = [
        {
          derived: true,
          columns: getExpectedColumns("tbl"),
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1, tbl2 JOIN tbl3 ON ^", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1, tbl2 JOIN tbl3 ON ^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl3"],
          columns: getExpectedColumns("tbl3"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1, tbl2 CROSS JOIN tbl3 WHERE ^", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1, tbl2 CROSS JOIN tbl3 WHERE ^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl3"],
          columns: getExpectedColumns("tbl3"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1 JOIN (SELECT ^ FROM tbl2)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1 JOIN (SELECT ^ FROM tbl2)"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1 JOIN (tbl2 JOIN tbl3 USING ^)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1 JOIN (tbl2 JOIN tbl3 USING ^)"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl3"],
          columns: getExpectedColumns("tbl3"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1 UNION SELECT * FROM tbl2 WHERE ^", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1 UNION SELECT * FROM tbl2 WHERE ^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1 JOIN (SELECT * FROM tbl2 UNION SELECT * FROM tbl3 WHERE ^)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1 JOIN (SELECT * FROM tbl2 UNION SELECT * FROM tbl3 WHERE ^)"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl3"],
          columns: getExpectedColumns("tbl3"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tbl1 JOIN (SELECT * FROM tbl2 UNION (tbl3 JOIN tbl4 ON ^))", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tbl1 JOIN (SELECT * FROM tbl2 UNION (tbl3 JOIN tbl4 ON ^))"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl3"],
          columns: getExpectedColumns("tbl3"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl4"],
          columns: getExpectedColumns("tbl4"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT tblA.* FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB) tblB)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT tblA.* FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB) tblB)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "colA", origin: "expression" }],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT * FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB) tblB)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT * FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB) tblB)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [
            { name: "colA", origin: "expression" },
            { name: "colB", origin: "expression" },
          ],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT tblA.colA FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB) tblB)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT tblA.colA FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB) tblB)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "colA", origin: "expression" }],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT tblA.colA, tblB.colC FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB, 3 as colC) tblB)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT tblA.colA, tblB.colC FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB, 3 as colC) tblB)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [
            { name: "colA", origin: "expression" },
            { name: "colC", origin: "expression" },
          ],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT tblA.colA, tblB.colC FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB, 3 as colC) tblB)", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT tblA.colA, tblB.colC FROM (SELECT 1 as colA) tblA CROSS JOIN (SELECT 2 as colB, 3 as colC) tblB)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [
            { name: "colA", origin: "expression" },
            { name: "colC", origin: "expression" },
          ],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT 1 as colA, 2 as colA)", async () => {
      // This verifies the behavior of non-unique name columns getting silently dropped
      // Note: If you run in SQL runner SELECT * FROM (...) it shows colA and colA0 but colA0 will have value 1 and
      // isn't actually materialized (SELECT colA0 FROM .. fails) so we don't want to show it as suggestion
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT 1 as colA, 2 as colA)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "colA", origin: "expression" }],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT 1 as colA, * FROM (SELECT 2 as colA))", async () => {
      // This verifies the behavior of non-unique name columns getting uniqueified if projected from a star expansion
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT 1 as colA, * FROM (SELECT 2 as colA))"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [
            { name: "colA", origin: "expression" },
            { name: "colA0", origin: "expression" },
          ],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT *, 1 as colA FROM (SELECT 2 as colA))", async () => {
      // This verifies the behavior of non-unique name columns getting dropped if not projected from a star expansion
      // Note the order of the select items differs from the previous test case
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT *, 1 as colA FROM (SELECT 2 as colA))"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "colA", origin: "expression" }],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (SELECT *, * FROM (SELECT 1 as colA))", async () => {
      // This verifies the behavior of non-unique name columns getting uniqueified if projected from a star expansion
      // with multiple star expansions
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (SELECT *, * FROM (SELECT 1 as colA))"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [
            { name: "colA", origin: "expression" },
            { name: "colA0", origin: "expression" },
          ],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tblA AS aliased WHERE aliased.^", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tblA AS aliased WHERE aliased.^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tblA"],
          columns: getExpectedColumns("tblA"),
          type: expectedType,
          alias: "aliased",
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT * FROM tblA AS aliased (colAliased1, colAliased2) WHERE aliased.^", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT * FROM tblA AS aliased (colAliased1, colAliased2) WHERE aliased.^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tblA"],
          columns: getExpectedColumns("tblA"),
          type: expectedType,
          alias: "aliased",
          columnAliases: ["colAliased1", "colAliased2"],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("UPDATE tbl1 SET ^ FROM tbl2", async () => {
      const queryPlan = testGetQueryPlan("UPDATE tbl1 SET ^ FROM tbl2");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("UPDATE tbl1 SET col1 = 1 FROM tbl2 WHERE ^", async () => {
      const queryPlan = testGetQueryPlan(
        "UPDATE tbl1 SET col1 = 1 FROM tbl2 WHERE ^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("DELETE FROM tbl1 USING tbl2 WHERE ^", async () => {
      const queryPlan = testGetQueryPlan("DELETE FROM tbl1 USING tbl2 WHERE ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("DESCRIBE TABLE tbl ^", async () => {
      const queryPlan = testGetQueryPlan("DESCRIBE TABLE tbl ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("DESCRIBE (SELECT ^ FROM tbl)", async () => {
      const queryPlan = testGetQueryPlan("DESCRIBE (SELECT ^ FROM tbl)");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("DESCRIBE (SELECT ^ FROM tbl)", async () => {
      const queryPlan = testGetQueryPlan("DESCRIBE (SELECT ^ FROM tbl)");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("DESCRIBE tbl ^", async () => {
      const queryPlan = testGetQueryPlan("DESCRIBE tbl ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("UPDATE tbl1 SET ^ FROM tbl2 WHERE", async () => {
      const queryPlan = testGetQueryPlan("UPDATE tbl1 SET ^ FROM tbl2 WHERE");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("UPDATE tbl1 SET col = 1 FROM tbl2 WHERE ^", async () => {
      const queryPlan = testGetQueryPlan(
        "UPDATE tbl1 SET col = 1 FROM tbl2 WHERE ^"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("MERGE INTO tbl1 USING tbl2 ON ^", async () => {
      const queryPlan = testGetQueryPlan("MERGE INTO tbl1 USING tbl2 ON ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl1"],
          columns: getExpectedColumns("tbl1"),
          type: expectedType,
        },
        {
          derived: false,
          path: ["tbl2"],
          columns: getExpectedColumns("tbl2"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("ANALYZE TABLE tbl FOR COLUMNS (^)", async () => {
      const queryPlan = testGetQueryPlan("ANALYZE TABLE tbl FOR COLUMNS (^)");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("REFRESH DATASET tbl FOR PARTITIONS (^)", async () => {
      const queryPlan = testGetQueryPlan(
        "REFRESH DATASET tbl FOR PARTITIONS (^)"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("ALTER TABLE tbl ADD PRIMARY KEY ^", async () => {
      const queryPlan = testGetQueryPlan("ALTER TABLE tbl ADD PRIMARY KEY ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("ALTER VDS tbl ADD PRIMARY KEY ^", async () => {
      const queryPlan = testGetQueryPlan("ALTER VDS tbl ADD PRIMARY KEY ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("ALTER VIEW tbl ADD PRIMARY KEY ^", async () => {
      const queryPlan = testGetQueryPlan("ALTER VIEW tbl ADD PRIMARY KEY ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("ALTER PDS tbl ADD PRIMARY KEY ^", async () => {
      const queryPlan = testGetQueryPlan("ALTER PDS tbl ADD PRIMARY KEY ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("ALTER DATASET tbl ADD PRIMARY KEY ^", async () => {
      const queryPlan = testGetQueryPlan("ALTER DATASET tbl ADD PRIMARY KEY ^");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("CREATE OR REPLACE VIEW tbl (^)", async () => {
      const queryPlan = testGetQueryPlan("CREATE OR REPLACE VIEW tbl (^)");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("CREATE OR REPLACE VDS tbl (^)", async () => {
      const queryPlan = testGetQueryPlan("CREATE OR REPLACE VDS tbl (^)");
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("DESCRIBE STATEMENT SELECT ^ FROM tbl", async () => {
      const queryPlan = testGetQueryPlan(
        "DESCRIBE STATEMENT SELECT ^ FROM tbl"
      );
      const expected: Table[] = [
        {
          derived: false,
          path: ["tbl"],
          columns: getExpectedColumns("tbl"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("CREATE VIEW tbl (^)", async () => {
      const queryPlan = testGetQueryPlan("CREATE VIEW tbl (^)");
      const expected: Table[] = [];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("WITH tmp AS (SELECT 1) SELECT ^ FROM tmp", async () => {
      const queryPlan = testGetQueryPlan(
        "WITH tmp AS (SELECT 1) SELECT ^ FROM tmp"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "EXPR$0", origin: "expression" }],
          alias: "tmp",
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("WITH tmp(colA) AS (SELECT 1) SELECT ^ FROM tmp", async () => {
      const queryPlan = testGetQueryPlan(
        "WITH tmp(colA) AS (SELECT 1) SELECT ^ FROM tmp"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "EXPR$0", origin: "expression" }],
          alias: "tmp",
          columnAliases: ["colA"],
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("WITH tmp AS (SELECT 1 as colA) SELECT ^ FROM tmp", async () => {
      const queryPlan = testGetQueryPlan(
        "WITH tmp AS (SELECT 1 as colA) SELECT ^ FROM tmp"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "colA", origin: "expression" }],
          alias: "tmp",
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("WITH tmp AS (SELECT 1) SELECT * FROM (SELECT ^ FROM tmp)", async () => {
      const queryPlan = testGetQueryPlan(
        "WITH tmp AS (SELECT 1) SELECT * FROM (SELECT ^ FROM tmp)"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: [{ name: "EXPR$0", origin: "expression" }],
          alias: "tmp",
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("WITH tmp AS (SELECT 1) SELECT ^ FROM space1.tmp", async () => {
      const queryPlan = testGetQueryPlan(
        "WITH tmp AS (SELECT 1) SELECT ^ FROM space1.tmp"
      );
      // Doesn't use the with table since it mismatches
      const expected: Table[] = [
        {
          derived: false,
          path: ["space1", "tmp"],
          columns: getExpectedColumns("space1", "tmp"),
          type: expectedType,
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("WITH tmp AS (SELECT 1) SELECT ^ FROM (SELECT * FROM tbl) tmp", async () => {
      const queryPlan = testGetQueryPlan(
        "WITH tmp AS (SELECT 1) SELECT ^ FROM (SELECT * FROM tbl) tmp"
      );
      // Doesn't use the with table since there is a closer matching derived table
      const expected: Table[] = [
        {
          derived: true,
          columns: getExpectedColumns("tbl"),
          alias: "tmp",
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });

    it("SELECT ^ FROM (WITH tmp AS (SELECT 1) SELECT * FROM tbl) tmp", async () => {
      const queryPlan = testGetQueryPlan(
        "SELECT ^ FROM (WITH tmp AS (SELECT 1) SELECT * FROM tbl) tmp"
      );
      const expected: Table[] = [
        {
          derived: true,
          columns: getExpectedColumns("tbl"),
          alias: "tmp",
        },
      ];
      expect((await queryPlan).tables).toEqual(expected);
    });
  });
});
