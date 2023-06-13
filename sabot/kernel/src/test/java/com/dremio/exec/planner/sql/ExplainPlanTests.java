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
package com.dremio.exec.planner.sql;

import static com.dremio.BaseTestQuery.test;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Table;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static org.assertj.core.api.Assertions.assertThat;

import com.dremio.test.UserExceptionAssert;

/**
 * Explain Plan tests.
 *
 * Note: Add tests used across all platforms here.
 */
public class ExplainPlanTests {

  public static void testMalformedExplainPlanQueries(String source) throws Exception {
    try (Table targetTable = createBasicTable(source,2, 0)) {
      testMalformedExplainQuery(targetTable.fqn,
              "EXPLAIN",
              "EXPLAIN PLAN",
              "EXPLAIN PLAN FOR",
              "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR %s",
              "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITHOUT IMPLEMENTATION FOR %s",
              "EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR %s",
              "EXPLAIN PLAN FOR %s"
      );
    }
  }

  // INSERT
  public static void testExplainLogicalPlanOnInsert(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR insert into %s values(5, 'taco')", new Object[]{targetTable.fqn});
      }
  }

  public static void testExplainPhysicalPlanOnInsert(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN FOR insert into %s values(5, 'taco')", new Object[]{targetTable.fqn});
    }
  }

  public static void testExplainPlanWithDetailLevelOnInsert(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR insert into %s values(5, 'taco')", new Object[]{targetTable.fqn});
    }
  }

  // DELETE
  public static void testExplainLogicalPlanOnDelete(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR DELETE FROM %s where id = %s", new Object[]{targetTable.fqn, targetTable.originalData[0][0]});
    }
  }

  public static void testExplainPhysicalPlanOnDelete(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN FOR DELETE FROM %s where id = %s", new Object[]{targetTable.fqn, targetTable.originalData[0][0]});
    }
  }

  public static void testExplainPlanWithDetailLevelOnDelete(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR DELETE FROM %s where id = %s", new Object[]{targetTable.fqn, targetTable.originalData[0][0]});
    }
  }

  // UPDATE
  public static void testExplainLogicalPlanOnUpdate(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR UPDATE %s SET id = 0", new Object[]{targetTable.fqn});
    }
  }

  public static void testExplainPhysicalPlanOnUpdate(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN FOR UPDATE %s SET id = 0", new Object[]{targetTable.fqn});
    }
  }

  public static void testExplainPlanWithDetailLevelOnUpdate(String source) throws Exception {
    try (Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR UPDATE %s SET id = 0", new Object[]{targetTable.fqn});
    }
  }

  // MERGE
  public static void testExplainLogicalPlanOnMerge(String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 5);
          Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR MERGE INTO %s USING %s ON (%s.id = %s.id)"
              + " WHEN MATCHED THEN UPDATE SET column_0 = column_1",
              new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn});
    }
  }

  public static void testExplainPhysicalPlanOnMerge(String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 5);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN FOR MERGE INTO %s USING %s ON (%s.id = %s.id)"
                      + " WHEN MATCHED THEN UPDATE SET column_0 = column_1",
              new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn});
    }
  }

  public static void testExplainPlanWithDetailLevelOnMerge(String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 5);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testValidExplainQuery("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR MERGE INTO %s USING %s ON (%s.id = %s.id)"
                      + " WHEN MATCHED THEN UPDATE SET column_0 = column_1",
              new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn});
    }
  }

  /*Private Test Functions*/
  private static void testMalformedExplainQuery(String table, String... malformedQueries) {
    for (String malformedQuery : malformedQueries) {
      String fullQuery = String.format(malformedQuery, table);
      UserExceptionAssert.assertThatThrownBy(() -> test(fullQuery))
              .withFailMessage("Query failed to generate the expected error:\n" + fullQuery)
              .satisfiesAnyOf(
                      ex -> assertThat(ex).hasMessageContaining("PARSE ERROR:"),
                      ex -> assertThat(ex).hasMessageContaining("VALIDATION ERROR:"));
    }
  }

  private static void testValidExplainQuery(String query, Object[] args) throws Exception {
    // Run the Explain Plan query
    String fullQuery = String.format(query, args);
    test(fullQuery);
  }
}
