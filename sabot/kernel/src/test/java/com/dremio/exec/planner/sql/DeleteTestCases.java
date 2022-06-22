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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;

/**
 * DELETE test case repository. It's done this way to allow test cases to be used by different sources.
 *
 * Guidance for adding tests to different suites:
 *  - ITMerge in OSS only really needs a small baseline set of tests to sanity-check that DML is functional in OSS.
 *    Not adding every test here to OSS saves us a bit of test execution time without sacrificing much in the way of
 *    meaningful coverage.
 *  - All (except EE-only) tests here should be a part of CE and Hive suites as these represent core customer configurations.
 */
public class DeleteTestCases extends DmlQueryTestCasesBase {

  public static void testMalformedDeleteQueries(String source) throws Exception {
    try (Table table = createBasicTable(source, 1, 0)) {
      testMalformedDmlQueries(new Object[]{table.fqn, table.columns[0]},
        "DELETE",
        "DELETE FROM",
        "DELETE %s",
        "DELETE FROM %s AS",
        "DELETE FROM %s WHERE",
        "DELETE FROM %s t WHERE",
        "DELETE FROM %s AS t WHERE",
        "DELETE FROM %s AS t WHERE %s ="
      );
    }
  }

  public static void testDeleteAll(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s", new Object[]{table.fqn}, table, 10, EMPTY_EXPECTED_DATA);
      }
    }
  }

  public static void testDeleteById(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id = %s", new Object[]{table.fqn, table.originalData[0][0]}, table, 1,
          ArrayUtils.subarray(table.originalData, 1, table.originalData.length));
      }
    }
  }

  public static void testDeleteTargetTableWithAndWithoutAlias(BufferAllocator allocator, String source) throws Exception {
    // without target table aliasing
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id = %s", new Object[]{table.fqn, table.originalData[0][0]}, table, 1,
          ArrayUtils.subarray(table.originalData, 1, table.originalData.length));
      }
    }

    //  with target table aliasing
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s as t WHERE t.id = %s", new Object[]{table.fqn, table.originalData[0][0]}, table, 1,
          ArrayUtils.subarray(table.originalData, 1, table.originalData.length));
      }
    }
  }

  public static void testDeleteByIdInEquality(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id > 4", new Object[]{table.fqn}, table, 5,
          ArrayUtils.subarray(table.originalData, 0, 5));
      }
    }
  }

  public static void testDeleteByEvenIds(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id %% 2 = 0", new Object[]{table.fqn}, table, 5,
          table.originalData[1], table.originalData[3], table.originalData[5], table.originalData[7], table.originalData[9]);
      }
    }
  }

  public static void testDeleteByIdAndColumn0(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id > 0 AND column_0 = '%s'", new Object[]{table.fqn, table.originalData[4][1]}, table, 1,
          ArrayUtils.addAll(ArrayUtils.subarray(table.originalData, 0, 4),
            ArrayUtils.subarray(table.originalData, 5, table.originalData.length)));
      }
    }
  }

  public static void testDeleteByIdOrColumn0(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id = %s OR column_0 = '%s'",
          new Object[]{table.fqn, table.originalData[9][0], table.originalData[4][1]}, table, 2,
          ArrayUtils.addAll(ArrayUtils.subarray(table.originalData, 0, 4),
            ArrayUtils.subarray(table.originalData, 5, 9)));
      }
    }
  }

  public static void testDeleteWithMultiplePushDownFilters(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id < 5 AND column_1 = '3_1'",
          new Object[]{table.fqn}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(table.originalData, 0, 3),
            ArrayUtils.subarray(table.originalData, 4, 10)));
      }
    }
  }

  public static void testDeleteWithOnePathContext(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "DELETE FROM %s.%s WHERE id = %s",
        new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, 1,
        table.originalData[1]);
    }
  }

  public static void testDeleteWithTwoPathsContext(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 2, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "DELETE FROM %s.%s.%s WHERE id = %s",
        new Object[]{table.paths[0], table.paths[1], table.name, table.originalData[0][0]}, table, 1,
        table.originalData[1]);
    }
  }

  public static void testDeleteWithTwoPathsContextVariation(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 2, 2, 2);
      AutoCloseable ignored = setContext(allocator, source + "." + table.paths[0])) {
      testDmlQuery(allocator,
        "DELETE FROM %s.%s WHERE id = %s",
        new Object[]{table.paths[1], table.name, table.originalData[0][0]}, table, 1,
        table.originalData[1]);
    }
  }

  public static void testDeleteWithBadContext(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
          "DELETE FROM %s.%s WHERE id = %s",
          new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining(String.format("Table [%s.%s] not found", table.paths[0], table.name));
    }
  }

  // BEGIN: EE-only Tests
  public static void testDeleteWithColumnMasking(BufferAllocator allocator, String source) throws Exception {
    testDeleteWithColumnMasking(allocator, source, false);
  }

  public static void testDeleteWithColumnMaskingAndFullPath(BufferAllocator allocator, String source) throws Exception {
    testDeleteWithColumnMasking(allocator, source, true);
  }

  public static void testDeleteWithColumnMasking(BufferAllocator allocator, String source, boolean fullPath) throws Exception {
    String function = getColumnMaskingPolicyFnForAnon(source, "salary", SqlTypeName.FLOAT, 0.0f);
    try (
      Table table = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("salary", SqlTypeName.FLOAT, false, String.format("MASKING POLICY %s(salary)", function))
        },
        new Object[][]{
          new Object[]{1, 100.0f},
          new Object[]{2, 200.0f}
        });
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator, "DELETE FROM %s WHERE id = 1",
        new Object[]{fullPath ? table.fqn : table.name}, table, 1,
        new Object[]{2, 0.0f});
      getColumnUnmaskingPolicyFnForAnon(source, "salary", SqlTypeName.FLOAT, 0.0f);
      verifyData(allocator, table,
        new Object[]{2, 200.0f});
    }
  }

  public static void testDeleteWithRowFiltering(BufferAllocator allocator, String source) throws Exception {
    testDeleteWithRowFiltering(allocator, source, false);
  }

  public static void testDeleteWithRowFilteringAndFullPath(BufferAllocator allocator, String source) throws Exception {
    testDeleteWithRowFiltering(allocator, source, true);
  }

  public static void testDeleteWithRowFiltering(BufferAllocator allocator, String source, boolean fullPath) throws Exception {
    String function = getRowFilterPolicyFnForAnon(source, "id", SqlTypeName.INTEGER, "id % 2 = 0");
    try (
      Table table = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("salary", SqlTypeName.FLOAT, false)
        },
        new Object[][]{
          new Object[]{0, 000.0f},
          new Object[]{1, 100.0f},
          new Object[]{2, 200.0f},
          new Object[]{3, 300.0f}
        });
      AutoCloseable ignored = setContext(allocator, source)) {
      setRowFilterPolicy(table.fqn, function, "id");
      //noinspection RedundantArrayCreation
      testDmlQuery(allocator, "DELETE FROM %s",
        new Object[]{fullPath ? table.fqn : table.name}, table, 2,
        new Object[0][]);
      getRowUnfilterPolicyFnForAnon(source, "id", SqlTypeName.INTEGER);
      verifyData(allocator, table,
        new Object[]{1, 100.0f},
        new Object[]{3, 300.0f});
    }
  }
  // END: EE-only Tests
}
