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

import java.math.BigDecimal;
import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;

/**
 * UPDATE test case repository. It's done this way to allow test cases to be used by different sources.
 *
 * Guidance for adding tests to different suites:
 *  - ITMerge in OSS only really needs a small baseline set of tests to sanity-check that DML is functional in OSS.
 *    Not adding every test here to OSS saves us a bit of test execution time without sacrificing much in the way of
 *    meaningful coverage.
 *  - All (except EE-only) tests here should be a part of CE and Hive suites as these represent core customer configurations.
 */
public class UpdateTestCases extends DmlQueryTestCasesBase {

  public static void testMalformedUpdateQueries(String source) throws Exception {
    try (Table table = createBasicTable(source, 4, 0)) {
      testMalformedDmlQueries(new Object[]{table.fqn, table.columns[1], table.columns[2], table.columns[3]},
        "UPDATE",
        "UPDATE %s",
        "UPDATE %s SET",
        "UPDATE %s SET %s",
        "UPDATE %s SET =",
        "UPDATE %s SET = value",
        "UPDATE %s SET %s =",
        "UPDATE %s SET %s, %s, %s",
        "UPDATE %s SET %s = 'value1', %s =",
        "UPDATE %s SET %s = 'value1' WHERE"
      );
    }
  }

  public static void testUpdateAll(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = 0", new Object[]{table.fqn}, table, 10,
          new Object[]{0, table.originalData[0][1]},
          new Object[]{0, table.originalData[1][1]},
          new Object[]{0, table.originalData[2][1]},
          new Object[]{0, table.originalData[3][1]},
          new Object[]{0, table.originalData[4][1]},
          new Object[]{0, table.originalData[5][1]},
          new Object[]{0, table.originalData[6][1]},
          new Object[]{0, table.originalData[7][1]},
          new Object[]{0, table.originalData[8][1]},
          new Object[]{0, table.originalData[9][1]});
      }
    }
  }

  public static void testUpdateAllColumns(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = 0, %s = '%s'", new Object[]{table.fqn, table.columns[1], table.originalData[0][1]}, table, 10,
          Collections.nCopies(table.originalData.length, table.originalData[0]).toArray(
            new Object[table.originalData.length][]));
      }
    }
  }

  public static void testUpdateById(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = %s WHERE id = %s",
          new Object[]{table.fqn, (int) table.originalData[5][0] * 10, table.originalData[5][0]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{50, table.originalData[5][1]},
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateTargetTableWithAndWithoutAlias(BufferAllocator allocator, String source) throws Exception {
    // without target table aliasing
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = %s WHERE id = %s",
          new Object[]{table.fqn, (int) table.originalData[5][0] * 10, table.originalData[5][0]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{50, table.originalData[5][1]},
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }

    //  with target table aliasing
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s as t SET id = %s WHERE t.id = %s",
          new Object[]{table.fqn, (int) table.originalData[5][0] * 10, table.originalData[5][0]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{50, table.originalData[5][1]},
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateByIdInEquality(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = 777 WHERE id > 4", new Object[]{table.fqn}, table, 5,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{777, table.originalData[5][1]},
            new Object[]{777, table.originalData[6][1]},
            new Object[]{777, table.originalData[7][1]},
            new Object[]{777, table.originalData[8][1]},
            new Object[]{777, table.originalData[9][1]}));
      }
    }
  }

  public static void testUpdateWithInClauseFilter(BufferAllocator allocator, String source) throws Exception {
    Object[] newId = new Object[]{777};
    try (Table table = createBasicTable(source, 1, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET id = 777 WHERE id in (5, 6, 7, 8, 9, 10)", new Object[]{table.fqn}, table, 5,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          newId, newId, newId, newId, newId));
    }
  }

  public static void testUpdateByIdAndColumn0(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET column_1 = '%s' WHERE id > 0 AND column_0 = '%s'",
          new Object[]{table.fqn, table.originalData[4][1], table.originalData[4][1]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 4),
            new Object[]{table.originalData[4][0], table.originalData[4][1], table.originalData[4][1]},
            table.originalData[5],
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateByIdOrColumn0(BufferAllocator allocator, String source) throws Exception {
    String newValue = "777";
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET column_0 = '%s' WHERE id = %s OR column_0 = '%s'",
          new Object[]{table.fqn, newValue, table.originalData[9][0], table.originalData[4][1]}, table, 2,
          ArrayUtils.addAll(ArrayUtils.subarray(table.originalData, 0, 4),
            new Object[]{table.originalData[4][0], newValue, table.originalData[4][2]},
            table.originalData[5],
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            new Object[]{table.originalData[9][0], newValue, table.originalData[9][2]}));
      }
    }
  }

  public static void testUpdateByIdTwoColumns(BufferAllocator allocator, String source) throws Exception {
    String newColumn0 = "000";
    String newColumn1 = "111";
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET column_0 = '%s', column_1 = '%s' WHERE id = %s",
          new Object[]{table.fqn, newColumn0, newColumn1, table.originalData[5][0]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{table.originalData[5][0], newColumn0, newColumn1},
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateByIdWithDecimals(BufferAllocator allocator, String source) throws Exception {
    try (Table table = createBasicTableWithDecimals(source, 3, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET column_0 = 1.0, column_1 = 7.77 WHERE id = %s",
        new Object[]{table.fqn, table.originalData[5][0]}, table, 1,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          new Object[]{table.originalData[5][0], new BigDecimal(1), new BigDecimal(8)},
          table.originalData[6],
          table.originalData[7],
          table.originalData[8],
          table.originalData[9]));
    }
  }

  public static void testUpdateByIdWithDoubles(BufferAllocator allocator, String source) throws Exception {
    try (Table table = createBasicTableWithDoubles(source, 3, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET column_0 = 1.0, column_1 = 7.77 WHERE id = %s",
        new Object[]{table.fqn, table.originalData[5][0]}, table, 1,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          new Object[]{table.originalData[5][0], 1.0, 7.77},
          table.originalData[6],
          table.originalData[7],
          table.originalData[8],
          table.originalData[9]));
    }
  }

  public static void testUpdateByIdWithFloats(BufferAllocator allocator, String source) throws Exception {
    try (Table table = createBasicTableWithFloats(source, 3, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET column_0 = 1.0, column_1 = 7.77 WHERE id = %s",
        new Object[]{table.fqn, table.originalData[5][0]}, table, 1,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          new Object[]{table.originalData[5][0], 1.0f, 7.77f},
          table.originalData[6],
          table.originalData[7],
          table.originalData[8],
          table.originalData[9]));
    }
  }

  public static void testUpdateByIdWithSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET column_0 = (SELECT column_1 FROM %s WHERE id = %s LIMIT 1) WHERE id = %s",
          new Object[]{table.fqn, table.fqn, table.originalData[5][0], table.originalData[5][0]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{table.originalData[5][0], table.originalData[5][2], table.originalData[5][2]},
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateByIdWithSubQueryWithFloat(BufferAllocator allocator, String source) throws Exception {
    try (Table table = createBasicTableWithFloats(source, 3, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET column_0 = (SELECT column_1 FROM %s WHERE id = %s LIMIT 1) WHERE id = %s",
        new Object[]{table.fqn, table.fqn, table.originalData[5][0], table.originalData[5][0]}, table, 1,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          new Object[]{table.originalData[5][0], table.originalData[5][2], table.originalData[5][2]},
          table.originalData[6],
          table.originalData[7],
          table.originalData[8],
          table.originalData[9]));
    }
  }

  public static void testUpdateByIdWithSubQueryAndLiteralWithFloats(BufferAllocator allocator, String source) throws Exception {
    try (Table table = createBasicTableWithFloats(source, 3, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET column_1 = (SELECT column_1 FROM %s WHERE id = %s LIMIT 1), column_0 = 1.0 WHERE id = %s",
        new Object[]{table.fqn, table.fqn, table.originalData[5][0], table.originalData[5][0]}, table, 1,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          new Object[]{table.originalData[5][0], 1.0f, table.originalData[5][2]},
          table.originalData[6],
          table.originalData[7],
          table.originalData[8],
          table.originalData[9]));
    }
  }

  public static void testUpdateByIdWithSubQueryWithDecimal(BufferAllocator allocator, String source) throws Exception {
    try (Table table = createBasicTableWithDecimals(source, 3, 10)) {
      testDmlQuery(allocator, "UPDATE %s SET column_0 = (SELECT column_1 FROM %s WHERE id = %s LIMIT 1) WHERE id = %s",
        new Object[]{table.fqn, table.fqn, table.originalData[5][0], table.originalData[5][0]}, table, 1,
        ArrayUtils.addAll(
          ArrayUtils.subarray(
            table.originalData, 0, 5),
          new Object[]{table.originalData[5][0], table.originalData[5][2], table.originalData[5][2]},
          table.originalData[6],
          table.originalData[7],
          table.originalData[8],
          table.originalData[9]));
    }
  }

  public static void testUpdateByIdWithScalarSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = (SELECT MAX(id) FROM %s)",
          new Object[]{table.fqn, table.fqn}, table, 10,
          new Object[]{9, table.originalData[0][1], table.originalData[0][2]},
          new Object[]{9, table.originalData[1][1], table.originalData[1][2]},
          new Object[]{9, table.originalData[2][1], table.originalData[2][2]},
          new Object[]{9, table.originalData[3][1], table.originalData[3][2]},
          new Object[]{9, table.originalData[4][1], table.originalData[4][2]},
          new Object[]{9, table.originalData[5][1], table.originalData[5][2]},
          new Object[]{9, table.originalData[6][1], table.originalData[6][2]},
          new Object[]{9, table.originalData[7][1], table.originalData[7][2]},
          new Object[]{9, table.originalData[8][1], table.originalData[8][2]},
          new Object[]{9, table.originalData[9][1], table.originalData[9][2]});
      }
    }
  }

  public static void testUpdateByIdWithSubQueries(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET column_0 = (SELECT column_1 FROM %s WHERE id = %s LIMIT 1), " +
            "column_1 = (SELECT column_0 FROM %s WHERE id = %s LIMIT 1) WHERE id = %s",
          new Object[]{table.fqn, table.fqn, table.originalData[5][0], table.fqn, table.originalData[5][0],
            table.originalData[5][0]}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(
              table.originalData, 0, 5),
            new Object[]{table.originalData[5][0], table.originalData[5][2], table.originalData[5][1]},
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateWithMultiplePushDownFilters(BufferAllocator allocator, String source) throws Exception {
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 3, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = 999 WHERE id < 5 AND column_1 = '3_1'",
          new Object[] { table.fqn}, table, 1,
          ArrayUtils.addAll(
            ArrayUtils.subarray(table.originalData, 0, 3),
            new Object[] { 999, table.originalData[3][1], table.originalData[3][2] },
            table.originalData[4],
            table.originalData[5],
            table.originalData[6],
            table.originalData[7],
            table.originalData[8],
            table.originalData[9]));
      }
    }
  }

  public static void testUpdateWithOnePathContext(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "UPDATE %s.%s SET %s = 'One' WHERE id = %s",
        new Object[]{table.paths[0], table.name, table.columns[1], table.originalData[0][0]}, table, 1,
        new Object[]{0, "One"},
        table.originalData[1]);
    }
  }

  public static void testUpdateWithBadContext(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
          "UPDATE %s.%s SET id = 1 WHERE id = %s",
          new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining(String.format("Table [%s.%s] not found", table.paths[0], table.name));
    }
  }

  // BEGIN: EE-only Tests
  public static void testUpdateWithColumnMasking(BufferAllocator allocator, String source) throws Exception {
    testUpdateWithColumnMasking(allocator, source, false);
  }
  public static void testUpdateWithColumnMaskingAndFullPath(BufferAllocator allocator, String source) throws Exception {
    testUpdateWithColumnMasking(allocator, source, true);
  }

  public static void testUpdateWithColumnMasking(BufferAllocator allocator, String source, boolean fullPath) throws Exception {
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
      testDmlQuery(allocator, "UPDATE %s SET id = 10 WHERE id = 1",
        new Object[]{fullPath ? table.fqn : table.name}, table, 1,
        new Object[]{10, 0.0f},
        new Object[]{2, 0.0f});
      getColumnUnmaskingPolicyFnForAnon(source, "salary", SqlTypeName.FLOAT, 0.0f);
      verifyData(allocator, table,
        new Object[]{10, 100.0f},
        new Object[]{2, 200.0f});
    }
  }

  public static void testUpdateWithColumnMaskingToAnotherColumn(BufferAllocator allocator, String source) throws Exception {
    testUpdateWithColumnMaskingToAnotherColumn(allocator, source, false);
  }

  public static void testUpdateWithColumnMaskingToAnotherColumnAndFullPath(BufferAllocator allocator, String source) throws Exception {
    testUpdateWithColumnMaskingToAnotherColumn(allocator, source, true);
  }

  private static void testUpdateWithColumnMaskingToAnotherColumn(BufferAllocator allocator, String source, boolean fullPath) throws Exception {
    String function = getColumnMaskingPolicyFnForAnon(source, "ssn", SqlTypeName.VARCHAR, "'XXX-XX-XXXX'");
    try (
      Table table = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("ssn", SqlTypeName.VARCHAR, false, String.format("MASKING POLICY %s(ssn)", function)),
          new ColumnInfo("ssn_copy", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{0, "000-00-0000", null},
          new Object[]{1, "111-11-1111", null}
        });
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator, "UPDATE %s SET ssn_copy = ssn",
        new Object[]{fullPath ? table.fqn : table.name}, table, 2,
        new Object[]{0, "XXX-XX-XXXX", "XXX-XX-XXXX"},
        new Object[]{1, "XXX-XX-XXXX", "XXX-XX-XXXX"});
      getColumnUnmaskingPolicyFnForAnon(source, "ssn", SqlTypeName.VARCHAR, "'XXX-XX-XXXX'");
      verifyData(allocator, table,
        new Object[]{0, "000-00-0000", "XXX-XX-XXXX"},
        new Object[]{1, "111-11-1111", "XXX-XX-XXXX"});
    }
  }

  public static void testUpdateWithRowFiltering(BufferAllocator allocator, String source) throws Exception {
    testUpdateWithRowFiltering(allocator, source, false);
  }

  public static void testUpdateWithRowFilteringAndFullPath(BufferAllocator allocator, String source) throws Exception {
    testUpdateWithRowFiltering(allocator, source, true);
  }

  public static void testUpdateWithRowFiltering(BufferAllocator allocator, String source, boolean fullPath) throws Exception {
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
      testDmlQuery(allocator, "UPDATE %s SET salary = 0.0",
        new Object[]{fullPath ? table.fqn : table.name}, table, 2,
        new Object[]{0, 000.0f},
        new Object[]{2, 000.0f});
      getRowUnfilterPolicyFnForAnon(source, "id", SqlTypeName.INTEGER);
      verifyData(allocator, table,
        new Object[]{0, 000.0f},
        new Object[]{1, 100.0f},
        new Object[]{2, 000.0f},
        new Object[]{3, 300.0f});
    }
  }
  // END: EE-only Tests
}
