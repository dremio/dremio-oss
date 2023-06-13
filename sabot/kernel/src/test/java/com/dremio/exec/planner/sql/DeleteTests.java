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

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.ColumnInfo;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.EMPTY_EXPECTED_DATA;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.PARTITION_COLUMN_ONE_INDEX_SET;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Table;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Tables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicNonPartitionedAndPartitionedTables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createRandomId;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createView;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createViewFromTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.setContext;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testDmlQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testMalformedDmlQueries;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.verifyData;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;

/**
 * DELETE tests.
 *
 * Note: Add tests used across all platforms here.
 */
public class DeleteTests {

  public static void testMalformedDeleteQueries(String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 0);
         Table targetTable = createBasicTable(source,2, 0)) {
      testMalformedDmlQueries(new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.columns[0], sourceTable.columns[0]},
        "DELETE",
        "DELETE FROM",
        "DELETE %s",
        "DELETE FROM %s AS",
        "DELETE FROM %s WHERE",
        "DELETE FROM %s t WHERE",
        "DELETE FROM %s AS t WHERE",
        "DELETE FROM %s AS t using",
        "DELETE FROM %s AS t using %s as",
        "DELETE FROM %s AS t using %s where t.%s =",
        "DELETE FROM %s AS t using (select * from %s) as",
        "DELETE FROM %s AS t using (select * from %s) as s where",
        "DELETE FROM %s AS t using (select * from %s) as s where t.%s =",
        "DELETE FROM %s AS t using select * from %s as s where t.%s = s.%s"
        );
    }
  }

  public static void testDeleteOnView(BufferAllocator allocator, String source) throws Exception {
    String name = createRandomId();
    try (AutoCloseable ignored = createView(source, name)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator, "DELETE FROM %s.%s", new Object[]{source, name}, null, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("DELETE is not supported on this VIEW at [%s.%s].", source, name);
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

  public static void testDeleteByIdWithEqualNull(BufferAllocator allocator, String source) throws Exception {
    // column = null should return false and no data should be deleted
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "DELETE FROM %s WHERE id = %s", new Object[]{table.fqn, null}, table, 0,
            ArrayUtils.subarray(table.originalData, 0, table.originalData.length));
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

  public static void testDeleteWithOneSourceTable(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 5);
         Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using %s where t.id = %s.id",
        new Object[]{targetTable.fqn, sourceTable.fqn, sourceTable.fqn}, targetTable, 5,
        ArrayUtils.subarray(targetTable.originalData, 5, 10));
    }
  }

  public static void testDeleteWithOneSourceTableWithoutFullPath(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 5);
         Table targetTable = createBasicTable(source, 2, 10);
         AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using %s where t.id = %s.id",
              new Object[]{targetTable.name, sourceTable.name, sourceTable.name}, targetTable, 5,
              ArrayUtils.subarray(targetTable.originalData, 5, 10));
    }
  }

  public static void testDeleteWithViewUsedAsSourceTable(BufferAllocator allocator, String source) throws Exception {
    String viewName = createRandomId();
    try (Table targetTable = createBasicTable(source, 3, 5);
         AutoCloseable ignore = createViewFromTable(source, viewName, targetTable.fqn)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using %s.%s where t.id = %s.%s.id",
              new Object[]{targetTable.fqn, source, viewName, source, viewName}, targetTable, 5,
              EMPTY_EXPECTED_DATA);
    }
  }

  public static void testDeleteWithOneSourceTableQueryUsingSource(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 4);
         Table targetTable = createBasicTable(source,2, 10)) {
              testDmlQuery(allocator, "DELETE FROM %s AS t using (SELECT id from %s WHERE id < 4) AS s where t.id = s.id",
                      new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, 4,
                      ArrayUtils.subarray(targetTable.originalData, 4, 10));
    }
  }

  public static void testDeleteWithSourceTableAsQueryUsingTarget(BufferAllocator allocator, String source) throws Exception {
    try (Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using (SELECT id from %s WHERE id < 4) AS s where t.id = s.id",
              new Object[]{targetTable.fqn, targetTable.fqn}, targetTable, 4,
              ArrayUtils.subarray(targetTable.originalData, 4, 10));
    }
  }

  public static void testDeleteWithOneUnusedSourceTable(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 4);
         Table targetTable = createBasicTable(source,2, 10)) {
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "DELETE FROM %s AS t using %s",
        new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testDeleteWithUnrelatedConditionToSourceTable(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 4);
         Table targetTable = createBasicTable(source,2, 10)) {
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "DELETE FROM %s AS t using %s where t.id < 2",
        new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testDeleteWithPartialUnrelatedConditionToSourceTable(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 4);
         Table targetTable = createBasicTable(source,2, 10)) {
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "DELETE FROM %s AS t using (select * from %s where id > 0 and id < 4) s " +
          "where t.id = s.id or t.id = 0",
        new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testDeleteWithTwoSourceTables(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable1 = createBasicTable(source,3, 4);
         Table sourceTable2 = createBasicTable(source,3, 5);
         Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using %s, %s where t.id = %s.id and t.id = %s.id",
        new Object[]{targetTable.fqn, sourceTable1.fqn, sourceTable2.fqn, sourceTable1.fqn, sourceTable2.fqn}, targetTable, 4,
        ArrayUtils.subarray(targetTable.originalData, 4, 10));
    }
  }

  public static void testDeleteWithTwoSourceTableOneSourceQuery(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable1 = createBasicTable(source,3, 4);
         Table sourceTable2 = createBasicTable(source,3, 5);
         Table sourceTable3 = createBasicTable(source,3, 6);
         Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using %s as s1, %s as s2, (select id from %s) s3" +
          " where t.id = s1.id and t.id = s2.id and t.id = s3.id",
        new Object[]{targetTable.fqn, sourceTable1.fqn, sourceTable2.fqn, sourceTable3.fqn}, targetTable, 4,
        ArrayUtils.subarray(targetTable.originalData, 4, 10));
    }
  }

  public static void testDeleteWithTwoSourceTableTwoSourceQuery(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable1 = createBasicTable(source,3, 4);
         Table sourceTable2 = createBasicTable(source,3, 5);
         Table sourceTable3 = createBasicTable(source,3, 6);
         Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using %s as s1, (select id from %s WHERE id < 4) as s2, (select id from %s) s3" +
                      " where t.id = s1.id and t.id = s2.id and t.id = s3.id",
              new Object[]{targetTable.fqn, sourceTable1.fqn, sourceTable2.fqn, sourceTable3.fqn}, targetTable, 4,
              ArrayUtils.subarray(targetTable.originalData, 4, 10));
    }
  }

  public static void testDeleteWithSourceTableWithSubquery(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 5);
         Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using (select * from %s where %s.id < 3) as s where t.id = s.id",
        new Object[]{targetTable.fqn, sourceTable.fqn, sourceTable.fqn}, targetTable, 3,
        ArrayUtils.subarray(targetTable.originalData, 3, 10));
    }
  }

  public static void testDeleteWithSourceTableMultipleConditions(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 5);
         Table targetTable = createBasicTable(source,2, 10)) {
      testDmlQuery(allocator, "DELETE FROM %s AS t using (select * from %s) as s where t.id = s.id and t.%s = s.%s",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.columns[0], sourceTable.columns[0]}, targetTable, 5,
        ArrayUtils.subarray(targetTable.originalData, 5, 10));
    }
  }

  // BEGIN: Contexts + Paths
  public static void testDeleteWithWrongContextWithFqn(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2)) {
      testDmlQuery(allocator,
        "DELETE FROM %s WHERE id = %s",
        new Object[]{table.fqn, table.originalData[0][0]}, table, 1, table.originalData[1]);
    }
  }

  public static void testDeleteWithWrongContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
        "DELETE FROM %s.%s WHERE id = %s",
        new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, -1))
      .isInstanceOf(Exception.class)
      .hasMessageContaining("Table [%s.%s] does not exist.", table.paths[0], table.name);
    }
  }

  public static void testDeleteWithWrongContextWithTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
          "DELETE FROM %s WHERE id = %s",
          new Object[]{table.name, table.originalData[0][0]}, table, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Table [%s] does not exist.", table.name);
    }
  }

  public static void testDeleteWithContextWithFqn(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "DELETE FROM %s WHERE id = %s",
        new Object[]{table.fqn, table.originalData[0][0]}, table, 1, table.originalData[1]);
    }
  }

  public static void testDeleteWithContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "DELETE FROM %s.%s WHERE id = %s",
        new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, 1, table.originalData[1]);
    }
  }

  public static void testDeleteWithContextWithTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
          "DELETE FROM %s WHERE id = %s",
          new Object[]{table.name, table.originalData[0][0]}, table, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Table [%s] does not exist.", table.name);
    }
  }

  public static void testDeleteWithContextPathWithFqn(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source + "." + table.paths[0])) {
      testDmlQuery(allocator,
        "DELETE FROM %s WHERE id = %s",
        new Object[]{table.fqn, table.originalData[0][0]}, table, 1, table.originalData[1]);
    }
  }

  public static void testDeleteWithContextPathWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source + "." + table.paths[0])) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
          "DELETE FROM %s.%s WHERE id = %s",
          new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Table [%s.%s] does not exist.", table.paths[0], table.name);
    }
  }

  public static void testDeleteWithContextPathWithTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source + "." + table.paths[0])) {
      testDmlQuery(allocator,
        "DELETE FROM %s WHERE id = %s",
        new Object[]{table.name, table.originalData[0][0]}, table, 1, table.originalData[1]);
    }
  }

  // Two tables:
  //  1. source.source.table
  //  2. source.table
  // Context: [empty]
  //  Query: DELETE source.table => table 2
  // Context: source
  //  Query: DELETE source.table => table 1
  public static void testDeleteWithSourceAsPathTableWithWrongContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    String tableName = createRandomId();
    try (
      Table sourceTable = createTable(source, tableName, new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{0, "zero"},
          new Object[]{1, "one"}
        });
      Table sourceSourceTable = createTable(source, new String[]{source}, tableName, new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{0, "zero"},
          new Object[]{1, "one"}
        })) {
      testDmlQuery(allocator,
        "DELETE FROM %s.%s WHERE id = %s",
        new Object[]{source, tableName, 0}, sourceTable, 1, sourceTable.originalData[1]);
      verifyData(allocator, sourceSourceTable, sourceSourceTable.originalData);
    }
  }

  public static void testDeleteWithSourceAsPathTableWithContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    String tableName = createRandomId();
    try (
      Table sourceTable = createTable(source, tableName, new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{0, "zero"},
          new Object[]{1, "one"}
        })) {
      try (
        Table sourceSourceTable = createTable(source, new String[]{source}, tableName, new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{0, "zero"},
          new Object[]{1, "one"}
        });
        AutoCloseable ignored = setContext(allocator, source)) {
        testDmlQuery(allocator,
          "DELETE FROM %s.%s WHERE id = %s",
          new Object[]{source, tableName, 0}, sourceSourceTable, 1, sourceSourceTable.originalData[1]);
      }
      verifyData(allocator, sourceTable, sourceTable.originalData);
    }
  }

  public static void testDeleteWithStockIcebergTable(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = DmlQueryTestUtils.createStockIcebergTable(
      source, 2, 2, "test_delete_into_stock_iceberg");
         AutoCloseable ignored = setContext(allocator, source)) {

      testDmlQuery(allocator, "DELETE FROM %s WHERE id = 1", new Object[]{table.fqn, table.columns[1]},
        table, 0, null);
    }
  }
  // END: Contexts + Paths
}
