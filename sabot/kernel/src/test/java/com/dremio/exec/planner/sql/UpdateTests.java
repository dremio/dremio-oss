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

import static com.dremio.BaseTestQuery.getDfsTestTmpSchemaLocation;
import static com.dremio.BaseTestQuery.getIcebergTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.PARTITION_COLUMN_ONE_INDEX_SET;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Table;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Tables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicNonPartitionedAndPartitionedTables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTableWithDecimals;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTableWithDoubles;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTableWithFloats;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createRandomId;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createView;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createViewFromTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.setContext;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testDmlQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testMalformedDmlQueries;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.math.BigDecimal;
import java.util.Collections;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.iceberg.DataFile;
import org.junit.Assert;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.google.common.collect.Iterables;

/**
 * UPDATE tests.
 * <p>
 * Note: Add tests used across all platforms here.
 */
public class UpdateTests {

  public static void testMalformedUpdateQueries(String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 0);
         Table targetTable = createBasicTable(source, 3, 0)) {
      testMalformedDmlQueries(new Object[]{targetTable.fqn, targetTable.columns[1],
                      targetTable.columns[2], sourceTable.columns[2], sourceTable.fqn},
              "UPDATE",
              "UPDATE %s",
              "UPDATE %s SET",
              "UPDATE %s SET %s",
              "UPDATE %s SET =",
              "UPDATE %s SET = value",
              "UPDATE %s SET %s =",
              "UPDATE %s SET %s, %s, %s",
              "UPDATE %s SET %s = 'value1', %s =",
              "UPDATE %s SET %s = 'value1' WHERE",
              "UPDATE FROM %s AS t SET %s = 'value1' from",
              "UPDATE FROM %s AS t SET %s = 'value1 from %s as '",
              "UPDATE FROM %s AS t SET %s = 'value1' from %s where t.%s =",
              "UPDATE FROM %s AS t SET %s = 'value1' from (select * from %s) as",
              "UPDATE FROM %s AS t SET %s = 'value1' from (select * from %s) as s  where",
              "UPDATE FROM %s AS t SET %s = 'value1' from (select * from %s) as s  where t.%s =",
              "UPDATE FROM %s AS t SET %s = 'value1' from select * from %s as s where t.%s = s.%s"
      );
    }
  }

  public static void testUpdateOnView(BufferAllocator allocator, String source) throws Exception {
    String name = createRandomId();
    try (AutoCloseable ignored = createView(source, name)) {
      assertThatThrownBy(() ->
              testDmlQuery(allocator, "UPDATE %s.%s SET id = 0", new Object[]{source, name}, null, -1))
              .isInstanceOf(Exception.class)
              .hasMessageContaining("UPDATE is not supported on this VIEW at [%s.%s].", source, name);
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

  public static void testUpdateByIdWithEqualNull(BufferAllocator allocator, String source) throws Exception {
    // column = null should return false and no data should be updated
    try (Tables tables = createBasicNonPartitionedAndPartitionedTables(source, 2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (Table table : tables.tables) {
        testDmlQuery(allocator, "UPDATE %s SET id = %s WHERE id = %s",
            new Object[]{table.fqn, (int) table.originalData[5][0] * 10, null}, table, 0,
            ArrayUtils.subarray(table.originalData, 0, table.originalData.length));
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
        testDmlQuery(allocator,
          "" +
            "UPDATE %s SET column_0 = (SELECT column_1 FROM %s WHERE id = %s LIMIT 1)\n" +
            "WHERE id = %s",
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

  public static void testUpdateALLWithOneUnusedSourceTable(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 4);
         Table targetTable = createBasicTable(source, 2, 10)) {
      assertThatThrownBy(()
              -> testDmlQuery(allocator,
              "UPDATE %s SET id = 0 FROM %s",
              new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
              .isInstanceOf(Exception.class)
              .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testUpdateWithOneSourceTableFullPath(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 10);
         Table targetTable = createBasicTable(source, 2, 5);
         AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator, "UPDATE %s SET id = -%s.id,  column_0 = 'ASDF' " +
                      "FROM %s where %s.id=%s.id and %s.id < 3",
              new Object[]{targetTable.name, sourceTable.name, sourceTable.name,
                      targetTable.name, sourceTable.name, sourceTable.name}, targetTable, 3,
              new Object[]{0, "ASDF"},
              new Object[]{-1, "ASDF"},
              new Object[]{-2, "ASDF"},
              new Object[]{3, "3_0"},
              new Object[]{4, "4_0"});
    }
  }

  public static void testUpdateWithViewAsSourceTable(BufferAllocator allocator, String source) throws Exception {
    String viewName = createRandomId();
    try (Table sourceTable = createBasicTable(source, 3, 10);
         Table targetTable = createBasicTable(source, 2, 5);
         AutoCloseable ignore = createViewFromTable(source, viewName, sourceTable.fqn)) {
      testDmlQuery(allocator, "UPDATE %s SET id = -s.id,  column_0 = 'ASDF' " +
                      "FROM %s.%s AS s where %s.id=s.id and s.id < 3",
              new Object[]{targetTable.fqn, source, viewName,
                      targetTable.name}, targetTable, 3,
              new Object[]{0, "ASDF"},
              new Object[]{-1, "ASDF"},
              new Object[]{-2, "ASDF"},
              new Object[]{3, "3_0"},
              new Object[]{4, "4_0"});
    }
  }

  public static void testUpdateWithOneSourceTableNoAlias(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 10);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testDmlQuery(allocator, "UPDATE %s SET id = -%s.id,  column_0 = 'ASDF' " +
                      "FROM %s where %s.id=%s.id and %s.id < 3",
              new Object[]{targetTable.fqn, sourceTable.name, sourceTable.fqn,
                      targetTable.name, sourceTable.name, sourceTable.name}, targetTable, 3,
              new Object[]{0, "ASDF"},
              new Object[]{-1, "ASDF"},
              new Object[]{-2, "ASDF"},
              new Object[]{3, "3_0"},
              new Object[]{4, "4_0"});
    }
  }

  public static void testUpdateWithOneSourceTableUseAlias(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 10);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testDmlQuery(allocator,
        ""
          + "UPDATE %s AS t\n"
          + "SET id = -s.id,  column_0 = 'ASDF'\n"
          + "FROM %s s\n"
          + "WHERE t.id=s.id AND s.id < 3",
              new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, 3,
              new Object[]{0, "ASDF"},
              new Object[]{-1, "ASDF"},
              new Object[]{-2, "ASDF"},
              new Object[]{3, "3_0"},
              new Object[]{4, "4_0"});
    }
  }

  public static void testUpdateWithOneSourceTableSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 10);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testDmlQuery(allocator, "UPDATE %s as t SET id = -s.id,  column_0 = 'ASDF' " +
                      "FROM (select * from %s where id < 3) s where t.id=s.id",
              new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, 3,
              new Object[]{0, "ASDF"},
              new Object[]{-1, "ASDF"},
              new Object[]{-2, "ASDF"},
              new Object[]{3, "3_0"},
              new Object[]{4, "4_0"});
    }
  }

  public static void testUpdateWithTwoSourceTables(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable1 = createBasicTable(source, 3, 2);
         Table sourceTable2 = createBasicTable(source, 3, 3);
         Table targetTable = createBasicTable(source, 3, 5)) {
      testDmlQuery(allocator,
        "" +
          "UPDATE %s AS t\n" +
          "SET id = -s1.id,  column_0 = concat('s2_', s2.column_0)\n" +
          "FROM %s as s1, %s s2\n" +
          "WHERE t.id = s1.id and t.id = s2.id",
        new Object[]{targetTable.fqn, sourceTable1.fqn, sourceTable2.fqn},
        targetTable,
        2,
        ArrayUtils.addAll(
          new Object[][]{
            new Object[]{0, "s2_" + sourceTable1.originalData[0][1], targetTable.originalData[0][2]},
            new Object[]{-1, "s2_" + sourceTable1.originalData[1][1], targetTable.originalData[1][2]}},
          ArrayUtils.subarray(targetTable.originalData, 2, 5)));
    }
  }

  public static void testUpdateWithTwoSourceTableOneSourceQuery(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable1 = createBasicTable(source, 3, 2);
         Table sourceTable2 = createBasicTable(source, 3, 3);
         Table sourceTable3 = createBasicTable(source, 3, 4);
         Table targetTable = createBasicTable(source, 4, 5)) {
      testDmlQuery(allocator,
        ""
          + "UPDATE %s AS t\n"
          + "SET id = -s1.id,  column_0 = concat('s2_', s2.column_0), column_1 =  concat('s3_', s3.column_1)\n"
          + "FROM %s as s1, (SELECT * from %s) as s2, %s s3\n"
          + "WHERE t.id = s1.id AND t.id = s2.id AND t.id = s3.id",
              new Object[]{targetTable.fqn, sourceTable1.fqn, sourceTable2.fqn, sourceTable3.fqn}, targetTable, 2,
              ArrayUtils.addAll(
                      new Object[][]{
                              new Object[]{0, "s2_" + sourceTable1.originalData[0][1], "s3_" + sourceTable2.originalData[0][2], targetTable.originalData[0][3]},
                              new Object[]{-1, "s2_" + sourceTable1.originalData[1][1], "s3_" + sourceTable2.originalData[1][2], targetTable.originalData[1][3]}},
                      ArrayUtils.subarray(
                              targetTable.originalData, 2, 5)));
    }
  }

  public static void testUpdateWithDupsInSource(BufferAllocator allocator, String source) throws Exception {
    try (
            Table sourceTable = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
                            new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
                            new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.VARCHAR, false),
                            new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.VARCHAR, false)
                    },
                    new Object[][]{
                            new Object[]{10, "insert-1", "source 1.1"},
                            new Object[]{10, "insert-1 dupe key", "source 1.2"},
                            new Object[]{20, "second insert-1", "source 2.1"}
                    });
            Table targetTable = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
                            new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
                            new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.VARCHAR, false),
                            new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.VARCHAR, false)
                    },
                    new Object[][]{
                            new Object[]{10, "insert-1", "target 1.1"},
                            new Object[]{20, "second insert-1", "target 2.1"},
                            new Object[]{30, "third insert-1", "target 3.1"}
                    })) {
      assertThatThrownBy(()
              -> testDmlQuery(allocator,
              "UPDATE %s t SET data1 = s.data1 from %s s where t.id = s.id",
              new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
              .isInstanceOf(Exception.class)
              .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testUpdateWithUnrelatedConditionToSourceTableNoCondition(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 4);
         Table targetTable = createBasicTable(source, 2, 10)) {
      assertThatThrownBy(()
              -> testDmlQuery(allocator,
              "UPDATE %s AS t Set id = t.id + s.id from %s s",
              new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
              .isInstanceOf(Exception.class)
              .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testUpdateWithUnrelatedConditionToSourceTable(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 3, 4);
         Table targetTable = createBasicTable(source, 2, 10)) {
      assertThatThrownBy(()
              -> testDmlQuery(allocator,
              "UPDATE %s AS t Set id = -t.id from %s where t.id < 2",
              new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
              .isInstanceOf(Exception.class)
              .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  public static void testUpdateWithSchemaNotMatch(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
          new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
          new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.DATE, false),
          new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, "2001-01-01" , "source 1.2"},
          new Object[]{20, "2001-01-01", "source 2.2"},
          new Object[]{30, "2001-01-01",  "source 3.2"}
        });
      Table targetTable = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
          new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
          new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.FLOAT, false),
          new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, 1.0, "target 1.1"},
          new Object[]{20, 2.0, "target 2.1"},
          new Object[]{30, 3.0, "target 3.1"}
        })) {

      // One target column
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "UPDATE %s AS t Set data1 = s.data1 from %s s where s.id = t.id",
        new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("VALIDATION ERROR");

      // two target columns
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "UPDATE %s AS t Set id = -s.id,  data1 = s.data1 from %s s where s.id = t.id",
        new Object[]{targetTable.fqn, sourceTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("VALIDATION ERROR");
    }
  }

  public static void testUpdateWithImplicitTypeCasting1(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table1 = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
          new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
          new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.VARCHAR, false),
          new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.DOUBLE, false)
        },
        new Object[][]{
          new Object[]{10, "1.1", 4.1d},
          new Object[]{20, "2.1", 5.2d},
          new Object[]{30, "3.1", 6.2d}
        });
      Table table2 = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
          new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
          new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.FLOAT, false),
          new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.INTEGER, false)
        },
        new Object[][]{
          new Object[]{10, 1.0, 1},
          new Object[]{20, 2.0, 2},
          new Object[]{30, 3.0, 3}
        })) {
      // test1 as the source table, test2 as the target table
      testDmlQuery(allocator,
        "UPDATE %s AS t Set data1 = s.data1, data2 = s.data2 from %s s where s.id = t.id",
        new Object[]{table2.fqn, table1.fqn}, table2, 3,
        new Object[][]{
          new Object[]{10, 1.1f, 4},
          new Object[]{20, 2.1f, 5},
          new Object[]{30, 3.1f, 6}
        });
    }
  }

  public static void testUpdateWithImplicitTypeCasting2(BufferAllocator allocator, String source) throws Exception {
    try (
      Table table1 = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
          new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
          new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.VARCHAR, false),
          new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.DOUBLE, false)
        },
        new Object[][]{
          new Object[]{10, "1.1", 4.1d},
          new Object[]{20, "2.1", 5.2d},
          new Object[]{30, "3.1", 6.2d}
        });
      Table table2 = createTable(source, createRandomId(), new DmlQueryTestUtils.ColumnInfo[]{
          new DmlQueryTestUtils.ColumnInfo("id", SqlTypeName.INTEGER, false),
          new DmlQueryTestUtils.ColumnInfo("data1", SqlTypeName.FLOAT, false),
          new DmlQueryTestUtils.ColumnInfo("data2", SqlTypeName.INTEGER, false)
        },
        new Object[][]{
          new Object[]{10, 1.0, 1},
          new Object[]{20, 2.0, 2},
          new Object[]{30, 3.0, 3}
        })) {
      // test2 as the source table, test1 as the target table
      testDmlQuery(allocator,
        "UPDATE %s AS t Set data1 = s.data1, data2 = s.data2 from %s s where s.id = t.id",
        new Object[]{table1.fqn, table2.fqn}, table1, 3,
        new Object[][]{
          new Object[]{10, "1.0", 1.0d},
          new Object[]{20, "2.0", 2.0d},
          new Object[]{30, "3.0", 3.0d}
        });
    }
  }

  private  static void testUpdateWithPartitionTransformation(BufferAllocator allocator,
                                                             Table table,
                                                             String droppedPartitionTransformField,
                                                             String addedPartitionTransformClause,
                                                             org.apache.iceberg.Table icebergTable,
                                                             int expectedDataFileCount) throws Exception {
    if (droppedPartitionTransformField != null) {
      String partitionTransformQuery = String.format("ALTER TABLE  %s DROP PARTITION FIELD %s", table.fqn, droppedPartitionTransformField);
      BaseTestQuery.test(partitionTransformQuery);
    }

    String partitionTransformQuery = String.format("ALTER TABLE  %s ADD PARTITION FIELD %s", table.fqn, addedPartitionTransformClause);
    BaseTestQuery.test(partitionTransformQuery);

    testDmlQuery(allocator, "UPDATE %s SET id = id",
      new Object[]{table.fqn}, table, 10000, table.originalData);

    icebergTable.refresh();
    Iterable<DataFile> dataFiles = icebergTable.currentSnapshot().addedDataFiles(icebergTable.io());
    Assert.assertEquals(expectedDataFileCount, Iterables.size(dataFiles));
  }

  public  static void testUpdateWithPartitionTransformation(BufferAllocator allocator, String source) throws Exception {
    String tableName = "updateWithPartitionTransformation";
    try (Table table = createBasicTable(0, source, 2, 10000, tableName)) {
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), tableName);
      org.apache.iceberg.Table icebergTable = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);

      // transformation function: "bucket(3, id)", 3 data files are expected to generate
      testUpdateWithPartitionTransformation(allocator, table, null, "bucket(3, id)", icebergTable, 3);

      // transformation function: "truncate(1, column_0)", 10 data files are expected to generate for truncated values (0..9)
      testUpdateWithPartitionTransformation(allocator, table, "bucket(3, id)", "truncate(1, column_0)", icebergTable, 10);
    }
  }

  // BEGIN: Contexts + Paths
  public static void testUpdateWithWrongContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
            Table table = createBasicTable(source, 1, 2, 2)) {
      assertThatThrownBy(() ->
              testDmlQuery(allocator,
                      "UPDATE %s.%s SET id = 1 WHERE id = %s",
                      new Object[]{table.paths[0], table.name, table.originalData[0][0]}, table, -1))
              .isInstanceOf(Exception.class)
              .hasMessageContaining("Table [%s.%s] does not exist.", table.paths[0], table.name);
    }
  }

  public static void testUpdateWithContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
            Table table = createBasicTable(source, 1, 2, 2);
            AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
              "UPDATE %s.%s SET %s = 'One' WHERE id = %s",
              new Object[]{table.paths[0], table.name, table.columns[1], table.originalData[0][0]}, table, 1,
              new Object[]{0, "One"}, table.originalData[1]);
    }
  }

  public static void testUpdateWithStockIcebergTable(BufferAllocator allocator, String source) throws Exception {
    try (DmlQueryTestUtils.Table table = DmlQueryTestUtils.createStockIcebergTable(
      source, 2, 2, "test_update_into_stock_iceberg");
         AutoCloseable ignored = setContext(allocator, source)) {

      testDmlQuery(allocator, "UPDATE %s SET %s = 'One' WHERE id = 1", new Object[]{table.fqn, table.columns[1]},
        table, 0, null);
    }
  }
  // END: Contexts + Paths
}
