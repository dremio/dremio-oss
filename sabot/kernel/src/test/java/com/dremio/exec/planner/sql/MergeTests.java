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
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.PARTITION_COLUMN_ONE_INDEX_SET;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Table;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.Tables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicNonPartitionedAndPartitionedTables;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTableWithFloats;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createRandomId;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createStockIcebergTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createView;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.setContext;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testDmlQuery;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.testMalformedDmlQueries;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.lang3.ArrayUtils;

/**
 * MERGE tests.
 *
 * Note: Add tests used across all platforms here.
 */
public class MergeTests {

  private static final Object[][] COLUMN_ALL_777S = new Object[][]{
    new Object[]{0, "777"},
    new Object[]{1, "777"},
    new Object[]{2, "777"},
    new Object[]{3, "777"},
    new Object[]{4, "777"}};
  private static final Object[][] COLUMN_ALL_777S_WITH_FLOATS = new Object[][]{
    new Object[]{0, 777.0f},
    new Object[]{1, 777.0f},
    new Object[]{2, 777.0f},
    new Object[]{3, 777.0f},
    new Object[]{4, 777.0f}};
  private static final Object[][] COLUMN_AND_ID_ALL_777S = new Object[][]{
    new Object[]{777, "777"},
    new Object[]{777, "777"},
    new Object[]{777, "777"},
    new Object[]{777, "777"},
    new Object[]{777, "777"}};
  private static final Object[][] NULL_IDS_AND_COLUMN_ALL_777S_WITH_FLOATS = new Object[][]{
    new Object[]{null, 777.0f},
    new Object[]{null, 777.0f},
    new Object[]{null, 777.0f},
    new Object[]{null, 777.0f},
    new Object[]{null, 777.0f}};
  private static final Object[][] COLUMN_AND_ID_ALL_777S_WITH_FLOATS = new Object[][]{
    new Object[]{777, 777.0f},
    new Object[]{777, 777.0f},
    new Object[]{777, 777.0f},
    new Object[]{777, 777.0f},
    new Object[]{777, 777.0f}};

  public static void testMalformedMergeQueries(String source) throws Exception {
    try (Table sourceTable = createBasicTable(source,3, 0);
         Table targetTable = createBasicTable(source,2, 0)) {
      testMalformedDmlQueries(new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, targetTable.columns[1]},
        "MERGE",
        "MERGE INTO",
        "MERGE INTO %s",
        "MERGE INTO %s USING",
        "MERGE INTO %s USING %s",
        "MERGE INTO %s USING %s ON",
        "MERGE INTO %s USING %s ON %s.id = %s.id",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE SET",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE SET id =",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE SET id = 2 WHEN NOT MATCHED THEN",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE SET id = 2 WHEN NOT MATCHED THEN INSERT",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE SET id = 2 WHEN NOT MATCHED THEN INSERT(id, %s)",
        "MERGE INTO %s USING %s ON %s.id = %s.id WHEN MATCHED THEN UPDATE SET id = 2 WHEN NOT MATCHED THEN INSERT(id, %s) VALUES"
      );
    }
  }

  public static void testMergeOnView(BufferAllocator allocator, String source) throws Exception {
    String name = createRandomId();
    try (AutoCloseable ignored = createView(source, name)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator, "MERGE INTO %s.%s target USING %s.%s source ON (target.id = source.id) WHEN MATCHED THEN UPDATE SET id = 0",
          new Object[]{source, name, source, name}, null, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("MERGE is not supported on this VIEW at [%s.%s].", source, name);
    }
  }

  public static void testMergeUpdateAllWithLiteral(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = %s",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, "777"},
          targetTable, 5, COLUMN_ALL_777S);
      }
    }
  }

  public static void testMergeUpdateAllWithScalar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = column_1",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn}, targetTable, 5,
          new Object[]{0, sourceTable.originalData[0][2]},
          new Object[]{1, sourceTable.originalData[1][2]},
          new Object[]{2, sourceTable.originalData[2][2]},
          new Object[]{3, sourceTable.originalData[3][2]},
          new Object[]{4, sourceTable.originalData[4][2]});
      }
    }
  }

  public static void testMergeUpdateAllWithSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = (SELECT column_1 FROM %s WHERE id = %s)",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, sourceTable.fqn,
            sourceTable.originalData[3][0]}, targetTable, 5,
          new Object[]{0, sourceTable.originalData[3][2]},
          new Object[]{1, sourceTable.originalData[3][2]},
          new Object[]{2, sourceTable.originalData[3][2]},
          new Object[]{3, sourceTable.originalData[3][2]},
          new Object[]{4, sourceTable.originalData[3][2]});
      }
    }
  }

  public static void testMergeUpdateHalfWithLiteral(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = %s",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, "777"},
          targetTable, 5,
          ArrayUtils.addAll(COLUMN_ALL_777S,
            ArrayUtils.subarray(targetTable.originalData, 5, targetTable.originalData.length)));
      }
    }
  }

  public static void testMergeUpdateHalfWithScalar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = column_1",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn}, targetTable, 5,
          ArrayUtils.addAll(
            new Object[][]{
              new Object[]{0, sourceTable.originalData[0][2]},
              new Object[]{1, sourceTable.originalData[1][2]},
              new Object[]{2, sourceTable.originalData[2][2]},
              new Object[]{3, sourceTable.originalData[3][2]},
              new Object[]{4, sourceTable.originalData[4][2]}},
            ArrayUtils.subarray(targetTable.originalData, 5, targetTable.originalData.length)));
      }
    }
  }

  public static void testMergeUpdateHalfWithSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = (SELECT column_1 FROM %s WHERE id = %s)",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, sourceTable.fqn,
            sourceTable.originalData[3][0]}, targetTable, 5,
          ArrayUtils.addAll(
            new Object[][]{
              new Object[]{0, sourceTable.originalData[3][2]},
              new Object[]{1, sourceTable.originalData[3][2]},
              new Object[]{2, sourceTable.originalData[3][2]},
              new Object[]{3, sourceTable.originalData[3][2]},
              new Object[]{4, sourceTable.originalData[3][2]}},
            ArrayUtils.subarray(targetTable.originalData, 5, targetTable.originalData.length)));
      }
    }
  }

  public static void testMergeUpdateWithFloat(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTableWithFloats(source,3, 10);
         Table targetTable = createBasicTableWithFloats(source,2, 5)) {
      testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
          + " WHEN MATCHED THEN UPDATE SET column_0 = %s",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, 777.0f},
        targetTable, 5,
        COLUMN_ALL_777S_WITH_FLOATS);
    }
  }

  public static void testMergeUpdateUsingSubQueryWithLiteral(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 15, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING (SELECT * FROM %s WHERE id < 5) s ON (%s.id = s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = %s",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, "777"}, targetTable, 5,
          ArrayUtils.addAll(COLUMN_ALL_777S,
            ArrayUtils.subarray(targetTable.originalData, 5, targetTable.originalData.length)));
      }
    }
  }

  public static void testMergeUpdateWithStar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,2, 3, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING (select id, concat(column_0, '_updated') from %s) as s ON (%s.id = s.id)"
            + " WHEN MATCHED THEN UPDATE SET *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, 3,
          new Object[][]{
            new Object[]{0, sourceTable.originalData[0][1] + "_updated"},
            new Object[]{1, sourceTable.originalData[1][1] + "_updated"},
            new Object[]{2, sourceTable.originalData[2][1] + "_updated"},
            new Object[]{3, targetTable.originalData[3][1]},
            new Object[]{4, targetTable.originalData[4][1]}});
      }
    }
  }

  public static void testMergeUpdateWithStarColumnCountNotMatch1(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 3, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        assertThatThrownBy(()
          -> testDmlQuery(allocator, "MERGE INTO %s USING %s as s ON %s.id = s.id"
            + " WHEN MATCHED THEN UPDATE SET *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, -1))
          .isInstanceOf(Exception.class)
          .hasMessageContaining("VALIDATION ERROR");
      }
    }
  }

  public static void testMergeUpdateWithStarColumnCountNotMatch2(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,2, 3, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,3, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        assertThatThrownBy(()
          -> testDmlQuery(allocator, "MERGE INTO %s USING %s as s ON %s.id = s.id"
            + " WHEN MATCHED THEN UPDATE SET *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, -1))
          .isInstanceOf(Exception.class)
          .hasMessageContaining("VALIDATION ERROR");
      }
    }
  }

  public static void testMergeUpdateWithStarSchemaNotMatchUpdateOnly(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data1", SqlTypeName.DATE, false),
          new ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, "2001-01-01", "source 1.2"},
          new Object[]{20, "2001-01-01", "source 2.2"},
          new Object[]{30, "2001-01-01", "source 3.2"}
        });
      Table targetTable = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data1", SqlTypeName.FLOAT, false),
          new ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, 1.0, "target 1.1"},
          new Object[]{20, 2.0, "target 2.1"},
          new Object[]{30, 3.0, "target 3.1"}
        })) {
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "MERGE INTO %s\n" +
          "USING %s source\n" +
          "ON source.id = %s.id\n" +
          "WHEN MATCHED THEN\n" +
          "UPDATE SET * ",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("VALIDATION ERROR");
    }
  }

  public static void testMergeUpdateWithStarSchemaNotMatch(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data1", SqlTypeName.VARCHAR, false),
          new ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, "insert-1", "source 1.1"},
          new Object[]{20, "insert-1 dupe key", "source 1.2"},
          new Object[]{30, "second insert-1", "source 2.1"}
        });
      Table targetTable = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data1", SqlTypeName.FLOAT, false),
          new ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, 1.0, "target 1.1"},
          new Object[]{20, 2.0, "target 2.1"},
          new Object[]{30, 3.0, "target 3.1"}
        })) {
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "MERGE INTO %s\n" +
          "USING %s source\n" +
          "ON source.id = %s.id\n" +
          "WHEN MATCHED THEN\n" +
          "UPDATE SET * " +
          "WHEN NOT MATCHED THEN\n" +
          "INSERT VALUES(source.id, source.data1, 'Inserted from src on merge')",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("VALIDATION ERROR");
    }
  }

  public static void testMergeUpdateInsertWithStar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 3, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s as s ON (%s.id = s.id)"
            + " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, 5,
          ArrayUtils.subarray(sourceTable.originalData, 0, sourceTable.originalData.length));
      }
    }
  }

  public static void testMergeUpdateInsertWithReversedColumnSelectInSource(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 2, 5);
         Table targetTable = createBasicTable(source,2, 3)) {
        testDmlQuery(allocator, "MERGE INTO %s USING (select id, column_0 from (select column_0, id from %s)) as s ON (%s.id = s.id)"
            + " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, 5,
          ArrayUtils.subarray(sourceTable.originalData, 0, sourceTable.originalData.length));
    }
  }

  public static void testMergeInsertWithScalar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING (SELECT * FROM %s) as s ON (%s.id = s.id)"
          + " WHEN NOT MATCHED THEN INSERT VALUES(s.id, s.column_0)",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, 5,
          ArrayUtils.addAll(sourceTable.originalData));
      }
    }
  }

  public static void testMergeInsertWithLiteral(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN NOT MATCHED THEN INSERT VALUES(%s, '%s')",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, 777, "777"}, targetTable, 5,
          ArrayUtils.addAll(COLUMN_AND_ID_ALL_777S, targetTable.originalData));
      }
    }
  }

  public static void testMergeInsertWithFloat(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTableWithFloats(source,3, 10);
         Table targetTable = createBasicTableWithFloats(source,2, 5)) {
      testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
          + " WHEN NOT MATCHED THEN INSERT (column_0) VALUES(%s)",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, 777.0f}, targetTable, 5,
        ArrayUtils.addAll(NULL_IDS_AND_COLUMN_ALL_777S_WITH_FLOATS, targetTable.originalData));
    }
  }

  public static void testMergeInsertWithStar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN NOT MATCHED THEN INSERT *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn}, targetTable, 5,
          ArrayUtils.addAll(
            ArrayUtils.subarray(sourceTable.originalData, 5, sourceTable.originalData.length),
            ArrayUtils.subarray(targetTable.originalData, 0, targetTable.originalData.length)));
      }
    }
  }

  public static void testMergeUpdateInsertWithLiteral(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = '%s'"
            + " WHEN NOT MATCHED THEN INSERT VALUES(%s, '%s')",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, "777", 777, "777"}, targetTable, 10,
          ArrayUtils.addAll(COLUMN_ALL_777S, COLUMN_AND_ID_ALL_777S));
      }
    }
  }

  public static void testMergeInsertWithStarColumnCountNotMatch(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 3, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        assertThatThrownBy(()
          -> testDmlQuery(allocator, "MERGE INTO %s USING %s as s ON %s.id = s.id"
            + " WHEN NOT MATCHED THEN INSERT *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, -1))
          .isInstanceOf(Exception.class)
          .hasMessageContaining("VALIDATION ERROR");
      }
    }
  }

  public static void testMergeUpdateInsertWithFloats(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTableWithFloats(source,3, 10);
         Table targetTable = createBasicTableWithFloats(source,2, 5)) {
      testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
          + " WHEN MATCHED THEN UPDATE SET column_0 = %s"
          + " WHEN NOT MATCHED THEN INSERT VALUES(%s, %s)",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, 777.0f, 777, 777.0f},
        targetTable, 10,
        ArrayUtils.addAll(COLUMN_ALL_777S_WITH_FLOATS, COLUMN_AND_ID_ALL_777S_WITH_FLOATS));
    }
  }

  public static void testMergeUpdateInsertWithScalar(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = column_1"
            + " WHEN NOT MATCHED THEN INSERT VALUES(%s, '%s')",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, 777, "777"},
          targetTable, 10,
          ArrayUtils.addAll(
            new Object[][]{
              new Object[]{0, sourceTable.originalData[0][2]},
              new Object[]{1, sourceTable.originalData[1][2]},
              new Object[]{2, sourceTable.originalData[2][2]},
              new Object[]{3, sourceTable.originalData[3][2]},
              new Object[]{4, sourceTable.originalData[4][2]}},
            COLUMN_AND_ID_ALL_777S));
      }
    }
  }

  public static void testMergeUpdateInsertWithSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,3, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id and %s.column_0 = %s.column_0)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = (SELECT column_1 FROM %s WHERE id = %s)"
            + " WHEN NOT MATCHED THEN INSERT VALUES(%s, '%s')",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn,
            sourceTable.fqn, sourceTable.originalData[3][0], 777, "777"}, targetTable, 10,
          ArrayUtils.addAll(
            new Object[][]{
              new Object[]{0, sourceTable.originalData[3][2]},
              new Object[]{1, sourceTable.originalData[3][2]},
              new Object[]{2, sourceTable.originalData[3][2]},
              new Object[]{3, sourceTable.originalData[3][2]},
              new Object[]{4, sourceTable.originalData[3][2]}},
            COLUMN_AND_ID_ALL_777S));
      }
    }
  }

  public static void testMergeUpdateInsertStarWithSubQuery(BufferAllocator allocator, String source) throws Exception {
    try (Tables sourceTables = createBasicNonPartitionedAndPartitionedTables(source,2, 10, PARTITION_COLUMN_ONE_INDEX_SET);
         Tables targetTables = createBasicNonPartitionedAndPartitionedTables(source,2, 5, PARTITION_COLUMN_ONE_INDEX_SET)) {
      for (int i = 0; i < sourceTables.tables.length; i++) {
        Table sourceTable = sourceTables.tables[i];
        Table targetTable = targetTables.tables[i];
        testDmlQuery(allocator, "MERGE INTO %s USING %s ON (%s.id = %s.id and %s.column_0 = %s.column_0)"
            + " WHEN MATCHED THEN UPDATE SET column_0 = (SELECT column_0 FROM %s WHERE id = %s)"
            + " WHEN NOT MATCHED THEN INSERT *",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn, targetTable.fqn, sourceTable.fqn,
            sourceTable.fqn, sourceTable.originalData[2][0]}, targetTable, 10,
          ArrayUtils.addAll(
            new Object[][]{
              new Object[]{0, sourceTable.originalData[2][1]},
              new Object[]{1, sourceTable.originalData[2][1]},
              new Object[]{2, sourceTable.originalData[2][1]},
              new Object[]{3, sourceTable.originalData[2][1]},
              new Object[]{4, sourceTable.originalData[2][1]}},
            ArrayUtils.subarray(sourceTable.originalData, 5, sourceTable.originalData.length)));
      }
    }
  }

  public static void testMergeWithSubQuerySourceAndInsert(BufferAllocator allocator, String source) throws Exception {
    try (Table sourceTable = createBasicTable(source, 2, 10);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testDmlQuery(allocator, "MERGE INTO %s USING (SELECT * FROM %s WHERE id < 5 OR id > 7) s ON (%s.id = s.id)"
              + " WHEN MATCHED THEN UPDATE SET column_0 = %s"
              + " WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.column_0)",
          new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, "777", 777, "777"}, targetTable, 7,
          ArrayUtils.addAll(
              COLUMN_ALL_777S,
              ArrayUtils.subarray(sourceTable.originalData, 8, sourceTable.originalData.length)));
    }
  }

  public static void testMergeTargetTableWithAndWithoutAlias(BufferAllocator allocator, String source) throws Exception {
    // without target table aliasing
    try (Table sourceTable = createBasicTable(source, 2, 10);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testDmlQuery(allocator, "MERGE INTO %s USING (SELECT * FROM %s WHERE id < 5 OR id > 7) s ON (%s.id = s.id)"
          + " WHEN MATCHED THEN UPDATE SET column_0 = %s"
          + " WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.column_0)",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn, "777", 777, "777"}, targetTable, 7,
        ArrayUtils.addAll(
          COLUMN_ALL_777S,
          ArrayUtils.subarray(sourceTable.originalData, 8, sourceTable.originalData.length)));
    }

    //  with target table aliasing
    try (Table sourceTable = createBasicTable(source, 2, 10);
         Table targetTable = createBasicTable(source, 2, 5)) {
      testDmlQuery(allocator, "MERGE INTO %s as t USING (SELECT * FROM %s WHERE id < 5 OR id > 7) s ON (t.id = s.id)"
          + " WHEN MATCHED THEN UPDATE SET column_0 = %s"
          + " WHEN NOT MATCHED THEN INSERT VALUES (s.id, s.column_0)",
        new Object[]{targetTable.fqn, sourceTable.fqn, "777", 777, "777"}, targetTable, 7,
        ArrayUtils.addAll(
          COLUMN_ALL_777S,
          ArrayUtils.subarray(sourceTable.originalData, 8, sourceTable.originalData.length)));
    }
  }

  public static void testMergeWithDupsInSource(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data1", SqlTypeName.VARCHAR, false),
          new ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, "insert-1", "source 1.1"},
          new Object[]{10, "insert-1 dupe key", "source 1.2"},
          new Object[]{20, "second insert-1", "source 2.1"}
        });
      Table targetTable = createTable(source, createRandomId(), new ColumnInfo[]{
          new ColumnInfo("id", SqlTypeName.INTEGER, false),
          new ColumnInfo("data1", SqlTypeName.VARCHAR, false),
          new ColumnInfo("data2", SqlTypeName.VARCHAR, false)
        },
        new Object[][]{
          new Object[]{10, "insert-1", "target 1.1"},
          new Object[]{20, "second insert-1", "target 2.1"},
          new Object[]{30, "third insert-1", "target 3.1"}
        })) {
      assertThatThrownBy(()
        -> testDmlQuery(allocator,
        "MERGE INTO %s\n" +
          "USING %s source\n" +
          "ON source.id = %s.id\n" +
          "WHEN MATCHED THEN\n" +
          "UPDATE SET data2 = 'Updated from source on merge'\n" +
          "WHEN NOT MATCHED THEN\n" +
          "INSERT VALUES(source.id, source.data1, 'Inserted from src on merge')",
        new Object[]{targetTable.fqn, sourceTable.fqn, targetTable.fqn}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("A target row matched more than once. Please update your query.");
    }
  }

  // BEGIN: Contexts + Paths
  public static void testMergeWithWrongContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createBasicTable(source, 1, 2, 3);
      Table targetTable = createBasicTable(source, 1, 2, 2)) {
      assertThatThrownBy(() ->
        testDmlQuery(allocator,
          "MERGE INTO %s.%s target\n" +
            "USING %s.%s source\n" +
            "ON source.id = target.id\n" +
            "WHEN MATCHED THEN\n" +
            "UPDATE SET %s = source.%s\n" +
            "WHEN NOT MATCHED THEN\n" +
            "INSERT VALUES(source.%s, source.%s)",
          new Object[]{targetTable.paths[0], targetTable.name, sourceTable.paths[0], sourceTable.name,
            targetTable.columns[1], sourceTable.columns[1], sourceTable.columns[0], sourceTable.columns[1]}, targetTable, -1))
        .isInstanceOf(Exception.class)
        .hasMessageContaining("Table [%s.%s] does not exist.", targetTable.paths[0], targetTable.name);
    }
  }

  public static void testMergeWithContextWithPathTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createBasicTable(source, 1, 2, 3);
      Table targetTable = createBasicTable(source, 1, 2, 2);
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "MERGE INTO %s.%s target\n" +
          "USING %s.%s source\n" +
          "ON source.id = target.id\n" +
          "WHEN MATCHED THEN\n" +
          "UPDATE SET %s = source.%s\n" +
          "WHEN NOT MATCHED THEN\n" +
          "INSERT VALUES(source.%s, source.%s)",
        new Object[]{targetTable.paths[0], targetTable.name, sourceTable.paths[0], sourceTable.name,
          targetTable.columns[1], sourceTable.columns[1], sourceTable.columns[0], sourceTable.columns[1]}, targetTable, 3,
        new Object[]{targetTable.originalData[0][0], sourceTable.originalData[0][1]},
        new Object[]{targetTable.originalData[1][0], sourceTable.originalData[1][1]},
        sourceTable.originalData[2]);
    }
  }

  public static void testMergeWithStockIcebergTargetTable(BufferAllocator allocator, String source) throws Exception {
    try (
      Table sourceTable = createBasicTable(source, 1, 2, 0);
      Table targetTable = createStockIcebergTable(source, 1, 2, "merge_target_table");
      AutoCloseable ignored = setContext(allocator, source)) {
      testDmlQuery(allocator,
        "MERGE INTO %s.%s target\n" +
          "USING %s.%s source\n" +
          "ON source.id = target.id\n" +
          "WHEN MATCHED THEN\n" +
          "UPDATE SET %s = source.%s\n" +
          "WHEN NOT MATCHED THEN\n" +
          "INSERT VALUES(source.%s, source.%s)",
        new Object[]{targetTable.paths[0], targetTable.name, sourceTable.paths[0], sourceTable.name,
          targetTable.columns[1], sourceTable.columns[1], sourceTable.columns[0], sourceTable.columns[1]}, targetTable, 0,
        null);
    }
  }
  // END: Contexts + Paths
}
