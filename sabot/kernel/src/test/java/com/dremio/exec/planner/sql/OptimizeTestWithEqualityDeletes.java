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

import static com.dremio.exec.ExecConstants.ENABLE_OPTIMIZE_WITH_EQUALITY_DELETE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.dremio.BaseTestQuery;
import com.dremio.TestBuilder;
import com.dremio.exec.store.iceberg.IcebergTestTables;
import org.apache.arrow.memory.BufferAllocator;

public class OptimizeTestWithEqualityDeletes extends BaseTestQuery {
  private static IcebergTestTables.Table tableWithEqDeletes;

  public static void init() throws Exception {
    setSystemOption(ENABLE_OPTIMIZE_WITH_EQUALITY_DELETE.getOptionName(), "true");
    tableWithEqDeletes = IcebergTestTables.PRODUCTS_WITH_EQ_DELETES.get();
  }

  public static void teardown() throws Exception {
    resetSystemOption(ENABLE_OPTIMIZE_WITH_EQUALITY_DELETE.getOptionName());
    String forgetQuery =
        String.format("ALTER TABLE %s FORGET METADATA", tableWithEqDeletes.getTableName());
    runSQL(forgetQuery);
    tableWithEqDeletes.close();
  }

  private static void assertOptimizeWithOptions(
      BufferAllocator allocator,
      String table,
      String options,
      long expectedRewrittenDataFiles,
      long expectedRewrittenDeleteFiles,
      long expectedNewDataFiles)
      throws Exception {
    new TestBuilder(allocator)
        .sqlQuery(String.format("optimize table %s REWRITE DATA(%s)", table, options))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(
            expectedRewrittenDataFiles, expectedRewrittenDeleteFiles, expectedNewDataFiles)
        .build()
        .run();
  }

  private static void assertActualCountAfterOptimize(String table, int expectedCount)
      throws Exception {
    int actualCount = testSql(String.format("select * from %s", table));
    assertEquals(expectedCount, actualCount);
  }

  public static void testV2OptimizeEqualityDeletes(BufferAllocator allocator) throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    new TestBuilder(allocator)
        .sqlQuery(String.format("Optimize table %s ", tableWithEqDeletes.getTableName()))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(5L, 7L, 3L)
        .build()
        .run();

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesMinInputFiles(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    assertOptimizeWithOptions(
        allocator, tableWithEqDeletes.getTableName(), "MIN_INPUT_FILES=10", 5L, 7L, 3L);

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesLargeMinInputFiles(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    assertOptimizeWithOptions(
        allocator, tableWithEqDeletes.getTableName(), "MIN_INPUT_FILES=50", 0L, 0L, 0L);

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesMinInputFilesAndSize(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    assertOptimizeWithOptions(
        allocator,
        tableWithEqDeletes.getTableName(),
        "MIN_INPUT_FILES=5, MIN_FILE_SIZE_MB=0",
        5L,
        7L,
        3L);

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesPartitionEvolution(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    int originalNumberPartitions =
        testSql(
            String.format(
                "SELECT \"partition\" FROM TABLE(TABLE_FILES('%s'))",
                tableWithEqDeletes.getTableName()));

    runSQL(
        String.format(
            "alter table %s add partition field color", tableWithEqDeletes.getTableName()));

    assertOptimizeWithOptions(
        allocator,
        tableWithEqDeletes.getTableName(),
        "MIN_INPUT_FILES=1, MIN_FILE_SIZE_MB=0",
        5L,
        7L,
        30L);

    // verify number of partitions updated to follow the partition evolution
    int newNumberPartitions =
        testSql(
            String.format(
                "SELECT \"partition\" FROM TABLE(TABLE_FILES('%s'))",
                tableWithEqDeletes.getTableName()));
    assertNotEquals(originalNumberPartitions, newNumberPartitions);

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesForIdentityPartitions(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "optimize table %s FOR PARTITIONS category IN ('gadget', 'widget')",
                tableWithEqDeletes.getTableName()))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(3L, 5L, 2L)
        .build()
        .run();

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesManifestOnly(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    new TestBuilder(allocator)
        .sqlQuery(
            String.format("optimize table %s REWRITE MANIFESTS", tableWithEqDeletes.getTableName()))
        .unOrdered()
        .baselineColumns("summary")
        .baselineValues("Optimize table successful")
        .build()
        .run();

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }

  public static void testV2OptimizeEqualityDeletesDataBinpack(BufferAllocator allocator)
      throws Exception {
    int countBeforeOptimize =
        testSql(String.format("select * from %s", tableWithEqDeletes.getTableName()));

    new TestBuilder(allocator)
        .sqlQuery(
            String.format(
                "Optimize table %s REWRITE DATA USING BIN_PACK", tableWithEqDeletes.getTableName()))
        .unOrdered()
        .baselineColumns(
            "rewritten_data_files_count", "rewritten_delete_files_count", "new_data_files_count")
        .baselineValues(5L, 7L, 3L)
        .build()
        .run();

    // Verify select query returns the same count after optimize
    assertActualCountAfterOptimize(tableWithEqDeletes.getTableName(), countBeforeOptimize);
  }
}
