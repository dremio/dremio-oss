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

import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addBucketPartition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addColumn;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addDayPartition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addIdentityPartition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addMonthPartition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addTruncate2Partition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.addYearPartition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.createBasicTable;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.dropBucketPartition;
import static com.dremio.exec.planner.sql.DmlQueryTestUtils.insertIntoTable;
import static com.dremio.exec.planner.sql.OptimizeTests.assertFileCount;
import static com.dremio.exec.planner.sql.OptimizeTests.insertCommits;
import static com.dremio.exec.planner.sql.OptimizeTests.testOptimizeCommand;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserRemoteException;
import com.google.common.collect.ImmutableSet;
import org.apache.arrow.memory.BufferAllocator;

/** Tests for OPTIMIZE TABLE iceberg_table FOR PARTITIONS (expr) */
public class OptimizeTestForPartitions extends BaseTestQuery {

  public static void testOptimizeTableForStringIdentityPartitions(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      // It should optimize only partition where column_0='0_0' which have 5 files.
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS column_0='0_0'", table.fqn),
          5L,
          0L,
          1L);
      // 20 files for partitions(0_1,0_2,0_3,0_4) and 1 file for 0_0. total = 21.
      assertFileCount(table.fqn, 21L, allocator);
    }
  }

  public static void testOptimizeTableForIdentityPartitionsWithLikeExpression(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      // It should optimize only partition where column_0='0_0' which have 5 files.
      testOptimizeCommand(
          allocator,
          "OPTIMIZE TABLE " + table.fqn + " FOR PARTITIONS column_0 like '0_0'",
          5L,
          0L,
          1L);
      // 20 files for partitions(0_1,0_2,0_3,0_4) and 1 file for 0_0. total = 21.
      assertFileCount(table.fqn, 21L, allocator);
    }
  }

  public static void testOptimizeTableForIntegerIdentityPartitions(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_int", "INTEGER");
      addIdentityPartition(table, allocator, "part_col_int");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_int)", "(1,'a', 1)");
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_int=1", table.fqn),
          10L,
          0L,
          2L);
      assertFileCount(table.fqn, 2L, allocator);
    }
  }

  public static void testOptimizeTableForIntegerIdentityPartitionsWithExpression(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_int", "INTEGER");
      addIdentityPartition(table, allocator, "part_col_int");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_int)", "(1,'a', 2)");
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_int/2=1", table.fqn),
          5L,
          0L,
          1L);
      assertFileCount(table.fqn, 6L, allocator);
    }
  }

  public static void testOptimizeTableForTimestampIdentityPartitions(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_ts", "TIMESTAMP");
      addIdentityPartition(table, allocator, "part_col_ts");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2022-01-01')");
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2023-01-01')");
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_ts='2022-01-01'", table.fqn),
          5L,
          0L,
          1L);
      assertFileCount(table.fqn, 11L, allocator);
    }
  }

  public static void testOptimizeTableForYearIdentityPartitions(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_ts", "TIMESTAMP");
      addIdentityPartition(table, allocator, "part_col_ts");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2022-01-01')");
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2023-01-01')");
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS year(part_col_ts)=2022", table.fqn),
          5L,
          0L,
          1L);
      assertFileCount(table.fqn, 11L, allocator);
    }
  }

  public static void testOptimizeTableForTwoIdentityPartitions(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_int_1", "INTEGER");
      addColumn(table, allocator, "part_col_int_2", "INTEGER");
      addIdentityPartition(table, allocator, "part_col_int_1");
      addIdentityPartition(table, allocator, "part_col_int_2");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(
            table.fqn, "(id, column_0, part_col_int_1, part_col_int_2)", "(1,'a', 1,11)");
      }

      testOptimizeCommand(
          allocator,
          String.format(
              "OPTIMIZE TABLE %s FOR PARTITIONS part_col_int_2-part_col_int_1=10", table.fqn),
          5L,
          0L,
          1L);
      assertFileCount(table.fqn, 6L, allocator);
    }
  }

  public static void testOptimizeTableForNonPartition(String SCHEMA, BufferAllocator allocator)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 3, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      // column_1 is non partition column, column_0 is an identity partition column.
      assertThatThrownBy(
              () ->
                  runSQL(
                      String.format("OPTIMIZE TABLE %s FOR PARTITIONS column_1='0_0'", table.fqn)))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "OPTIMIZE command is only supported on the partition columns - [column_0]");
    }
  }

  public static void testOptimizeTableForPartitionEvolBucketWithIdentity(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_bucket", "INTEGER");
      addBucketPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 1)");
      }
      assertFileCount(table.fqn, 30L, allocator);

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn),
          30L,
          0L,
          6L);

      addIdentityPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 1)");
      }
      assertFileCount(table.fqn, 11L, allocator);

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn),
          6L,
          0L,
          1L);

      assertFileCount(table.fqn, 6L, allocator);
    }
  }

  public static void testOptimizeTableForPartitionEvolIdentityWithBucket(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_bucket", "INTEGER");
      addIdentityPartition(table, allocator, "part_col_bucket");
      // 5 files for part_col_bucket=1
      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 1)");
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(2,'b', 2)");
      }
      assertFileCount(table.fqn, 35L, allocator);
      // rewritten_data_files_count 25(null for part_col_bucket) + 5(part_col_bucket=1)
      // new_data_files_count = 5 for column_0+part_col_bucket=null and 1 for column_0=a and
      // part_col_bucket=1
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn),
          30L,
          0L,
          6L);

      assertFileCount(table.fqn, 11L, allocator);

      addBucketPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 1)");
      }

      assertFileCount(table.fqn, 16L, allocator);

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn),
          6L,
          0L,
          1L);

      assertFileCount(table.fqn, 11L, allocator);
    }
  }

  public static void testOptimizeTableForPartitionEvolBucketToIdentity(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_bucket", "INTEGER");
      addBucketPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 1)");
      }

      dropBucketPartition(table, allocator, "part_col_bucket");
      // part_col_bucket is not a partition column now.
      assertThatThrownBy(
              () ->
                  runSQL(
                      String.format(
                          "OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn)))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "OPTIMIZE command is only supported on the partition columns - [column_0]");

      addIdentityPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 2)");
      }
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=2", table.fqn),
          30L,
          0L,
          6L);

      assertFileCount(table.fqn, 11L, allocator);
    }
  }

  public static void testOptimizeTableForPartitionEvolBucketToIdentityWithExpression(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of(1))) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 25L, allocator); // 5 files per partition

      addColumn(table, allocator, "part_col_bucket", "INTEGER");
      addBucketPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 1)");
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn),
          30L,
          0L,
          6L);

      dropBucketPartition(table, allocator, "part_col_bucket");
      // part_col_bucket is not a partition column now.
      assertThatThrownBy(
              () ->
                  runSQL(
                      String.format(
                          "OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket=1", table.fqn)))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "OPTIMIZE command is only supported on the partition columns - [column_0]");

      addIdentityPartition(table, allocator, "part_col_bucket");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_bucket)", "(1,'a', 2)");
      }
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_bucket/2=1", table.fqn),
          5L,
          0L,
          1L);

      assertFileCount(table.fqn, 7L, allocator);
    }
  }

  public static void testOptimizeTableForPartitionWithInvalidExpression(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "col_int", "INTEGER");
      addColumn(table, allocator, "part_col_int", "INTEGER");
      addIdentityPartition(table, allocator, "part_col_int");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, col_int, part_col_int)", "(1,'a', 2,2)");
      }

      assertThatThrownBy(
              () ->
                  runSQL(
                      String.format(
                          "OPTIMIZE TABLE %s FOR PARTITIONS part_col_int/col_int=1", table.fqn)))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "OPTIMIZE command is only supported on the partition columns - [part_col_int]");

      assertThatThrownBy(
              () ->
                  runSQL(
                      String.format(
                          "OPTIMIZE TABLE %s FOR PARTITIONS col_int/part_col_int=1", table.fqn)))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "OPTIMIZE command is only supported on the partition columns - [part_col_int]");

      assertThatThrownBy(
              () ->
                  runSQL(
                      String.format(
                          "OPTIMIZE TABLE %s FOR PARTITIONS column_0 like '0_0' ", table.fqn)))
          .isInstanceOf(UserRemoteException.class)
          .hasMessageContaining(
              "OPTIMIZE command is only supported on the partition columns - [part_col_int]");
    }
  }

  public static void testOptimizeTableForTruncatePartitions(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_str", "VARCHAR");
      addTruncate2Partition(table, allocator, "part_col_str");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_str)", "(1,'a', '1value')");
        insertIntoTable(
            table.fqn,
            "(id, column_0, part_col_str)",
            "(1,'a', '2value')"); // will not optimize for filter part_col_str='1value'
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_str='1value'", table.fqn),
          10L,
          0L,
          2L);
      assertFileCount(table.fqn, 7L, allocator);
    }
  }

  public static void testOptimizeTableForTruncatePartitionWithLike(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_str", "VARCHAR");
      addTruncate2Partition(table, allocator, "part_col_str");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_str)", "(1,'a', '1value')");
        insertIntoTable(
            table.fqn,
            "(id, column_0, part_col_str)",
            "(1,'a', '2value')"); // will not optimize for filter part_col_str='1value'
      }

      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_str like '1%s'", table.fqn, "%"),
          15L,
          0L,
          3L);
      assertFileCount(table.fqn, 3L, allocator);
    }
  }

  public static void testOptimizeTableForYearPartitions(String SCHEMA, BufferAllocator allocator)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_ts", "TIMESTAMP");
      addYearPartition(table, allocator, "part_col_ts");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2022-01-01')");
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2023-01-01')");
      }
      // Optimize all the files, since expression is not supported on transformed partition.
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS year(part_col_ts)=2022", table.fqn),
          15L,
          0L,
          3L);
      assertFileCount(table.fqn, 3L, allocator);
    }
  }

  public static void testOptimizeTableForYearPartitionsWithEquality(
      String SCHEMA, BufferAllocator allocator) throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_ts", "TIMESTAMP");
      addYearPartition(table, allocator, "part_col_ts");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2022-01-01')");
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2023-01-01')");
      }
      // Optimize all the files, since expression is not supported on transformed partition.
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS part_col_ts='2022-01-01'", table.fqn),
          15L,
          0L,
          3L);
      assertFileCount(table.fqn, 3L, allocator);
    }
  }

  public static void testOptimizeTableForMonthPartitions(String SCHEMA, BufferAllocator allocator)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_ts", "TIMESTAMP");
      addMonthPartition(table, allocator, "part_col_ts");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2022-02-02')");
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2023-01-01')");
      }
      // Optimize all the files, since expression is not supported on transformed partition.
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS month(part_col_ts)=01", table.fqn),
          15L,
          0L,
          3L);
      assertFileCount(table.fqn, 3L, allocator);
    }
  }

  public static void testOptimizeTableForDayPartitions(String SCHEMA, BufferAllocator allocator)
      throws Exception {
    try (DmlQueryTestUtils.Table table = createBasicTable(SCHEMA, 0, 2, 0, ImmutableSet.of())) {
      insertCommits(table, 5);
      assertFileCount(table.fqn, 5L, allocator);

      addColumn(table, allocator, "part_col_ts", "TIMESTAMP");
      addDayPartition(table, allocator, "part_col_ts");

      for (int i = 0; i < 5; i++) {
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2022-02-02')");
        insertIntoTable(table.fqn, "(id, column_0, part_col_ts)", "(1,'a', '2023-01-01')");
      }
      // Optimize all the files, since expression is not supported on transformed partition.
      testOptimizeCommand(
          allocator,
          String.format("OPTIMIZE TABLE %s FOR PARTITIONS month(part_col_ts)=01", table.fqn),
          15L,
          0L,
          3L);
      assertFileCount(table.fqn, 3L, allocator);
    }
  }
}
