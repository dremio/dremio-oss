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
package com.dremio.exec.sql;

import static org.junit.Assert.assertTrue;

import com.dremio.PlanTestBase;
import com.dremio.common.util.TestTools;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.google.common.collect.Iterators;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.junit.Assert;
import org.junit.Test;

public class TestIcebergCTASWithPartition extends PlanTestBase {
  private final int NUM_COLUMNS = 9; // orders table has 9 columns.

  @Test
  public void testInvalidPartitionColumns() throws Exception {
    final String tableLower = "tableLower1";

    try {
      final String tableLowerCreate =
          String.format(
              "CREATE TABLE %s.%s(id int, code int) partition by (name, region)",
              TEMP_SCHEMA_HADOOP, tableLower);
      errorMsgTestHelper(
          tableLowerCreate, "Partition column(s) [name, region] are not found in table.");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableLower));
    }
  }

  @Test
  public void testTimePartitionColumn() throws Exception {
    final String newTblName = "ctas_with_time_partition";
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/supplier";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (col_time)  "
                  + " AS SELECT to_time(s_suppkey) as col_time from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              TEMP_SCHEMA_HADOOP,
              newTblName);
      errorMsgTestHelper(ctasQuery, "Partition type TIME for column 'col_time' is not supported");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  private void verifyCtasWithIntPartition(
      String newTblName, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (o_orderkey)  "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              schema,
              newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder
      verifyPartitionValue(tableFolder, Integer.class, Integer.valueOf(1), catalogType);

      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", schema, newTblName))
          .unOrdered()
          .sqlBaselineQuery("SELECT * from dfs.\"" + parquetFiles + "\" limit 1")
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasWithIntPartition() throws Exception {
    verifyCtasWithIntPartition(
        "ctas_with_int_partition_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
  }

  private void verifyCtasWithStringPartition(
      String newTblName, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (o_orderstatus)  "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              schema,
              newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder
      verifyPartitionValue(tableFolder, String.class, "O", catalogType);

      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", schema, newTblName))
          .unOrdered()
          .sqlBaselineQuery("SELECT * from dfs.\"" + parquetFiles + "\" limit 1")
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasWithStringPartition() throws Exception {
    verifyCtasWithStringPartition(
        "ctas_with_string_partition", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
  }

  private void verifyCtasWithDoublePartition(
      String newTblName, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (o_totalprice) "
                  + " AS SELECT * from dfs.\""
                  + parquetFiles
                  + "\" limit 1",
              schema,
              newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder
      verifyPartitionValue(tableFolder, Double.class, Double.valueOf("172799.49"), catalogType);

      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", schema, newTblName))
          .unOrdered()
          .sqlBaselineQuery("SELECT * from dfs.\"" + parquetFiles + "\" limit 1")
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasWithDoublePartition() throws Exception {
    verifyCtasWithDoublePartition(
        "ctas_with_double_partition_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
  }

  private void verifyCtasWithDatePartition(
      String newTblName, String dfsSchema, String testSchema, IcebergCatalogType catalogType)
      throws Exception {
    try {
      final String testWorkingPath = TestTools.getWorkingPath();
      final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
      final String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (o_orderdate) "
                  + " AS SELECT * from "
                  + dfsSchema
                  + ".\""
                  + parquetFiles
                  + "\" limit 1",
              testSchema,
              newTblName);

      test(ctasQuery);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder
      verifyPartitionValue(tableFolder, Integer.class, 9497, catalogType);

      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", testSchema, newTblName))
          .unOrdered()
          .sqlBaselineQuery("SELECT * from " + dfsSchema + ".\"" + parquetFiles + "\" limit 1")
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasWithDatePartition() throws Exception {
    final String newTblName = "ctas_with_date_partition";
    verifyCtasWithDatePartition(
        "ctas_with_date_partition_v2", "dfs_hadoop", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
  }

  private void verifyCtasWithNullPartition(
      String newTblName, String schema, IcebergCatalogType catalogType) throws Exception {
    try {
      final String ctasSrcQuery =
          String.format("create table %s.%s_src (col1 int, col2 int)", schema, newTblName);

      test(ctasSrcQuery);
      Thread.sleep(1001);
      final String insertSrc =
          String.format("insert into %s.%s_src select 1, cast(null as int)", schema, newTblName);
      test(insertSrc);
      Thread.sleep(1001);
      final String ctasQuery =
          String.format(
              "create table %s.%s (col1 int, col2 int) partition by (col2)", schema, newTblName);
      test(ctasQuery);
      Thread.sleep(1001);
      final String insertDest =
          String.format(
              "insert into %s.%s select * from %s.%s_src", schema, newTblName, schema, newTblName);
      test(insertDest);
      Thread.sleep(1001);

      File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
      assertTrue(tableFolder.exists()); // table folder
      verifyPartitionValue(tableFolder, Integer.class, null, catalogType);

      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", schema, newTblName))
          .unOrdered()
          .sqlBaselineQuery("SELECT * from %s.%s_src", schema, newTblName)
          .build()
          .run();
      testBuilder()
          .sqlQuery(String.format("select * from %s.%s", schema, newTblName))
          .unOrdered()
          .baselineColumns("col1", "col2")
          .baselineValues(1, null)
          .build()
          .run();
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
    }
  }

  @Test
  public void ctasWithNullPartitionValues() throws Exception {
    final String newTblName = "ctas_with_null_partition";
    verifyCtasWithNullPartition(
        "ctas_with_null_partition_v2", TEMP_SCHEMA_HADOOP, IcebergCatalogType.HADOOP);
  }

  private void verifyPartitionValue(
      File tableFolder, Class expectedClass, Object expectedValue, IcebergCatalogType catalogType) {
    Table table = getIcebergTable(tableFolder, catalogType);
    for (FileScanTask fileScanTask : table.newScan().planFiles()) {
      StructLike structLike = fileScanTask.file().partition();
      Assert.assertEquals(structLike.get(0, expectedClass), expectedValue);
    }
  }

  @Test
  public void testCtasWithPartitionTransformation() throws Exception {
    String selectTable = "select_partition_transform";
    String ctasTable = "partition_transform";
    try {
      String schema = TEMP_SCHEMA_HADOOP;
      IcebergCatalogType catalogType = IcebergCatalogType.HADOOP;
      String createQuery =
          String.format("CREATE TABLE %s.%s (col1 varchar, col2 int)", schema, selectTable);
      test(createQuery);
      Thread.sleep(1001);
      String insertQuery =
          String.format(
              "INSERT INTO %s.%s values ('abcdefg', 12), ('abc', 10), ('pq', 20)",
              schema, selectTable);
      test(insertQuery);
      Thread.sleep(1001);
      String ctasQuery =
          String.format(
              "CREATE TABLE %s.%s PARTITION BY (truncate(3, col1))  " + " AS SELECT * from %s.%s",
              schema, ctasTable, schema, selectTable);
      test(ctasQuery);
      Thread.sleep(1001);
      File tableFolder = new File(getDfsTestTmpSchemaLocation(), ctasTable);
      Table table = getIcebergTable(tableFolder, catalogType);
      Assert.assertEquals(2, Iterators.size(table.newScan().planFiles().iterator()));
      FileScanTask[] fileScanTasks =
          Iterators.toArray(table.newScan().planFiles().iterator(), FileScanTask.class);
      Assert.assertEquals(fileScanTasks[0].file().partition().get(0, String.class), "abc");
      Assert.assertEquals(fileScanTasks[1].file().partition().get(0, String.class), "pq");
    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), selectTable));
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), ctasTable));
    }
  }
}
