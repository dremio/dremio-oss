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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.joda.time.DateTimeConstants;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.PlanTestBase;
import com.dremio.common.util.TestTools;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.datastore.LegacyProtobufSerializer;
import com.dremio.exec.store.iceberg.IcebergTableWrapper;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ColumnValueCount;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf.ParquetDatasetSplitXAttr;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.google.common.collect.ImmutableList;

public class TestIcebergCTASWithPartition extends PlanTestBase {
  private final int NUM_COLUMNS = 9; // orders table has 9 columns.

  @Test
  public void testInvalidPartitionColumns() throws Exception {
    final String tableLower = "tableLower1";

    try (AutoCloseable c = enableIcebergTables()) {
      final String tableLowerCreate = String.format("CREATE TABLE %s.%s(id int, code int) partition by (name, region)",
        TEMP_SCHEMA, tableLower);
      errorMsgTestHelper(tableLowerCreate, "Partition column(s) [name, region] are not found in table.");

    } finally {
      FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), tableLower));
    }
  }

  @Test
  public void testTimePartitionColumn() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "ctas_with_time_partition";

      try {
        final String testWorkingPath = TestTools.getWorkingPath();
        final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/supplier";
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (col_time)  " +
            " AS SELECT to_time(s_suppkey) as col_time from dfs.\"" + parquetFiles + "\" limit 1",
          TEMP_SCHEMA, newTblName);
        errorMsgTestHelper(ctasQuery, "Partition column col_time of type time is not supported");
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void ctasWithIntPartition() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "ctas_with_int_partition";

      try {
        final String testWorkingPath = TestTools.getWorkingPath();
        final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (o_orderkey)  " +
            " AS SELECT * from dfs.\"" + parquetFiles + "\" limit 1",
          TEMP_SCHEMA, newTblName);

        test(ctasQuery);
        File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
        assertTrue(tableFolder.exists()); // table folder
        verifyPartitionValue(tableFolder.getPath(), Integer.class, Integer.valueOf(1));
        verifyPartitionChunk(tableFolder.getPath(), PartitionValue.of("o_orderkey", 1), NUM_COLUMNS);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void ctasWithStringPartition() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "ctas_with_string_partition";

      try {
        final String testWorkingPath = TestTools.getWorkingPath();
        final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (o_orderstatus)  " +
            " AS SELECT * from dfs.\"" + parquetFiles + "\" limit 1",
          TEMP_SCHEMA, newTblName);

        test(ctasQuery);
        File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
        assertTrue(tableFolder.exists()); // table folder
        verifyPartitionValue(tableFolder.getPath(), String.class, "O");
        verifyPartitionChunk(tableFolder.getPath(), PartitionValue.of("o_orderstatus", "O"), NUM_COLUMNS);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }


  @Test
  public void ctasWithDoublePartition() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "ctas_with_double_partition";

      try {
        final String testWorkingPath = TestTools.getWorkingPath();
        final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (o_totalprice) " +
            " AS SELECT * from dfs.\"" + parquetFiles + "\" limit 1",
          TEMP_SCHEMA, newTblName);

        test(ctasQuery);
        File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
        assertTrue(tableFolder.exists()); // table folder
        verifyPartitionValue(tableFolder.getPath(), Double.class, Double.valueOf("172799.49"));
        verifyPartitionChunk(tableFolder.getPath(), PartitionValue.of("o_totalprice", 172799.49), NUM_COLUMNS);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  @Test
  public void ctasWithDatePartition() throws Exception {
    try (AutoCloseable c = enableIcebergTables()) {
      final String newTblName = "ctas_with_date_partition";

      try {
        final String testWorkingPath = TestTools.getWorkingPath();
        final String parquetFiles = testWorkingPath + "/src/test/resources/iceberg/orders";
        final String ctasQuery = String.format("CREATE TABLE %s.%s PARTITION BY (o_orderdate) " +
            " AS SELECT * from dfs.\"" + parquetFiles + "\" limit 1",
          TEMP_SCHEMA, newTblName);

        test(ctasQuery);
        File tableFolder = new File(getDfsTestTmpSchemaLocation(), newTblName);
        assertTrue(tableFolder.exists()); // table folder
        verifyPartitionValue(tableFolder.getPath(), Integer.class, 9497);
        verifyPartitionChunk(tableFolder.getPath(), PartitionValue.of("o_orderdate",
          (long)9497 * DateTimeConstants.MILLIS_PER_DAY), NUM_COLUMNS);
      } finally {
        FileUtils.deleteQuietly(new File(getDfsTestTmpSchemaLocation(), newTblName));
      }
    }
  }

  private void verifyPartitionValue(String tableFolder, Class expectedClass, Object expectedValue) {
    Table table = new HadoopTables(new Configuration()).load(tableFolder);
    for (FileScanTask fileScanTask : table.newScan().planFiles()) {
      StructLike structLike = fileScanTask.file().partition();
      Assert.assertEquals(structLike.get(0, expectedClass), expectedValue);
    }
  }

  private void verifyPartitionChunk(String tableFolder, PartitionValue expectedValue, int expectedColumns) throws IOException {
    IcebergTableWrapper tableWrapper = new IcebergTableWrapper(getSabotContext(), localFs,
      new Configuration(), tableFolder);

    List<PartitionChunk> chunks =
      ImmutableList.copyOf(tableWrapper.getTableInfo().getPartitionChunkListing().iterator());
    assertEquals(1, chunks.size());
    assertEquals(1, chunks.get(0).getPartitionValues().size());
    assertEquals(expectedValue, chunks.get(0).getPartitionValues().get(0));

    DatasetSplit split = chunks.get(0).getSplits().iterator().next();
    ParquetDatasetSplitXAttr xattr =  LegacyProtobufSerializer
      .parseFrom(ParquetDatasetSplitXAttr.PARSER, MetadataProtoUtils.toProtobuf(split.getExtraInfo()));
    assertEquals(expectedColumns, xattr.getColumnValueCountsCount());
    for (ColumnValueCount columnValueCount : xattr.getColumnValueCountsList()) {
      assertEquals(columnValueCount.getCount(), 1);
    }
  }
}
