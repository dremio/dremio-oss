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
package com.dremio.exec.store.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.google.common.collect.ImmutableList;

public class TestIcebergPartitions extends BaseTestQuery {
  private static FileSystem fs;
  private static Configuration conf;
  private static final String ID = "id";
  private static final String NAME = "name";
  private Schema schema;
  private PartitionSpec spec;

  @ClassRule
  public static final TemporaryFolder tempDir = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    conf.set("fs.default.name", "local");

    fs = FileSystem.get(conf);
  }

  @Before
  public void setUp() {
    schema = new Schema(
      NestedField.optional(1, ID, Types.IntegerType.get()),
      NestedField.optional(2, NAME, Types.StringType.get())
    );

    spec = PartitionSpec
      .builderFor(schema)
      .identity(ID)
      .identity(NAME)
      .build();
  }

  private DataFile createDataFile(File dir, String fileName, int idValue, String nameValue,
    int recordCount) throws IOException {
    File dataFile = new File(dir, fileName);
    dataFile.createNewFile();

    return DataFiles.builder(spec)
      .withInputFile(Files.localInput(dataFile))
      .withPartitionPath(ID + "=" + idValue + "/" + NAME + "=" + nameValue)
      .withRecordCount(recordCount)
      .withFormat(FileFormat.PARQUET)
      .build();
  }

  private PartitionChunk findPartition(List<PartitionChunk> chunks, int idValue, String nameValue) {
    List<PartitionValue> expected = Arrays.asList(PartitionValue.of(ID, idValue), PartitionValue.of(NAME, nameValue));
    for (PartitionChunk chunk : chunks) {
      if (chunk.getPartitionValues().equals(expected)) {
        return chunk;
      }
    }
    return null;
  }

  @Test
  public void testPartitions() throws Exception {
    File root = tempDir.newFolder();
    IcebergModel icebergModel = getIcebergModel(root);
    HadoopTables tables = new HadoopTables(conf);
    Table table = tables.create(schema, spec, root.getAbsolutePath());

    // test empty table.
    IcebergTableInfo tableInfo = new IcebergTableWrapper(getSabotContext(),
      HadoopFileSystem.get(fs), icebergModel, root.getAbsolutePath()).getTableInfo();
    assertEquals(tableInfo.getRecordCount(), 0);

    List<String> expectedColumns = Arrays.asList(ID, NAME);
    assertEquals(expectedColumns, tableInfo.getPartitionColumns());

    assertEquals(0, ImmutableList.copyOf(tableInfo.getPartitionChunkListing().iterator()).size());

    // Append some data files.
    Transaction transaction = table.newTransaction();
    AppendFiles appendFiles = transaction.newAppend();
    appendFiles.appendFile(createDataFile(root, "d1", 1, "jack", 100));
    appendFiles.appendFile(createDataFile(root, "d2", 1, "jack", 200));
    appendFiles.appendFile(createDataFile(root, "d3", 2, "jill", 300));
    appendFiles.appendFile(createDataFile(root, "d4", 2, "jill", 400));
    appendFiles.appendFile(createDataFile(root, "d5", 2, "jill", 500));
    appendFiles.commit();
    transaction.commitTransaction();

    tableInfo = new IcebergTableWrapper(getSabotContext(),
      HadoopFileSystem.get(fs), icebergModel, root.getAbsolutePath()).getTableInfo();
    assertEquals(1500, tableInfo.getRecordCount());
    assertEquals(2, ImmutableList.copyOf(tableInfo.getPartitionChunkListing().iterator()).size());

    // validate first partition
    final AtomicLong recordCount = new AtomicLong(0);
    PartitionChunk p1 = findPartition(ImmutableList.copyOf(tableInfo.getPartitionChunkListing().iterator()), 1, "jack");
    assertNotNull(p1);
    assertEquals(2, p1.getSplitCount());
    p1.getSplits().iterator().forEachRemaining(x -> recordCount.addAndGet(x.getRecordCount()));
    assertEquals(300, recordCount.intValue());

    // validate second partition
    PartitionChunk p2 = findPartition(ImmutableList.copyOf(tableInfo.getPartitionChunkListing().iterator()), 2, "jill");
    assertNotNull(p2);

    assertEquals(3, p2.getSplitCount());
    recordCount.set(0);
    p2.getSplits().iterator().forEachRemaining(x -> recordCount.addAndGet(x.getRecordCount()));
    assertEquals(1200, recordCount.intValue());
  }

  @Test
  public void testNonIdentityPartitions() throws Exception {
    File root = tempDir.newFolder();
    IcebergModel icebergModel = getIcebergModel(root);
    HadoopTables tables = new HadoopTables(conf);
    PartitionSpec partitionSpec = PartitionSpec
        .builderFor(schema)
        .bucket(NAME, 2)
        .build();
    Table table = tables.create(schema, partitionSpec, root.getAbsolutePath());

    // Append some data files.
    Transaction transaction = table.newTransaction();
    AppendFiles appendFiles = transaction.newAppend();
    appendFiles.appendFile(createDataFile(root, "d1", 1, "jack", 100));
    appendFiles.appendFile(createDataFile(root, "d2", 1, "jack", 200));
    appendFiles.appendFile(createDataFile(root, "d3", 2, "jill", 300));
    appendFiles.appendFile(createDataFile(root, "d4", 2, "jill", 400));
    appendFiles.appendFile(createDataFile(root, "d5", 2, "jill", 500));
    appendFiles.commit();
    transaction.commitTransaction();

    try {
      IcebergTableInfo tableInfo = new IcebergTableWrapper(getSabotContext(),
          HadoopFileSystem.get(fs), icebergModel, root.getAbsolutePath()).getTableInfo();
      fail("Expected error while reading metadata of iceberg table with non-identity partition field");
    } catch (Exception ex) {
      Assert.assertTrue("UserException expected", ex instanceof UserException);
      UserException uex = ((UserException) ex);
      Assert.assertEquals("Invalid ErrorType. Expected " + UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION
              + " but got " + uex.getErrorType(), UserBitShared.DremioPBError.ErrorType.UNSUPPORTED_OPERATION, uex.getErrorType());
      String expectedErrorMsg = "Column values and partition values are not same for [name] column";
      Assert.assertTrue("Expected message to contain " + expectedErrorMsg + " but was "
          + uex.getOriginalMessage() + " instead", uex.getOriginalMessage().contains(expectedErrorMsg));
    }
  }

}
