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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.sabot.exec.context.OperatorStats;
import com.google.common.collect.Lists;

public class TestIcebergPartitionData extends BaseTestQuery {

  private Schema schema;
  private OperatorStats operatorStats;

  public TestIcebergPartitionData() {
    this.operatorStats = mock(OperatorStats.class);
    doNothing().when(operatorStats).addLongStat(any(), anyLong());
  }

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  static FileSystem fs;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);
  }

  @Before
  public void setUp() {
    schema = new Schema(
      required(0, "id", Types.LongType.get()),
      required(1, "data", Types.StringType.get()),
      required(2, "b", Types.BooleanType.get()),
      required(3, "i", Types.IntegerType.get()),
      required(4, "l", Types.LongType.get()),
      required(5, "f", Types.FloatType.get()),
      required(6, "d", Types.DoubleType.get()),
      required(7, "date", Types.DateType.get()),
      required(8, "ts", Types.TimestampType.withZone()),
      required(9, "s", Types.StringType.get()),
      required(10, "bytes", Types.BinaryType.get()),
      required(11, "dec_9_0", Types.DecimalType.of(9, 0)),
      required(12, "dec_11_2", Types.DecimalType.of(11, 2)),
      required(13, "dec_38_10", Types.DecimalType.of(38, 10))
    );
  }

  @Test
  public void testIntSpec() throws Exception{
    String columnName = "i";
    Integer expectedValue = 12322;
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setInteger(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, Integer.class, expectedValue);
  }

  @Test
  public void testStringSpec() throws Exception{
    String columnName = "data";
    String expectedValue = "abc";
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setString(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, String.class, expectedValue);
  }

  @Test
  public void testLongSpec() throws Exception{
    String columnName = "id";
    Long expectedValue = 123L;
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setLong(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, Long.class, expectedValue);
  }

  @Test
  public void testBigDecimalpec() throws Exception{
    String columnName = "dec_9_0";
    BigDecimal expectedValue = new BigDecimal(234);
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setBigDecimal(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, BigDecimal.class, expectedValue);
  }

  @Test
  public void testFloatSpec() throws Exception{
    String columnName = "f";
    Float expectedValue = 1.23f;
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setFloat(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, Float.class, expectedValue);
  }

  @Test
  public void testDoubleSpec() throws Exception{
    String columnName = "d";
    Double expectedValue = Double.valueOf(1.23f);
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setDouble(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, Double.class, expectedValue);
  }

  @Test
  public void testBooleanSpec() throws Exception{
    String columnName = "b";
    Boolean expectedValue = true;
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setBoolean(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, Boolean.class, expectedValue);
  }

  @Test
  public void testBinarySpec() throws Exception{
    String columnName = "bytes";
    byte[] expectedValue = "test".getBytes();
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setBytes(0, expectedValue);
    verifyPartitionValue(partitionSpec, icebergPartitionData, columnName, ByteBuffer.class, expectedValue);
  }

  private void verifyPartitionValue(PartitionSpec partitionSpec, IcebergPartitionData partitionData,
                                    String columnName, Class expectedClass, Object expectedValue) throws Exception {
    String tableName = "icebergPartitionTest";
    File tableFolder = new File(folder.getRoot(), tableName);
    try {
      tableFolder.mkdir();
      File dataFile = new File(folder.getRoot(), "a.parquet");

      dataFile.createNewFile();

      DataFile d1 = DataFiles.builder(partitionSpec)
        .withInputFile(Files.localInput(dataFile))
        .withRecordCount(50)
        .withFormat(FileFormat.PARQUET)
        .withPartition(partitionData)
        .build();

      FileSystemPlugin fileSystemPlugin = BaseTestQuery.getMockedFileSystemPlugin();
      IcebergHadoopModel icebergHadoopModel = new IcebergHadoopModel(new Configuration(), fileSystemPlugin);
      when(fileSystemPlugin.getIcebergModel()).thenReturn(icebergHadoopModel);
      SchemaConverter schemaConverter = new SchemaConverter(tableName);
      IcebergOpCommitter committer = icebergHadoopModel.getCreateTableCommitter(tableName,
        icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()),
        schemaConverter.fromIceberg(schema), Lists.newArrayList(columnName), null);
      committer.commit();

      committer = icebergHadoopModel.getInsertTableCommitter(icebergHadoopModel.getTableIdentifier(tableFolder.toPath().toString()), operatorStats);
      ManifestFile m1 = writeManifest("manifest1", Arrays.asList(d1), tableFolder);
      committer.consumeManifestFile(m1);
      committer.commit();

      Table table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
      for (FileScanTask fileScanTask : table.newScan().planFiles()) {
        StructLike structLike = fileScanTask.file().partition();
        if (expectedClass == ByteBuffer.class) {
          Assert.assertEquals(structLike.get(0, expectedClass).hashCode(), ByteBuffer.wrap((byte[])expectedValue).hashCode());
        } else {
          Assert.assertTrue(structLike.get(0, expectedClass).equals(expectedValue));
        }
      }

    }
    finally {
      tableFolder.delete();
    }

  }

  ManifestFile writeManifest(String fileName, List<DataFile> files, File tableFolder) throws IOException {
    return writeManifest(fileName, null, files, tableFolder);
  }

  ManifestFile writeManifest(String fileName, Long snapshotId, List<DataFile> files, File tableFolder) throws IOException {
    File metadataFolder = new File(tableFolder, "metadata");
    metadataFolder.mkdir();
    File manifestFile =  new File(metadataFolder, fileName + ".avro");
    Table table = getIcebergTable(tableFolder, IcebergCatalogType.HADOOP);
    OutputFile outputFile = table.io().newOutputFile(manifestFile.getCanonicalPath());

    ManifestWriter<DataFile> writer = ManifestFiles.write(1, table.spec(), outputFile, snapshotId);
    try {
      for (DataFile file : files) {
        writer.add(file);
      }
    } finally {
      writer.close();
    }
    return writer.toManifestFile();
  }
}
