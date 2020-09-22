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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.io.file.Path;
import com.google.common.collect.Lists;

public class TestIcebergManifests extends BaseTestQuery {

  private Schema schema;

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
  public void testManifestCount() throws Exception{
    int insertCount = 5;
    int partitionValueSize = 1024;
    int dataFilesCount = 1;
    String columnName = "data";
    String expectedValue = "abc";
    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity(columnName)
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.setString(0, expectedValue);
    int manifestCount = getManifestFileCount(partitionSpec, partitionValueSize, dataFilesCount, columnName, insertCount);
    Assert.assertTrue(manifestCount < insertCount);
  }

  List<DataFile> getDataFiles(PartitionSpec partitionSpec, int partitionValueSize, int dataFilesCount, String columnName) {
    List<DataFile> dataFiles = new ArrayList<>();
    for( int i=0; i<dataFilesCount; ++i) {
      String partitionValue = RandomStringUtils.randomAlphanumeric(partitionValueSize);
      String datafileName = RandomStringUtils.randomAlphanumeric(64);
      dataFiles.add(DataFiles.builder(partitionSpec)
        .withInputFile(Files.localInput(datafileName+".parquet"))
        .withRecordCount(50)
        .withFormat(FileFormat.PARQUET)
        .withPartitionPath(columnName+"="+partitionValue)
        .build());
    }
    return dataFiles;
  }

  public int getManifestFileCount(PartitionSpec partitionSpec, int partitionValueSize, int dataFilesCount,
                                   String columnName, int insertCount) throws Exception {
    File tableFolder = new File(folder.getRoot(), "icebergPartitionTest");
    try {
      tableFolder.mkdir();

      IcebergOpCommitter committer = IcebergOperation.getCreateTableCommitter(Path.of(tableFolder.toPath().toString()),
        (new SchemaConverter()).fromIceberg(schema), Lists.newArrayList(columnName), new Configuration());
      committer.consumeData(getDataFiles(partitionSpec, partitionValueSize, dataFilesCount, columnName));
      committer.commit();

      Table table = new HadoopTables(new Configuration()).load(tableFolder.getPath());
      Assert.assertEquals(1, table.currentSnapshot().manifests().size());

      table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "20480")
        .commit();

      for (int i=0; i<insertCount; ++i) {
        committer = IcebergOperation.getInsertTableCommitter(Path.of(tableFolder.toPath().toString()),
          (new SchemaConverter()).fromIceberg(schema), Lists.newArrayList(columnName), new Configuration());
        committer.consumeData(getDataFiles(partitionSpec, partitionValueSize, dataFilesCount, columnName));
        committer.commit();
      }
      table = new HadoopTables(new Configuration()).load(tableFolder.getPath());
      return table.currentSnapshot().manifests().size();
    }
    finally {
      tableFolder.delete();
    }
  }
}
