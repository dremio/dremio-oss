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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;

public class TestIcebergSerDe extends BaseTestQuery {

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
  public void testDataFileSerDe() throws Exception{
    File dataFile = new File(folder.getRoot(), "a.parquet");
    dataFile.createNewFile();

    PartitionSpec partitionSpec = PartitionSpec
      .builderFor(schema)
      .identity("i")
      .identity("data")
      .build();

    IcebergPartitionData icebergPartitionData = new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.set(0, Integer.valueOf(10));
    icebergPartitionData.set(1, "def");

    DataFile d1 = DataFiles.builder(partitionSpec)
      .withInputFile(Files.localInput(dataFile))
      .withRecordCount(50)
      .withFormat(FileFormat.PARQUET)
      .withPartition(icebergPartitionData)
      .build();

    long d1RecordCount = d1.recordCount();
    byte[] dataFileBytes = IcebergSerDe.serializeDataFile(d1);
    DataFile d2 = IcebergSerDe.deserializeDataFile(dataFileBytes);
    long d2RecordCount = d2.recordCount();
    Assert.assertEquals(d1RecordCount, d2RecordCount);
    Assert.assertEquals((Integer)(d2.partition().get(0, Integer.class)), Integer.valueOf(10));
    Assert.assertEquals((String)(d2.partition().get(1, String.class)), "def");
  }
}
