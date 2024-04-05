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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.dremio.BaseTestQuery;
import com.dremio.common.expression.CompleteType;
import com.dremio.connector.metadata.PartitionValue;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.implicit.DecimalTools;
import com.dremio.service.namespace.MetadataProtoUtils;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestIcebergSerDe extends BaseTestQuery {

  private Schema schema;

  @Rule public TemporaryFolder folder = new TemporaryFolder();
  static FileSystem fs;

  @BeforeClass
  public static void initFs() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "local");

    fs = FileSystem.get(conf);
  }

  @Before
  public void setUp() {
    schema =
        new Schema(
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
            required(13, "dec_38_10", Types.DecimalType.of(38, 10)));
  }

  @Test
  public void testDataFileSerDe() throws Exception {
    File dataFile = new File(folder.getRoot(), "a.parquet");
    dataFile.createNewFile();

    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("i").identity("data").build();

    IcebergPartitionData icebergPartitionData =
        new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.set(0, Integer.valueOf(10));
    icebergPartitionData.set(1, "def");

    DataFile d1 =
        DataFiles.builder(partitionSpec)
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
    Assert.assertEquals((Integer) (d2.partition().get(0, Integer.class)), Integer.valueOf(10));
    Assert.assertEquals((String) (d2.partition().get(1, String.class)), "def");
  }

  @Test
  public void testDeleteFileSerDe() throws Exception {
    File deleteFile = new File(folder.getRoot(), "a.parquet");
    deleteFile.createNewFile();

    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(schema).identity("i").identity("data").build();

    IcebergPartitionData icebergPartitionData =
        new IcebergPartitionData(partitionSpec.partitionType());
    icebergPartitionData.set(0, Integer.valueOf(10));
    icebergPartitionData.set(1, "def");

    DeleteFile df1 =
        FileMetadata.deleteFileBuilder(partitionSpec)
            .ofPositionDeletes()
            .withInputFile(Files.localInput(deleteFile))
            .withRecordCount(50)
            .withFormat(FileFormat.PARQUET)
            .withPartition(icebergPartitionData)
            .build();
    long df1RecordCount = df1.recordCount();

    DeleteFile df2 =
        FileMetadata.deleteFileBuilder(partitionSpec)
            .ofEqualityDeletes()
            .withInputFile(Files.localInput(deleteFile))
            .withRecordCount(20)
            .withFormat(FileFormat.PARQUET)
            .withPartition(icebergPartitionData)
            .build();
    long df2RecordCount = df2.recordCount();

    byte[] positionalDeleteFileBytes = IcebergSerDe.serializeDeleteFile(df1);
    DeleteFile deserializePositionalDeleteFile =
        IcebergSerDe.deserializeDeleteFile(positionalDeleteFileBytes);
    long positionalDeleteFileRecordCount = deserializePositionalDeleteFile.recordCount();
    Assert.assertEquals(df1RecordCount, positionalDeleteFileRecordCount);
    Assert.assertEquals(
        (Integer) (deserializePositionalDeleteFile.partition().get(0, Integer.class)),
        Integer.valueOf(10));
    Assert.assertEquals(
        (String) (deserializePositionalDeleteFile.partition().get(1, String.class)), "def");
    Assert.assertEquals(deserializePositionalDeleteFile.content().toString(), "POSITION_DELETES");

    byte[] equalityDeleteFileBytes = IcebergSerDe.serializeDeleteFile(df2);
    DeleteFile deserializeEqualityDeleteFile =
        IcebergSerDe.deserializeDeleteFile(equalityDeleteFileBytes);
    long equalityDeleteFileRecordCount = deserializeEqualityDeleteFile.recordCount();
    Assert.assertEquals(df2RecordCount, equalityDeleteFileRecordCount);
    Assert.assertEquals(
        (Integer) (deserializeEqualityDeleteFile.partition().get(0, Integer.class)),
        Integer.valueOf(10));
    Assert.assertEquals(
        (String) (deserializeEqualityDeleteFile.partition().get(1, String.class)), "def");
    Assert.assertEquals(deserializeEqualityDeleteFile.content().toString(), "EQUALITY_DELETES");
  }

  @Test
  public void testManifestFileSerDe() {
    ManifestFile manifestFile = getManifestFile();
    byte[] ManifestFileBytes = IcebergSerDe.serializeManifestFile(manifestFile);
    ManifestFile mf = IcebergSerDe.deserializeManifestFile(ManifestFileBytes);
    Assert.assertEquals(
        "wasbs://testdir@azurev1databricks2.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041-1-bb5ecd1c-80fb-494f-9716-d2baa8f69eff.avro",
        mf.path());
    Assert.assertEquals(5000L, mf.length());
    Assert.assertEquals((Integer) 2, mf.addedFilesCount());
  }

  private ManifestFile getManifestFile() {
    GenericManifestFile genericManifestFile =
        new GenericManifestFile(
            "wasbs://testdir@azurev1databricks2.blob.core.windows.net/Automation/regression/iceberg/alltypes/metadata/snap-6325739561998439041-1-bb5ecd1c-80fb-494f-9716-d2baa8f69eff.avro",
            5000,
            0,
            ManifestContent.DATA,
            0,
            0,
            null,
            2,
            5000,
            0,
            0,
            0,
            0,
            null,
            null);

    return genericManifestFile;
  }

  @Test
  public void testPartitionValuesToIcebergDataSerialization() throws UnsupportedEncodingException {
    BatchSchema tableSchema =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"),
            CompleteType.FLOAT.toField("floatCol"),
            CompleteType.DOUBLE.toField("doubleCol"),
            CompleteType.BIT.toField("bitCol"),
            CompleteType.VARCHAR.toField("varCharCol"),
            CompleteType.BIGINT.toField("bigIntCol"),
            CompleteType.fromDecimalPrecisionScale(0, 4).toField("decimalCol"),
            CompleteType.VARBINARY.toField("varBinaryCol"),
            CompleteType.TIMESTAMP.toField("timeStampCol"),
            CompleteType.INT.toField("randomColumnIntIgnore"),
            CompleteType.FLOAT.toField("randomColumnFloatIgnore"));

    List<PartitionValue> partitionValues = new ArrayList<>();
    partitionValues.add(PartitionValue.of("integerCol", 20));
    partitionValues.add(PartitionValue.of("floatCol", 20.22F));
    partitionValues.add(PartitionValue.of("doubleCol", 40.2342D));
    partitionValues.add(PartitionValue.of("bitCol", true));
    partitionValues.add(PartitionValue.of("varCharCol", "tempVarCharValue"));
    partitionValues.add(PartitionValue.of("bigIntCol", 200000000L));

    BigDecimal bd = new BigDecimal("-12345.6789");
    BigInteger bi = bd.movePointRight(bd.scale()).unscaledValue();
    partitionValues.add(
        PartitionValue.of(
            "decimalCol", ByteBuffer.wrap(DecimalTools.signExtend16(bi.toByteArray()))));

    byte[] bytes = "randomString".getBytes(UTF_8);
    partitionValues.add(PartitionValue.of("varBinaryCol", ByteBuffer.wrap(bytes)));

    partitionValues.add(PartitionValue.of("timeStampCol", 23412341L));

    // Convert to partitionProtos
    List<PartitionProtobuf.PartitionValue> partitionValueProtos =
        partitionValues.stream().map(MetadataProtoUtils::toProtobuf).collect(Collectors.toList());

    // Convert to IcebergPartitionData
    IcebergPartitionData partitionData =
        IcebergSerDe.partitionValueToIcebergPartition(partitionValueProtos, tableSchema);

    Assert.assertEquals((Integer) partitionData.get(0, Integer.class), Integer.valueOf(20));
    Assert.assertEquals((Float) partitionData.get(1, Float.class), Float.valueOf(20.22f));
    Assert.assertEquals((Double) partitionData.get(2, Double.class), Double.valueOf(40.2342));
    Assert.assertEquals((Boolean) partitionData.get(3, Boolean.class), true);
    Assert.assertEquals((String) partitionData.get(4, String.class), "tempVarCharValue");
    Assert.assertEquals((Long) partitionData.get(5, Long.class), Long.valueOf(200000000L));
    Assert.assertEquals(
        (BigDecimal) partitionData.get(6, BigDecimal.class), new BigDecimal("-12345.6789"));

    ByteBuffer b = partitionData.get(7, ByteBuffer.class);
    String s = UTF_8.decode(b).toString();
    Assert.assertEquals(s, "randomString");

    Assert.assertEquals((Long) partitionData.get(8, Long.class), Long.valueOf(23412341000L));
    Assert.assertEquals(
        "struct<1000: integerCol: optional int, 1001: floatCol: optional float, 1002: doubleCol: optional double, 1003: bitCol: optional boolean, 1004: varCharCol: optional string, 1005: bigIntCol: optional long, 1006: decimalCol: optional decimal(0, 4), 1007: varBinaryCol: optional binary, 1008: timeStampCol: optional timestamptz>",
        partitionData.getPartitionType().toString());
  }

  @Test(expected = java.lang.IllegalStateException.class)
  public void testPartitionValuesToIcebergDataSerializationWithUnknownPartitionColumn() {
    BatchSchema tableSchema =
        BatchSchema.of(
            CompleteType.INT.toField("integerCol"), CompleteType.FLOAT.toField("floatCol"));

    // partition values have a random col which is not present in batch

    List<PartitionValue> partitionValues = new ArrayList<>();
    partitionValues.add(PartitionValue.of("integerCol", 20));
    partitionValues.add(PartitionValue.of("randomCol", 20.22F));

    // Convert to partitionProtos
    List<PartitionProtobuf.PartitionValue> partitionValueProtos =
        partitionValues.stream().map(MetadataProtoUtils::toProtobuf).collect(Collectors.toList());

    // should raise error
    IcebergPartitionData partitionData =
        IcebergSerDe.partitionValueToIcebergPartition(partitionValueProtos, tableSchema);
  }
}
