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

import static com.dremio.sabot.Fixtures.struct;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.tuple;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.Files;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitIdentity;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.util.TestUtilities;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import io.protostuff.ByteString;

public class TestManifestScanTableFunction extends BaseTestTableFunction {

  private static final String TABLE_NAME = "table1";

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.optional(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "date", Types.DateType.get()),
      Types.NestedField.optional(3, "name", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC_1 = PartitionSpec.builderFor(SCHEMA)
      .withSpecId(0)
      .identity("date")
      .build();

  private static final Map<Integer, PartitionSpec> PARTITION_SPEC_MAP = ImmutableMap.of(
      PARTITION_SPEC_1.specId(), PARTITION_SPEC_1);

  private static final Map<Integer, ByteBuffer> LOWER_BOUNDS = ImmutableMap.of(
      1, Conversions.toByteBuffer(Types.IntegerType.get(), 0),
      2, Conversions.toByteBuffer(Types.DateType.get(), 100),
      3, Conversions.toByteBuffer(Types.StringType.get(), "a"));

  private static final Map<Integer, ByteBuffer> UPPER_BOUNDS = ImmutableMap.of(
      1, Conversions.toByteBuffer(Types.IntegerType.get(), 9),
      2, Conversions.toByteBuffer(Types.DateType.get(), 1000),
      3, Conversions.toByteBuffer(Types.StringType.get(), "t"));

  private static final byte[] COL_IDS = getIcebergDatasetXAttr(SCHEMA);

  private static ManifestFile manifestFile1;
  private static ManifestFile manifestFile2;
  private static ManifestFile deleteManifestFile1;
  private static ManifestFile deleteManifestFile2;
  private static ManifestFile deleteManifestFile3;
  private static byte[] serializedKey1;
  private static byte[] serializedKey2;
  private static FileSystem fs;

  @Mock
  private StoragePluginId pluginId;
  @Mock(extraInterfaces = { SupportsIcebergRootPointer.class })
  private MutablePlugin plugin;

  @BeforeClass
  public static void initStatics() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), new Configuration());

    PartitionKey partitionKey1 = new PartitionKey(PARTITION_SPEC_1, SCHEMA);
    partitionKey1.set(0, 10);
    PartitionKey partitionKey2 = new PartitionKey(PARTITION_SPEC_1, SCHEMA);
    partitionKey2.set(0, 20);
    serializedKey1 = IcebergSerDe.serializeToByteArray(new Object[] { 10 });
    serializedKey2 = IcebergSerDe.serializeToByteArray(new Object[] { 20 });

    String tempDir = TestUtilities.createTempDir();
    String manifestPath1 = Paths.get(tempDir, "manifest1.avro").toString();
    String manifestPath2 = Paths.get(tempDir, "manifest2.avro").toString();
    String deleteManifestPath1 = Paths.get(tempDir, "deletemanifest1.avro").toString();
    String deleteManifestPath2 = Paths.get(tempDir, "deletemanifest2.avro").toString();
    String deleteManifestPath3 = Paths.get(tempDir, "deletemanifest3.avro").toString();

    manifestFile1 = createDataManifest(manifestPath1, PARTITION_SPEC_1, ImmutableList.of(
        DataFiles.builder(PARTITION_SPEC_1)
            .withPartition(partitionKey1)
            .withPath("datafile1.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build(),
        DataFiles.builder(PARTITION_SPEC_1)
            .withPartition(partitionKey2)
            .withPath("datafile2.parquet")
            .withFileSizeInBytes(200)
            .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()));

    manifestFile2 = createDataManifest(manifestPath2, PARTITION_SPEC_1, ImmutableList.of(
        DataFiles.builder(PARTITION_SPEC_1)
            .withPartition(partitionKey2)
            .withPath("datafile3.parquet")
            .withFileSizeInBytes(300)
            .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()));

    deleteManifestFile1 = createDeleteManifest(deleteManifestPath1, PARTITION_SPEC_1, ImmutableList.of(
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey1)
            .withPath("deletefile1.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build(),
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey2)
            .withPath("deletefile2.parquet")
            .withFileSizeInBytes(200)
            .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()),
        ImmutableList.of(1L, 2L));

    deleteManifestFile2 = createDeleteManifest(deleteManifestPath2, PARTITION_SPEC_1, ImmutableList.of(
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey2)
            .withPath("deletefile3.parquet")
            .withFileSizeInBytes(300)
            .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()),
        ImmutableList.of(3L));

    deleteManifestFile3 = createDeleteManifest(deleteManifestPath3, PARTITION_SPEC_1, ImmutableList.of(
            FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
                .ofPositionDeletes()
                .withPartition(partitionKey2)
                .withPath("deletefile3.parquet")
                .withFileSizeInBytes(300)
                .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
                .withFormat(FileFormat.PARQUET)
                .build(),
            FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
                .ofEqualityDeletes(1)
                .withPartition(partitionKey2)
                .withPath("deletefile4.parquet")
                .withFileSizeInBytes(200)
                .withMetrics(createMetrics(SCHEMA, LOWER_BOUNDS, UPPER_BOUNDS))
                .withFormat(FileFormat.PARQUET)
                .build()),
        ImmutableList.of(3L, 4L));

  }

  @Before
  public void prepareMocks() throws Exception {
    when(fec.getStoragePlugin(pluginId)).thenReturn(plugin);
    SupportsIcebergRootPointer sirp = (SupportsIcebergRootPointer) plugin;
    when(sirp.createFSWithAsyncOptions(anyString(), anyString(), any())).thenReturn(fs);
    when(sirp.getFsConfCopy()).thenReturn(new Configuration());
  }

  @Test
  public void testPathGeneratingProcessor() throws Exception {
    Table input = inputTable();
    Table output = t(
        th(
            SystemSchemas.DATAFILE_PATH,
            SystemSchemas.FILE_SIZE,
            SystemSchemas.SEQUENCE_NUMBER,
            SystemSchemas.PARTITION_SPEC_ID,
            SystemSchemas.PARTITION_KEY,
            SystemSchemas.PARTITION_INFO,
            SystemSchemas.COL_IDS),
        tr("datafile1.parquet", 100L, 1L, 0, serializedKey1,
            createDatePartitionInfo("date", 10, 0), COL_IDS),
        tr("datafile2.parquet", 200L, 1L, 0, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS),
        tr("datafile3.parquet", 300L, 1L, 0, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS));

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA;
    validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testPathGeneratingProcessorWithMinMaxColValues() throws Exception {
    Table input = inputTable();
    Table output = t(
        th(
            SystemSchemas.DATAFILE_PATH,
            SystemSchemas.FILE_SIZE,
            SystemSchemas.SEQUENCE_NUMBER,
            SystemSchemas.PARTITION_SPEC_ID,
            SystemSchemas.PARTITION_KEY,
            SystemSchemas.PARTITION_INFO,
            SystemSchemas.COL_IDS,
            "id_min",
            "id_max"),
        tr("datafile1.parquet", 100L, 1L, 0, serializedKey1,
            createDatePartitionInfo("date", 10, 0), COL_IDS, 0, 9),
        tr("datafile2.parquet", 200L, 1L, 0, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 0, 9),
        tr("datafile3.parquet", 300L, 1L, 0, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 0, 9));

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
        ImmutableList.of(
            Field.nullable("id_min", new ArrowType.Int(32, true)),
            Field.nullable("id_max", new ArrowType.Int(32, true))));

    validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testPathGeneratingProcessorWithPartColValues() throws Exception {
    Table input = inputTable();
    Table output = t(
        th(
            SystemSchemas.DATAFILE_PATH,
            SystemSchemas.FILE_SIZE,
            SystemSchemas.SEQUENCE_NUMBER,
            SystemSchemas.PARTITION_SPEC_ID,
            SystemSchemas.PARTITION_KEY,
            SystemSchemas.PARTITION_INFO,
            SystemSchemas.COL_IDS,
            "date_val"),
        tr("datafile1.parquet", 100L, 1L, 0, serializedKey1,
            createDatePartitionInfo("date", 10, 0), COL_IDS, 10),
        tr("datafile2.parquet", 200L, 1L, 0, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 20),
        tr("datafile3.parquet", 300L, 1L, 0, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 20));

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
        ImmutableList.of(
            Field.nullable("date_val", new ArrowType.Int(32, true))));

    validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testPathGeneratingProcessorWithDeleteManifests() throws Exception {
    Table input = deletesInputTable();
    Table output = t(
        th(
            SystemSchemas.DELETEFILE_PATH,
            SystemSchemas.FILE_CONTENT,
            SystemSchemas.SEQUENCE_NUMBER,
            SystemSchemas.PARTITION_SPEC_ID,
            SystemSchemas.PARTITION_KEY),
        tr("deletefile1.parquet", FileContent.POSITION_DELETES.id(), 1L, 0, serializedKey1),
        tr("deletefile2.parquet", FileContent.POSITION_DELETES.id(), 2L, 0, serializedKey2),
        tr("deletefile3.parquet", FileContent.POSITION_DELETES.id(), 3L, 0, serializedKey2));

    validateSingle(getPop(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA, ManifestContent.DELETES),
        TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testPathGeneratingProcessorWithEqDeletesFails() throws Exception {
    Table input = t(
        th(
            struct(SystemSchemas.SPLIT_IDENTITY, ImmutableList.of(
                SplitIdentity.PATH, SplitIdentity.OFFSET, SplitIdentity.LENGTH, SplitIdentity.FILE_LENGTH)),
            SystemSchemas.SPLIT_INFORMATION,
            SystemSchemas.COL_IDS),
        inputRow(deleteManifestFile3, COL_IDS));
    Table output = t(
        th(
            SystemSchemas.DELETEFILE_PATH,
            SystemSchemas.FILE_CONTENT,
            SystemSchemas.SEQUENCE_NUMBER,
            SystemSchemas.PARTITION_SPEC_ID,
            SystemSchemas.PARTITION_KEY),
        true,
        tr("", 0, 0L, 0, new byte[] {})); // fake row to match types

    assertThatThrownBy(() -> validateSingle(
        getPop(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA, ManifestContent.DELETES),
        TableFunctionOperator.class, input, output, 3))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Equality deletes are not supported.");
  }

  @Test
  public void testOutputBufferNotReused() throws Exception {
    Table input = inputTable();
    validateOutputBufferNotReused(getPop(SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA, ManifestContent.DATA), input, 2);
  }

  private Table inputTable() {
    return t(
        th(
            struct(SystemSchemas.SPLIT_IDENTITY, ImmutableList.of(
                SplitIdentity.PATH, SplitIdentity.OFFSET, SplitIdentity.LENGTH, SplitIdentity.FILE_LENGTH)),
            SystemSchemas.SPLIT_INFORMATION,
            SystemSchemas.COL_IDS),
        inputRow(manifestFile1, COL_IDS),
        inputRow(manifestFile2, COL_IDS));
  }

  private Table deletesInputTable() {
    return t(
        th(
            struct(SystemSchemas.SPLIT_IDENTITY, ImmutableList.of(
                SplitIdentity.PATH, SplitIdentity.OFFSET, SplitIdentity.LENGTH, SplitIdentity.FILE_LENGTH)),
            SystemSchemas.SPLIT_INFORMATION,
            SystemSchemas.COL_IDS),
        inputRow(deleteManifestFile1, COL_IDS),
        inputRow(deleteManifestFile2, COL_IDS));
  }

  private Fixtures.DataRow inputRow(ManifestFile manifestFile, byte[] colIds) {
    return tr(
        tuple(manifestFile.path(), 0, manifestFile.length(), manifestFile.length()),
        IcebergSerDe.serializeManifestFile(manifestFile),
        colIds);
  }

  private byte[] createDatePartitionInfo(String colName, int partitionVal, int version)
      throws Exception {
    PartitionProtobuf.NormalizedPartitionInfo.Builder partitionInfoBuilder = PartitionProtobuf.NormalizedPartitionInfo.newBuilder().setId(String.valueOf(1));

    PartitionProtobuf.PartitionValue.Builder partitionValueBuilder = PartitionProtobuf.PartitionValue.newBuilder();
    partitionValueBuilder.setColumn(colName);
    partitionValueBuilder.setLongValue(TimeUnit.DAYS.toMillis(partitionVal));
    partitionInfoBuilder.addValues(partitionValueBuilder.build());

    partitionValueBuilder.setColumn(IncrementalUpdateUtils.UPDATE_COLUMN);
    partitionValueBuilder.setLongValue(version);
    partitionInfoBuilder.addValues(partitionValueBuilder.build());

    return IcebergSerDe.serializeToByteArray(partitionInfoBuilder.build());
  }

  private TableFunctionPOP getPop(BatchSchema outputSchema, ManifestContent manifestContent) throws Exception {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_SCAN,
            true,
            new ManifestScanTableFunctionContext(
                null,
                ByteString.copyFrom(IcebergSerDe.serializePartitionSpecAsJsonMap(PARTITION_SPEC_MAP)),
                SchemaParser.toJson(SCHEMA),
                IcebergSerDe.serializeToByteArray(null),
                null,
                outputSchema,
                new SchemaConverter(TABLE_NAME).fromIceberg(SCHEMA),
                ImmutableList.of(ImmutableList.of(TABLE_NAME)),
                null,
                pluginId,
                null,
                outputSchema.getFields().stream()
                    .map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
                null,
                null,
                null,
                false,
                false,
                true,
                null,
                manifestContent)));
  }

  private static ManifestFile createDataManifest(String path, PartitionSpec spec,  List<DataFile> files)
      throws Exception {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(2, spec, Files.localOutput(path), 1L);
    try {
      files.forEach(f -> writer.add(f, 1));
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }

  private static ManifestFile createDeleteManifest(String path, PartitionSpec spec,  List<DeleteFile> files,
      List<Long> sequenceNumbers)
      throws Exception {
    ManifestWriter<DeleteFile> writer =
        ManifestFiles.writeDeleteManifest(2, spec, Files.localOutput(path), 1L);
    try {
      for (int i = 0; i < files.size(); i++) {
        writer.add(files.get(i), sequenceNumbers.get(i));
      }
    } finally {
      writer.close();
    }

    return writer.toManifestFile();
  }


  private static Metrics createMetrics(Schema schema, Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    Map<Integer, Long> counts = schema.columns().stream()
        .collect(Collectors.toMap(Types.NestedField::fieldId, f -> 1L));
    return new Metrics(1L, counts, counts, counts, counts,
        lowerBounds, upperBounds);
  }

  private static byte[] getIcebergDatasetXAttr(Schema schema) {
    return IcebergProtobuf.IcebergDatasetXAttr.newBuilder()
        .addAllColumnIds(IcebergUtils.getIcebergColumnNameToIDMap(schema).entrySet().stream()
            .map(c -> IcebergProtobuf.IcebergSchemaField.newBuilder()
                .setSchemaPath(c.getKey())
                .setId(c.getValue())
                .build())
            .collect(Collectors.toList()))
        .build()
        .toByteArray();
  }
}
