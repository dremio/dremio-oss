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

import static com.dremio.sabot.RecordSet.li;
import static com.dremio.sabot.RecordSet.r;
import static com.dremio.sabot.RecordSet.rs;
import static com.dremio.sabot.RecordSet.st;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.NUM_DATA_FILE;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.NUM_DELETE_MANIFESTS;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.NUM_MANIFEST_FILE;
import static com.dremio.sabot.op.tablefunction.TableFunctionOperator.Metric.NUM_POS_DELETE_FILES;
import static org.assertj.core.api.Assertions.assertThat;
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
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanFilters;
import com.dremio.exec.physical.config.ManifestScanTableFunctionContext;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.physical.config.TableFunctionPOP;
import com.dremio.exec.planner.acceleration.IncrementalUpdateUtils;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.util.LongRange;
import com.dremio.exec.util.TestUtilities;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.BaseTestTableFunction;
import com.dremio.sabot.RecordSet;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.tablefunction.TableFunctionOperator;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.google.common.base.Strings;
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
      .withSpecId(1)
      .identity("date")
      .build();

  private static final Map<Integer, PartitionSpec> PARTITION_SPEC_MAP = ImmutableMap.of(
      PARTITION_SPEC_1.specId(), PARTITION_SPEC_1);

  private static final Map<Integer, PartitionSpec> DELETE_PARTITION_SPEC_MAP = ImmutableMap.of(
      PARTITION_SPEC_1.specId(), PARTITION_SPEC_1,
      PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned());

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
  private static ManifestFile deleteManifestFile4;
  private static ManifestFile deleteManifestWithLongPaths;
  private static byte[] serializedKey1;
  private static byte[] serializedKey2;
  private static byte[] serializedUnpartitionedKey;
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
    PartitionKey unpartitionedKey = new PartitionKey(PartitionSpec.unpartitioned(), SCHEMA);
    serializedKey1 = IcebergSerDe.serializeToByteArray(new Object[] { 10 });
    serializedKey2 = IcebergSerDe.serializeToByteArray(new Object[] { 20 });
    serializedUnpartitionedKey = IcebergSerDe.serializeToByteArray(new Object[] {});

    String tempDir = TestUtilities.createTempDir();
    String manifestPath1 = Paths.get(tempDir, "manifest1.avro").toString();
    String manifestPath2 = Paths.get(tempDir, "manifest2.avro").toString();
    String deleteManifestPath1 = Paths.get(tempDir, "deletemanifest1.avro").toString();
    String deleteManifestPath2 = Paths.get(tempDir, "deletemanifest2.avro").toString();
    String deleteManifestPath3 = Paths.get(tempDir, "deletemanifest3.avro").toString();
    String deleteManifestPath4 = Paths.get(tempDir, "deletemanifest4.avro").toString();
    String longPathsManifestPath = Paths.get(tempDir, "manifest_with_long_paths.avro").toString();

    manifestFile1 = createDataManifest(manifestPath1, PARTITION_SPEC_1, ImmutableList.of(
        DataFiles.builder(PARTITION_SPEC_1)
            .withPartition(partitionKey1)
            .withPath("datafile1.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(createMetrics(SCHEMA, 10, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build(),
        DataFiles.builder(PARTITION_SPEC_1)
            .withPartition(partitionKey2)
            .withPath("datafile2.parquet")
            .withFileSizeInBytes(200)
            .withMetrics(createMetrics(SCHEMA, 20, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()));

    manifestFile2 = createDataManifest(manifestPath2, PARTITION_SPEC_1, ImmutableList.of(
        DataFiles.builder(PARTITION_SPEC_1)
            .withPartition(partitionKey2)
            .withPath("datafile3.parquet")
            .withFileSizeInBytes(300)
            .withMetrics(createMetrics(SCHEMA, 30, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()));

    deleteManifestFile1 = createDeleteManifest(deleteManifestPath1, PARTITION_SPEC_1, ImmutableList.of(
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey1)
            .withPath("deletefile1.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(createMetrics(SCHEMA, 10, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build(),
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey2)
            .withPath("deletefile2.parquet")
            .withFileSizeInBytes(200)
            .withMetrics(createMetrics(SCHEMA, 20, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()),
        ImmutableList.of(1L, 2L));

    deleteManifestFile2 = createDeleteManifest(deleteManifestPath2, PARTITION_SPEC_1, ImmutableList.of(
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey2)
            .withPath("deletefile3.parquet")
            .withFileSizeInBytes(300)
            .withMetrics(createMetrics(SCHEMA, 30, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()),
        ImmutableList.of(3L));

    deleteManifestFile3 = createDeleteManifest(deleteManifestPath3, PARTITION_SPEC_1, ImmutableList.of(
            FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
                .ofPositionDeletes()
                .withPartition(partitionKey2)
                .withPath("deletefile3.parquet")
                .withFileSizeInBytes(300)
                .withMetrics(createMetrics(SCHEMA, 30, LOWER_BOUNDS, UPPER_BOUNDS))
                .withFormat(FileFormat.PARQUET)
                .build(),
            FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
                .ofEqualityDeletes(1, 3)
                .withPartition(partitionKey2)
                .withPath("deletefile4.parquet")
                .withFileSizeInBytes(200)
                .withMetrics(createMetrics(SCHEMA, 20, LOWER_BOUNDS, UPPER_BOUNDS))
                .withFormat(FileFormat.PARQUET)
                .build()),
        ImmutableList.of(3L, 4L));

    deleteManifestFile4 = createDeleteManifest(deleteManifestPath4, PartitionSpec.unpartitioned(),
        ImmutableList.of(
            FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
                .ofEqualityDeletes(1)
                .withPartition(unpartitionedKey)
                .withPath("deletefile5.parquet")
                .withFileSizeInBytes(200)
                .withMetrics(createMetrics(SCHEMA, 20, LOWER_BOUNDS, UPPER_BOUNDS))
                .withFormat(FileFormat.PARQUET)
                .build()),
        ImmutableList.of(3L));

    deleteManifestWithLongPaths = createDeleteManifest(longPathsManifestPath, PARTITION_SPEC_1, ImmutableList.of(
        FileMetadata.deleteFileBuilder(PARTITION_SPEC_1)
            .ofPositionDeletes()
            .withPartition(partitionKey1)
            .withPath("deletefile-" + Strings.repeat("0", 2000) + "1.parquet")
            .withFileSizeInBytes(100)
            .withMetrics(createMetrics(SCHEMA, 10, LOWER_BOUNDS, UPPER_BOUNDS))
            .withFormat(FileFormat.PARQUET)
            .build()),
        ImmutableList.of(1L));
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
    RecordSet input = inputRecordSet();

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA;
    RecordSet output = rs(outputSchema,
        r("datafile1.parquet", 100L, 1L, 1, serializedKey1,
            createDatePartitionInfo("date", 10, 0), COL_IDS),
        r("datafile2.parquet", 200L, 1L, 1, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS),
        r("datafile3.parquet", 300L, 1L, 1, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS));

    validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testPathGeneratingProcessorWithMinMaxColValues() throws Exception {
    RecordSet input = inputRecordSet();

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
        ImmutableList.of(
            Field.nullable("id_min", new ArrowType.Int(32, true)),
            Field.nullable("id_max", new ArrowType.Int(32, true))));
    RecordSet output = rs(outputSchema,
        r("datafile1.parquet", 100L, 1L, 1, serializedKey1,
            createDatePartitionInfo("date", 10, 0), COL_IDS, 0, 9),
        r("datafile2.parquet", 200L, 1L, 1, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 0, 9),
        r("datafile3.parquet", 300L, 1L, 1, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 0, 9));

     validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testPathGeneratingProcessorWithPartColValues() throws Exception {
    RecordSet input = inputRecordSet();

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
        ImmutableList.of(
            Field.nullable("date_val", new ArrowType.Int(32, true))));
    RecordSet output = rs(outputSchema,
        r("datafile1.parquet", 100L, 1L, 1, serializedKey1,
            createDatePartitionInfo("date", 10, 0), COL_IDS, 10),
        r("datafile2.parquet", 200L, 1L, 1, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 20),
        r("datafile3.parquet", 300L, 1L, 1, serializedKey2,
            createDatePartitionInfo("date", 20, 0), COL_IDS, 20));

    validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testPathGeneratingProcessorWithDeleteManifests() throws Exception {
    RecordSet input = deletesInputRecordSet();
    RecordSet output = rs(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA,
        r(st("deletefile1.parquet", FileContent.POSITION_DELETES.id(), 10L, null), 1L, 1, serializedKey1),
        r(st("deletefile2.parquet", FileContent.POSITION_DELETES.id(), 20L, null), 2L, 1, serializedKey2),
        r(st("deletefile3.parquet", FileContent.POSITION_DELETES.id(), 30L, null), 3L, 1, serializedKey2));

    validateSingle(
        getPop(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA, ManifestContent.DELETES, DELETE_PARTITION_SPEC_MAP),
        TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testPathGeneratingProcessorWithEqDeletes() throws Exception {
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
        inputRow(deleteManifestFile3, COL_IDS),
        inputRow(deleteManifestFile4, COL_IDS));
    RecordSet output = rs(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA,
        r(st("deletefile3.parquet", FileContent.POSITION_DELETES.id(), 30L, null), 3L, 1, serializedKey2),
        r(st("deletefile4.parquet", FileContent.EQUALITY_DELETES.id(), 20L, li(1, 3)), 4L, 1, serializedKey2),
        r(st("deletefile5.parquet", FileContent.EQUALITY_DELETES.id(), 20L, li(1)), 3L, 0,
            serializedUnpartitionedKey));

    validateSingle(
        getPop(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA, ManifestContent.DELETES, DELETE_PARTITION_SPEC_MAP),
        TableFunctionOperator.class, input, output, 3);
  }

  @Test
  public void testWithLongDeleteFilePath() throws Exception {
    RecordSet input = rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
        inputRow(deleteManifestWithLongPaths, COL_IDS));

    BatchSchema outputSchema = SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA;
    RecordSet output = rs(outputSchema,
        r(st("deletefile-" + Strings.repeat("0", 2000) + "1.parquet", FileContent.POSITION_DELETES.id(), 10L, null),
            1L, 1, serializedKey1));

    validateSingle(getPop(outputSchema, ManifestContent.DELETES), TableFunctionOperator.class, input, output, 2);
  }

  @Test
  public void testDataManifestStats() throws Exception {
    RecordSet input = inputRecordSet();
    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA;
    OperatorStats stats = validateSingle(getPop(outputSchema, ManifestContent.DATA), TableFunctionOperator.class,
        input, null, 2);

    assertThat(stats.getLongStat(NUM_MANIFEST_FILE)).isEqualTo(2);
    assertThat(stats.getLongStat(NUM_DATA_FILE)).isEqualTo(3);
    assertThat(stats.getLongStat(NUM_DELETE_MANIFESTS)).isEqualTo(0);
    assertThat(stats.getLongStat(NUM_POS_DELETE_FILES)).isEqualTo(0);
  }

  @Test
  public void testDeleteManifestStats() throws Exception {
    RecordSet input = deletesInputRecordSet();
    OperatorStats stats = validateSingle(
        getPop(SystemSchemas.ICEBERG_DELETE_MANIFEST_SCAN_SCHEMA, ManifestContent.DELETES),
        TableFunctionOperator.class, input, null, 3);

    assertThat(stats.getLongStat(NUM_MANIFEST_FILE)).isEqualTo(0);
    assertThat(stats.getLongStat(NUM_DATA_FILE)).isEqualTo(0);
    assertThat(stats.getLongStat(NUM_DELETE_MANIFESTS)).isEqualTo(2);
    assertThat(stats.getLongStat(NUM_POS_DELETE_FILES)).isEqualTo(3);
  }

  @Test
  public void testOutputBufferNotReused() throws Exception {
    RecordSet input = inputRecordSet();
    validateOutputBufferNotReused(getPop(SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA, ManifestContent.DATA), input, 2);
  }

  @Test
  public void testFilterOnFileSizeExcludeAll() throws Exception {
    RecordSet input = inputRecordSet();

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
      ImmutableList.of(
        Field.nullable("date_val", new ArrowType.Int(32, true))));
    LongRange excludeAllRange = new LongRange(0L, Long.MAX_VALUE);
    ManifestScanFilters manifestScanFilters = new ImmutableManifestScanFilters.Builder()
      .setSkipDataFileSizeRange(excludeAllRange).setMinPartitionSpecId(1).build();
    OperatorStats stats = validateSingle(getPop(outputSchema, ManifestContent.DATA, PARTITION_SPEC_MAP, manifestScanFilters),
      TableFunctionOperator.class, input, null, 10);

    assertThat(stats.getLongStat(NUM_MANIFEST_FILE)).isEqualTo(2);
    assertThat(stats.getLongStat(NUM_DATA_FILE)).isEqualTo(0);
  }

  @Test
  public void testFilterOnFileSizeExcludeSome() throws Exception {
    RecordSet input = inputRecordSet();

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
      ImmutableList.of(
        Field.nullable("date_val", new ArrowType.Int(32, true))));

    // Data file 2 skipped from output because it's size is 200 bytes
    RecordSet output = rs(outputSchema,
      r("datafile1.parquet", 100L, 1L, 1, serializedKey1,
        createDatePartitionInfo("date", 10, 0), COL_IDS, 10),
      r("datafile3.parquet", 300L, 1L, 1, serializedKey2,
        createDatePartitionInfo("date", 20, 0), COL_IDS, 20));
    LongRange excludeDataFile2Range = new LongRange(200L, 250L);
    ManifestScanFilters manifestScanFilters = new ImmutableManifestScanFilters.Builder()
      .setSkipDataFileSizeRange(excludeDataFile2Range).setMinPartitionSpecId(1).build();
    validateSingle(getPop(outputSchema, ManifestContent.DATA, PARTITION_SPEC_MAP, manifestScanFilters),
      TableFunctionOperator.class, input, output, 10);
  }

  @Test
  public void testFilterWithPartitionSpecEvolutionChecks() throws Exception {
    RecordSet input = inputRecordSet();

    BatchSchema outputSchema = SystemSchemas.ICEBERG_MANIFEST_SCAN_SCHEMA.addColumns(
      ImmutableList.of(
        Field.nullable("date_val", new ArrowType.Int(32, true))));

    RecordSet output = rs(outputSchema,
      r("datafile1.parquet", 100L, 1L, 1, serializedKey1,
        createDatePartitionInfo("date", 10, 0), COL_IDS, 10),
      r("datafile2.parquet", 200L, 1L, 1, serializedKey2,
        createDatePartitionInfo("date", 20, 0), COL_IDS, 20),
      r("datafile3.parquet", 300L, 1L, 1, serializedKey2,
        createDatePartitionInfo("date", 20, 0), COL_IDS, 20));

    LongRange excludeAllRange = new LongRange(0L, Long.MAX_VALUE);
    int evolvedPartitionSpecId = PARTITION_SPEC_1.specId() + 1;
    ManifestScanFilters manifestScanFilters = new ImmutableManifestScanFilters.Builder()
      .setSkipDataFileSizeRange(excludeAllRange).setMinPartitionSpecId(evolvedPartitionSpecId).build();

    validateSingle(getPop(outputSchema, ManifestContent.DATA, PARTITION_SPEC_MAP, manifestScanFilters),
      TableFunctionOperator.class, input, output, 10);
  }

  private RecordSet inputRecordSet() {
    return rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
        inputRow(manifestFile1, COL_IDS),
        inputRow(manifestFile2, COL_IDS));
  }

  private RecordSet deletesInputRecordSet() {
    return rs(SystemSchemas.SPLIT_GEN_AND_COL_IDS_SCAN_SCHEMA,
        inputRow(deleteManifestFile1, COL_IDS),
        inputRow(deleteManifestFile2, COL_IDS));
  }

  private RecordSet.Record inputRow(ManifestFile manifestFile, byte[] colIds) {
    return r(
        st(manifestFile.path(), 0L, manifestFile.length(), manifestFile.length()),
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
    return getPop(outputSchema, manifestContent, PARTITION_SPEC_MAP);
  }

  private TableFunctionPOP getPop(BatchSchema outputSchema, ManifestContent manifestContent,
                                  Map<Integer, PartitionSpec> partitionSpecMap) throws Exception {
    return getPop(outputSchema, manifestContent, partitionSpecMap, ManifestScanFilters.empty());
  }

  private TableFunctionPOP getPop(BatchSchema outputSchema, ManifestContent manifestContent,
      Map<Integer, PartitionSpec> partitionSpecMap, ManifestScanFilters manifestScanFilters) throws Exception {
    return new TableFunctionPOP(
        PROPS,
        null,
        new TableFunctionConfig(
            TableFunctionConfig.FunctionType.ICEBERG_MANIFEST_SCAN,
            true,
            new ManifestScanTableFunctionContext(
                null,
                ByteString.copyFrom(IcebergSerDe.serializePartitionSpecAsJsonMap(partitionSpecMap)),
                SchemaParser.toJson(SCHEMA),
                null,
                outputSchema,
                SchemaConverter.getBuilder().setTableName(TABLE_NAME).build().fromIceberg(SCHEMA),
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
                manifestContent,
                manifestScanFilters)));
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


  private static Metrics createMetrics(Schema schema, long recordCount, Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    Map<Integer, Long> counts = schema.columns().stream()
        .collect(Collectors.toMap(Types.NestedField::fieldId, f -> 1L));
    return new Metrics(recordCount, counts, counts, counts, counts,
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
