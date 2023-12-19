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

import static com.dremio.BaseTestQuery.createTempDirWithName;
import static com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode.EXPIRED_SNAPSHOTS;
import static com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode.LIVE_SNAPSHOTS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.hadoop.IcebergHadoopModel;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link SingleTableIcebergExpirySnapshotsReader}
 */
public class TestSingleTableIcebergExpirySnapshotsReader {

  private static final HadoopTables hadoopTables = new HadoopTables(new Configuration());
  private static final File baseFolder = createTempDirWithName("test");
  private static final Configuration CONF = new Configuration();
  private static FileSystem fs;
  private static ExecutorService executorService;

  private final BufferAllocator allocator = new RootAllocator();

  @BeforeClass
  public static void initStatics() throws Exception {
    fs = HadoopFileSystem.get(Path.of("/"), CONF);
    executorService = Executors.newCachedThreadPool();
  }

  @AfterClass
  public static void cleanup() {
    try {
      executorService.shutdown();
      executorService.awaitTermination(1, TimeUnit.MINUTES);

      FileUtils.deleteDirectory(baseFolder);
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testLiveSnapshotsEmptyTable() throws Exception {
    Table table = createTable(0);
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);
    SingleTableIcebergExpirySnapshotsReader reader = new SingleTableIcebergExpirySnapshotsReader(operatorContext(),
      toSplit(table), plugin(), props(), scanOptions);
    reader.setup(outputMutator());
    int recordCount = reader.next();

    assertThat(recordCount).isEqualTo(0);

    table.refresh();
    assertThat(table.currentSnapshot()).isNull();
  }

  @Test
  public void testLiveSnapshotsAllSurvived() throws Exception {
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);

    Table table = createTable(5);
    SingleTableIcebergExpirySnapshotsReader reader = new SingleTableIcebergExpirySnapshotsReader(operatorContext(),
      toSplit(table), plugin(), props(), scanOptions);
    OutputMutator outputMutator = outputMutator();

    reader.setup(outputMutator);
    int recordCount = reader.next();

    assertThat(recordCount).isEqualTo(5);

    String metaPath = String.format("file://%s", ((BaseTable) table).operations().current().metadataFileLocation());
    List<SnapshotEntry> expectedSnapshots = StreamSupport.stream(table.snapshots().spliterator(), false)
      .map(s -> new SnapshotEntry(metaPath, s)).collect(Collectors.toList());

    validate(outputMutator, expectedSnapshots);
  }

  @Test
  public void testExpiredSnapshotsNoRecords() throws Exception {
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(EXPIRED_SNAPSHOTS, System.currentTimeMillis(), 1);

    Table table = createTable(5);
    SingleTableIcebergExpirySnapshotsReader reader = new SingleTableIcebergExpirySnapshotsReader(operatorContext(),
      toSplit(table), plugin(), props(), scanOptions);
    OutputMutator outputMutator = outputMutator();

    reader.setup(outputMutator);
    int recordCount = reader.next();

    assertThat(recordCount).isEqualTo(0);
  }

  @Test
  public void testLiveSnapshotsOneSurvived() throws Exception {
    Table table = createTable(5);
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);

    SingleTableIcebergExpirySnapshotsReader reader = new SingleTableIcebergExpirySnapshotsReader(operatorContext(),
      toSplit(table), plugin(), props(), scanOptions);
    OutputMutator outputMutator = outputMutator();

    reader.setup(outputMutator);
    int recordCount = reader.next();

    assertThat(recordCount).isEqualTo(1);

    table.refresh();
    String metaPath = String.format("file://%s", ((BaseTable) table).operations().current().metadataFileLocation());
    List<SnapshotEntry> expectedSnapshots = ImmutableList.of(new SnapshotEntry(metaPath, table.currentSnapshot()));

    validate(outputMutator, expectedSnapshots);
  }

  @Test
  public void testExpiredSnapshotsOneSurvived() throws Exception {
    Table table = createTable(5);
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(EXPIRED_SNAPSHOTS, System.currentTimeMillis(), 1);

    SingleTableIcebergExpirySnapshotsReader reader = new SingleTableIcebergExpirySnapshotsReader(operatorContext(),
      toSplit(table), plugin(), props(), scanOptions);
    OutputMutator outputMutator = outputMutator();

    reader.setup(outputMutator);
    int recordCount = reader.next();

    assertThat(recordCount).isEqualTo(4);

    String metaPath = String.format("file://%s", ((BaseTable) table).operations().current().metadataFileLocation());
    List<SnapshotEntry> expectedSnapshots = StreamSupport.stream(table.snapshots().spliterator(), false)
      .map(s -> new SnapshotEntry(metaPath, s)).filter(s -> s.getSnapshotId() != table.currentSnapshot().snapshotId())
      .collect(Collectors.toList());
    validate(outputMutator, expectedSnapshots);
  }

  @Test
  public void testLiveSnapshotsExceedsBatchSize() throws Exception {
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);
    Table table = createTable(10);

    SingleTableIcebergExpirySnapshotsReader reader = new SingleTableIcebergExpirySnapshotsReader(operatorContext(),
      toSplit(table), plugin(), props(), scanOptions);
    OutputMutator outputMutator = outputMutator();

    reader.setup(outputMutator);

    int recordCount1 = reader.next();
    assertThat(recordCount1).isEqualTo(5);
    List<SnapshotEntry> actualEntries1 = getActualEntries(outputMutator);

    table.refresh();

    int recordCount2 = reader.next();
    assertThat(recordCount2).isEqualTo(5);
    List<SnapshotEntry> actualEntries2 = getActualEntries(outputMutator);

    List<SnapshotEntry> actualEntriesAll = Stream.concat(actualEntries1.stream(), actualEntries2.stream()).collect(Collectors.toList());

    String metaPath = String.format("file://%s", ((BaseTable) table).operations().current().metadataFileLocation());
    List<SnapshotEntry> expectedSnapshots = StreamSupport.stream(table.snapshots().spliterator(), false)
      .map(s -> new SnapshotEntry(metaPath, s)).collect(Collectors.toList());

    assertThat(actualEntriesAll).containsExactlyElementsOf(expectedSnapshots);
  }

  private void validate(OutputMutator outputMutator, List<SnapshotEntry> expectedEntries) {
    List<SnapshotEntry> actualEntries = getActualEntries(outputMutator);
    assertThat(actualEntries).containsAll(expectedEntries);
  }

  private List<SnapshotEntry> getActualEntries(OutputMutator outputMutator) {
    BigIntVector snapshotIdVector = (BigIntVector) outputMutator.getVector(SystemSchemas.SNAPSHOT_ID);
    VarCharVector metadataVector = (VarCharVector) outputMutator.getVector(SystemSchemas.METADATA_FILE_PATH);
    VarCharVector manifestListVector = (VarCharVector) outputMutator.getVector(SystemSchemas.MANIFEST_LIST_PATH);

    return IntStream.range(0, snapshotIdVector.getValueCount())
      .mapToObj(i -> new SnapshotEntry(
        new String(metadataVector.get(i), StandardCharsets.UTF_8),
        snapshotIdVector.get(i),
        new String(manifestListVector.get(i), StandardCharsets.UTF_8)))
      .collect(Collectors.toList());
  }

  private OutputMutator outputMutator() {
    TestOutputMutator outputMutator = new TestOutputMutator(allocator);
    FieldVector metaVector = TypeHelper.getNewVector(Field.nullable(SystemSchemas.METADATA_FILE_PATH, new ArrowType.Utf8()), allocator);
    FieldVector snapshotVector = TypeHelper.getNewVector(Field.nullable(SystemSchemas.SNAPSHOT_ID, new ArrowType.Int(64, true)), allocator);
    FieldVector manifestListVector = TypeHelper.getNewVector(Field.nullable(SystemSchemas.MANIFEST_LIST_PATH, new ArrowType.Utf8()), allocator);
    outputMutator.addField(metaVector);
    outputMutator.addField(snapshotVector);
    outputMutator.addField(manifestListVector);
    return outputMutator;
  }

  private IcebergProtobuf.IcebergDatasetSplitXAttr toSplit(Table table) {
    return IcebergProtobuf.IcebergDatasetSplitXAttr.newBuilder()
      .setDbName("default")
      .setPath(((BaseTable)table).operations().current().metadataFileLocation())
      .setTableName(table.name())
      .build();
  }

  private SupportsIcebergMutablePlugin plugin() throws IOException {
    SupportsIcebergMutablePlugin plugin = mock(SupportsIcebergMutablePlugin.class);
    when(plugin.createFS(anyString(), anyString(), any(OperatorContext.class))).thenReturn(fs);

    FileIO io = new DremioFileIO(fs, null, Collections.emptyList(), null, 100L,
      new HadoopFileSystemConfigurationAdapter(CONF));
    when(plugin.createIcebergFileIO(any(), any(), any(), any(), any())).thenReturn(io);
    when(plugin.getSystemUserFS()).thenReturn(fs);
    when(plugin.getFsConfCopy()).thenReturn(CONF);

    IcebergModel icebergModel = new IcebergHadoopModel(plugin);
    when(plugin.getIcebergModel(any(IcebergTableProps.class), anyString(), any(OperatorContext.class), eq(io)))
      .thenReturn(icebergModel);

    return plugin;
  }

  private OpProps props() {
    OpProps props = mock(OpProps.class);
    when(props.getUserName()).thenReturn(SystemUser.SYSTEM_USERNAME);
    return props;
  }

  private OperatorContext operatorContext() {
    OperatorContext context = mock(OperatorContext.class);

    OperatorStats operatorStats = mock(OperatorStats.class);
    doNothing().when(operatorStats).addLongStat(any(MetricDef.class), anyLong());
    doNothing().when(operatorStats).setReadIOStats();

    when(context.getStats()).thenReturn(operatorStats);
    when(context.getTargetBatchSize()).thenReturn(5);

    when(context.getExecutor()).thenReturn(executorService);
    return context;
  }

  private Table createTable(int numberOfSnapshots) {
    Schema icebergTableSchema = new Schema(ImmutableList.of(Types.NestedField.required(0, "id", new Types.IntegerType())));
    Table table = hadoopTables.create(icebergTableSchema, baseFolder.getAbsolutePath() + "/" + generateUniqueTableName());
    for (int i = 0; i < numberOfSnapshots; i++) {
      table.newFastAppend().appendFile(DataFiles.builder(table.spec())
        .withPath(String.format("%s/data/data-%d.parquet", table.location(), i))
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(1024L)
        .withRecordCount(100L)
        .build()).commit();
    }
    return table;
  }

  static int randomInt() {
    return ThreadLocalRandom.current().nextInt(1, 100000);
  }

  public static String generateUniqueTableName() {
    return "table" + randomInt();
  }
}
