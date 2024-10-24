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
package com.dremio.plugins.dataplane.exec;

import static com.dremio.exec.store.iceberg.SnapshotsScanOptions.Mode.LIVE_SNAPSHOTS;
import static com.dremio.exec.store.iceberg.TestSingleTableIcebergExpirySnapshotsReader.generateUniqueTableName;
import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.apache.iceberg.DremioTableProperties.NESSIE_GC_ENABLED;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.MIN_SNAPSHOTS_TO_KEEP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.expr.TypeHelper;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.IcebergFileType;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.SupportsIcebergMutablePlugin;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedModel;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClientImpl;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.util.ContainerNotFoundException;
import com.dremio.sabot.exec.context.MetricDef;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.scan.OutputMutator;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.stubbing.Answer;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Reference;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

/** Tests for {@link NessieIcebergExpirySnapshotsReader} */
@ExtendWith(OlderNessieServersExtension.class)
@NotThreadSafe
public class TestNessieIcebergExpirySnapshotsReader {
  private static final String workingBranchName = "test";
  private static final Configuration CONF = new Configuration();
  private static FileSystem fs;
  @NessieAPI private static NessieApiV2 nessieApi;
  @TempDir private static File baseDir;
  private static NessieCatalog nessieIcebergCatalog;
  private static FileIO io;
  private static ExecutorService executorService;
  private static String scheme = "file";

  private final BufferAllocator allocator = new RootAllocator();

  @BeforeAll
  public static void setup() throws Exception {
    executorService = Executors.newCachedThreadPool();

    fs = HadoopFileSystem.get(Path.of("/"), CONF);

    setupIO();
    setupNessieIcebergCatalog();
  }

  @BeforeEach
  public void resetWorkingBranch() throws Exception {
    Branch defaultBranch = nessieApi.getDefaultBranch();

    // Reset working branch
    deleteWorkingBranch();
    Branch workingBranch = Branch.of(workingBranchName, defaultBranch.getHash());
    nessieApi
        .createReference()
        .sourceRefName(defaultBranch.getName())
        .reference(workingBranch)
        .create();
  }

  @AfterAll
  public static void tearDown() throws Exception {
    executorService.shutdown();
    executorService.awaitTermination(1, TimeUnit.MINUTES);
  }

  private void deleteWorkingBranch() {
    try {
      Reference workingBranch = nessieApi.getReference().refName(workingBranchName).get();
      nessieApi
          .deleteBranch()
          .branchName(workingBranch.getName())
          .hash(workingBranch.getHash())
          .delete();
    } catch (NessieNotFoundException nfe) {
      // Do nothing
    } catch (NessieConflictException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testEmptyCatalog() throws Exception {
    SnapshotsScanOptions scanOptions =
        new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);
    NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(), props(), scanOptions, scheme, scheme);

    OutputMutator outputMutator = outputMutator();
    reader.setup(outputMutator);

    int records = reader.next();

    assertThat(records).isEqualTo(0);
  }

  @Test
  public void testEmptyTables() throws Exception {
    // Create tables with no snapshots
    Table table1 = createTable(0);
    Table table2 = createTable(0);
    String meta1 = ((BaseTable) table1).operations().current().metadataFileLocation();
    String meta2 = ((BaseTable) table2).operations().current().metadataFileLocation();

    SnapshotsScanOptions scanOptions =
        new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);
    NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(), props(), scanOptions, scheme, scheme);

    Results results = getResults(reader);

    assertThat(results.rowCount).isEqualTo(2);
    assertThat(results.metadataPaths).containsExactlyInAnyOrder(meta1, meta2);
    assertThat(results.snapshotEntries).isEmpty();
  }

  @Test
  public void testPartialExpiry() throws Exception {
    final int noOfSnapshots = 20;

    Table table1 = createTable(noOfSnapshots);
    Table table2 = createTable(noOfSnapshots);

    long cutoff = System.currentTimeMillis();
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, cutoff, 1);

    // Following should be retained
    IntStream.range(0, noOfSnapshots)
        .forEach(
            i -> {
              newSnapshot(table1, noOfSnapshots + i);
              newSnapshot(table2, noOfSnapshots + i);
            });

    // NessieCatalog defaults GC_ENABLED to true. Hence, set it explicitly. This action will create
    // another metadata json.
    table1
        .updateProperties()
        .set(NESSIE_GC_ENABLED, "true")
        .set(GC_ENABLED, "false")
        .set(COMMIT_NUM_RETRIES, "5")
        .commit();
    table2
        .updateProperties()
        .set(NESSIE_GC_ENABLED, "true")
        .set(GC_ENABLED, "false")
        .set(COMMIT_NUM_RETRIES, "5")
        .commit();

    NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(), props(), scanOptions, scheme, scheme);

    List<String> expectedMeta = new ArrayList<>();
    Results results = getResults(reader);

    addMetaPaths(expectedMeta, table1, table2);
    ((BaseTable) table1)
        .operations().current().previousFiles().stream()
            .filter(f -> f.timestampMillis() >= cutoff)
            .forEach(f -> expectedMeta.add(f.file()));
    ((BaseTable) table2)
        .operations().current().previousFiles().stream()
            .filter(f -> f.timestampMillis() >= cutoff)
            .forEach(f -> expectedMeta.add(f.file()));

    int expectedCountPerTable =
        noOfSnapshots + 1 + 1; // one per snapshot, one for prop update and one for expiry
    assertThat(results.rowCount).isEqualTo(expectedCountPerTable * 2); // two tables
    assertThat(results.metadataPaths).containsExactlyInAnyOrderElementsOf(expectedMeta);

    assertThat(results.snapshotEntries).isEmpty();
  }

  private void addMetaPaths(List<String> expectedMeta, Table... tables) {
    for (Table table : tables) {
      table.refresh();
      expectedMeta.add(((BaseTable) table).operations().current().metadataFileLocation());
    }
  }

  @Test
  public void testCutoffExpiryInMultipleTables() throws Exception {
    final int noOfSnapshots = 5;
    final int noOfTables = 16;

    List<Table> tables =
        IntStream.range(0, noOfTables)
            .mapToObj(i -> createTable(noOfSnapshots))
            .collect(Collectors.toList());

    long cutoff = System.currentTimeMillis();
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, cutoff, 1);

    // Following should be retained
    tables.forEach(
        t -> IntStream.range(0, noOfSnapshots).forEach(s -> newSnapshot(t, noOfSnapshots + s)));
    tables.forEach(
        t ->
            t.updateProperties()
                .set(NESSIE_GC_ENABLED, "true")
                .set(GC_ENABLED, "false")
                .set(COMMIT_NUM_RETRIES, "5")
                .commit());

    NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(), props(), scanOptions, scheme, scheme);
    tables.forEach(Table::refresh);

    Results results = getResults(reader);

    List<String> expectedMeta = new ArrayList<>();
    tables.forEach(t -> addMetaPaths(expectedMeta, t));
    tables.forEach(
        t ->
            ((BaseTable) t)
                .operations().current().previousFiles().stream()
                    .filter(f -> f.timestampMillis() >= cutoff)
                    .forEach(f -> expectedMeta.add(f.file())));

    int expectedCountPerTable =
        noOfSnapshots + 1 + 1; // one per snapshot, one for prop update and one for expiry
    assertThat(results.rowCount).isEqualTo(expectedCountPerTable * noOfTables);
    assertThat(results.metadataPaths).containsExactlyInAnyOrderElementsOf(expectedMeta);

    assertThat(results.snapshotEntries).isEmpty();
  }

  @Test
  public void testGCDisabled() throws Exception {
    final int noOfSnapshots = 5;
    final int noOfTables = 2;

    List<Table> tables =
        IntStream.range(0, noOfTables)
            .mapToObj(i -> createTable(noOfSnapshots))
            .collect(Collectors.toList());

    long cutoff = System.currentTimeMillis();
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, cutoff, 1);

    // Following should be retained
    tables.forEach(
        t ->
            t.updateProperties()
                .set(NESSIE_GC_ENABLED, "false")
                .set(COMMIT_NUM_RETRIES, "5")
                .commit());

    NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(), props(), scanOptions, scheme, scheme);
    tables.forEach(Table::refresh);

    List<String> expectedMeta =
        tables.stream()
            .map(t -> ((BaseTable) t).operations().current().metadataFileLocation())
            .collect(Collectors.toList());
    tables.forEach(
        t ->
            ((BaseTable) t)
                .operations()
                .current()
                .previousFiles()
                .forEach(f -> expectedMeta.add(f.file())));

    List<SnapshotEntry> expected =
        tables.stream()
            .map(t -> ((BaseTable) t).operations().current())
            .flatMap(
                t ->
                    t.snapshots().stream()
                        .filter(s -> s.timestampMillis() <= cutoff)
                        .sorted(Comparator.comparing(Snapshot::timestampMillis).reversed())
                        .skip(1) // Skip latest
                        .map(s -> new SnapshotEntry(t.metadataFileLocation(), s)))
            .collect(Collectors.toList());

    Results results = getResults(reader);

    assertThat(results.metadataPaths).containsExactlyInAnyOrderElementsOf(expectedMeta);
    assertThat(results.snapshotEntries).containsExactlyInAnyOrderElementsOf(expected);
  }

  @Test
  public void testMinSnapshotsToKeepOverride() throws Exception {
    final int noOfSnapshots = 5;
    final int noOfTables = 2;

    List<Table> tables =
        IntStream.range(0, noOfTables)
            .mapToObj(i -> createTable(noOfSnapshots))
            .collect(Collectors.toList());

    long cutoff = System.currentTimeMillis();
    SnapshotsScanOptions scanOptions = new SnapshotsScanOptions(LIVE_SNAPSHOTS, cutoff, 1);

    tables.forEach(
        t ->
            t.updateProperties()
                .set(NESSIE_GC_ENABLED, "true")
                .set(GC_ENABLED, "false")
                .set(MIN_SNAPSHOTS_TO_KEEP, "4")
                .commit());

    NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(), props(), scanOptions, scheme, scheme);
    tables.forEach(Table::refresh);

    List<String> expectedMeta =
        tables.stream()
            .map(t -> ((BaseTable) t).operations().current().metadataFileLocation())
            .collect(Collectors.toList());
    tables.forEach(
        t ->
            ((BaseTable) t)
                .operations().current().previousFiles().stream()
                    .sorted(Comparator.comparing(TableMetadata.MetadataLogEntry::timestampMillis))
                    .skip(2)
                    .forEach(f -> expectedMeta.add(f.file())));

    Set<Long> expectedSnapshotIds =
        tables.stream()
            .map(t -> ((BaseTable) t).operations().current())
            .flatMap(
                t ->
                    t.snapshots().stream()
                        .filter(s -> s.timestampMillis() <= cutoff)
                        .sorted(Comparator.comparing(Snapshot::timestampMillis).reversed())
                        .limit(4)
                        .skip(1) // min snapshots to keep excluding the latest one = 4-1
                        .map(Snapshot::snapshotId))
            .collect(Collectors.toSet());

    Results results = getResults(reader);
    tables.forEach(t -> addMetaPaths(expectedMeta, t));

    assertThat(results.metadataPaths).containsExactlyInAnyOrderElementsOf(expectedMeta);

    Set<Long> actualSnapshotIds =
        results.snapshotEntries.stream()
            .map(SnapshotEntry::getSnapshotId)
            .collect(Collectors.toSet());
    assertThat(actualSnapshotIds).containsExactlyInAnyOrderElementsOf(expectedSnapshotIds);
  }

  @Test
  public void testContainerNotFoundException() throws Exception {
    // Create a table for which ContainerNotFoundExceptions will be thrown on metadata access
    // The choice of brokenSnaps is arbitrary (exact value does not matter to the assertions)
    final int brokenSnaps = 2;
    final String noContainer = "nocontainer";
    createTable(brokenSnaps, "-" + noContainer);

    // Create a table intended for normal read behavior
    // The choice of workingSnaps is arbitrary (exact value does not matter to the assertions)
    final int workingSnaps = 6;
    final Table workingTable = createTable(workingSnaps);
    final String workingMeta =
        ((BaseTable) workingTable).operations().current().metadataFileLocation();

    final String exceptionMessageFragment = "Simulated exception for testing";

    /* Spy on FileIO to produce a ContainerNotFoundException for newInputFile(...) calls involving
     * the first table created above, while handling the second table normally.
     * newInputFile(...) is called by the static method TableMetadata.read(...), where the latter
     * is called from the system under test.
     */
    final FileIO exceptionalIO = spy(io);
    doThrow(
            UserException.ioExceptionError(new ContainerNotFoundException(exceptionMessageFragment))
                .build())
        .when(exceptionalIO)
        .newInputFile(contains(noContainer));

    final SnapshotsScanOptions scanOptions =
        new SnapshotsScanOptions(LIVE_SNAPSHOTS, System.currentTimeMillis(), 1);
    final NessieIcebergExpirySnapshotsReader reader =
        new NessieIcebergExpirySnapshotsReader(
            operatorContext(), plugin(exceptionalIO), props(), scanOptions, scheme, scheme);
    final Results results = getResults(reader);

    verify(exceptionalIO, times(1)).newInputFile(contains(noContainer));

    assertThat(results.rowCount).isEqualTo(workingSnaps * 2);
    assertThat(results.metadataPaths).contains(workingMeta);
    assertThat(results.metadataPaths).hasSize(workingSnaps + 1);
    assertThat(results.snapshotEntries).isNotEmpty();
    assertThat(results.snapshotEntries).hasSize(workingSnaps - 1);
  }

  private static void setupIO() {
    io =
        new DremioFileIO(
            fs,
            null,
            Collections.emptyList(),
            null,
            100L,
            new HadoopFileSystemConfigurationAdapter(CONF));
  }

  protected static void setupNessieIcebergCatalog() throws NessieNotFoundException {
    nessieIcebergCatalog = new NessieCatalog();
    Branch defaultRef = nessieApi.getDefaultBranch();

    NessieIcebergClient nessieIcebergClient =
        new NessieIcebergClient(
            nessieApi, defaultRef.getName(), defaultRef.getHash(), new HashMap<>());

    nessieIcebergCatalog.initialize(
        "test",
        nessieIcebergClient,
        io,
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, baseDir.getAbsolutePath()));
  }

  private Results getResults(RecordReader reader) throws Exception {
    OutputMutator outputMutator = outputMutator();
    reader.setup(outputMutator);

    int batchRecords;
    Results results = new Results(new ArrayList<>(), new ArrayList<>(), 0);
    do {
      outputMutator.getVectors().forEach(ValueVector::reset);
      batchRecords = reader.next();
      results.merge(getActualEntries(outputMutator));
    } while (batchRecords != 0);

    return results;
  }

  private Results getActualEntries(OutputMutator outputMutator) {
    BigIntVector snapshotIdVector =
        (BigIntVector) outputMutator.getVector(SystemSchemas.SNAPSHOT_ID);
    VarCharVector metadataVector =
        (VarCharVector) outputMutator.getVector(SystemSchemas.METADATA_FILE_PATH);
    VarCharVector manifestListVector =
        (VarCharVector) outputMutator.getVector(SystemSchemas.MANIFEST_LIST_PATH);
    VarCharVector fileType = (VarCharVector) outputMutator.getVector(SystemSchemas.FILE_TYPE);
    VarCharVector filePath = (VarCharVector) outputMutator.getVector(SystemSchemas.FILE_PATH);

    int recordCount =
        snapshotIdVector.getValueCount(); // value count is expected to be same in all vectors

    List<SnapshotEntry> snapshotEntries = new ArrayList<>();
    List<String> metadataJsonPaths = new ArrayList<>();
    for (int i = 0; i < recordCount; i++) {
      if (!fileType.isNull(i) && fileType.get(i).length > 0) {
        assertThat(new String(fileType.get(i), StandardCharsets.UTF_8))
            .isEqualTo(IcebergFileType.METADATA_JSON.name());
        metadataJsonPaths.add(new String(filePath.get(i), StandardCharsets.UTF_8));
      } else {
        SnapshotEntry snapshotEntry =
            new SnapshotEntry(
                new String(metadataVector.get(i), StandardCharsets.UTF_8),
                snapshotIdVector.get(i),
                new String(manifestListVector.get(i), StandardCharsets.UTF_8),
                null);
        snapshotEntries.add(snapshotEntry);
      }
    }
    return new Results(snapshotEntries, metadataJsonPaths, recordCount);
  }

  private OutputMutator outputMutator() {
    TestOutputMutator outputMutator = new TestOutputMutator(allocator);
    FieldVector metaVector =
        TypeHelper.getNewVector(
            Field.nullable(SystemSchemas.METADATA_FILE_PATH, new ArrowType.Utf8()), allocator);
    FieldVector snapshotVector =
        TypeHelper.getNewVector(
            Field.nullable(SystemSchemas.SNAPSHOT_ID, new ArrowType.Int(64, true)), allocator);
    FieldVector manifestListVector =
        TypeHelper.getNewVector(
            Field.nullable(SystemSchemas.MANIFEST_LIST_PATH, new ArrowType.Utf8()), allocator);
    FieldVector fileTypeVector =
        TypeHelper.getNewVector(
            Field.nullable(SystemSchemas.FILE_TYPE, new ArrowType.Utf8()), allocator);
    FieldVector filePathVector =
        TypeHelper.getNewVector(
            Field.nullable(SystemSchemas.FILE_PATH, new ArrowType.Utf8()), allocator);

    outputMutator.addField(metaVector);
    outputMutator.addField(snapshotVector);
    outputMutator.addField(manifestListVector);
    outputMutator.addField(fileTypeVector);
    outputMutator.addField(filePathVector);
    return outputMutator;
  }

  private SupportsIcebergMutablePlugin plugin() throws IOException {
    return plugin(io);
  }

  private SupportsIcebergMutablePlugin plugin(final FileIO icebergFileIO) throws IOException {
    DataplanePlugin plugin = mock(DataplanePlugin.class);
    when(plugin.createFS(anyString(), anyString(), any(OperatorContext.class))).thenReturn(fs);
    when(plugin.getNessieApi()).thenReturn(nessieApi);

    when(plugin.createIcebergFileIO(any(), any(), any(), any(), any())).thenReturn(icebergFileIO);
    when(plugin.getSystemUserFS()).thenReturn(fs);
    when(plugin.getFsConfCopy()).thenReturn(CONF);

    OptionManager optionManager = optionManager();
    NessieClient nessieClient = new NessieClientImpl(nessieApi, optionManager);
    when(plugin.getIcebergModel(
            any(IcebergTableProps.class),
            anyString(),
            any(OperatorContext.class),
            eq(icebergFileIO),
            any()))
        .then(
            (Answer<IcebergModel>)
                invocation -> {
                  Object[] args = invocation.getArguments();
                  IcebergTableProps tableProps = (IcebergTableProps) args[0];
                  String user = (String) args[1];
                  OperatorContext operatorContext = (OperatorContext) args[2];

                  List<String> tableKeyAsList =
                      Arrays.asList(tableProps.getTableName().split(Pattern.quote(".")));
                  ResolvedVersionContext version = tableProps.getVersion();

                  return new IcebergNessieVersionedModel(
                      tableKeyAsList,
                      CONF,
                      icebergFileIO,
                      nessieClient,
                      operatorContext,
                      version,
                      plugin,
                      user,
                      null,
                      s -> s);
                });

    return plugin;
  }

  private OptionManager optionManager() {
    OptionValidatorListingImpl optionValidatorListing =
        new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    return new DefaultOptionManager(optionValidatorListing);
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

    ExecProtos.FragmentHandle fragmentHandle =
        ExecProtos.FragmentHandle.newBuilder()
            .setMajorFragmentId(0)
            .setMinorFragmentId(0)
            .setQueryId(QueryIdHelper.getQueryIdFromString(UUID.randomUUID().toString()))
            .build();
    when(context.getFragmentHandle()).thenReturn(fragmentHandle);

    when(context.getExecutor()).thenReturn(executorService);
    return context;
  }

  private Table createTable(int numberOfSnapshots) {
    return createTable(numberOfSnapshots, "");
  }

  private Table createTable(int numberOfSnapshots, String suffix) {
    Schema icebergTableSchema =
        new Schema(ImmutableList.of(Types.NestedField.required(0, "id", new Types.IntegerType())));
    String tableName = generateUniqueTableName() + suffix;
    String table1QualifiedName = String.format("%s@%s", tableName, workingBranchName);
    Table table =
        nessieIcebergCatalog.createTable(
            TableIdentifier.of(table1QualifiedName),
            icebergTableSchema,
            PartitionSpec.unpartitioned(),
            Map.of(NESSIE_GC_ENABLED, "false"));
    // Note that gc.enabled is hard-coded to false in NessieCatalog. So, the expiry action will be
    // ignored by default.

    IntStream.range(0, numberOfSnapshots).forEach(i -> newSnapshot(table, i));
    return table;
  }

  private void newSnapshot(Table table, int i) {
    table
        .newFastAppend()
        .appendFile(
            DataFiles.builder(table.spec())
                .withPath(String.format("%s/data/data-%d.parquet", table.location(), i))
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(1024L)
                .withRecordCount(100L)
                .build())
        .commit();
  }

  private static class Results {
    private final List<SnapshotEntry> snapshotEntries;
    private final List<String> metadataPaths;
    private int rowCount;

    public Results(List<SnapshotEntry> snapshotEntries, List<String> metadataPaths, int rowCount) {
      this.snapshotEntries = snapshotEntries;
      this.metadataPaths = metadataPaths;
      this.rowCount = rowCount;
    }

    public void merge(Results that) {
      this.snapshotEntries.addAll(that.snapshotEntries);
      this.metadataPaths.addAll(that.metadataPaths);
      this.rowCount += that.rowCount;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Results results = (Results) o;
      return Objects.equals(snapshotEntries, results.snapshotEntries)
          && Objects.equals(metadataPaths, results.metadataPaths)
          && rowCount == results.rowCount;
    }

    @Override
    public int hashCode() {
      return Objects.hash(snapshotEntries, metadataPaths, rowCount);
    }

    @Override
    public String toString() {
      return "Results{count="
          + rowCount
          + "\n\nsnapshotEntries=\n"
          + snapshotEntries.stream().map(Object::toString).collect(Collectors.joining("\n"))
          + "\n\nmetadataPaths=\n"
          + String.join("\n", metadataPaths)
          + '}';
    }
  }
}
