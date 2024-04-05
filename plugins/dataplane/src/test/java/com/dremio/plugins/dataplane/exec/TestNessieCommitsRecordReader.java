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

import static com.dremio.exec.ExecConstants.ICEBERG_CATALOG_TYPE_KEY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.CompleteType;
import com.dremio.context.UserContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.hadoop.HadoopFileSystem;
import com.dremio.exec.hadoop.HadoopFileSystemConfigurationAdapter;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.SampleMutator;
import com.dremio.exec.store.SystemSchemas;
import com.dremio.exec.store.iceberg.DremioFileIO;
import com.dremio.exec.store.iceberg.NessieCommitsSubScan;
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.projectnessie.client.api.GetAllReferencesBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.FetchOption;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.LogResponse;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.projectnessie.model.ReferencesResponse;
import org.projectnessie.tools.compatibility.api.NessieAPI;

public class TestNessieCommitsRecordReader extends BaseTestOperator {
  private static final int DEFAULT_BATCH_SIZE = 2;
  private static final long SNAPSHOT_ID = 4709042947025192029L;
  private static final List<Reference> REFERENCES =
      Collections.singletonList(Branch.of("dev", "07b92b065b57ec8d69c5249daa33c329259f7284"));
  private static final Configuration CONF = new Configuration();
  private static FileSystem fs;
  private OperatorContextImpl context;
  private SampleMutator mutator;
  private RecordReader reader;
  private static Snapshot snapshot;

  @BeforeClass
  public static void initStatics() throws Exception {
    CONF.set(ICEBERG_CATALOG_TYPE_KEY, IcebergCatalogType.HADOOP.name());
    fs = HadoopFileSystem.get(com.dremio.io.file.Path.of("/"), CONF);

    snapshot = mock(Snapshot.class);
    when(snapshot.snapshotId()).thenReturn(SNAPSHOT_ID);
    when(snapshot.manifestListLocation()).thenReturn("file:///manifest_list.avro");
  }

  @NessieAPI private static NessieApiV2 nessieApi;

  @Before
  public void beforeTest() throws Exception {
    context = testContext.getNewOperatorContext(getTestAllocator(), null, DEFAULT_BATCH_SIZE, null);
    testCloseables.add(context);

    setUpNessie();
    DataplanePlugin plugin = mock(DataplanePlugin.class);
    when(fec.getStoragePlugin(any())).thenReturn(plugin);

    when(plugin.getNessieApi()).thenReturn(nessieApi);
    when(plugin.createFSWithAsyncOptions(anyString(), anyString(), any(OperatorContext.class)))
        .thenReturn(fs);

    FileIO fileIO =
        new DremioFileIO(
            fs, null, null, null, null, new HadoopFileSystemConfigurationAdapter(CONF));
    when(plugin.createIcebergFileIO(eq(fs), any(), any(), any(), any())).thenReturn(fileIO);
  }

  @After
  public void afterTest() throws Exception {
    mutator.close();
    getTestAllocator().close();
    reader.close();
  }

  @Test
  public void testReadNessieCommits() throws ExecutionSetupException, IOException {
    setUpReferences(getCommit(1));
    setUpReader();
    List<String> actual = new ArrayList<>();

    int records;
    VarCharVector pathVector = (VarCharVector) mutator.getVector(SystemSchemas.METADATA_FILE_PATH);
    while ((records = reader.next()) > 0) {
      for (int i = 0; i < records; i++) {
        actual.add(new String(pathVector.get(i), StandardCharsets.UTF_8));
      }
    }
    verify(reader, times(2)).next();
    assertEquals(1, actual.size());
    assertEquals("v1.metadata.json", actual.get(0));
  }

  @Test
  public void testLeanReadNessieCommitsMultiBatch() throws Exception {
    setUpReferences(getCommit(3));
    setupLeanReader();
    List<String> actual = new ArrayList<>();

    int records;
    VarCharVector pathVector = (VarCharVector) mutator.getVector(SystemSchemas.METADATA_FILE_PATH);

    while ((records = reader.next()) > 0) {
      for (int i = 0; i < records; i++) {
        actual.add(new String(pathVector.get(i), StandardCharsets.UTF_8));
      }
    }
    verify(reader, times(2)).next();
    assertEquals(1, actual.size());
    assertTrue(actual.containsAll(Arrays.asList("v1.metadata.json")));
  }

  @Test
  public void testReadNessieCommitsMultiBatch() throws Exception {
    setUpReferences(getCommit(3));
    setUpReader();
    List<String> actual = new ArrayList<>();

    int records;
    VarCharVector pathVector = (VarCharVector) mutator.getVector(SystemSchemas.METADATA_FILE_PATH);
    VarCharVector manifestListVector =
        (VarCharVector) mutator.getVector(SystemSchemas.MANIFEST_LIST_PATH);
    BigIntVector snapshotIdVector = (BigIntVector) mutator.getVector(SystemSchemas.SNAPSHOT_ID);

    while ((records = reader.next()) > 0) {
      for (int i = 0; i < records; i++) {
        actual.add(new String(pathVector.get(i), StandardCharsets.UTF_8));
        assertEquals(
            "file:///manifest_list.avro",
            new String(manifestListVector.get(i), StandardCharsets.UTF_8));
        assertEquals(SNAPSHOT_ID, snapshotIdVector.get(i));
      }
    }
    verify(reader, times(2)).next();
    assertEquals(1, actual.size());
    assertTrue(actual.containsAll(Arrays.asList("v1.metadata.json")));
  }

  @Test
  public void testReadNessieCommitsContentSetExceedsBatchSize() throws Exception {
    setUpReferences(Collections.singletonList(getMultiOperationCommit()));
    setUpReader();
    List<String> actual = new ArrayList<>();

    int records;
    VarCharVector pathVector = (VarCharVector) mutator.getVector(SystemSchemas.METADATA_FILE_PATH);
    while ((records = reader.next()) > 0) {
      for (int i = 0; i < records; i++) {
        actual.add(new String(pathVector.get(i), StandardCharsets.UTF_8));
      }
    }
    verify(reader, times(3)).next();
    assertThat(actual).hasSize(3);
    assertThat(actual)
        .containsExactlyInAnyOrder("v2.metadata.json", "v3.metadata.json", "v4.metadata.json");
  }

  @Test
  public void testNonExistingMetadataReferences() throws Exception {
    setUpReferences(ImmutableList.of(getNonExistentOpCommit()));
    setUpReader();
    assertThat(reader.next()).isEqualTo(0);
  }

  protected void setUpNessie() {
    nessieApi = mock(NessieApiV2.class, RETURNS_DEEP_STUBS);
  }

  protected void setUpReader() throws ExecutionSetupException, IOException {
    NessieCommitsSubScan subScan = subScan();

    NessieCommitsRecordReader nessieCommitsRecordReader =
        spy(new NessieCommitsRecordReader(fec, context, subScan));
    doReturn(snapshot)
        .when(nessieCommitsRecordReader)
        .loadSnapshot(eq("v1.metadata.json"), anyLong());
    doReturn(snapshot)
        .when(nessieCommitsRecordReader)
        .loadSnapshot(eq("v2.metadata.json"), anyLong());
    doReturn(snapshot)
        .when(nessieCommitsRecordReader)
        .loadSnapshot(eq("v3.metadata.json"), anyLong());
    doReturn(snapshot)
        .when(nessieCommitsRecordReader)
        .loadSnapshot(eq("v4.metadata.json"), anyLong());

    doThrow(new NotFoundException("vx"))
        .when(nessieCommitsRecordReader)
        .loadSnapshot(eq("vx.metadata.json"), anyLong());

    reader = nessieCommitsRecordReader;

    mutator = new SampleMutator(getTestAllocator());
    mutator.addField(
        CompleteType.VARCHAR.toField(SystemSchemas.METADATA_FILE_PATH), VarCharVector.class);
    mutator.addField(CompleteType.BIGINT.toField(SystemSchemas.SNAPSHOT_ID), BigIntVector.class);
    mutator.addField(
        CompleteType.VARCHAR.toField(SystemSchemas.MANIFEST_LIST_PATH), VarCharVector.class);
    mutator.addField(CompleteType.VARCHAR.toField(SystemSchemas.FILE_PATH), VarCharVector.class);
    mutator.addField(CompleteType.VARCHAR.toField(SystemSchemas.FILE_TYPE), VarCharVector.class);
    mutator.getContainer().buildSchema();

    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);
  }

  private void setupLeanReader() throws ExecutionSetupException {
    NessieCommitsSubScan subScan = subScan();
    reader = spy(new LeanNessieCommitsRecordReader(fec, context, subScan));

    mutator = new SampleMutator(getTestAllocator());
    mutator.addField(
        CompleteType.VARCHAR.toField(SystemSchemas.METADATA_FILE_PATH), VarCharVector.class);
    mutator.getContainer().buildSchema();

    reader.allocate(mutator.getFieldVectorMap());
    reader.setup(mutator);
  }

  private NessieCommitsSubScan subScan() {
    NessieCommitsSubScan subScan = mock(NessieCommitsSubScan.class);
    long now = Instant.now().toEpochMilli();
    OpProps props = mock(OpProps.class);
    when(props.getUserName()).thenReturn(UserContext.SYSTEM_USER_NAME);
    when(subScan.getProps()).thenReturn(props);
    when(subScan.getSnapshotsScanOptions())
        .thenReturn(new SnapshotsScanOptions(SnapshotsScanOptions.Mode.ALL_SNAPSHOTS, now, 1));
    StoragePluginId storagePluginId = mock(StoragePluginId.class);
    when(storagePluginId.getName()).thenReturn("DataplanePlugin");
    when(subScan.getPluginId()).thenReturn(storagePluginId);
    return subScan;
  }

  private void setUpReferences(List<LogResponse.LogEntry> logEntry) {
    GetAllReferencesBuilder getAllReferencesBuilder = mock(GetAllReferencesBuilder.class);
    ReferencesResponse referencesResponse = mock(ReferencesResponse.class);

    try {
      when(getAllReferencesBuilder.stream()).thenReturn(REFERENCES.stream());
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
    when(getAllReferencesBuilder.get()).thenReturn(referencesResponse);
    when(referencesResponse.getReferences()).thenReturn(REFERENCES);
    when(nessieApi.getAllReferences()).thenReturn(getAllReferencesBuilder);
    try {
      when(nessieApi.getCommitLog().reference(any()).fetch(FetchOption.ALL).stream())
          .thenReturn(logEntry.stream());
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private List<LogResponse.LogEntry> getCommit(int keyCount) {
    List<LogResponse.LogEntry> results = new ArrayList<>(keyCount);

    int count = 1;
    while (count <= keyCount) {
      ContentKey key = ContentKey.of("a.b." + count + "c.txt");
      IcebergTable icebergTable =
          IcebergTable.of(
              "v" + count + ".metadata.json", SNAPSHOT_ID, 42, 42, 42, Integer.toString(count));
      results.add(
          LogResponse.LogEntry.builder()
              .commitMeta(
                  CommitMeta.builder()
                      .message("msg")
                      .hash("a0f4f33a14fa610c75ff8cd89b6a54f5df61fcb" + count)
                      .commitTime(Instant.EPOCH)
                      .build())
              .addOperations(Operation.Put.of(key, icebergTable))
              .build());
      count++;
    }
    return results;
  }

  private LogResponse.LogEntry getMultiOperationCommit() {
    ContentKey key1 = ContentKey.of("a.b2.c.txt");
    ContentKey key2 = ContentKey.of("a.b3.c.txt");
    ContentKey key3 = ContentKey.of("a.b4.c.txt");
    IcebergTable icebergTable1 = IcebergTable.of("v2.metadata.json", SNAPSHOT_ID, 42, 42, 42, "1");
    IcebergTable icebergTable2 = IcebergTable.of("v3.metadata.json", SNAPSHOT_ID, 42, 42, 42, "1");
    IcebergTable icebergTable3 = IcebergTable.of("v4.metadata.json", SNAPSHOT_ID, 42, 42, 42, "1");
    return LogResponse.LogEntry.builder()
        .commitMeta(
            CommitMeta.builder()
                .message("msg")
                .hash("a0f4f33a14fa610c75ff8cd89b6a54f5df61fcb7")
                .commitTime(Instant.EPOCH)
                .build())
        .addOperations(
            Operation.Put.of(key1, icebergTable1),
            Operation.Put.of(key2, icebergTable2),
            Operation.Put.of(key3, icebergTable3))
        .build();
  }

  private LogResponse.LogEntry getNonExistentOpCommit() {
    ContentKey key1 = ContentKey.of("a.b2.x.txt");
    IcebergTable icebergTable1 = IcebergTable.of("vx.metadata.json", SNAPSHOT_ID, 42, 42, 42, "1");
    return LogResponse.LogEntry.builder()
        .commitMeta(
            CommitMeta.builder()
                .message("msg")
                .hash("a1f4f33a14fa610c75ff8cd89b6a54f5df61fcb8")
                .commitTime(Instant.EPOCH)
                .build())
        .addOperations(Operation.Put.of(key1, icebergTable1))
        .build();
  }
}
