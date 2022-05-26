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
package com.dremio.service.nessie.upgrade.storage;

import static com.dremio.service.nessie.upgrade.storage.MigrateToNessieAdapter.MAX_ENTRIES_PER_COMMIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.ImmutableNessieDatabaseAdapterConfig;
import com.dremio.service.nessie.NessieDatabaseAdapterConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.google.protobuf.ByteString;

/**
 * Unit tests for {@link MigrateIcebergMetadataPointer}.
 */
class TestMigrateIcebergMetadataPointer {

  private static final ScanResult scanResult = ClassPathScanner.fromPrescan(SabotConfig.create());

  // This legacy data was produced using Nessie 0.14 code.
  // The data encodes IcebergTable.of("test-metadata-location", "test-id-data", "test-content-id")
  private static final String LEGACY_REF_STATE_BASE64 = "ChgKFnRlc3QtbWV0YWRhdGEtbG9jYXRpb24qD3Rlc3QtY29udGVudC1pZA==";
  private static final String LEGACY_GLOBAL_STATE_BASE64 = "Eg4KDHRlc3QtaWQtZGF0YSoPdGVzdC1jb250ZW50LWlk";

  private static final String UPGRADE_BRANCH_NAME = "upgrade-test";

  private final MigrateIcebergMetadataPointer task = new MigrateIcebergMetadataPointer();
  private LocalKVStoreProvider storeProvider;
  private DatabaseAdapter adapter;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(scanResult, null, true, false); // in-memory
    storeProvider.start();

    NessieDatastoreInstance nessieDatastore = new NessieDatastoreInstance();
    nessieDatastore.configure(new ImmutableDatastoreDbConfig.Builder()
      .setStoreProvider(() -> storeProvider)
      .build());
    nessieDatastore.initialize();

    NessieDatabaseAdapterConfig adapterCfg = new ImmutableNessieDatabaseAdapterConfig.Builder().build();
    adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConfig(adapterCfg)
      .withConnector(nessieDatastore).build();
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  private void commitLegacyData(Key key, ContentId contentId, TableCommitMetaStoreWorker worker)
    throws ReferenceNotFoundException, ReferenceConflictException {

    ByteString refState = ByteString.copyFrom(Base64.getDecoder().decode(LEGACY_REF_STATE_BASE64));
    ByteString globalState = ByteString.copyFrom(Base64.getDecoder().decode(LEGACY_GLOBAL_STATE_BASE64));

    adapter.commit(ImmutableCommitAttempt.builder()
      .commitToBranch(BranchName.of("main"))
      .commitMetaSerialized(worker.getMetadataSerializer().toBytes(CommitMeta.fromMessage("test")))
      .putGlobal(contentId, globalState)
      .addPuts(KeyWithBytes.of(
        key,
        contentId,
        (byte) 1, // Iceberg Table
        refState))
      .build());
  }


  @ParameterizedTest
  @CsvSource({
    "0,   1",
    "1,   1",
    "2,   1",
    "19,  1",
    "19,  1000",
    "20,  1",
    "20,  1000",
    "21,  1",
    "21,  1000",
    "40,  1",
    "101, 1",
    "101, 20",
    "101, 101",
    "101, 1000",
  })
  void testUpgrade(int numExtraTables, int batchSize) throws Exception {
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

    adapter.initializeRepo("main");
    // Load a legacy entry into the adapter
    List<Key> keys = new ArrayList<>();
    Key key1 = Key.of("test", "table", "11111");
    ContentId contentId1 = ContentId.of("test-content-id");
    commitLegacyData(key1, contentId1, worker);
    keys.add(key1);

    VersionStore<Content, CommitMeta, Content.Type> versionStore = new PersistVersionStore<>(adapter, worker);

    // Create some extra Iceberg tables in current Nessie format
    for (int i = 0; i < numExtraTables; i++) {
      Key extraKey = Key.of("test", "table", "current-" + i);
      versionStore.commit(BranchName.of("main"), Optional.empty(), CommitMeta.fromMessage("test"),
        Collections.singletonList(ImmutablePut.<Content>builder()
          .key(extraKey)
          .value(IcebergTable.of("test-metadata-location", 1, 2, 3, 4, "extra-content-id-" + i))
          .build()));
      keys.add(extraKey);
    }

    task.upgrade(storeProvider, batchSize, UPGRADE_BRANCH_NAME);

    // Make sure the transient upgrade branch is deleted when the upgrade is over
    assertThat(adapter.namedRefs(GetNamedRefsParams.DEFAULT))
      .noneMatch(r -> r.getNamedRef().getName().equals(UPGRADE_BRANCH_NAME));

    Map<Key, Content> tables = versionStore.getValues(BranchName.of("main"), keys);

    assertThat(tables.keySet()).containsExactlyInAnyOrder(keys.toArray(new Key[0]));
    assertThat(tables).allSatisfy((k,v) -> {
      assertThat(v).isInstanceOf(IcebergTable.class)
        .extracting("metadataLocation")
        .isEqualTo("test-metadata-location"); // encoded in LEGACY_REF_STATE_BASE64
    });

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    List<CommitLogEntry> mainLog = adapter.commitLog(main.getHash()).collect(Collectors.toList());

    // Each upgrade commit contains at most MAX_ENTRIES_PER_COMMIT entries
    int logSize = keys.size() / MAX_ENTRIES_PER_COMMIT
      + (keys.size() % MAX_ENTRIES_PER_COMMIT == 0 ? 0 : 1) // + 1 for the final non-full commit
      + 1; // + 1 for the empty key list commit
    assertThat(mainLog).hasSize(logSize);

    // The top-most (last) commit should have a key list.
    assertThat(mainLog.get(0).getKeyList())
      .isNotNull()
      .satisfies(l -> assertThat(l.getKeys()).isNotEmpty());

    // The second top-most (last commit with tables) log entry may or may not be "full".
    assertThat(mainLog.get(1).getPuts().size()).isLessThanOrEqualTo(MAX_ENTRIES_PER_COMMIT);
    // Deeper entries should be "full".
    assertThat(mainLog.subList(2, mainLog.size()))
      .allSatisfy(e -> assertThat(e.getPuts().size()).isEqualTo(MAX_ENTRIES_PER_COMMIT));
  }

  @ParameterizedTest
  @ValueSource(ints = {0, -1, -100})
  void testInvalidBatchSize(int batchSize) throws Exception {
    assertThatThrownBy(() -> task.upgrade(storeProvider, batchSize, UPGRADE_BRANCH_NAME))
      .hasMessageContaining("Invalid batch size");
  }

  @Test
  void testEmptyUpgrade() throws Exception {
    task.upgrade(storeProvider, 10, UPGRADE_BRANCH_NAME);

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(main.getHash()).isEqualTo(adapter.noAncestorHash()); // no change during upgrade
  }

  @Test
  void testUnnecessaryUpgrade() throws Exception {
    IcebergTable table = IcebergTable.of("metadata1", 1, 2, 3, 4, "id123");

    adapter.initializeRepo("main");
    // Create an Iceberg table in current Nessie format
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    VersionStore<Content, CommitMeta, Content.Type> versionStore = new PersistVersionStore<>(adapter, worker);
    Hash head = versionStore.commit(BranchName.of("main"), Optional.empty(), CommitMeta.fromMessage("test"),
      Collections.singletonList(ImmutablePut.<Content>builder()
        .key(Key.of("test-key"))
        .value(table)
        .build()));

    task.upgrade(storeProvider, 10, UPGRADE_BRANCH_NAME);

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(main.getHash()).isEqualTo(head); // no change during upgrade
  }

  @Test
  void testUnnecessaryUpgradeOfDeletedEntry() throws Exception {
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

    adapter.initializeRepo("main");
    // Load a legacy entry into the adapter
    Key key1 = Key.of("test", "table", "11111");
    ContentId contentId1 = ContentId.of("test-content-id");
    commitLegacyData(key1, contentId1, worker);

    // Delete the legacy entry
    Hash head = adapter.commit(ImmutableCommitAttempt.builder()
      .commitToBranch(BranchName.of("main"))
      .commitMetaSerialized(worker.getMetadataSerializer().toBytes(CommitMeta.fromMessage("test delete")))
      .addDeletes(key1)
      .build());

    task.upgrade(storeProvider, 10, UPGRADE_BRANCH_NAME);

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(main.getHash()).isEqualTo(head); // no change during upgrade
  }

  @Test
  void testUnnecessaryUpgradeOfReplacedEntry() throws Exception {
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

    adapter.initializeRepo("main");
    // Load a legacy entry into the adapter
    Key key1 = Key.of("test", "table", "11111");
    ContentId contentId1 = ContentId.of("test-content-id");
    commitLegacyData(key1, contentId1, worker);

    // Replace the table using current Nessie format
    IcebergTable table = IcebergTable.of("metadata1", 1, 2, 3, 4, "id123");
    VersionStore<Content, CommitMeta, Content.Type> versionStore = new PersistVersionStore<>(adapter, worker);
    Hash head = versionStore.commit(BranchName.of("main"), Optional.empty(), CommitMeta.fromMessage("test"),
      Collections.singletonList(ImmutablePut.<Content>builder()
        .key(key1)
        .value(table)
        .build()));

    task.upgrade(storeProvider, 10, UPGRADE_BRANCH_NAME);

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(main.getHash()).isEqualTo(head); // no change during upgrade
  }

  @Test
  void testUpgradeBranchReset() throws Exception {
    adapter.initializeRepo("main");
    TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    commitLegacyData(Key.of("test1"), ContentId.of("test-cid"), worker);

    adapter.create(BranchName.of(UPGRADE_BRANCH_NAME), adapter.noAncestorHash());
    task.upgrade(storeProvider, 10, UPGRADE_BRANCH_NAME);

    assertThat(adapter.namedRefs(GetNamedRefsParams.DEFAULT))
      .noneMatch(r -> r.getNamedRef().getName().equals(UPGRADE_BRANCH_NAME));
  }
}
