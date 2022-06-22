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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ImmutablePut;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.store.PersistVersionStore;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;

/**
 * Unit tests for {@link MigrateToNessieAdapter}.
 */
class TestMigrateToNessieAdapter {

  private static final ScanResult scanResult = ClassPathScanner.fromPrescan(SabotConfig.create());

  private static final String UPGRADE_BRANCH_NAME = "upgrade-test";

  private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();

  private MigrateToNessieAdapter task;
  private LocalKVStoreProvider storeProvider;
  private DatabaseAdapter adapter;

  @BeforeEach
  void createKVStore() throws Exception {
    task = new MigrateToNessieAdapter();
    storeProvider = new LocalKVStoreProvider(scanResult, null, true, false); // in-memory
    storeProvider.start();

    NessieDatastoreInstance nessieDatastore = new NessieDatastoreInstance();
    nessieDatastore.configure(new ImmutableDatastoreDbConfig.Builder()
      .setStoreProvider(() -> storeProvider)
      .build());
    nessieDatastore.initialize();

    NonTransactionalDatabaseAdapterConfig adapterCfg = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig
      .builder()
      .build();
    adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConfig(adapterCfg)
      .withConnector(nessieDatastore).build(worker);
    // Note: adapter.initializeRepo() will be called by the upgrade task
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void unexpectedBranch() {
    Assertions.assertThatThrownBy(() ->
      task.upgrade(storeProvider, UPGRADE_BRANCH_NAME, commitConsumer ->
          commitConsumer.migrateCommit("unexpected123", Collections.singletonList("key"), "location")))
      .hasMessageContaining("unexpected123");
  }

  @Test
  void testEmptyUpgrade() throws Exception {
    task.upgrade(storeProvider, UPGRADE_BRANCH_NAME, c -> {});

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    // repository was initialized, but no changes were made
    assertThat(main.getHash()).isEqualTo(adapter.noAncestorHash());
  }

  @Test
  void testUpgradeWithPreviousHistory() throws Exception {
    // This particular test needs to initialize the repo to be able to inject previous history
    adapter.initializeRepo("main");

    VersionStore<Content, CommitMeta, Content.Type> versionStore = new PersistVersionStore<>(adapter, worker);

    Key extraKey = Key.of("existing", "table", "abc");
    versionStore.commit(BranchName.of("main"), Optional.empty(), CommitMeta.fromMessage("test"),
      Collections.singletonList(ImmutablePut.<Content>builder()
        .key(extraKey)
        .value(IcebergTable.of("test-metadata-location", 1, 2, 3, 4, "extra-content-id"))
        .build()));

    task.upgrade(storeProvider, UPGRADE_BRANCH_NAME, c -> {});

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    assertThat(adapter.keys(main.getHash(), KeyFilterPredicate.ALLOW_ALL))
      .anySatisfy(kt -> assertThat(kt.getKey()).isEqualTo(extraKey));
  }

  @Test
  void testUpgradeBranchReset() throws Exception {
    // This particular test needs to initialize the repo to be able to pre-create the upgrade branch
    adapter.initializeRepo("main");

    adapter.create(BranchName.of(UPGRADE_BRANCH_NAME), adapter.noAncestorHash());
    task.upgrade(storeProvider, UPGRADE_BRANCH_NAME, c -> {});

    assertThat(adapter.namedRefs(GetNamedRefsParams.DEFAULT))
      .noneMatch(r -> r.getNamedRef().getName().equals(UPGRADE_BRANCH_NAME));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 40, 99, 100, 101, 499, 500, 501})
  void testUpgrade(int numCommits) throws Exception {
    List<Key> keys = new ArrayList<>();
    List<String> testEntries = new ArrayList<>();

    task.upgrade(storeProvider, UPGRADE_BRANCH_NAME, c -> {
      for (int i = 0; i < numCommits; i++) {
        List<String> key = ImmutableList.of("key", "" + i);
        String location = "location-" + i;

        c.migrateCommit("main", key, location);

        Key nessieKey = Key.of(key.toArray(new String[0]));
        keys.add(nessieKey);
        testEntries.add(location + "|" + nessieKey);
      }
    });

    VersionStore<Content, CommitMeta, Content.Type> versionStore = new PersistVersionStore<>(adapter, worker);

    Map<Key, Content> tables = versionStore.getValues(BranchName.of("main"), keys);

    assertThat(tables.entrySet().stream().map(e -> {
      Key key = e.getKey();
      IcebergTable table = (IcebergTable) e.getValue();
      return table.getMetadataLocation() + "|" + key;
    })).containsExactlyInAnyOrder(testEntries.toArray(new String[0]));

    ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    List<CommitLogEntry> mainLog = adapter.commitLog(main.getHash()).collect(Collectors.toList());

    // Each upgrade commit contains at most MAX_ENTRIES_PER_COMMIT entries
    int logSize = numCommits / MAX_ENTRIES_PER_COMMIT
      + (numCommits % MAX_ENTRIES_PER_COMMIT == 0 ? 0 : 1) // + 1 for the final non-full commit
      + 1; // + 1 for the "empty" commit that generate the key list
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

  @Test
  void testErase() {
    @SuppressWarnings("unchecked") Document<String, Integer> doc1 = mock(Document.class);
    when(doc1.getKey()).thenReturn("key1");

    @SuppressWarnings("unchecked") Document<String, Integer> doc2 = mock(Document.class);
    when(doc2.getKey()).thenReturn("key2");

    @SuppressWarnings("unchecked") KVStore<String, Integer> store = mock(KVStore.class);
    when(store.find(any())).thenReturn(ImmutableList.of(doc1, doc2));

    task.eraseStore("test", store);

    verify(store, times(1)).delete(eq("key1"));
    verify(store, times(1)).delete(eq("key2"));
  }
}
