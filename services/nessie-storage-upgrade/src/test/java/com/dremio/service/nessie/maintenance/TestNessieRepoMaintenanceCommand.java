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
package com.dremio.service.nessie.maintenance;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.ContentKey;
import org.projectnessie.nessie.relocated.protobuf.ByteString;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.slf4j.helpers.MessageFormatter;

import com.dremio.common.SuppressForbidden;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.service.namespace.NamespaceServiceImpl.NamespaceStoreCreator;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.dremio.service.nessie.maintenance.NessieRepoMaintenanceCommand.Options;
import com.google.common.collect.ImmutableList;

class TestNessieRepoMaintenanceCommand {

  private LocalKVStoreProvider storeProvider;
  private DatabaseAdapter adapter;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    NonTransactionalDatabaseAdapterConfig adapterCfg = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig
      .builder()
      .validateNamespaces(false)
      .build();
    NessieDatastoreInstance store = new NessieDatastoreInstance();
    store.configure(new ImmutableDatastoreDbConfig.Builder().setStoreProvider(() -> storeProvider).build());
    store.initialize();
    adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConnector(store)
      .withConfig(adapterCfg)
      .build();
    adapter.initializeRepo("main");
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void testBasicPurgeExecution() throws Exception {
    // This is just a smoke test. Functional test for this purge are in ITCommitLogMaintenance.
    assertThat(NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[]{"--purge-key-lists"})))
      .contains("deletedKeyListEntities");
  }

  @Test
  void testListKeys() throws Exception {
    // Just a smoke test. This option is not meant for production use.
    assertThat(NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[]{"--list-keys"})))
      .isNotNull();
  }

  @Test
  @SuppressForbidden // Nessie's relocated ByteString is required to interface with Nessie Database Adapters.
  void testListLiveCommits() throws Exception {
    Hash commit1 = adapter.commit(ImmutableCommitParams.builder()
      .addPuts(KeyWithBytes.of(ContentKey.of("test1"), ContentId.of("id1"), (byte) 1, ByteString.EMPTY))
      .toBranch(BranchName.of("main"))
      .commitMetaSerialized(ByteString.copyFrom("test-meta", StandardCharsets.UTF_8))
      .build())
      .getCommitHash();

    Hash commit2 = adapter.commit(ImmutableCommitParams.builder()
      .addPuts(KeyWithBytes.of(ContentKey.of("ns2", "test2"), ContentId.of("id1"), (byte) 1, ByteString.EMPTY))
      .toBranch(BranchName.of("main"))
      .commitMetaSerialized(ByteString.copyFrom("test-meta", StandardCharsets.UTF_8))
      .build())
      .getCommitHash();

    Function<Hash, Instant> commitTime = hash -> {
      try (Stream<CommitLogEntry> logStream = adapter.commitLog(hash)) {
        long ts = logStream.findFirst().map(CommitLogEntry::getCreatedTime).orElse(0L);
        return Instant.ofEpochMilli(TimeUnit.MICROSECONDS.toMillis(ts));
      } catch (ReferenceNotFoundException e) {
        throw new RuntimeException(e);
      }
    };

    List<String> log = new ArrayList<>();
    assertThat(NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[]{"--list-live-commits"}),
        (msg, args) -> log.add(MessageFormatter.arrayFormat(msg, args).getMessage())))
      .isNotNull();

    // Note: reverse chronological order
    assertThat(log).containsExactly(
      String.format("%s: %s: ns2.test2", commit2.asString(), commitTime.apply(commit2)),
      String.format("%s: %s: test1", commit1.asString(), commitTime.apply(commit1))
    );

    log.clear();
    assertThat(NessieRepoMaintenanceCommand.execute(
      storeProvider,
      Options.parse(new String[]{"--list-keys"}),
      (msg, args) -> log.add(MessageFormatter.arrayFormat(msg, args).getMessage())))
      .isNotNull();

    assertThat(log).containsExactly("ns2.test2", "test1");
  }

  @Test
  @SuppressForbidden // Nessie's relocated ByteString is required to interface with Nessie Database Adapters.
  void testListObsoleteInternalKeys() throws Exception {
    String tableId2 = "8ec5373f-d2a6-4b1a-a870-e18046bbd6ae";
    String tableId3 = "34dadc3a-ae44-4e61-b78e-0295c089df70";
    adapter.commit(ImmutableCommitParams.builder()
      .addPuts(KeyWithBytes.of(ContentKey.of("test1"), ContentId.of("id1"), (byte) 1, ByteString.EMPTY))
      .addPuts(KeyWithBytes.of(ContentKey.of("dremio.internal", "test2/" + tableId2), ContentId.of("id2"), (byte) 1, ByteString.EMPTY))
      .addPuts(KeyWithBytes.of(ContentKey.of("dremio.internal", "test3/" + tableId3), ContentId.of("id2"), (byte) 1, ByteString.EMPTY))
      .toBranch(BranchName.of("main"))
      .commitMetaSerialized(ByteString.copyFrom("test-meta", StandardCharsets.UTF_8))
      .build());

    IndexedStore<String, NameSpaceContainer> namespace = storeProvider.getStore(NamespaceStoreCreator.class);
    IcebergMetadata metadata = new IcebergMetadata();
    metadata.setTableUuid(tableId3);
    metadata.setMetadataFileLocation("test-location");
    namespace.put("dataset3", new NameSpaceContainer()
      .setFullPathList(ImmutableList.of("ns", "dataset"))
      .setType(NameSpaceContainer.Type.DATASET)
      .setDataset(new DatasetConfig()
        .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
        .setId(new EntityId("ds-id3"))
        .setPhysicalDataset(new PhysicalDataset().setIcebergMetadata(metadata))));

    List<String> log = new ArrayList<>();
    assertThat(NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[]{"--list-obsolete-internal-keys"}),
        (msg, args) -> log.add(MessageFormatter.arrayFormat(msg, args).getMessage())))
      .isNotNull();

    // Note: "test1" is not an "internal" key, so it is not reported
    assertThat(log).containsExactly("dremio.internal|test2/8ec5373f-d2a6-4b1a-a870-e18046bbd6ae");

    String tableId4 = "e14e4ecb-d39c-4311-84fc-2127cc11f195";
    IcebergMetadata metadata4 = new IcebergMetadata();
    metadata4.setTableUuid(tableId4);
    metadata4.setMetadataFileLocation("test-location");
    namespace.put("dataset4", new NameSpaceContainer()
      .setFullPathList(ImmutableList.of("ns", "dataset4"))
      .setType(NameSpaceContainer.Type.DATASET)
      .setDataset(new DatasetConfig()
        .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
        .setId(new EntityId("ds-id4"))
        .setPhysicalDataset(new PhysicalDataset().setIcebergMetadata(metadata4))));

    log.clear();
    assertThatThrownBy(() -> NessieRepoMaintenanceCommand.execute(
      storeProvider,
      Options.parse(new String[]{"--list-obsolete-internal-keys"}),
      (msg, args) -> log.add(MessageFormatter.arrayFormat(msg, args).getMessage())))
      .isInstanceOf(IllegalStateException.class)
      .hasMessageContaining("Keys for some table IDs were not found");

    assertThat(log).containsExactly("dremio.internal|test2/8ec5373f-d2a6-4b1a-a870-e18046bbd6ae",
      "Live metadata table ID: e14e4ecb-d39c-4311-84fc-2127cc11f195 does not have a corresponding Nessie key");
  }
}
