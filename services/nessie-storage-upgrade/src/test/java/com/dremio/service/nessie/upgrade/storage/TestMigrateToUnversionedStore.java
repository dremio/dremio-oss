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

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.projectnessie.versioned.CommitMetaSerializer.METADATA_SERIALIZER;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.embedded.catalog.EmbeddedUnversionedStore;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieCommitLogStoreBuilder;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.dremio.service.nessie.NessieGlobalLogStoreBuilder;
import com.dremio.service.nessie.NessieGlobalPointerStoreBuilder;
import com.dremio.service.nessie.NessieKeyListStoreBuilder;
import com.dremio.service.nessie.NessieNamedRefHeadsStoreBuilder;
import com.dremio.service.nessie.NessieRefLogStoreBuilder;
import com.dremio.service.nessie.NessieRefNamesStoreBuilder;
import com.dremio.service.nessie.NessieRepoDescriptionStoreBuilder;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Namespace;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.store.DefaultStoreWorker;

/** Unit tests for {@link MigrateToUnversionedStore}. */
class TestMigrateToUnversionedStore {

  private MigrateToUnversionedStore task;
  private LocalKVStoreProvider storeProvider;
  private DatabaseAdapter adapter;
  private EmbeddedUnversionedStore store;

  @BeforeEach
  void createKVStore() throws Exception {
    task = new MigrateToUnversionedStore();
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    NessieDatastoreInstance nessieDatastore = new NessieDatastoreInstance();
    nessieDatastore.configure(
        new ImmutableDatastoreDbConfig.Builder().setStoreProvider(() -> storeProvider).build());
    nessieDatastore.initialize();

    NonTransactionalDatabaseAdapterConfig adapterCfg =
        ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder()
            .validateNamespaces(false)
            .build();
    adapter =
        new DatastoreDatabaseAdapterFactory()
            .newBuilder()
            .withConfig(adapterCfg)
            .withConnector(nessieDatastore)
            .build();
    adapter.initializeRepo("main");

    store = new EmbeddedUnversionedStore(() -> storeProvider);
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void testEmptyUpgrade() throws Exception {
    task.upgrade(storeProvider, 10);
  }

  private void putTable(ContentKey key, String location)
      throws ReferenceNotFoundException, ReferenceConflictException {
    put(key, IcebergTable.of(location, 1, 2, 3, 4, UUID.randomUUID().toString()));
  }

  private void putNamespace(Namespace ns)
      throws ReferenceNotFoundException, ReferenceConflictException {
    put(ns.toContentKey(), ns.withId(UUID.randomUUID().toString()));
  }

  private void put(ContentKey key, Content content)
      throws ReferenceNotFoundException, ReferenceConflictException {
    ContentId contentId = ContentId.of(UUID.randomUUID().toString());
    ImmutableCommitParams.Builder commit = ImmutableCommitParams.builder();
    adapter.commit(
        commit
            .toBranch(BranchName.of("main"))
            .commitMetaSerialized(
                METADATA_SERIALIZER.toBytes(CommitMeta.fromMessage("test-" + key)))
            .addPuts(
                KeyWithBytes.of(
                    key,
                    contentId,
                    (byte) DefaultStoreWorker.payloadForContent(content),
                    DefaultStoreWorker.instance().toStoreOnReferenceState(content)))
            .build());
  }

  @Test
  void testMigrateEntries() throws Exception {
    ContentKey key1 = ContentKey.of("dremio.internal", "table1");
    putTable(key1, "loc111");
    ContentKey key2 = ContentKey.of("dremio.internal", "table2");
    putTable(key2, "loc222");
    Namespace ns = Namespace.of("dremio.internal");
    putNamespace(ns);

    task.upgrade(storeProvider, 1);

    assertThat(store.getValue(BranchName.of("main"), key1, false).content())
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("loc111");
    assertThat(store.getValue(BranchName.of("main"), key2, false).content())
        .asInstanceOf(type(IcebergTable.class))
        .extracting(IcebergTable::getMetadataLocation)
        .isEqualTo("loc222");
    assertThat(store.getValue(BranchName.of("main"), ns.toContentKey(), false).content())
        .asInstanceOf(type(Namespace.class))
        .extracting(Namespace::toContentKey)
        .isEqualTo(ns.toContentKey());
  }

  @Test
  void testEraseLegacyData() throws Exception {
    ContentKey key1 = ContentKey.of("dremio.internal", "table1");
    putTable(key1, "loc111");
    putTable(key1, "loc222");
    putTable(key1, "loc333");
    Namespace ns = Namespace.of("dremio.internal");
    putNamespace(ns);

    task.upgrade(storeProvider, 2);

    assertThat(storeProvider.getStore(NessieRefNamesStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieRefLogStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieRepoDescriptionStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieGlobalLogStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieGlobalPointerStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieNamedRefHeadsStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieKeyListStoreBuilder.class).find()).isEmpty();
    assertThat(storeProvider.getStore(NessieCommitLogStoreBuilder.class).find()).isEmpty();
  }
}
