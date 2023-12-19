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
import static org.projectnessie.versioned.CommitMetaSerializer.METADATA_SERIALIZER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.store.DefaultStoreWorker;

import com.dremio.common.SuppressForbidden;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;

class TestRebuildKeyList {

  private final RebuildKeyList task = new RebuildKeyList();
  private LocalKVStoreProvider storeProvider;
  private DatabaseAdapter adapter;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    NessieDatastoreInstance nessieDatastore = new NessieDatastoreInstance();
    nessieDatastore.configure(new ImmutableDatastoreDbConfig.Builder()
      .setStoreProvider(() -> storeProvider)
      .build());
    nessieDatastore.initialize();

    NonTransactionalDatabaseAdapterConfig adapterCfg = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig
      .builder()
      .validateNamespaces(false)
      .keyListDistance(10) // build key lists every 10 commits
      .maxKeyListSize(0) // force key list entities to be used even for small keys (i.e. prevent in-commit key lists)
      .maxKeyListEntitySize(0)
      .build();
    adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConnector(nessieDatastore)
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

  private void put(ContentKey key) throws ReferenceNotFoundException, ReferenceConflictException {
    IcebergTable table = IcebergTable.of(key.toString() + "-loc", 1, 2, 3, 4, UUID.randomUUID().toString());

    ContentId contentId = ContentId.of(UUID.randomUUID().toString());
    ImmutableCommitParams.Builder commit = ImmutableCommitParams.builder();
    adapter.commit(commit
      .toBranch(BranchName.of("main"))
      .commitMetaSerialized(METADATA_SERIALIZER.toBytes(CommitMeta.fromMessage("test-" + key)))
      .addPuts(KeyWithBytes.of(key, contentId, (byte) DefaultStoreWorker.payloadForContent(table),
        DefaultStoreWorker.instance().toStoreOnReferenceState(table)))
      .build());
  }

  @SuppressForbidden // This method has to use Nessie's relocated ByteString in method parameters.
  private void validateActiveKeys(Collection<ContentKey> activeKeys) throws ReferenceNotFoundException {
    ReferenceInfo<?> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
    try (Stream<KeyListEntry> keys = adapter.keys(main.getHash(), KeyFilterPredicate.ALLOW_ALL)) {
      assertThat(keys.map(KeyListEntry::getKey)).containsExactlyInAnyOrderElementsOf(activeKeys);
    }

    Map<ContentKey, ContentAndState> values = adapter.values(main.getHash(), activeKeys, KeyFilterPredicate.ALLOW_ALL);

    assertThat(values).hasSize(activeKeys.size());
    activeKeys.forEach(k -> {
      ContentAndState value = values.get(k);
      IcebergTable table = (IcebergTable) DefaultStoreWorker.instance().valueFromStore(
        value.getPayload(), value.getRefState(), () -> null);
      assertThat(table.getMetadataLocation()).isEqualTo(k.toString() + "-loc");
    });
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 5, 9, 10, 11, 99, 100, 101, 200, 1000})
  void testUpgrade(int numKeys) throws Exception {
    List<ContentKey> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      ContentKey key = ContentKey.of("test", "key-" + i);
      put(key);
      keys.add(key);
    }

    task.upgrade(storeProvider);

    validateActiveKeys(keys);
  }
}
