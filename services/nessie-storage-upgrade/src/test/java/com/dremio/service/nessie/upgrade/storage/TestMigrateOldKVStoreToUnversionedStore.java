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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.projectnessie.versioned.VersionStore.KeyRestrictions.NO_KEY_RESTRICTIONS;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.embedded.catalog.EmbeddedUnversionedStore;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.ContentResult;

/** Unit tests for {@link MigrateOldKVStoreToUnversionedStore}. */
class TestMigrateOldKVStoreToUnversionedStore {

  private MigrateOldKVStoreToUnversionedStore task;
  private LocalKVStoreProvider storeProvider;
  private EmbeddedUnversionedStore store;

  @BeforeEach
  void createKVStore() throws Exception {
    task = new MigrateOldKVStoreToUnversionedStore();
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();

    store = new EmbeddedUnversionedStore(() -> storeProvider);
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void unexpectedBranch() {
    Assertions.assertThatThrownBy(
            () ->
                task.upgrade(
                    storeProvider,
                    commitConsumer ->
                        commitConsumer.migrateCommit(
                            "unexpected123", Collections.singletonList("key"), "location")))
        .hasMessageContaining("unexpected123");
  }

  @Test
  void testEmptyUpgrade() throws Exception {
    task.upgrade(storeProvider, c -> {});
    assertThat(store.getKeys(BranchName.of("main"), null, false, NO_KEY_RESTRICTIONS).hasNext())
        .isFalse();
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 40, 99, 100, 101, 499, 500, 501})
  void testUpgrade(int numCommits) throws Exception {
    List<ContentKey> keys = new ArrayList<>();
    List<String> testEntries = new ArrayList<>();

    task.upgrade(
        storeProvider,
        c -> {
          for (int i = 0; i < numCommits; i++) {
            List<String> key = ImmutableList.of("key", "" + i);
            String location = "location-" + i;

            c.migrateCommit("main", key, location);

            ContentKey nessieKey = ContentKey.of(key.toArray(new String[0]));
            keys.add(nessieKey);
            testEntries.add(location + "|" + nessieKey);
          }
        });

    Map<ContentKey, ContentResult> tables = store.getValues(BranchName.of("main"), keys, false);

    assertThat(
            tables.entrySet().stream()
                .map(
                    e -> {
                      ContentKey key = e.getKey();
                      IcebergTable table = (IcebergTable) e.getValue().content();
                      return table.getMetadataLocation() + "|" + key;
                    }))
        .containsExactlyInAnyOrder(testEntries.toArray(new String[0]));
  }

  @Test
  void testErase() {
    @SuppressWarnings("unchecked")
    Document<String, Integer> doc1 = mock(Document.class);
    when(doc1.getKey()).thenReturn("key1");

    @SuppressWarnings("unchecked")
    Document<String, Integer> doc2 = mock(Document.class);
    when(doc2.getKey()).thenReturn("key2");

    @SuppressWarnings("unchecked")
    KVStore<String, Integer> store = mock(KVStore.class);
    when(store.find(any())).thenReturn(ImmutableList.of(doc1, doc2));

    task.eraseStore("test", store);

    verify(store, times(1)).delete(eq("key1"));
    verify(store, times(1)).delete(eq("key2"));
  }
}
