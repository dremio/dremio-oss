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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.nessie.NessieRefLogStoreBuilder;

/**
 * Unit tests for {@link TestDeleteReflogData}.
 */
class TestDeleteReflogData extends AbstractNessieUpgradeTest {

  private final DeleteReflogData task = new DeleteReflogData();
  private LocalKVStoreProvider storeProvider;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(scanResult, null, true, false); // in-memory
    storeProvider.start();
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void testUpgrade() throws Exception {
    KVStore<String, byte[]> store = storeProvider.getStore(NessieRefLogStoreBuilder.class);
    assertThat(store).isNotNull();

    // Generate enough some test data to make sure progress reporting does not throw exceptions.
    for (int i = 0; i < 2000; i++) {
      store.put("test-" + i, new byte[1]);
    }

    assertThat(store.find()).isNotEmpty();
    task.deleteEntries(storeProvider, 100);
    assertThat(store.find()).isEmpty();
  }
}
