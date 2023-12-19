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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStore;
import com.dremio.service.nessie.NessieRefLogStoreBuilder;

/**
 * Unit tests for {@link RemoveRefLog}.
 */
class TestRemoveRefLog {

  private final RemoveRefLog task = new RemoveRefLog();
  private LocalKVStoreProvider storeProvider;
  private KVStore<String, byte[]> store;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false); // in-memory
    storeProvider.start();
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  private void makeStoreEntries() {
    store = storeProvider.getStore(NessieRefLogStoreBuilder.class);
    for (int i = 0; i < 20; i++) {
      store.put("test-key-" + i, new byte[] {(byte) i});
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 3, 100})
  void testUpgrade(int progressCycle) throws Exception {
    makeStoreEntries();
    task.upgrade(storeProvider, progressCycle);

    assertThat(store.find()).isEmpty();
  }

  @Test
  void testEmptyUpgrade() throws Exception {
    task.upgrade(storeProvider, 10);
  }
}
