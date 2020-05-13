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
package com.dremio.datastore.indexed;

import org.junit.Ignore;
import org.junit.Test;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.IndexedStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.indexed.doughnut.Doughnut;
import com.dremio.datastore.indexed.doughnut.DoughnutIndexedStore;
import com.dremio.test.DremioTest;

/**
 * Test for local indexed OCC store.
 */
public class TestLocalIndexedOCCStore extends AbstractTestIndexedOCCKVStore {
  @Override
  protected KVStoreProvider createKVStoreProvider() throws Exception {
    final KVStoreProvider provider =
      new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    provider.start();
    return provider;
  }

  @Override
  protected IndexedStore<String, Doughnut> createKVStore() {
    return getProvider().getStore(DoughnutIndexedStore.class);
  }

  @Ignore("[DX-9909] Not query doesn't work as expected for RocksDB.")
  @Test
  @Override
  public void not() {
  }
}
