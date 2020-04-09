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
package com.dremio.datastore;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.test.DremioTest;

/**
 * Test local mapdb kvstore.
 */
public class TestLocalKVStore<K, V> extends AbstractTestKVStore<K, V> {
  @ClassRule
  public static final TemporaryFolder tmpFolder = new TemporaryFolder();


  private KVStoreProvider localKVStoreProvider;

  @Override
  public KVStoreProvider initProvider() throws Exception {
    localKVStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT,
      tmpFolder.getRoot().toString(), true, true);
    localKVStoreProvider.start();
    return localKVStoreProvider;
  }

  @Override
  public void closeProvider() throws Exception {
    if (localKVStoreProvider != null) {
      localKVStoreProvider.close();
    }
    localKVStoreProvider = null;
  }
}
