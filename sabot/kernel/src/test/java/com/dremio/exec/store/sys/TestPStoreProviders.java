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
package com.dremio.exec.store.sys;

import javax.inject.Provider;

import org.junit.Test;

import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.ExecTest;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.test.DremioTest;

public class TestPStoreProviders extends ExecTest {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestPStoreProviders.class);

  @Test
  public void verifyKVPersistentStore() throws Exception {
    final KVStoreProvider storeProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    storeProvider.start();
    final Provider<KVStoreProvider> storeProviderProvider = new Provider<KVStoreProvider>() {
      @Override
      public KVStoreProvider get() {
        return storeProvider;
      }
    };

    try (PersistentStoreProvider provider = new KVPersistentStoreProvider(storeProviderProvider)) {
      provider.start();
      PStoreTestUtil.test(provider);
    }
  }

}
