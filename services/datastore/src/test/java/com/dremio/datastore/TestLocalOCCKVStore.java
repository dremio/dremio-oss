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

import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.test.DremioTest;

/** Test Local OCC Store. */
public class TestLocalOCCKVStore<K, V> extends AbstractTestOCCKVStore<K, V> {
  @Override
  protected KVStoreProvider createKVStoreProvider() throws Exception {
    final LocalKVStoreProvider provider =
        new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    provider.start();
    return provider;
  }
}
