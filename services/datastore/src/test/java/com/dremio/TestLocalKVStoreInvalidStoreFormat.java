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
package com.dremio;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.dremio.datastore.AbstractTestInvalidStoreFormat;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.test.DremioTest;

/**
 * Test stores with invalid formats with LocalKVStoreProvider.
 * @param <K> key type K.
 * @param <V> value type V.
 */
public class TestLocalKVStoreInvalidStoreFormat<K,V> extends AbstractTestInvalidStoreFormat<K, V> {
  @ClassRule
  public static final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Override
  protected KVStoreProvider createProvider() {
    return new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT,
      tmpFolder.getRoot().toString(), true, true);
  }
}
