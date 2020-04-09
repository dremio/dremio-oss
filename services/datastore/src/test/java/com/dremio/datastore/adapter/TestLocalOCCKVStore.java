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
package com.dremio.datastore.adapter;

import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.test.DremioTest;

/**
 * Test the local kv store implicit occ interface through the legacy api.
 * Uses an in memory store because cleaning up rocks in between tests requires more development effort.
 */
public class TestLocalOCCKVStore<K, V> extends AbstractLegacyTestOCCKVStore<K, V> {

  @ClassRule
  public static final TemporaryFolder tmpFolder = new TemporaryFolder();

  @Override
  public LegacyKVStoreProvider createProvider() {
    return new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT,
      tmpFolder.getRoot().toString(), true, true).asLegacy();
  }
}
