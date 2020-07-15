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
package com.dremio.services.configuration;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.services.configuration.proto.ConfigurationEntry;
import com.dremio.test.DremioTest;

import io.protostuff.ByteString;

/**
 * Tests for ConfigurationStore
 */
public class TestConfigurationStore {
  @Test
  public void testStore() throws Exception {
    try(final LegacyKVStoreProvider kvstore =
      LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT)) {
      kvstore.start();
      ConfigurationStore store = new ConfigurationStore(kvstore);

      ConfigurationEntry supportEntry = new ConfigurationEntry();
      supportEntry.setType("mytype");
      supportEntry.setValue(ByteString.copyFrom("test string", "UTF8"));
      store.put("key", supportEntry);

      ConfigurationEntry retrieved = store.get("key");
      Assert.assertEquals(retrieved.getType(), supportEntry.getType());
      Assert.assertEquals(retrieved.getValue(), supportEntry.getValue());

      store.delete("key");
      retrieved = store.get("key");
      Assert.assertEquals(retrieved, null);
    }
  }
}
