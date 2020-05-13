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
package com.dremio.dac.cmd;

import static com.dremio.dac.service.search.SearchIndexManager.CONFIG_KEY;
import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.junit.Test;

import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.services.configuration.ConfigurationStore;
import com.dremio.services.configuration.proto.ConfigurationEntry;


/**
 * Tests dremio-admin reset-catalog-search command.
 */
public class TestResetCatalogSearch extends TestClean {

  /**
   * Check if the kvstore entry is cleared after running the command.
   */
  @Test
  public void testResetCatalogSearchCommand() throws Exception {
    getCurrentDremioDaemon().close();
    ResetCatalogSearch.go(new String[] {});
    final Optional<LegacyKVStoreProvider> providerOptional = CmdUtils.getLegacyKVStoreProvider(getDACConfig().getConfig());
    if (!providerOptional.isPresent()) {
      throw new Exception("No KVStore detected.");
    }
    try (LegacyKVStoreProvider provider = providerOptional.get()) {
      provider.start();
      final ConfigurationStore configStore = new ConfigurationStore(provider);
      final ConfigurationEntry configurationEntry = configStore.get(CONFIG_KEY);
      assertEquals(configurationEntry, null);
    }
  }
}
