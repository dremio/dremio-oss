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

import com.dremio.dac.server.DACConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.services.configuration.ConfigurationStore;
import java.util.Optional;

/** Reset catalog search command line. */
@AdminCommand(value = "reset-catalog-search", description = "Reset index to recover catalog search")
public class ResetCatalogSearch {

  private static void parse(String[] args) {
    if (args != null && args.length > 0) {
      AdminLogger.log("Error: Unknown command: " + args[0]);
      AdminLogger.log("Usage: dremio-admin reset-catalog-search");
      System.exit(1);
    }
  }

  static void go(String[] args) throws Exception {
    final DACConfig dacConfig = DACConfig.newConfig();
    parse(args);

    if (!dacConfig.isMaster) {
      throw new UnsupportedOperationException("Reset catalog search should be run on master node");
    }

    final Optional<LocalKVStoreProvider> providerOptional =
        CmdUtils.getKVStoreProvider(dacConfig.getConfig());
    if (!providerOptional.isPresent()) {
      AdminLogger.log("Failed to complete catalog search reset. No KVStore detected.");
      return;
    }

    try (LocalKVStoreProvider provider = providerOptional.get()) {
      provider.start();

      AdminLogger.log("Resetting catalog search...");
      final ConfigurationStore configStore = new ConfigurationStore(provider.asLegacy());
      configStore.delete(CONFIG_KEY);
      AdminLogger.log("Catalog search reset will be completed in 1 minute after Dremio starts.");
    }
  }

  public static void main(String[] args) {
    try {
      go(args);
      System.exit(0);
    } catch (Exception e) {
      AdminLogger.log("Failed to complete catalog search reset.", e);
      System.exit(1);
    }
  }
}
