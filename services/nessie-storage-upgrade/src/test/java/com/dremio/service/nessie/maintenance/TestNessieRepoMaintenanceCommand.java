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
package com.dremio.service.nessie.maintenance;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;

import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.ClassPathScanner;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.dremio.service.nessie.maintenance.NessieRepoMaintenanceCommand.Options;

class TestNessieRepoMaintenanceCommand {

  private static final ScanResult scanResult = ClassPathScanner.fromPrescan(SabotConfig.create());

  private LocalKVStoreProvider storeProvider;

  @BeforeEach
  void createKVStore() throws Exception {
    storeProvider = new LocalKVStoreProvider(scanResult, null, true, false); // in-memory
    storeProvider.start();

    NonTransactionalDatabaseAdapterConfig adapterCfg = ImmutableAdjustableNonTransactionalDatabaseAdapterConfig
      .builder()
      .validateNamespaces(false)
      .build();
    NessieDatastoreInstance store = new NessieDatastoreInstance();
    store.configure(new ImmutableDatastoreDbConfig.Builder().setStoreProvider(() -> storeProvider).build());
    store.initialize();
    DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
      .withConnector(store)
      .withConfig(adapterCfg)
      .build();
    adapter.initializeRepo("main");
  }

  @AfterEach
  void stopKVStore() throws Exception {
    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  @Test
  void testBasicPurgeExecution() throws Exception {
    // This is just a smoke test. Functional test for this purge are in ITCommitLogMaintenance.
    Assertions.assertThat(NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[]{"--purge-key-lists"})))
      .contains("deletedKeyListEntities");
  }

  @Test
  void testListKeys() throws Exception {
    // Just a smoke test. This option is not meant for production use.
    Assertions.assertThat(NessieRepoMaintenanceCommand.execute(
        storeProvider,
        Options.parse(new String[]{"--list-keys"})))
      .isNotNull();
  }

}
