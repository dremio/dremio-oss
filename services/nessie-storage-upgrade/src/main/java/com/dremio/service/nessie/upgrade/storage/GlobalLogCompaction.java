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
package com.dremio.service.nessie.upgrade.storage;

import java.util.Map;

import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.persist.adapter.ImmutableGlobalLogCompactionParams;
import org.projectnessie.versioned.persist.adapter.RepoMaintenanceParams;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapter;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

public class GlobalLogCompaction extends UpgradeTask {

  public static final String TASK_ID = "8e4f33b1-59ab-4f48-97a6-3560ffb7fc51";

  public GlobalLogCompaction() {
    super("Global log compaction for nessie", ImmutableList.of());
  }

  @Override
  public String getTaskUUID() {
    return TASK_ID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    upgrade(context.getKvStoreProvider());
  }

  @VisibleForTesting
  Map<String, Map<String, String>> upgrade(KVStoreProvider storeProvider) throws Exception {
    // run with default parameters
    RepoMaintenanceParams params = RepoMaintenanceParams.builder()
      .globalLogCompactionParams(ImmutableGlobalLogCompactionParams.builder()
        .build())
      .build();

    try (NessieDatastoreInstance store = new NessieDatastoreInstance()) {
      store.configure(new ImmutableDatastoreDbConfig.Builder()
        .setStoreProvider(() -> storeProvider)
        .build());
      store.initialize();
      TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
      DatastoreDatabaseAdapter adapter = new DatastoreDatabaseAdapter(
        ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build(),
        store,
        worker);

      Map<String, Map<String, String>> result = adapter.repoMaintenance(params);

      // Pretty print maintenance output to the log as JSON
      ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
      String stringResult = writer.writeValueAsString(result);

      AdminLogger.log("Finished nessie global log compaction with results: {}", stringResult);
      return result;
    }
  }
}
