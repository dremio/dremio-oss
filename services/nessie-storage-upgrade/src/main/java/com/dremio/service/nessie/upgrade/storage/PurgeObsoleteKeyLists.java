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
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.GlobalLogCompactionParams;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.EmbeddedRepoMaintenanceParams;
import com.dremio.service.nessie.EmbeddedRepoPurgeParams;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.dremio.service.nessie.maintenance.ProgressReporter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

/**
 * This upgrade task performs a maintenance operation on the Embedded Nessie to remove obsolete key list
 * entities to reduce storage usage.
 * <p>This task only needs to run once as newer Nessie Service implementations will do similar purges
 * periodically.</p>
 */
public class PurgeObsoleteKeyLists extends UpgradeTask {
  public static final String TASK_ID = "38d99dc2-d492-41ea-8809-b41192cd87fd";

  private static final int PROGRESS_CYCLE = Integer.getInteger("nessie.upgrade.purge.progress", 10_000);

  public PurgeObsoleteKeyLists() {
    super("Purge obsolete Nessie key lists",
      ImmutableList.of(MigrateIcebergMetadataPointer.TASK_ID, MigrateToNessieAdapter.TASK_ID));
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
  String upgrade(KVStoreProvider storeProvider) throws Exception {
    EmbeddedRepoMaintenanceParams params = EmbeddedRepoMaintenanceParams.builder()
      .setGlobalLogCompactionParams(GlobalLogCompactionParams.builder().isEnabled(false).build())
      .setEmbeddedRepoPurgeParams(EmbeddedRepoPurgeParams.builder()
        .setDryRun(false)
        .setProgressReporter(new ProgressReporter(PROGRESS_CYCLE))
        .build())
      .build();

    try (NessieDatastoreInstance store = new NessieDatastoreInstance()) {
      store.configure(new ImmutableDatastoreDbConfig.Builder()
        .setStoreProvider(() -> storeProvider)
        .build());
      store.initialize();

      TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
      DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
        .withConnector(store)
        .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
        .build(worker);

      Map<String, Map<String, String>> result = adapter.repoMaintenance(params);

      // Pretty print maintenance output to the log as JSON
      ObjectWriter writer = new ObjectMapper().writerWithDefaultPrettyPrinter();
      String stringResult = writer.writeValueAsString(result);

      AdminLogger.log("Finished key list purge with results: {}", stringResult);
      return stringResult;
    }
  }
}
