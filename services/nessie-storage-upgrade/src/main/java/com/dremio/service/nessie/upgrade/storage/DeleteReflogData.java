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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.NessieRefLogStoreBuilder;
import com.google.common.annotations.VisibleForTesting;

/**
 * Deletes {@code reflog} tables from the KVStore because this data is not needed for Embedded Nessie.
 */
public class DeleteReflogData extends UpgradeTask {

  private static final int PROGRESS_CYCLE = Integer.getInteger("nessie.upgrade.delete_reflog.progress", 10_000);

  public DeleteReflogData() {
    super("Delete obsolete Nessie reflog data.", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return "293ef1c8-818b-4af2-8804-c8d91d7b357f";
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    deleteEntries(context.getKvStoreProvider(), PROGRESS_CYCLE);
  }

  @VisibleForTesting
  void deleteEntries(KVStoreProvider kvStoreProvider, int progressCycle) {
    KVStore<String, byte[]> store = kvStoreProvider.getStore(NessieRefLogStoreBuilder.class);
    AtomicInteger count = new AtomicInteger();
    store.find().forEach(entry -> {
      store.delete(entry.getKey(), KVStore.DeleteOption.NO_META);
      if (count.incrementAndGet() % progressCycle == 0) {
        AdminLogger.log("Deleted {} ref_log entries.", count.get());
      }
    });
  }
}
