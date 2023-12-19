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

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.NessieRefLogStoreBuilder;
import com.google.common.annotations.VisibleForTesting;

/**
 * Removes obsolete Nessie "reflog" data.
 */
public class RemoveRefLog extends UpgradeTask {

  public static final String TASK_ID = "e6a793c4-4234-4510-ad73-4c4b12ea14bd";

  private static final int PROGRESS_CYCLE = Integer.getInteger("nessie.upgrade.purge.progress", 10_000);

  public RemoveRefLog() {
    super("Remove Nessie RefLog data from KVStore", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return TASK_ID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    upgrade(context.getKvStoreProvider(), PROGRESS_CYCLE);
  }

  @VisibleForTesting
  void upgrade(KVStoreProvider kvStoreProvider, int progressCycle) throws Exception {
    KVStore<String, byte[]> store = kvStoreProvider.getStore(NessieRefLogStoreBuilder.class);

    int count = 0;
    Iterable<Document<String, byte[]>> documents = store.find();
    for (Document<String, byte[]> doc : documents) {
      store.delete(doc.getKey(), KVStore.DeleteOption.NO_META);
      count++;

      if (count % progressCycle == 0) {
        AdminLogger.log(String.format("Deleted %d entries from Nessie ref_log...", count));
      }
    }

    AdminLogger.log(String.format("Deleted %d entries from Nessie ref_log in total", count));
  }

}
