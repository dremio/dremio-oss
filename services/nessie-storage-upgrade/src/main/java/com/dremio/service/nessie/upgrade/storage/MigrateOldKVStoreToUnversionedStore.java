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

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.embedded.catalog.EmbeddedPointerStore;
import com.dremio.service.embedded.catalog.EmbeddedUnversionedStore;
import com.dremio.service.nessie.upgrade.version040.MetadataReader;
import com.dremio.service.nessie.upgrade.version040.MetadataReader040;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.UUID;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;

/**
 * Migrates legacy Nessie data stored in custom format in the KVStore to {@link
 * EmbeddedUnversionedStore}.
 */
public class MigrateOldKVStoreToUnversionedStore extends UpgradeTask {

  public static final String TASK_ID = "40dcb921-8f34-48f4-a686-0c77fd3006d6";

  public MigrateOldKVStoreToUnversionedStore() {
    super("Migrate Nessie Data from KVStore to non-versioned KVStore", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return TASK_ID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    KVStoreProvider kvStoreProvider = context.getKvStoreProvider();

    MetadataReader040 reader = new MetadataReader040(kvStoreProvider);
    upgrade(kvStoreProvider, reader);

    // Remove old Nessie data from KVStores
    eraseStore("Legacy Nessie Commits", reader.getCommitKVStore());
    eraseStore("Legacy Nessie References", reader.getRefKVStore());
  }

  @VisibleForTesting
  <K, V> void eraseStore(String name, KVStore<K, V> store) {
    int count = 0;
    Iterable<Document<K, V>> documents = store.find();
    for (Document<K, V> doc : documents) {
      store.delete(doc.getKey());
      count++;
    }

    AdminLogger.log(String.format("Deleted %d entries from %s KVStore", count, name));
  }

  @VisibleForTesting
  void upgrade(KVStoreProvider kvStoreProvider, MetadataReader reader) throws Exception {
    EmbeddedPointerStore store = new EmbeddedPointerStore(kvStoreProvider);

    reader.doUpgrade(
        (branchName, contentKey, location) -> {
          // Embedded Nessie use cases that kept data in KVStores used only the "main" branch
          if (!"main".equals(branchName)) {
            throw new IllegalStateException("Found unexpected branch: " + branchName);
          }

          ContentKey key = ContentKey.of(contentKey.toArray(new String[0]));

          AdminLogger.log("Migrating key: " + key + ", location: " + location);

          // Note: old embedded Nessie data does not contain content ID or Iceberg snapshots and
          // other IDs.
          // Note: clients of Embedded Nessie do not use those IDs in versions where this upgrade
          // step is relevant
          // (i.e. Dremio Software versions 18, 19, 20, 21), so use zeros.
          // If / when Embedded Nessie clients start using those IDs retrieved from the Nessie
          // Server, the data
          // will already be managed by a Nessie DatabaseAdapter, so no low-level upgrade tasks will
          // be required
          // at that time (hopefully).
          IcebergTable table = IcebergTable.of(location, 0, 0, 0, 0, UUID.randomUUID().toString());

          store.put(key, table);
        });
  }
}
