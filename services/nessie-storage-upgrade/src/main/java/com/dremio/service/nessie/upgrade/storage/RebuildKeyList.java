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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapter;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

/**
 * This upgrade task rebuilds the most recent key list to add missing commit IDs to key list entries.
 * <p>Nessie will put commit IDs into key list entries automatically to speed up reads, but that happens
 * only at key list reconstruction (change). This upgrade step will unconditionally rebuild the latest
 * key list to ensure that even older data get immediate read performance boost right after an upgrade,
 * and without waiting for changes to force the key list to be rebuilt at run time.</p>
 * <p>Note: only the latest key list needs to be rebuilt to fix read performance, because Embedded
 * Nessie use cases do not involve time travel.</p>
 */
public class RebuildKeyList extends UpgradeTask {
  public static final String TASK_ID = "451c04d7-bbb3-46bf-a067-0513d79d41fc";

  private static final int PROGRESS_CYCLE = Integer.getInteger("nessie.upgrade.keylist.commit_progress", 3000);

  public RebuildKeyList() {
    super("Rebuild the latest Nessie key list", Collections.singletonList(PurgeObsoleteKeyLists.TASK_ID));
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
  void upgrade(KVStoreProvider storeProvider) throws Exception {
    try (NessieDatastoreInstance store = new NessieDatastoreInstance()) {
      store.configure(new ImmutableDatastoreDbConfig.Builder()
        .setStoreProvider(() -> storeProvider)
        .build());
      store.initialize();
      TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
      DatastoreDatabaseAdapter adapter = (DatastoreDatabaseAdapter) new DatastoreDatabaseAdapterFactory().newBuilder()
        .withConnector(store)
        .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder().build())
        .build(worker);

      // Only process main. Other branches in the Embedded Nessie are not utilized during runtime.
      ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
      // Find the most recent key list
      AtomicReference<List<Hash>> keyListIds = new AtomicReference<>();
      DatabaseAdapterUtil.takeUntilExcludeLast(adapter.commitLog(main.getHash()), c -> keyListIds.get() != null)
        .forEach(commitLogEntry -> {
          if (!commitLogEntry.getKeyListsIds().isEmpty()) {
            keyListIds.set(commitLogEntry.getKeyListsIds());
          }
        });

      if (keyListIds.get() == null) {
        AdminLogger.log("No key lists found.");
        return;
      }

      // Process all key list entities together. This will use memory, but will speed up the upgrade process.
      // Testing with some larger data sets of 300K keys, ~170 key list entities shown that default
      // upgrade process memory settings were sufficient.
      Set<Key> activeKeys = new HashSet<>();
      Map<Hash, KeyList> keyLists = new HashMap<>();
      keyListIds.get().forEach(hash -> {
        Document<String, byte[]> doc = store.getKeyList().get(adapter.dbKey(hash));
        KeyList keyList = ProtoSerialization.protoToKeyList(doc.getValue());
        keyLists.put(hash, keyList);
        keyList.getKeys().forEach(keyListEntry -> activeKeys.add(keyListEntry.getKey()));
        AdminLogger.log("Loaded key list entity {}", hash);
      });

      // Find relevant commit IDs for active keys.
      Map<Key, Hash> activePuts = new HashMap<>();
      AtomicInteger numCommits = new AtomicInteger();
      DatabaseAdapterUtil.takeUntilExcludeLast(adapter.commitLog(main.getHash()), c -> activeKeys.isEmpty())
        .forEach(commitLogEntry -> commitLogEntry.getPuts().forEach(keyWithBytes -> {
          // Find the most recent "put" for each active key.
          if (activeKeys.remove(keyWithBytes.getKey())) {
            activePuts.put(keyWithBytes.getKey(), commitLogEntry.getHash());
          }

          if (numCommits.incrementAndGet() % PROGRESS_CYCLE == 0) {
            AdminLogger.log("Processed {} commits. {} keys remain to be found.", numCommits.get(), activeKeys.size());
          }
        }));

      // Update key list entities to ensure each entry has a commit ID.
      keyLists.forEach((keyListHash, keyList) -> {
        ImmutableKeyList.Builder updated = ImmutableKeyList.builder();
        keyList.getKeys().forEach(keyListEntry -> {
          Key key = keyListEntry.getKey();
          Hash commitHash = activePuts.get(key);
          if (commitHash == null) {
            throw new IllegalStateException("Put not found for key: " + key);
          }

          // Use the latest put's commit hash in the key list entry.
          updated.addKeys(ImmutableKeyListEntry.builder().from(keyListEntry).commitId(commitHash).build());
        });

        // Overwrite the key list entity at the same hash.
        store.getKeyList().put(adapter.dbKey(keyListHash), ProtoSerialization.toProto(updated.build()).toByteArray());
        AdminLogger.log("Updated key list {}", keyListHash);
      });
    }
  }
}
