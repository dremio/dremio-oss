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
import java.util.stream.Stream;

import org.projectnessie.model.ContentKey;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyList;
import org.projectnessie.versioned.persist.adapter.ImmutableKeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyList;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;

import com.dremio.common.SuppressForbidden;
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
      DatastoreDatabaseAdapter adapter = (DatastoreDatabaseAdapter) new DatastoreDatabaseAdapterFactory().newBuilder()
        .withConnector(store)
        .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder()
          .validateNamespaces(false)
          .build())
        .build();

      // Only process main. Other branches in the Embedded Nessie are not utilized during runtime.
      ReferenceInfo<?> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
      // Find the most recent key list
      AtomicReference<List<Hash>> keyListIds = new AtomicReference<>();
      try (Stream<CommitLogEntry> log = adapter.commitLog(main.getHash())) {
        DatabaseAdapterUtil.takeUntilExcludeLast(log, c -> keyListIds.get() != null)
          .forEach(commitLogEntry -> {
            if (!commitLogEntry.getKeyListsIds().isEmpty()) {
              keyListIds.set(commitLogEntry.getKeyListsIds());
            }
          });
      }

      if (keyListIds.get() == null) {
        AdminLogger.log("No key lists found.");
        return;
      }

      // Process all key list entities together. This will use memory, but will speed up the upgrade process.
      // Testing with some larger data sets of 300K keys, ~170 key list entities shown that default
      // upgrade process memory settings were sufficient.
      Set<ContentKey> activeKeys = new HashSet<>();
      Map<Hash, KeyList> keyLists = new HashMap<>();
      keyListIds.get().forEach(hash -> {
        Document<String, byte[]> doc = store.getKeyList().get(adapter.dbKey(hash));
        KeyList keyList = ProtoSerialization.protoToKeyList(doc.getValue());
        keyLists.put(hash, keyList);
        keyList.getKeys().forEach(keyListEntry -> {
          if (keyListEntry != null) {
            activeKeys.add(keyListEntry.getKey());
          }
        });
        AdminLogger.log("Loaded key list entity {}", hash);
      });

      // Find relevant commit IDs for active keys.
      Map<ContentKey, Hash> activePuts = new HashMap<>();
      AtomicInteger numCommits = new AtomicInteger();
      try (Stream<CommitLogEntry> log = adapter.commitLog(main.getHash())) {
        DatabaseAdapterUtil.takeUntilExcludeLast(log, c -> activeKeys.isEmpty())
          .forEach(commitLogEntry -> commitLogEntry.getPuts().forEach(keyWithBytes -> {
            // Find the most recent "put" for each active key.
            if (activeKeys.remove(keyWithBytes.getKey())) {
              activePuts.put(keyWithBytes.getKey(), commitLogEntry.getHash());
            }

            if (numCommits.incrementAndGet() % PROGRESS_CYCLE == 0) {
              AdminLogger.log("Processed {} commits. {} keys remain to be found.", numCommits.get(), activeKeys.size());
            }
          }));
      }

      // Update key list entities to ensure each non-null entry has a commit ID.
      // Note: null entries are valid, they represent empty slots in open hashing key tables.
      keyLists.forEach((keyListHash, keyList) -> {
        ImmutableKeyList.Builder updated = ImmutableKeyList.builder();
        keyList.getKeys().forEach(keyListEntry -> {
          if (keyListEntry == null) {
            updated.addKeys((KeyListEntry) null);
          } else {
            ContentKey key = keyListEntry.getKey();
            Hash commitHash = activePuts.get(key);
            if (commitHash == null) {
              throw new IllegalStateException("Put not found for key: " + key);
            }

            // Use the latest put's commit hash in the key list entry.
            updated.addKeys(ImmutableKeyListEntry.builder().from(keyListEntry).commitId(commitHash).build());
          }
        });

        // Overwrite the key list entity at the same hash.
        store.getKeyList().put(adapter.dbKey(keyListHash), toBytes(updated.build()));
        AdminLogger.log("Updated key list {}", keyListHash);
      });
    }
  }

  @SuppressForbidden // This method has to use Nessie's relocated ByteString in method parameters.
  private static byte[] toBytes(KeyList keyList) {
    return ProtoSerialization.toProto(keyList).toByteArray();
  }
}
