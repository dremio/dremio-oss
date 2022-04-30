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

import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.dremio.service.nessie.upgrade.version040.MetadataReader;
import com.dremio.service.nessie.upgrade.version040.MetadataReader040;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;

/**
 * Migrates legacy Nessie data stored in custom format in the KVStore to OSS format managed by
 * a Nessie {@link DatabaseAdapter}.
 */
public class MigrateToNessieAdapter extends UpgradeTask {

  static final int MAX_ENTRIES_PER_COMMIT = Integer.getInteger("nessie.upgrade.max_entries_per_commit", 20);

  public static final String TASK_ID = "40dcb921-8f34-48f4-a686-0c77fd3006d6";

  public MigrateToNessieAdapter() {
    super("Migrate Nessie Data from KVStore to database adapter",
      Collections.singletonList(MigrateIcebergMetadataPointer.TASK_ID));
  }

  @Override
  public String getTaskUUID() {
    return TASK_ID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    KVStoreProvider kvStoreProvider = context.getKvStoreProvider();

    MetadataReader040 reader = new MetadataReader040(kvStoreProvider);
    upgrade(kvStoreProvider, "upgrade-" + getTaskUUID(), reader);

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
  void upgrade(KVStoreProvider kvStoreProvider, String upgradeBranchName, MetadataReader reader) throws Exception {
    try (NessieDatastoreInstance store = new NessieDatastoreInstance()) {
      store.configure(new ImmutableDatastoreDbConfig.Builder()
        .setStoreProvider(() -> kvStoreProvider)
        .build());
      store.initialize();

      TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
      DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder().withConnector(store).build();

      // Ensure the Embedded Nessie repo is initialized. This is an idempotent operation in the context
      // of upgrade tasks since they run on only one machine.
      adapter.initializeRepo("main");

      // Create a transient upgrade branch to the HEAD of main
      ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);
      BranchName upgradeBranch = BranchName.of(upgradeBranchName);
      Hash upgradeStartHash = main.getHash();
      try {
        ReferenceInfo<ByteString> branchInfo = adapter.namedRef(upgradeBranchName, GetNamedRefsParams.DEFAULT);
        AdminLogger.log("Resetting old upgrade branch: " + branchInfo);
        adapter.assign(upgradeBranch, Optional.empty(), upgradeStartHash);
      } catch (Exception e1) {
        // Create a new upgrade branch
        try {
          adapter.create(upgradeBranch, upgradeStartHash);
        } catch (Exception e2) {
          IllegalStateException ex = new IllegalStateException("Unable to create upgrade branch: " + upgradeBranchName, e2);
          ex.addSuppressed(e1);
          throw ex;
        }
      }

      final AtomicReference<ImmutableCommitAttempt.Builder> commit = new AtomicReference<>();
      final AtomicInteger numEntries = new AtomicInteger();

      reader.doUpgrade((branchName, contentKey, location) -> {
        // Embedded Nessie use cases that kept data in KVStores used only the "main" branch
        if (!"main".equals(branchName)) {
          throw new IllegalStateException("Found unexpected branch: " + branchName);
        }

        if (commit.get() == null) {
          commit.set(ImmutableCommitAttempt.builder()
            .commitToBranch(upgradeBranch)
            .commitMetaSerialized(
              worker.getMetadataSerializer().toBytes(
                CommitMeta.builder()
                  .message("Upgrade")
                  .author("MigrateToNessieAdapter")
                  .authorTime(Instant.now())
                  .build()
              )));
        }

        // Note: old embedded Nessie data does not contain content ID or Iceberg snapshots and other IDs.
        // Note: clients of Embedded Nessie do not use those IDs in versions where this upgrade step is relevant
        // (i.e. Dremio Software versions 18, 19, 20, 21), so use zeros.
        // If / when Embedded Nessie clients start using those IDs retrieved from the Nessie Server, the data
        // will already be managed by a Nessie DatabaseAdapter, so no low-level upgrade tasks will be required
        // at that time (hopefully).
        IcebergTable table = IcebergTable.of(location, 0, 0, 0, 0);

        ContentId contentId = ContentId.of(worker.getId(table));
        commit.get().putGlobal(contentId, worker.toStoreGlobalState(table));
        commit.get().addPuts(
          KeyWithBytes.of(
            Key.of(contentKey.toArray(new String[0])),
            contentId,
            worker.getPayload(table),
            worker.toStoreOnReferenceState(table)));

        if (numEntries.incrementAndGet() >= MAX_ENTRIES_PER_COMMIT) {
          commit(adapter, commit.get(), numEntries.get());
          numEntries.set(0);
        }
      });

      commit(adapter, commit.get(), numEntries.get());

      // Tag old `main` branch
      TagName oldMain = TagName.of("main-before-upgrade-" + getTaskUUID());
      adapter.create(oldMain, main.getHash());

      // Reset `main` to the head of the upgraded commit chain
      ReferenceInfo<ByteString> upgradedHead = adapter.namedRef(upgradeBranch.getName(), GetNamedRefsParams.DEFAULT);
      adapter.delete(main.getNamedRef(), Optional.of(main.getHash()));
      adapter.create(main.getNamedRef(), upgradedHead.getHash());

      // Delete the transient upgrade branch
      adapter.delete(upgradeBranch, Optional.of(upgradedHead.getHash()));
    }
  }

  private void commit(DatabaseAdapter adapter, ImmutableCommitAttempt.Builder commit, int numEntries) {
    if (numEntries <= 0) {
      return;
    }

    try {
      Hash hash = adapter.commit(commit.build());
      AdminLogger.log("Committed " + numEntries + " migrated tables as {}", hash);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
