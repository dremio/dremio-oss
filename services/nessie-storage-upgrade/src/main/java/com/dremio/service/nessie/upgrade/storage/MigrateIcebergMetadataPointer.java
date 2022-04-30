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

import static com.dremio.service.nessie.upgrade.storage.MigrateToNessieAdapter.MAX_ENTRIES_PER_COMMIT;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.TableCommitMetaStoreWorker;
import org.projectnessie.store.ObjectTypes;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitAttempt;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.adapter.KeyWithType;

import com.dremio.dac.cmd.AdminLogger;
import com.dremio.dac.cmd.upgrade.UpgradeContext;
import com.dremio.dac.cmd.upgrade.UpgradeTask;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.service.nessie.DatastoreDatabaseAdapterFactory;
import com.dremio.service.nessie.ImmutableDatastoreDbConfig;
import com.dremio.service.nessie.NessieDatastoreInstance;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Migrates legacy on-reference state entries using the {@code ICEBERG_METADATA_POINTER} type to current format.
 */
public class MigrateIcebergMetadataPointer extends UpgradeTask {

  public static final String TASK_ID = "1a9ab6a1-c919-4c23-b6af-2119aa0a00b4";

  private static final int BATCH_SIZE = Integer.getInteger("nessie.upgrade.batch_size", 1000);

  public MigrateIcebergMetadataPointer() {
    super("Migrate ICEBERG_METADATA_POINTER in Nessie Data to current format", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return TASK_ID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    upgrade(context.getKvStoreProvider(), BATCH_SIZE, "upgrade-" + getTaskUUID());
  }

  @VisibleForTesting
  void upgrade(KVStoreProvider kvStoreProvider, int batchSize, String branchName) throws Exception {
    if (batchSize <= 0) {
      throw new IllegalArgumentException("Invalid batch size: " + batchSize);
    }

    try (NessieDatastoreInstance store = new NessieDatastoreInstance()) {
      store.configure(new ImmutableDatastoreDbConfig.Builder()
        .setStoreProvider(() -> kvStoreProvider)
        .build());
      store.initialize();

      DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder().withConnector(store).build();

      // Ensure the Embedded Nessie repo is initialized. This is an idempotent operation in the context
      // of upgrade tasks since they run on only one machine.
      adapter.initializeRepo("main");

      // Legacy data was stored only on `main`, so migrate data only on this branch.
      ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);

      Converter converter = new Converter(adapter, main.getHash(), batchSize);
      if (!converter.upgradeRequired()) {
        AdminLogger.log("No ICEBERG_METADATA_POINTER entries found. Nessie data was not changed.");
        return;
      }

      // Write upgraded data into a fresh rooted at noAncestorHash since history is not introspected
      // in the Embedded Nessie at the time this upgrade is necessary.
      BranchName upgradeBranch = BranchName.of(branchName);
      Hash upgradeStartHash = adapter.noAncestorHash();
      try {
        ReferenceInfo<ByteString> branchInfo = adapter.namedRef(branchName, GetNamedRefsParams.DEFAULT);
        AdminLogger.log("Resetting old upgrade branch: " + branchInfo);
        adapter.assign(upgradeBranch, Optional.empty(), upgradeStartHash);
      } catch (Exception e1) {
        // Create a new upgrade branch
        try {
          upgradeStartHash = adapter.create(upgradeBranch, upgradeStartHash);
        } catch (Exception e2) {
          IllegalStateException ex = new IllegalStateException("Unable to create upgrade branch: " + branchName, e2);
          ex.addSuppressed(e1);
          throw ex;
        }
      }

      Hash upgradedHead = converter.upgrade(upgradeBranch, upgradeStartHash);

      // Tag old `main` branch
      TagName oldMain = TagName.of("main-before-upgrade-" + getTaskUUID());
      adapter.create(oldMain, main.getHash());

      // Reset `main` to the head of the upgraded commit chain
      adapter.delete(main.getNamedRef(), Optional.of(main.getHash()));
      adapter.create(main.getNamedRef(), upgradedHead);

      // Delete the transient upgrade branch
      adapter.delete(upgradeBranch, Optional.of(upgradedHead));
    }
  }

  private static final class Converter {
    private final TableCommitMetaStoreWorker worker = new TableCommitMetaStoreWorker();
    private final DatabaseAdapter adapter;
    private final Hash sourceBranch;
    private final int batchSize;

    private BranchName targetBranch;
    private ImmutableCommitAttempt.Builder commit;
    private Hash head;
    private int numEntries;
    private int totalEntries;
    private int numCommits;

    private Converter(DatabaseAdapter adapter, Hash sourceBranch, int batchSize) {
      this.adapter = adapter;
      this.sourceBranch = sourceBranch;
      this.batchSize = batchSize;
    }

    private void reset() {
      numEntries = 0;
      commit = ImmutableCommitAttempt.builder()
        .commitToBranch(targetBranch)
        .expectedHead(Optional.of(head))
        .commitMetaSerialized(
          worker.getMetadataSerializer().toBytes(
            CommitMeta.builder()
              .message("Upgrade ICEBERG_METADATA_POINTER #" + numCommits)
              .author("MigrateIcebergMetadataPointer")
              .authorTime(Instant.now())
              .build()
          ));
    }

    private void drain() {
      if (numEntries <= 0) {
        return;
      }

      try {
        head = adapter.commit(commit.build());
        numCommits++;
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }

      AdminLogger.log("Committed " + numEntries + " migrated tables as {}", head);

      totalEntries += numEntries;
      reset();
    }

    private void processKey(Map<Key, KeyWithType> keys,
                            KeyWithType kt,
                            BiConsumer<KeyWithType, ContentAndState<ByteString>> action) {
      if (keys == null) {
        keys = new HashMap<>();
      }

      keys.put(kt.getKey(), kt);

      if (keys.size() >= batchSize) {
        drainKeys(keys, action);
      }
    }

    private void drainKeys(Map<Key, KeyWithType> keys, BiConsumer<KeyWithType, ContentAndState<ByteString>> action) {
      if (keys.isEmpty()) {
        return;
      }

      AdminLogger.log("Processing a batch of {} keys", keys.size());

      Map<Key, ContentAndState<ByteString>> values;
      try {
        values = adapter.values(sourceBranch, keys.keySet(), KeyFilterPredicate.ALLOW_ALL);
      } catch (ReferenceNotFoundException e) {
        throw new IllegalStateException(e);
      }

      for (Map.Entry<Key, ContentAndState<ByteString>> e : values.entrySet()) {
        KeyWithType kt = keys.get(e.getKey());
        if (kt == null) {
          throw new IllegalStateException("Unknown key: " + e.getKey());
        }

        action.accept(kt, e.getValue());
      }

      keys.clear();
    }

    private void processValues(BiConsumer<KeyWithType, ContentAndState<ByteString>> action)
      throws ReferenceNotFoundException {

      Map<Key, KeyWithType> batch = new HashMap<>();
      try (Stream<KeyWithType> stream = adapter.keys(sourceBranch, KeyFilterPredicate.ALLOW_ALL)) {
        stream.forEach(kt -> processKey(batch, kt, action));
      }
      drainKeys(batch, action);
    }

    private boolean upgradeRequired() throws ReferenceNotFoundException {
      AtomicLong legacy = new AtomicLong();
      AtomicLong current = new AtomicLong();

      processValues((kt, value) -> {
        ObjectTypes.Content.ObjectTypeCase refType = parseContent(value.getRefState()).getObjectTypeCase();
        if (refType == ObjectTypes.Content.ObjectTypeCase.ICEBERG_METADATA_POINTER) {
          legacy.incrementAndGet();
          AdminLogger.log("Key {} refers to a legacy entry", kt.getKey());
        } else {
          current.incrementAndGet();
          AdminLogger.log("Key {} refers to an entry in current format", kt.getKey());
        }
      });

      AdminLogger.log("Found {} legacy and {} contemporary entries.", legacy.get(), current.get());

      return legacy.get() > 0;
    }

    private Hash upgrade(BranchName upgradeBranch, Hash upgradeStartHash) throws ReferenceNotFoundException {
      totalEntries = 0;
      head = upgradeStartHash;
      targetBranch = upgradeBranch;
      reset();

      processValues(this::upgradeValue);
      drain(); // commit remaining entries

      AdminLogger.log("Processed {} entries.", totalEntries);
      return head;
    }

    private ObjectTypes.Content parseContent(ByteString content) {
      try {
        return ObjectTypes.Content.parseFrom(content);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }

    private void upgradeValue(KeyWithType kt, ContentAndState<ByteString> value) {
      ObjectTypes.Content refState = parseContent(value.getRefState());
      ObjectTypes.Content.ObjectTypeCase refType = refState.getObjectTypeCase();

      if (refType == ObjectTypes.Content.ObjectTypeCase.ICEBERG_METADATA_POINTER) {
        ObjectTypes.IcebergMetadataPointer pointer = refState.getIcebergMetadataPointer();
        String metadataLocation = pointer.getMetadataLocation();

        AdminLogger.log("Migrating old entry for table {}, metadata: {}", kt.getKey(), metadataLocation);

        // Note: old embedded Nessie data does not contain Iceberg snapshots and other IDs, so use zeros
        IcebergTable table = IcebergTable.of(metadataLocation, 0, 0, 0, 0, refState.getId());

        ContentId contentId = ContentId.of(worker.getId(table));
        commit.putGlobal(contentId, worker.toStoreGlobalState(table));
        commit.addPuts(
          KeyWithBytes.of(
            kt.getKey(),
            contentId,
            worker.getPayload(table),
            worker.toStoreOnReferenceState(table)));

      } else {
        // This case is not expected during actual upgrades. It is handled only for the sake of completeness.
        AdminLogger.log("Keeping current entry for table {}", kt.getKey());

        ContentId contentId = kt.getContentId();

        if (value.getGlobalState() != null) {
          commit.putGlobal(contentId, value.getGlobalState());
        }

        commit.addPuts(KeyWithBytes.of(kt.getKey(), contentId, kt.getType(), value.getRefState()));
      }

      if (++numEntries >= MAX_ENTRIES_PER_COMMIT) {
        drain();
      }
    }
  }
}
