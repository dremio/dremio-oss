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
import static java.util.Collections.singletonList;
import static org.projectnessie.versioned.CommitMetaSerializer.METADATA_SERIALIZER;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.takeUntilExcludeLast;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.server.store.proto.ObjectTypes;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.Key;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.ReferenceNotFoundException;
import org.projectnessie.versioned.TagName;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.ContentAndState;
import org.projectnessie.versioned.persist.adapter.ContentId;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.ImmutableCommitParams;
import org.projectnessie.versioned.persist.adapter.KeyFilterPredicate;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.KeyWithBytes;
import org.projectnessie.versioned.persist.nontx.ImmutableAdjustableNonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.store.DefaultStoreWorker;

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

  public MigrateIcebergMetadataPointer() {
    super("Migrate ICEBERG_METADATA_POINTER in Nessie Data to current format", Collections.emptyList());
  }

  @Override
  public String getTaskUUID() {
    return TASK_ID;
  }

  @Override
  public void upgrade(UpgradeContext context) throws Exception {
    upgrade(context.getKvStoreProvider(), "upgrade-" + getTaskUUID());
  }

  @VisibleForTesting
  void upgrade(KVStoreProvider kvStoreProvider, String branchName) throws Exception {
    try (NessieDatastoreInstance store = new NessieDatastoreInstance()) {
      store.configure(new ImmutableDatastoreDbConfig.Builder()
        .setStoreProvider(() -> kvStoreProvider)
        .build());
      store.initialize();

      DatabaseAdapter adapter = new DatastoreDatabaseAdapterFactory().newBuilder()
        .withConnector(store)
        .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder()
          // Suppress periodic key list generation by the DatabaseAdapter. We'll do that once at the end of the upgrade.
          .keyListDistance(Integer.MAX_VALUE)
          .build())
        .build();

      // Ensure the Embedded Nessie repo is initialized. This is an idempotent operation in the context
      // of upgrade tasks since they run on only one machine.
      adapter.initializeRepo("main");

      // Legacy data was stored only on `main`, so migrate data only on this branch.
      ReferenceInfo<ByteString> main = adapter.namedRef("main", GetNamedRefsParams.DEFAULT);

      // The upgrade is performed in two phases:
      // 1) Scan the commit log to find ICEBERG_METADATA_POINTER entries. Those entries can only exist in
      // on-ref state (i.e. directly in commit log "put" operations) because they were made by Nessie versions
      // before the global state was introduced.
      // 2) Scan the commit log again and convert ICEBERG_METADATA_POINTER entries to contemporary format.
      // The upgrade could probably be done in one pass, but we keep the two pass approach to absolutely avoid
      // making unnecessary changes during upgrades.
      // Note: the previous approach of using DatabaseAdapter.values(...) to load the data in batches of N keys
      // still showed suboptimal performance because in Nessie 0.22.0 (used in Dremio v. 20.*) the adapter did not
      // store relevant commit IDs in key lists (cf. Nessie OSS PR#3592), which resulted in re-scanning the commit
      // log for every values(...) call.
      Converter converter = new Converter(adapter, main.getHash());
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

      converter.upgrade(upgradeBranch, upgradeStartHash);

      // Use a fresh adapter instance with key list distance of 1 to force key list generation
      DatabaseAdapter adapter1 = new DatastoreDatabaseAdapterFactory().newBuilder()
        .withConnector(store)
        .withConfig(ImmutableAdjustableNonTransactionalDatabaseAdapterConfig.builder()
          .keyListDistance(1)
          .build())
        .build();

      ImmutableCommitParams keyListCommit = ImmutableCommitParams.builder()
        .toBranch(upgradeBranch)
        .commitMetaSerialized(
          METADATA_SERIALIZER.toBytes(
            CommitMeta.builder()
              .message("Upgrade - Generate key list")
              .author("MigrateIcebergMetadataPointer")
              .authorTime(Instant.now())
              .build()
          ))
        .build();

      // Make an empty commit to force key list computation in the adapter
      Hash upgradedHead = adapter1.commit(keyListCommit);
      AdminLogger.log("Committed post-upgrade key list as {}", upgradedHead);

      // Tag old `main` branch
      TagName oldMain = TagName.of("main-before-upgrade-" + getTaskUUID());
      adapter.create(oldMain, main.getHash());

      // Reset `main` to the head of the upgraded commit chain
      adapter.assign(main.getNamedRef(), Optional.of(main.getHash()), upgradedHead);

      // Delete the transient upgrade branch
      adapter.delete(upgradeBranch, Optional.of(upgradedHead));
    }
  }

  private static final class Converter {
    private final DatabaseAdapter adapter;
    private final Hash sourceBranch;
    private final Set<Key> activeKeys = new HashSet<>();

    private BranchName targetBranch;
    private ImmutableCommitParams.Builder commit;
    private Hash head;
    private int numEntries;
    private int totalEntries;
    private int numCommits;

    private Converter(DatabaseAdapter adapter, Hash sourceBranch) {
      this.adapter = adapter;
      this.sourceBranch = sourceBranch;

      // Load all active keys into memory to allow a simple sequential scan of the commit log during later upgrade steps.
      try(Stream<KeyListEntry> keys = adapter.keys(sourceBranch, KeyFilterPredicate.ALLOW_ALL)) {
        keys.forEach(k -> activeKeys.add(k.getKey()));
      } catch (ReferenceNotFoundException e) {
        throw new IllegalArgumentException(e);
      }

      AdminLogger.log("Found {} active keys", activeKeys.size());
    }

    private void reset() {
      numEntries = 0;
      commit = ImmutableCommitParams.builder()
        .toBranch(targetBranch)
        .expectedHead(Optional.of(head))
        .commitMetaSerialized(
          METADATA_SERIALIZER.toBytes(
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

    private void processValues(BiConsumer<CommitLogEntry, KeyWithBytes> action) {
      Set<Key> keysToProcess = new HashSet<>(activeKeys);
      try (Stream<CommitLogEntry> log = adapter.commitLog(sourceBranch);
           Stream<CommitLogEntry> commitLog = takeUntilExcludeLast(log, k -> keysToProcess.isEmpty())) {
        commitLog.forEach(entry -> entry.getPuts().forEach(put -> {
          if(keysToProcess.remove(put.getKey())) {
            action.accept(entry, put);
          }
        }));
      } catch (ReferenceNotFoundException e) {
        throw new IllegalArgumentException(e);
      }
    }

    private boolean upgradeRequired() {
      AtomicLong legacy = new AtomicLong();
      AtomicLong current = new AtomicLong();

      processValues((logEntry, keyWithBytes) -> {
        ObjectTypes.Content.ObjectTypeCase refType = parseContent(keyWithBytes.getValue()).getObjectTypeCase();

        if (refType == ObjectTypes.Content.ObjectTypeCase.ICEBERG_METADATA_POINTER) {
          legacy.incrementAndGet();
          AdminLogger.log("Key {} refers to a legacy entry", keyWithBytes.getKey());
        } else {
          current.incrementAndGet();
          AdminLogger.log("Key {} refers to an entry in current format", keyWithBytes.getKey());
        }
      });

      AdminLogger.log("Found {} legacy and {} contemporary entries.", legacy.get(), current.get());

      return legacy.get() > 0;
    }

    private void upgrade(BranchName upgradeBranch, Hash upgradeStartHash) {
      totalEntries = 0;
      head = upgradeStartHash;
      targetBranch = upgradeBranch;
      reset();

      processValues(this::upgradeValue);
      drain(); // commit remaining entries

      AdminLogger.log("Processed {} entries.", totalEntries);
    }

    private ObjectTypes.Content parseContent(ByteString content) {
      try {
        return ObjectTypes.Content.parseFrom(content);
      } catch (InvalidProtocolBufferException e) {
        throw new IllegalStateException(e);
      }
    }

    private void upgradeValue(CommitLogEntry logEntry, KeyWithBytes kb) {
      ObjectTypes.Content refState = parseContent(kb.getValue());
      ObjectTypes.Content.ObjectTypeCase refType = refState.getObjectTypeCase();

      if (refType == ObjectTypes.Content.ObjectTypeCase.ICEBERG_METADATA_POINTER) {
        ObjectTypes.IcebergMetadataPointer pointer = refState.getIcebergMetadataPointer();
        String metadataLocation = pointer.getMetadataLocation();

        AdminLogger.log("Migrating old entry for table {}, metadata: {}", kb.getKey(), metadataLocation);

        // Note: old embedded Nessie data does not contain Iceberg snapshots and other IDs, so use zeros
        IcebergTable table = IcebergTable.of(metadataLocation, 0, 0, 0, 0, refState.getId());

        ContentId contentId = ContentId.of(Objects.requireNonNull(table.getId()));
        commit.addPuts(
          KeyWithBytes.of(
            kb.getKey(),
            contentId,
            DefaultStoreWorker.payloadForContent(table),
            DefaultStoreWorker.instance().toStoreOnReferenceState(table, commit::addAttachments)));

      } else {
        // This case is not expected during actual upgrades. It is handled only for the sake of completeness.
        // So, we load global state individually for each key in expectation that this code path is not used often.
        AdminLogger.log("Keeping current entry for table {}", kb.getKey());

        ContentId contentId = kb.getContentId();

        ContentAndState value;
        try {
          value = adapter.values(logEntry.getHash(), singletonList(kb.getKey()), KeyFilterPredicate.ALLOW_ALL)
            .get(kb.getKey());

          if (value == null) {
            throw new IllegalStateException("Unable to load content for key: " + kb.getKey() + ", hash: " +
              logEntry.getHash());
          }
        } catch (ReferenceNotFoundException e) {
          throw new IllegalStateException(e);
        }

        if (value.getGlobalState() != null) {
          throw new IllegalStateException("Unexpected global state in value for key: " + kb.getKey());
        }

        commit.addPuts(KeyWithBytes.of(kb.getKey(), contentId, kb.getPayload(), value.getRefState()));
      }

      if (++numEntries >= MAX_ENTRIES_PER_COMMIT) {
        drain();
      }
    }
  }
}
