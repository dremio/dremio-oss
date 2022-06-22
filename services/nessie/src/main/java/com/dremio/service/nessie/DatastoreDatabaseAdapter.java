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
package com.dremio.service.nessie;

import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToCommitLogEntry;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToKeyList;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRefLog;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.protoToRepoDescription;
import static org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization.toProto;
import static org.projectnessie.versioned.persist.adapter.spi.DatabaseAdapterUtil.hashCollisionDetected;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.projectnessie.versioned.Hash;
import org.projectnessie.versioned.ReferenceConflictException;
import org.projectnessie.versioned.StoreWorker;
import org.projectnessie.versioned.persist.adapter.CommitLogEntry;
import org.projectnessie.versioned.persist.adapter.CommitParams;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapter;
import org.projectnessie.versioned.persist.adapter.KeyListEntity;
import org.projectnessie.versioned.persist.adapter.KeyListEntry;
import org.projectnessie.versioned.persist.adapter.RefLog;
import org.projectnessie.versioned.persist.adapter.RepoDescription;
import org.projectnessie.versioned.persist.adapter.serialize.ProtoSerialization;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapter;
import org.projectnessie.versioned.persist.nontx.NonTransactionalDatabaseAdapterConfig;
import org.projectnessie.versioned.persist.nontx.NonTransactionalOperationContext;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStateLogEntry;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.GlobalStatePointer;
import org.projectnessie.versioned.persist.serialize.AdapterTypes.RefLogEntry;

import com.dremio.datastore.api.Document;
import com.dremio.datastore.api.KVStore;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Datastore Database Adapter for Nessie
 */
public class DatastoreDatabaseAdapter extends NonTransactionalDatabaseAdapter<NonTransactionalDatabaseAdapterConfig> {

  private final NessieDatastoreInstance db;
  private final String keyPrefix;
  private final String globalPointerKey;

  protected DatastoreDatabaseAdapter(NonTransactionalDatabaseAdapterConfig config,
    NessieDatastoreInstance dbInstance,
    StoreWorker<?, ?, ?> storeWorker) {
    super(config, storeWorker);
    Objects.requireNonNull(dbInstance);
    this.db = dbInstance;
    this.keyPrefix = config.getRepositoryId();
    this.globalPointerKey = keyPrefix;
  }

  private String dbKey(Hash hash) {
    return keyPrefix.concat(hash.asString());
  }

  private String dbKey(ByteString key) {
    return dbKey(Hash.of(key));
  }

  /**
   * Calculate the expected size of the given {@link CommitLogEntry} in the database.
   *
   * @param entry
   */
  @Override
  protected int entitySize(CommitLogEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  /**
   * Calculate the expected size of the given {@link KeyListEntry} in the database.
   *
   * @param entry
   */
  @Override
  protected int entitySize(KeyListEntry entry) {
    return toProto(entry).getSerializedSize();
  }

  /**
   * Load the current global-state-pointer.
   *
   * @param ctx
   * @return the current global points if set, or {@code null} if not set.
   */
  @Override
  protected GlobalStatePointer doFetchGlobalPointer(NonTransactionalOperationContext ctx) {
    try {
      Document<String, byte[]> serialized = db.getGlobalPointer().get(globalPointerKey);
      return serialized != null ? GlobalStatePointer.parseFrom(serialized.getValue()) : null;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Write a new commit-entry, the given commit entry is to be persisted as is. All values of the
   * given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Implementations however can enforce strict consistency checks/guarantees, like a best-effort
   * approach to prevent hash-collisions but without any other consistency checks/guarantees.
   *
   * @param ctx
   * @param entry
   */
  @Override
  protected void doWriteIndividualCommit(NonTransactionalOperationContext ctx, CommitLogEntry entry)
    throws ReferenceConflictException {
    Lock lock = db.getLock().writeLock();
    lock.lock();

    try {
      String key = dbKey(entry.getHash());
      Document<String, byte[]> commitEntry = db.getCommitLog().get(key);
      if (commitEntry != null) {
        throw hashCollisionDetected();
      } else {
        db.getCommitLog().put(key, toProto(entry).toByteArray(), KVStore.PutOption.CREATE);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Write multiple new commit-entries, the given commit entries are to be persisted as is. All
   * values of the * given {@link CommitLogEntry} can be considered valid and consistent.
   *
   * <p>Implementations however can enforce strict consistency checks/guarantees, like a best-effort
   * approach to prevent hash-collisions but without any other consistency checks/guarantees.
   *
   * @param ctx
   * @param entries
   */
  @Override
  protected void doWriteMultipleCommits(NonTransactionalOperationContext ctx, List<CommitLogEntry> entries)
    throws ReferenceConflictException {
    Lock lock = db.getLock().writeLock();
    lock.lock();

    try {
      for (CommitLogEntry e : entries) {
        db.getCommitLog().put(dbKey(e.getHash()), toProto(e).toByteArray());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Write a new global-state-log-entry with a best-effort approach to prevent hash-collisions but
   * without any other consistency checks/guarantees. Some implementations however can enforce
   * strict consistency checks/guarantees.
   *
   * @param ctx
   * @param entry
   */
  @Override
  protected void doWriteGlobalCommit(NonTransactionalOperationContext ctx, GlobalStateLogEntry entry)
    throws ReferenceConflictException {
    Lock lock = db.getLock().writeLock();
    lock.lock();
    try {
      String key = dbKey(entry.getId());
      Document<String, byte[]> commitEntry = db.getGlobalLog().get(key);
      if (commitEntry != null) {
        throw hashCollisionDetected();
      }
      db.getGlobalLog().put(key, entry.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Unsafe operation to initialize a repository: unconditionally writes the global-state-pointer.
   *
   * @param ctx
   * @param pointer
   */
  @Override
  protected void unsafeWriteGlobalPointer(NonTransactionalOperationContext ctx, GlobalStatePointer pointer) {
    try {
      db.getGlobalPointer().put(globalPointerKey, pointer.toByteArray());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Atomically update the global-commit-pointer to the given new-global-head, if the value in the
   * database is the given expected-global-head.
   *
   * @param ctx
   * @param expected
   * @param newPointer
   */
  @Override
  protected boolean doGlobalPointerCas(NonTransactionalOperationContext ctx,
                                       GlobalStatePointer expected,
                                       GlobalStatePointer newPointer) {
    Lock lock = db.getLock().writeLock();
    lock.lock();
    try {
      Document<String, byte[]> bytes = db.getGlobalPointer().get(globalPointerKey);
      GlobalStatePointer oldPointer = bytes == null ? null : GlobalStatePointer.parseFrom(bytes.getValue());
      if (oldPointer == null || !oldPointer.getGlobalId().equals(expected.getGlobalId())) {
        return false;
      }
      db.getGlobalPointer().put(globalPointerKey, newPointer.toByteArray());
      return true;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * If a {@link #globalPointerCas(NonTransactionalOperationContext, GlobalStatePointer,
   * GlobalStatePointer)} failed, {@link
   * DatabaseAdapter#commit(CommitParams)} calls this
   * function to remove the optimistically written data.
   *
   * <p>Implementation notes: non-transactional implementations <em>must</em> delete entries for the
   * given keys, no-op for transactional implementations.
   *
   * @param ctx
   * @param globalId
   * @param branchCommits
   * @param newKeyLists
   */
  @Override
  protected void doCleanUpCommitCas(NonTransactionalOperationContext ctx, Optional<Hash> globalId,
                                  Set<Hash> branchCommits, Set<Hash> newKeyLists, Hash refLogId) {
    Lock lock = db.getLock().writeLock();
    lock.lock();
    try {
      globalId.ifPresent(hash -> db.getGlobalLog().delete(dbKey(hash)));
      for (Hash h : branchCommits) {
        db.getCommitLog().delete(dbKey(h));
      }
      for (Hash h : newKeyLists) {
        db.getKeyList().delete(dbKey(h));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void doCleanUpGlobalLog(NonTransactionalOperationContext ctx, Collection<Hash> globalIds) {
    Lock lock = db.getLock().writeLock();
    lock.lock();
    try {
      for (Hash h : globalIds) {
        db.getGlobalLog().delete(dbKey(h));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Load the global-log entry with the given id.
   *
   * @param ctx
   * @param id
   * @return the loaded entry if it is available, {@code null} if it does not exist.
   */
  @Override
  protected GlobalStateLogEntry doFetchFromGlobalLog(NonTransactionalOperationContext ctx, Hash id) {
    try {
      Document<String, byte[]> entry = db.getGlobalLog().get(dbKey(id));
      return entry != null ? GlobalStateLogEntry.parseFrom(entry.getValue()) : null;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Load the commit-log entry for the given hash, return {@code null}, if not found.
   *
   * @param ctx
   * @param hash
   */
  @Override
  protected CommitLogEntry doFetchFromCommitLog(NonTransactionalOperationContext ctx, Hash hash) {
    try {
      Document<String, byte[]> entry = db.getCommitLog().get(dbKey(hash));
      return protoToCommitLogEntry(entry != null ? entry.getValue() : null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetch multiple {@link CommitLogEntry commit-log-entries} from the commit-log. The returned list
   * must have exactly as many elements as in the parameter {@code hashes}. Non-existing hashes are
   * returned as {@code null}.
   *
   * @param ctx
   * @param hashes
   */
  @Override
  protected List<CommitLogEntry> doFetchMultipleFromCommitLog(NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(db.getCommitLog(), hashes, ProtoSerialization::protoToCommitLogEntry);
  }

  @Override
  protected List<GlobalStateLogEntry> doFetchPageFromGlobalLog(NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(db.getGlobalLog(),
      hashes,
      v -> {
        try {
          return v != null ? GlobalStateLogEntry.parseFrom(v) : null;
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      });
  }

  private <T> List<T> fetchPage(KVStore<String, byte[]> store, List<Hash> hashes, Function<byte[], T> deserializer) {
    try {
      List<String> keys = hashes.stream().map(this::dbKey).collect(Collectors.toList());
      Iterable<Document<String, byte[]>> iterable = store.get(keys);
      List<byte[]> result = new ArrayList<>();
      for (Document<String, byte[]> doc : iterable) {
        result.add(doc != null ? doc.getValue() : null);
      }
      return result.stream().map(deserializer).collect(Collectors.toList());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doWriteKeyListEntities(NonTransactionalOperationContext ctx, List<KeyListEntity> newKeyListEntities) {
    Lock lock = db.getLock().writeLock();
    lock.lock();
    try {
      for (KeyListEntity keyListEntity : newKeyListEntities) {
        db.getKeyList().put(dbKey(keyListEntity.getId()), toProto(keyListEntity.getKeys()).toByteArray());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected RefLog doFetchFromRefLog(
    NonTransactionalOperationContext ctx, Hash refLogId) {
    try {
      Document<String, byte[]> entry = db.getRefLog().get(dbKey(refLogId));
      return protoToRefLog(entry != null ? entry.getValue() : null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected List<RefLog> doFetchPageFromRefLog(
    NonTransactionalOperationContext ctx, List<Hash> hashes) {
    return fetchPage(db.getRefLog(), hashes, ProtoSerialization::protoToRefLog);
  }

  @Override
  protected void doWriteRefLog(
    NonTransactionalOperationContext ctx,
    RefLogEntry entry) throws ReferenceConflictException {
    Lock lock = db.getLock().writeLock();
    lock.lock();

    try {
      String key = dbKey(entry.getRefLogId());
      Document<String, byte[]> commitEntry = db.getCommitLog().get(key);
      if (commitEntry != null) {
        throw hashCollisionDetected();
      } else {
        db.getRefLog().put(key, entry.toByteArray(), KVStore.PutOption.CREATE);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected Stream<KeyListEntity> doFetchKeyLists(NonTransactionalOperationContext ctx, List<Hash> keyListsIds) {
    try {
      KVStore<String, byte[]> store = db.getKeyList();
      List<String> keys = keyListsIds.stream().map(this::dbKey).collect(Collectors.toList());
      Iterable<Document<String, byte[]>> entries = store.get(keys);
      Iterator<Document<String, byte[]>> iterator = entries.iterator();
      return IntStream.range(0, keyListsIds.size())
        .mapToObj(
          i -> {
            Document<String, byte[]> doc = iterator.next();
            return KeyListEntity.of(keyListsIds.get(i), protoToKeyList(doc != null ? doc.getValue() : null));
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void eraseRepo() {
    try {
      Stream.of(
          db.getGlobalPointer(),
          db.getGlobalLog(),
          db.getCommitLog(),
          db.getRepoDescription(),
          db.getKeyList(),
          db.getRefLog())
        .forEach(
          cf -> {
            List<String> deletes = new ArrayList<>();
            for (Document<String, byte[]> doc : cf.find()) {
              if (doc.getKey().startsWith(keyPrefix)) {
                deletes.add(doc.getKey());
              }
            }
            deletes.forEach(cf::delete);
          }
        );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected RepoDescription doFetchRepositoryDescription(NonTransactionalOperationContext ctx) {
    try {
      Document<String, byte[]> entry = db.getRepoDescription().get(globalPointerKey);
      return entry != null ? protoToRepoDescription(entry.getValue()) : null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected boolean doTryUpdateRepositoryDescription(
    NonTransactionalOperationContext ctx, RepoDescription expected, RepoDescription updateTo) {
    Lock lock = db.getLock().writeLock();
    lock.lock();
    try {
      Document<String, byte[]> entry = db.getRepoDescription().get(globalPointerKey);
      byte[] bytes = entry != null ? entry.getValue() : null;
      byte[] updatedBytes = toProto(updateTo).toByteArray();
      if ((bytes == null && expected == null)
        || (bytes != null && Arrays.equals(bytes, toProto(expected).toByteArray()))) {
        db.getRepoDescription().put(globalPointerKey, updatedBytes);
        return true;
      }
      return false;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }
}
