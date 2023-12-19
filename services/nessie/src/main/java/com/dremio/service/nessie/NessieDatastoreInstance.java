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

import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.StampedLock;

import javax.inject.Provider;

import org.projectnessie.versioned.persist.adapter.DatabaseConnectionProvider;

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;

/**
 * Creates KV stores for Nessie
 */
public class NessieDatastoreInstance implements DatabaseConnectionProvider<DatastoreDbConfig> {

  private KVStore<String, byte[]> repoDescription;
  private KVStore<String, byte[]> globalPointer;
  private KVStore<String, byte[]> namedRefHeads;
  private KVStore<String, byte[]> refNames;
  private KVStore<String, byte[]> globalLog;
  private KVStore<String, byte[]> commitLog;
  private KVStore<String, byte[]> keyList;

  private DatastoreDbConfig config;

  private final ReadWriteLock lock = new StampedLock().asReadWriteLock();

  // Embedded Nessie commits generally target the main branch (aside from specialized upgrade use
  // cases). In case two or more commit attempts run concurrently, only one will win the optimistic
  // lock (CAS) at the time of writing to storage, and the others will have to refresh the HEAD of
  // the main branch and re-build the commit data. Therefore, it makes sense to allow at most one
  // thread to attempt committing at any time to avoid wasting the efforts for preparing commit
  // objects that would be discarded during re-tries.
  private final Semaphore commitSemaphore = new Semaphore(1, true);

  public NessieDatastoreInstance() {
  }

  @Override
  public void configure(DatastoreDbConfig config) {
    this.config = config;
  }

  @Override
  public void initialize() {
    Provider<KVStoreProvider> kvStoreProvider = config.getStoreProvider();
    repoDescription = kvStoreProvider.get().getStore(NessieRepoDescriptionStoreBuilder.class);
    globalPointer = kvStoreProvider.get().getStore(NessieGlobalPointerStoreBuilder.class);
    namedRefHeads = kvStoreProvider.get().getStore(NessieNamedRefHeadsStoreBuilder.class);
    refNames = kvStoreProvider.get().getStore(NessieRefNamesStoreBuilder.class);
    globalLog = kvStoreProvider.get().getStore(NessieGlobalLogStoreBuilder.class);
    commitLog = kvStoreProvider.get().getStore(NessieCommitLogStoreBuilder.class);
    keyList = kvStoreProvider.get().getStore(NessieKeyListStoreBuilder.class);
  }

  @Override
  public void close() throws Exception {
  }

  public KVStore<String, byte[]> getRepoDescription() {
    return repoDescription;
  }

  public KVStore<String, byte[]> getCommitLog() {
    return commitLog;
  }

  public KVStore<String, byte[]> getGlobalLog() {
    return globalLog;
  }

  public KVStore<String, byte[]> getGlobalPointer() {
    return globalPointer;
  }

  public KVStore<String, byte[]> getNamedRefHeads() {
    return namedRefHeads;
  }

  public KVStore<String, byte[]> getRefNames() {
    return refNames;
  }

  public KVStore<String, byte[]> getKeyList() {
    return keyList;
  }

  public ReadWriteLock getLock() {
    return lock;
  }

  public Semaphore getCommitSemaphore() {
    return commitSemaphore;
  }
}
