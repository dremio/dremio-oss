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
  private KVStore<String, byte[]> globalLog;
  private KVStore<String, byte[]> commitLog;
  private KVStore<String, byte[]> keyList;
  private KVStore<String, byte[]> refLog;

  private DatastoreDbConfig config;

  private final ReadWriteLock lock = new StampedLock().asReadWriteLock();

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
    globalLog = kvStoreProvider.get().getStore(NessieGlobalLogStoreBuilder.class);
    commitLog = kvStoreProvider.get().getStore(NessieCommitLogStoreBuilder.class);
    keyList = kvStoreProvider.get().getStore(NessieKeyListStoreBuilder.class);
    refLog = kvStoreProvider.get().getStore(NessieRefLogStoreBuilder.class);
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

  public KVStore<String, byte[]> getKeyList() {
    return keyList;
  }

  public KVStore<String, byte[]> getRefLog() {
    return refLog;
  }

  public ReadWriteLock getLock() {
    return lock;
  }
}
