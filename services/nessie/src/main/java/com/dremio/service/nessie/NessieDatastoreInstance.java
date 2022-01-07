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

import com.dremio.datastore.api.KVStore;
import com.dremio.datastore.api.KVStoreProvider;

/**
 * Creates KV stores for Nessie
 */
public class NessieDatastoreInstance {

  private final KVStore<String, byte[]> globalPointer;
  private final KVStore<String, byte[]> globalLog;
  private final KVStore<String, byte[]> commitLog;
  private final KVStore<String, byte[]> keyList;

  private final ReadWriteLock lock = new StampedLock().asReadWriteLock();

  public NessieDatastoreInstance(Provider<KVStoreProvider> kvStoreProvider) {
    globalPointer = kvStoreProvider.get().getStore(NessieGlobalPointerStoreBuilder.class);
    globalLog = kvStoreProvider.get().getStore(NessieGlobalLogStoreBuilder.class);
    commitLog = kvStoreProvider.get().getStore(NessieCommitLogStoreBuilder.class);
    keyList = kvStoreProvider.get().getStore(NessieKeyListStoreBuilder.class);
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

  public ReadWriteLock getLock() {
    return lock;
  }
}
