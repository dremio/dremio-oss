/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.datastore;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.rocksdb.WriteBatch;

import com.google.common.collect.Sets;

/**
 * Adapts {@link ReplayHandler} to {@link WriteBatch.Handler}.
 *
 * Note to developers: the operation overrides in this adapter need to be in sync with the operations on the
 * {@link RocksDBStore#db underlying RocksDB's store} (and {@link ByteStoreManager#db}). Currently only {@link #put},
 * {@link #delete} and {@link #singleDelete} are supported.
 */
class ReplayHandlerAdapter extends WriteBatch.Handler {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ReplayHandlerAdapter.class);

  private final Map<Integer, String> familyIdToName;
  private final ReplayHandler replayHandler;

  private final Set<String> updatedStores = Sets.newHashSet();

  ReplayHandlerAdapter(ReplayHandler replayHandler, Map<Integer, String> familyIdToName) {
    this.replayHandler = replayHandler;
    this.familyIdToName = familyIdToName;
  }

  @Override
  public void put(int columnFamilyId, byte[] key, byte[] value) {
    final String tableName = familyIdToName.get(columnFamilyId);
    logger.trace("Put: {}:{}:{}", tableName, key, value);

    replayHandler.put(tableName, key, value);
    updatedStores.add(tableName);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    logger.warn("Ignoring put: {}:{}", key, value);
  }

  @Override
  public void merge(int columnFamilyId, byte[] key, byte[] value) {
    if (logger.isWarnEnabled()) {
      logger.warn("Ignoring merge: {}:{}:{}", familyIdToName.get(columnFamilyId), key, value);
    }
  }

  @Override
  public void merge(byte[] key, byte[] value) {
    logger.warn("Ignoring merge: {}:{}", key, value);
  }

  @Override
  public void delete(int columnFamilyId, byte[] key)  {
    final String tableName = familyIdToName.get(columnFamilyId);
    logger.trace("Delete: {}:{}", tableName, key);

    replayHandler.delete(tableName, key);
    updatedStores.add(tableName);
  }

  @Override
  public void delete(byte[] key) {
    logger.warn("Ignoring delete: {}", key);
  }

  @Override
  public void singleDelete(int columnFamilyId, byte[] key) {
    final String tableName = familyIdToName.get(columnFamilyId);
    logger.trace("Delete: {}:{}", tableName, key);

    replayHandler.delete(tableName, key);
    updatedStores.add(tableName);
  }

  @Override
  public void singleDelete(byte[] key) {
    logger.warn("Ignoring single delete: {}", key);
  }

  @Override
  public void deleteRange(int columnFamilyId, byte[] beginKey, byte[] endKey) {
    if (logger.isWarnEnabled()) {
      logger.warn("Ignoring delete range: {}:{}:{}", familyIdToName.get(columnFamilyId), beginKey, endKey);
    }
  }

  @Override
  public void deleteRange(byte[] beginKey, byte[] endKey) {
    logger.warn("Ignoring delete range: {}:{}", beginKey, endKey);
  }

  @Override
  public void logData(byte[] blob) {
    logger.warn("Ignoring log data: {}", blob);
  }

  @Override
  public void putBlobIndex(int columnFamilyId, byte[] key, byte[] value) {
    if (logger.isWarnEnabled()) {
      logger.warn("Ignoring put blob index: {}:{}:{}", columnFamilyId, key, value);
    }
  }

  @Override
  public void markBeginPrepare() {
    logger.warn("Ignoring mark begin prepare");
  }

  @Override
  public void markEndPrepare(byte[] xid) {
    logger.warn("Ignoring mark end prepare: {}", xid);
  }

  @Override
  public void markNoop(boolean emptyBatch) {
    logger.warn("Ignoring mark noop: {}", emptyBatch);
  }

  @Override
  public void markRollback(byte[] xid) {
    logger.warn("Ignoring mark rollback: {}", xid);
  }

  @Override
  public void markCommit(byte[] xid) {
    logger.warn("Ignoring mark commit: {}", xid);
  }

  Set<String> getUpdatedStores() {
    return Collections.unmodifiableSet(updatedStores);
  }
}
