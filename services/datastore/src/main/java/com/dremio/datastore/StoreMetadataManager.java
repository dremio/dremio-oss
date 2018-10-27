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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;

import org.rocksdb.RocksDBException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

/**
 * Manages metadata about stores. The metadata is stored in the "default" store.
 */
final class StoreMetadataManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoreMetadataManager.class);

  private static final Serializer<StoreMetadata> valueSerializer = Serializer.of(StoreMetadata.getSchema());

  // in-memory holder of the latest transaction numbers. The second-to-last transaction number is persisted
  private final ConcurrentMap<String, Long> latestTransactionNumbers = Maps.newConcurrentMap();

  private final ByteStoreManager storeManager;

  private ByteStore store;

  private volatile boolean allowUpdates;

  StoreMetadataManager(ByteStoreManager storeManager) {
    this.storeManager = storeManager;
    this.allowUpdates = false;
  }

  void start() throws RocksDBException {
    store = storeManager.getDefaultDB();
  }

  void blockUpdates() {
    allowUpdates = false;
  }

  void allowUpdates() {
    allowUpdates = true;
  }

  /**
   * Sets the latest transaction number for the given store.
   *
   * @param storeName store name
   * @param transactionNumber latest transaction number
   */
  void setLatestTransactionNumber(String storeName, long transactionNumber) {
    if (!allowUpdates) {
      return;
    }

    Optional<Long> lastNumber = Optional.ofNullable(latestTransactionNumbers.get(storeName));
    if (!lastNumber.isPresent()) {
      final Optional<StoreMetadata> lastMetadata = getValue(store.get(getKey(storeName)));
      lastNumber = lastMetadata.map(StoreMetadata::getLatestTransactionNumber);
    }

    // Replaying from current transaction number is necessary, but may not be sufficient. However,
    // replaying from the previous transaction number is sufficient (in fact, more than sufficient sometimes).
    final long penultimateNumber = lastNumber.orElse(transactionNumber);

    final StoreMetadata storeMetadata = new StoreMetadata()
        .setTableName(storeName)
        .setLatestTransactionNumber(penultimateNumber);

    latestTransactionNumbers.put(storeName, transactionNumber);
    logger.trace("Setting {} as transaction number for store '{}'", penultimateNumber, storeName);
    store.put(getKey(storeName), getValue(storeMetadata));
  }

  /**
   * Get the lowest transaction number across all stores.
   *
   * @return lowest transaction number, or {@link Long#MAX_VALUE} if lowest is not found
   */
  long getLowestTransactionNumber() {
    final long[] lowest = {Long.MAX_VALUE};
    for (Map.Entry<byte[], byte[]> tuple : store.find()) {
      final Optional<StoreMetadata> value = getValue(tuple.getValue());
      value.ifPresent(storeMetadata -> {
        final long transactionNumber = storeMetadata.getLatestTransactionNumber();
        if (transactionNumber < lowest[0]) {
          lowest[0] = transactionNumber;
        }
      });
    }
    return lowest[0];
  }

  @VisibleForTesting
  Long getFromCache(String storeName) {
    return latestTransactionNumbers.get(storeName);
  }

  private static byte[] getKey(String key) {
    return StringSerializer.INSTANCE.convert(key);
  }

  private static byte[] getValue(StoreMetadata info) {
    return valueSerializer.convert(info);
  }

  private static Optional<StoreMetadata> getValue(byte[] info) {
    return Optional.ofNullable(info).map(valueSerializer::revert);
  }
}
