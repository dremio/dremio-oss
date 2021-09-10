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
package com.dremio.exec.store.cache;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Manages Rocks DB specific things (creation of instances, column families etc.)
 * and presents a simpler interface to perform writes to and reads from Rocks DB.
 */
class RocksDbBroker {
  private static final Logger logger = LoggerFactory.getLogger(RocksDbBroker.class);
  private static final String COLUMN_FAMILY_NAME = "block-locations";
  private static final int TTL_TIME_IN_SEC = (int) TimeUnit.DAYS.toSeconds(30);
  private static final ColumnFamilyOptions CF_OPTIONS = new ColumnFamilyOptions().optimizeForSmallDb();
  private static final List<ColumnFamilyDescriptor> DESCRIPTORS = Arrays.asList(
    new ColumnFamilyDescriptor(COLUMN_FAMILY_NAME.getBytes(), CF_OPTIONS),
    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
  private static final List<byte[]> DEFINED_COLUMN_FAMILIES = DESCRIPTORS.stream().map(ColumnFamilyDescriptor::getName).collect(toList());

  private static RocksDbBroker instance;

  enum RocksDBState {
    OK, ERROR
  }

  private final String dbDirectory;
  private final List<ColumnFamilyHandle> columnFamilyHandles;

  private RocksDB db;
  private RocksDBState readState = RocksDBState.OK;
  private RocksDBState writeState = RocksDBState.OK;

  static RocksDbBroker getInstance(String dbDirectory) throws RocksDBException {
    if (instance != null) {
      Preconditions.checkArgument(dbDirectory.equals(instance.dbDirectory), "getInstance should be called on the same dir");
      return instance;
    }

    instance = new RocksDbBroker(dbDirectory);
    return instance;
  }

  byte[] get(byte[] key) {
    byte[] value = null;
    try {
      value = db.get(columnFamilyHandles.get(0), key);
      resetGetFailure();
    } catch (RocksDBException e) {
      setGetFailure(e);
    }
    return value;
  }

  void put(byte[] key, byte[] value) {
    try {
      db.put(columnFamilyHandles.get(0), key, value);
      resetPutFailure();
    } catch (RocksDBException e) {
      setPutFailure(e);
    }
  }

  @VisibleForTesting
  RocksDbBroker(String dbDir) throws RocksDBException {
    validatePresence(dbDir);
    DBOptions options = getDbOptions();
    List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(DESCRIPTORS.size());

    this.db = openDb(dbDir, options, columnFamilyHandles);
    this.columnFamilyHandles = columnFamilyHandles;
    this.dbDirectory = dbDir;
  }

  @VisibleForTesting
  RocksDB getDb() {
    return db;
  }

  @VisibleForTesting
  void setDb(RocksDB rocksDB) {
    db = rocksDB;
  }

  @VisibleForTesting
  ColumnFamilyHandle getColumnFamily() {
    return columnFamilyHandles.get(0);
  }

  @VisibleForTesting
  RocksDBState getReadState() {
    return readState;
  }

  @VisibleForTesting
  RocksDBState getWriteState() {
    return writeState;
  }

  @VisibleForTesting
  void resetInstance() {
    instance = null;
  }

  private void setGetFailure(RocksDBException e) {
    if (readState == RocksDBState.OK) {
      readState = RocksDBState.ERROR;
      logger.info("RocksDb::get failed", e);
      return;
    }
    logger.debug("RocksDb::get failed", e);
  }

  private void resetGetFailure() {
    if (readState == RocksDBState.ERROR) {
      readState = RocksDBState.OK;
      logger.info("Normal state restored for RocksDb::get");
    }
  }

  private void setPutFailure(RocksDBException e) {
    if (writeState == RocksDBState.OK) {
      writeState = RocksDBState.ERROR;
      logger.info("RocksDb::put failed", e);
      return;
    }
    logger.debug("RocksDb::put failed", e);
  }

  private void resetPutFailure() {
    if (writeState == RocksDBState.ERROR) {
      writeState = RocksDBState.OK;
      logger.info("Normal state restored for RocksDb::put");
    }
  }

  private static RocksDB openDb(String dbDirectory, DBOptions dbOptions, List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    List<Integer> ttlValues = Arrays.asList(TTL_TIME_IN_SEC, 0); // 0 TTL (no expiration) for the default column family
    RocksDB db = TtlDB.open(dbOptions, dbDirectory, DESCRIPTORS, columnFamilyHandles, ttlValues, false);
    validateColumnFamilyHandles(columnFamilyHandles);
    return db;
  }

  private static DBOptions getDbOptions() {
    DBOptions dbOptions = new DBOptions();
    dbOptions.setCreateIfMissing(true);
    dbOptions.setCreateMissingColumnFamilies(true);
    return dbOptions;
  }

  private static void validatePresence(String dbDirectory) throws RocksDBException {
    try (Options options = new Options()) {
      List<byte[]> loadedColumnFamilies = RocksDB.listColumnFamilies(options, dbDirectory);
      if (!loadedColumnFamilies.isEmpty()) {
        validateColumnFamilies(loadedColumnFamilies);
      }
    }
  }

  private static void validateColumnFamilyHandles(List<ColumnFamilyHandle> columnFamilyHandles) throws RocksDBException {
    List<byte[]> list = new ArrayList<>();
    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
      list.add(columnFamilyHandle.getName());
    }
    validateColumnFamilies(list);
  }

  @VisibleForTesting
  static void validateColumnFamilies(List<byte[]> loadedColumnFamilies) throws RocksDBException {
    int loadedSize = loadedColumnFamilies.size();
    if (DEFINED_COLUMN_FAMILIES.size() != loadedSize) {
      throw new RocksDBException("Did not load the expected number of column families. " +
        "Expected: " + DEFINED_COLUMN_FAMILIES.size() + ", but found: " + loadedSize);
    }

    for (byte[] loaded : loadedColumnFamilies) {
      if (!isKnownColumnFamily(loaded)) {
        throw new RocksDBException("Unexpected column family: " + Arrays.toString(loaded));
      }
    }
  }

  private static boolean isKnownColumnFamily(byte[] cf) {
    for (byte[] defined : DEFINED_COLUMN_FAMILIES) {
      if (Arrays.equals(cf, defined)) {
        return true;
      }
    }
    return false;
  }
}
