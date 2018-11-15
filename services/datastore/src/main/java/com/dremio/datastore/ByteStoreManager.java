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

import static com.dremio.datastore.MetricUtils.COLLECT_METRICS;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.Status;
import org.rocksdb.TickerType;
import org.rocksdb.TransactionLogIterator;

import com.dremio.common.DeferredException;
import com.dremio.datastore.CoreStoreProviderImpl.ForcedMemoryMode;
import com.dremio.datastore.MetricUtils.MetricSetBuilder;
import com.dremio.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Manages the underlying byte storage supporting a kvstore.
 */
class ByteStoreManager implements AutoCloseable {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(ByteStoreManager.class);

  private static final long WAL_TTL_SECONDS = Long.getLong("dremio.catalog.wal_ttl_seconds", 5 * 60L);
  private static final String METRICS_PREFIX = "kvstore.db";
  private static final String DEFAULT = "default";
  private static final int STRIPE_COUNT = 16;
  private static final long ROCKSDB_OPEN_SLEEP_MILLIS = 100L;

  static final String CATALOG_STORE_NAME = "catalog";

  private final boolean inMemory;
  private final int stripeCount;
  private final String baseDirectory;

  private RocksDB db;
  private ColumnFamilyHandle defaultHandle;
  private StoreMetadataManagerImpl metadataManager;

  private final DeferredException closeException = new DeferredException();

  // on #start all the existing tables are loaded, and if new stores are requested, #newStore is used
  private final ConcurrentMap<Integer, String> handleIdToNameMap = Maps.newConcurrentMap();
  private final LoadingCache<String, ByteStore> maps = CacheBuilder.newBuilder()
      .removalListener((RemovalListener<String, ByteStore>) notification -> {
        try {
          notification.getValue().close();
        } catch (Exception ex) {
          closeException.addException(ex);
        }
      }).build(new CacheLoader<String, ByteStore>() {
        @Override
        public ByteStore load(String name) throws RocksDBException {
          return newStore(name);
        }
      });

  public ByteStoreManager(String baseDirectory, boolean inMemory) {
    this.stripeCount = STRIPE_COUNT;
    this.baseDirectory = baseDirectory;
    this.inMemory = inMemory;
  }

  private ByteStore newStore(String name) throws RocksDBException {
    if (inMemory) {
      return new MapStore(name);
    } else {
      final ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(name.getBytes(UTF_8));
      ColumnFamilyHandle handle = db.createColumnFamily(columnFamilyDescriptor);
      handleIdToNameMap.put(handle.getID(), name);
      metadataManager.createEntry(name, false);
      return new RocksDBStore(name, columnFamilyDescriptor, handle, db, stripeCount);
    }
  }

  public void start() throws Exception {
    if (inMemory) {
      return;
    }

    final String baseDirectory = CoreStoreProviderImpl.MODE == ForcedMemoryMode.DISK && this.baseDirectory == null
        ? Files.createTempDirectory(null).toString()
        : this.baseDirectory.toString();

    final File dbDirectory = new File(baseDirectory, CATALOG_STORE_NAME);
    if (dbDirectory.exists()) {
      if (!dbDirectory.isDirectory()) {
        throw new DatastoreException(
            String.format("Invalid path %s for local catalog db, not a directory.", dbDirectory.getAbsolutePath()));
      }
    } else {
      if (!dbDirectory.mkdirs()) {
        throw new DatastoreException(
            String.format("Failed to create directory %s for local catalog db.", dbDirectory.getAbsolutePath()));
      }
    }

    final String path = dbDirectory.toString();

    final List<byte[]> families;
    try (final Options options = new Options()) {
      options.setCreateIfMissing(true);
      // get a list of existing families.
      families = new ArrayList<>(RocksDB.listColumnFamilies(options, path));
    }

    // if empty, add the default family
    if (families.isEmpty()) {
      families.add(RocksDB.DEFAULT_COLUMN_FAMILY);
    }
    final Function<byte[], ColumnFamilyDescriptor> func = ColumnFamilyDescriptor::new;

    List<ColumnFamilyHandle> familyHandles = new ArrayList<>();
    try (final DBOptions dboptions = new DBOptions()) {
      dboptions.setCreateIfMissing(true);

      // From docs, ... if WAL_ttl_seconds is not 0 and WAL_size_limit_MB is 0, then
      // WAL files will be checked every WAL_ttl_seconds / 2 and those that
      // are older than WAL_ttl_seconds will be deleted.
      dboptions.setWalSizeLimitMB(0);
      dboptions.setWalTtlSeconds(WAL_TTL_SECONDS);
      LOGGER.debug("WAL settings: size: '{} MB', TTL: '{}' seconds",
          dboptions.walSizeLimitMB(), dboptions.walTtlSeconds());

      if (COLLECT_METRICS) {
        registerMetrics(dboptions);
      }
      db = openDB(dboptions, path, Lists.transform(families, func), familyHandles);
    }
    // create an output list to be populated when we open the db.

    // populate the local cache with the existing tables.
    for (int i = 0; i < families.size(); i++) {
      byte[] family = families.get(i);
      if (Arrays.equals(family, RocksDB.DEFAULT_COLUMN_FAMILY)) {
        defaultHandle = familyHandles.get(i);
      } else {
        String name = new String(family, UTF_8);
        final ColumnFamilyHandle handle = familyHandles.get(i);
        handleIdToNameMap.put(handle.getID(), name);
        RocksDBStore store = new RocksDBStore(name, new ColumnFamilyDescriptor(family), handle, db, stripeCount);
        maps.put(name, store);
      }
    }

    // update the metadata manager
    metadataManager = new StoreMetadataManagerImpl();
    for (String tableName : handleIdToNameMap.values()) {
      metadataManager.createEntry(tableName, true);
    }
  }

  private void registerMetrics(DBOptions dbOptions) {
    // calling DBOptions.statisticsPtr() will create a Statistics object that will collect various stats from RocksDB and
    // will introduce a 5-10% overhead
    final Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    dbOptions.setStatistics(statistics);
    final MetricSetBuilder builder = new MetricSetBuilder(METRICS_PREFIX);
    // for now, let's add all ticker stats as gauge metrics
    for (TickerType tickerType : TickerType.values()) {
      if (tickerType == TickerType.TICKER_ENUM_MAX) {
        continue;
      }

      builder.gauge(tickerType.name(), () -> statistics.getTickerCount(tickerType));
    }
    Metrics.getInstance().registerAll(builder.build());
    // Note that Statistics also contains various histogram metrics, but those cannot be easily tracked through our metrics
  }

  public RocksDB openDB(final DBOptions dboptions, final String path, final List<ColumnFamilyDescriptor> columnNames,
      List<ColumnFamilyHandle> familyHandles) throws RocksDBException {
    boolean printLockMessage = true;

    while (true) {
      try {
        return RocksDB.open(dboptions, path, columnNames, familyHandles);
      } catch (RocksDBException e) {
        if (e.getStatus().getCode() != Status.Code.IOError || !e.getStatus().getState().contains("While lock")) {
          throw e;
        }

        if (printLockMessage) {
          LOGGER.info("Lock file to RocksDB is currently hold by another process. Will wait until lock is freed.");
          System.out.println("Lock file to RocksDB is currently hold by another process. Will wait until lock is freed.");
          printLockMessage = false;
        }
      }

      // Add some wait until the next attempt
      try {
        TimeUnit.MILLISECONDS.sleep(ROCKSDB_OPEN_SLEEP_MILLIS);
      } catch (InterruptedException e) {
        throw new RocksDBException(new Status(Status.Code.TryAgain, Status.SubCode.None, "While open db"));
      }
    }
  }

  void deleteEverything(Set<String> skipNames) throws IOException {
    for (Entry<String, ByteStore> entry : maps.asMap().entrySet()) {
      if (!skipNames.contains(entry.getKey())) {
        entry.getValue().deleteAllValues();
      }
    }
  }

  public ByteStore getStore(String name) {
    Preconditions.checkNotNull(name);
    Preconditions.checkArgument(!DEFAULT.equals(name), "The store name 'default' is reserved and cannot be used.");
    try {
      return maps.get(name);
    } catch (ExecutionException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Replay updates from the last commit on to the handler.
   *
   * @param replayHandler replay handler
   * @return if replaying succeeded
   */
  boolean replayDelta(ReplayHandler replayHandler) {
    if (inMemory) {
      return false;
    }

    final long lowest = metadataManager.getLowestTransactionNumber();
    if (lowest == -1L) {
      LOGGER.info("Could not deduce transaction number to replay from");
      return false;
    }

    LOGGER.debug("Replaying updates from {}", lowest);
    replaySince(lowest, replayHandler);
    return true;
  }

  void replaySince(final long transactionNumber, ReplayHandler replayHandler) {
    try (ReplayHandlerAdapter handler = new ReplayHandlerAdapter(replayHandler, handleIdToNameMap);
         TransactionLogIterator iterator = db.getUpdatesSince(transactionNumber)) {
      while (iterator.isValid()) {
        iterator.status();

        final TransactionLogIterator.BatchResult result = iterator.getBatch(); // requires isValid and status check
        LOGGER.debug("Requested sequence number: {}, iterator sequence number: {}",
            transactionNumber, result.sequenceNumber());

        result.writeBatch()
            .iterate(handler);

        if (!iterator.isValid()) {
          break;
        }
        iterator.next(); // requires isValid
      }

      for (String updatedStore : handler.getUpdatedStores()) {
        final long latestTransactionNumber = metadataManager.getLatestTransactionNumber();
        metadataManager.setLatestTransactionNumber(updatedStore, latestTransactionNumber, latestTransactionNumber);
      }
    } catch (RocksDBException e) {
      throw new DatastoreException(e);
    }
  }

  /**
   * Get the store metadata manager.
   *
   * @return store metadata manager
   */
  StoreMetadataManager getMetadataManager() {
    if (inMemory) {
      return StoreMetadataManager.NO_OP;
    }

    Preconditions.checkState(metadataManager != null, "#start was not invoked, so metadataManager is not available");
    return metadataManager;
  }

  @Override
  public void close() throws Exception {
    if (COLLECT_METRICS) {
      MetricUtils.removeAllMetricsThatStartWith(METRICS_PREFIX);
    }

    maps.invalidateAll();
    closeException.suppressingClose(defaultHandle);
    closeException.suppressingClose(db);
    closeException.close();
  }

  /**
   * Implementation that manages metadata with {@link RocksDB}.
   */
  final class StoreMetadataManagerImpl implements StoreMetadataManager {
    private final Serializer<StoreMetadata> valueSerializer = Serializer.of(StoreMetadata.getSchema());

    // in-memory holder of the latest transaction numbers. The second-to-last transaction number is persisted
    private final ConcurrentMap<String, Long> latestTransactionNumbers = Maps.newConcurrentMap();

    private final ByteStore metadataStore;

    private volatile boolean allowUpdates = false;

    private StoreMetadataManagerImpl() {
      metadataStore = new RocksDBStore(DEFAULT, null /* dropping and creating default is not supported */,
          db.getDefaultColumnFamily(), db, stripeCount);
    }

    /**
     * Creates an entry in the metadata store if the table is not {@code preexisting} or if the table was not
     * previously added.
     *
     * @param storeName store name
     * @param preexisting preexisting
     */
    private void createEntry(String storeName, boolean preexisting) {
      LOGGER.trace("Creating metastore entry for: '{}' and preexisting: '{}'", storeName, preexisting);
      final StoreMetadata storeMetadata = new StoreMetadata()
          .setTableName(storeName);

      if (preexisting) {
        // this is a old table, and so if there is no history, flag it as -1

        final Optional<StoreMetadata> value = getValue(metadataStore.get(getKey(storeName)));
        if (!value.isPresent()) {
          storeMetadata.setLatestTransactionNumber(-1L);
          metadataStore.put(getKey(storeName), getValue(storeMetadata));
        } else {
          LOGGER.trace("Metadata for {} already exists with transaction number {}",
              value.get().getTableName(), value.get().getLatestTransactionNumber());
        }
      } else {
        // this is a new table, and therefore the value should reflect the latest transaction number

        storeMetadata.setLatestTransactionNumber(getLatestTransactionNumber());
        assert !metadataStore.contains(getKey(storeName));

        metadataStore.put(getKey(storeName), getValue(storeMetadata));
      }
    }

    /**
     * Block updates to the metadata store.
     */
    @VisibleForTesting
    void blockUpdates() {
      allowUpdates = false;
    }

    @Override
    public void allowUpdates() {
      allowUpdates = true;
    }

    @Override
    public long getLatestTransactionNumber() {
      return db.getLatestSequenceNumber();
    }

    @Override
    public void setLatestTransactionNumber(String storeName, long transactionNumber) {
      if (!allowUpdates) {
        return;
      }

      Optional<Long> lastNumber = Optional.ofNullable(latestTransactionNumbers.get(storeName));
      if (!lastNumber.isPresent()) {
        final Optional<StoreMetadata> lastMetadata = getValue(metadataStore.get(getKey(storeName)));
        lastNumber = lastMetadata.map(StoreMetadata::getLatestTransactionNumber);
      }

      // Replaying from current transaction number is necessary, but may not be sufficient. However,
      // replaying from the previous transaction number is sufficient (in fact, more than sufficient sometimes).
      final long penultimateNumber = lastNumber.orElse(transactionNumber);

      setLatestTransactionNumber(storeName, transactionNumber, penultimateNumber);
    }

    private void setLatestTransactionNumber(String storeName, long transactionNumber, long penultimateNumber) {
      final StoreMetadata storeMetadata = new StoreMetadata()
          .setTableName(storeName)
          .setLatestTransactionNumber(penultimateNumber);

      latestTransactionNumbers.put(storeName, transactionNumber);
      LOGGER.trace("Setting {} as transaction number for store '{}'", penultimateNumber, storeName);
      metadataStore.put(getKey(storeName), getValue(storeMetadata));
    }

    /**
     * Get the lowest transaction number across all stores.
     *
     * @return lowest transaction number, or {@code -1} if lowest is not found
     */
    long getLowestTransactionNumber() {
      final long[] lowest = {Long.MAX_VALUE};
      for (Map.Entry<byte[], byte[]> tuple : metadataStore.find()) {
        final Optional<StoreMetadata> value = getValue(tuple.getValue());
        value.ifPresent(storeMetadata -> {
          final long transactionNumber = storeMetadata.getLatestTransactionNumber();
          if (transactionNumber < lowest[0]) {
            lowest[0] = transactionNumber;
          }
        });
      }

      if (lowest[0] == Long.MAX_VALUE) {
        lowest[0] = -1L;
      }
      return lowest[0];
    }

    @VisibleForTesting
    Long getFromCache(String storeName) {
      return latestTransactionNumbers.get(storeName);
    }

    private byte[] getKey(String key) {
      return StringSerializer.INSTANCE.convert(key);
    }

    private byte[] getValue(StoreMetadata info) {
      return valueSerializer.convert(info);
    }

    private Optional<StoreMetadata> getValue(byte[] info) {
      return Optional.ofNullable(info).map(valueSerializer::revert);
    }
  }
}
