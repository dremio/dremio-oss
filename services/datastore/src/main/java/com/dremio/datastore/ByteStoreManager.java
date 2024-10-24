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
package com.dremio.datastore;

import static com.dremio.datastore.RocksDBStore.BLOB_PATH;
import static com.dremio.datastore.RocksDBStore.FILTER_SIZE_IN_BYTES;
import static com.dremio.datastore.RocksDBStore.OVERRIDE_FILTER_SIZE_MAP;
import static com.dremio.telemetry.api.metrics.MeterProviders.newGauge;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

import com.dremio.common.DeferredException;
import com.dremio.datastore.CoreStoreProviderImpl.ForcedMemoryMode;
import com.dremio.datastore.RocksDBStore.BlobNotFoundException;
import com.dremio.datastore.RocksDBStore.RocksMetaManager;
import com.dremio.datastore.api.Document;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.StatsLevel;
import org.rocksdb.Status;
import org.rocksdb.TickerType;
import org.rocksdb.TransactionLogIterator;

/** Manages the underlying byte storage supporting a kvstore. */
class ByteStoreManager implements AutoCloseable {
  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ByteStoreManager.class);

  private static final boolean COLLECT_METRICS =
      System.getProperty("dremio.kvstore.metrics", null) != null;

  private static final long WAL_TTL_SECONDS =
      Long.getLong("dremio.catalog.wal_ttl_seconds", 5 * 60L);
  private static final String DEFAULT = "default";
  private static final int STRIPE_COUNT = 16;
  private static final long ROCKSDB_OPEN_SLEEP_MILLIS = 100L;
  // TODO: (DX-16211) this is a temporary hack for a blob whitelist
  static final String BLOB_WHITELIST_STORE = "dac-namespace";
  private static final Set<String> BLOB_WHITELIST =
      Set.of(BLOB_WHITELIST_STORE, "intermediate_profiles");
  static final String CATALOG_STORE_NAME = "catalog";

  private final boolean inMemory;
  private final boolean noDBOpenRetry;
  private final boolean noDBLogMessages;
  private final int stripeCount;
  private final String baseDirectory;

  private RocksDB db;
  private ColumnFamilyHandle defaultHandle;
  private StoreMetadataManagerImpl metadataManager;

  private final RocksDBOpenDelegate rocksDBOpenDelegate;
  private final boolean dbReadOnly;

  private final DeferredException closeException = new DeferredException();

  // on #start all the existing tables are loaded, and if new stores are requested, #newStore is
  // used
  private final ConcurrentMap<Integer, String> handleIdToNameMap = Maps.newConcurrentMap();

  @SuppressWarnings("NoGuavaCacheUsage") // TODO: fix as part of DX-51884
  private final LoadingCache<String, ByteStore> maps =
      CacheBuilder.newBuilder()
          .removalListener(
              (RemovalListener<String, ByteStore>)
                  notification -> {
                    try {
                      notification.getValue().close();
                    } catch (Exception ex) {
                      closeException.addException(ex);
                    }
                  })
          .build(
              new CacheLoader<String, ByteStore>() {
                @Override
                public ByteStore load(String name) throws RocksDBException {
                  return newStore(name);
                }
              });

  public ByteStoreManager(String baseDirectory, boolean inMemory) {
    this(baseDirectory, inMemory, false, false);
  }

  public ByteStoreManager(
      String baseDirectory, boolean inMemory, boolean noDBOpenRetry, boolean noDBLogMessages) {
    this(baseDirectory, inMemory, noDBOpenRetry, noDBLogMessages, false);
  }

  public ByteStoreManager(
      String baseDirectory,
      boolean inMemory,
      boolean noDBOpenRetry,
      boolean noDBLogMessages,
      boolean readOnly) {
    this.stripeCount = STRIPE_COUNT;
    this.baseDirectory = baseDirectory;
    this.inMemory = inMemory;
    this.noDBOpenRetry = noDBOpenRetry;
    this.noDBLogMessages = noDBLogMessages;
    this.rocksDBOpenDelegate = readOnly ? RocksDB::openReadOnly : RocksDB::open;
    this.dbReadOnly = readOnly;
  }

  private ByteStore newStore(String name) throws RocksDBException {
    if (inMemory) {
      return new MapStore(name);
    } else {
      final ColumnFamilyDescriptor columnFamilyDescriptor =
          new ColumnFamilyDescriptor(name.getBytes(UTF_8));
      ColumnFamilyHandle handle = db.createColumnFamily(columnFamilyDescriptor);
      handleIdToNameMap.put(handle.getID(), name);
      metadataManager.createEntry(name, false);

      return newRocksDBStore(name, columnFamilyDescriptor, handle);
    }
  }

  private RocksDBStore newRocksDBStore(
      String name, ColumnFamilyDescriptor columnFamilyDescriptor, ColumnFamilyHandle handle) {
    final RocksMetaManager rocksManager;
    rocksManager = new RocksMetaManager(baseDirectory, name, selectFilterSize(name));
    return new RocksDBStore(
        name, columnFamilyDescriptor, handle, db, stripeCount, rocksManager, dbReadOnly);
  }

  private static long selectFilterSize(String name) {
    if (BLOB_WHITELIST.contains(name)) {
      return OVERRIDE_FILTER_SIZE_MAP.getOrDefault(name, FILTER_SIZE_IN_BYTES);
    }
    return Long.MAX_VALUE;
  }

  // Validates that the first file found in the DB directory is owned by the currently running user.
  // Throws a DatastoreException if it's not.
  private void verifyDBOwner(File dbDirectory) throws IOException {
    // Skip file owner check if running on Windows
    if (StandardSystemProperty.OS_NAME.value().contains("Windows")) {
      return;
    }

    String procUser = StandardSystemProperty.USER_NAME.value();
    File[] dbFiles = dbDirectory.listFiles();
    for (File dbFile : dbFiles) {
      if (dbFile.isDirectory()) {
        continue;
      }

      String dbOwner = Files.getOwner(dbFile.toPath()).getName();
      if (!procUser.equals(dbOwner)) {
        throw new DatastoreException(
            format(
                "Process user (%s) doesn't match local catalog db owner (%s).  Please run process as %s.",
                procUser, dbOwner, dbOwner));
      }

      // Break once verified, we assume the rest are owned by the same user.
      break;
    }
  }

  public void start() throws Exception {
    if (inMemory) {
      return;
    }

    final String baseDirectory =
        CoreStoreProviderImpl.MODE == ForcedMemoryMode.DISK && this.baseDirectory == null
            ? Files.createTempDirectory(null).toString()
            : this.baseDirectory.toString();

    final File dbDirectory = new File(baseDirectory, CATALOG_STORE_NAME);
    if (dbDirectory.exists()) {
      if (!dbDirectory.isDirectory()) {
        throw new DatastoreException(
            format(
                "Invalid path %s for local catalog db, not a directory.",
                dbDirectory.getAbsolutePath()));
      }

      // If there are any files that exist within the dbDirectory, verify that the first file in the
      // directory is
      // owned by the process user.
      verifyDBOwner(dbDirectory);
    } else {
      if (!dbDirectory.mkdirs()) {
        throw new DatastoreException(
            format(
                "Failed to create directory %s for local catalog db.",
                dbDirectory.getAbsolutePath()));
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
      LOGGER.debug(
          "WAL settings: size: '{} MB', TTL: '{}' seconds",
          dboptions.walSizeLimitMB(),
          dboptions.walTtlSeconds());
      registerMetrics(dboptions);
      db = openDB(dboptions, path, new ArrayList<>(Lists.transform(families, func)), familyHandles);
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
        RocksDBStore store = newRocksDBStore(name, new ColumnFamilyDescriptor(family), handle);
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
    // calling DBOptions.statisticsPtr() will create a Statistics object that will collect various
    // stats from RocksDB and
    // will introduce a 5-10% overhead
    if (!COLLECT_METRICS) {
      return;
    }

    final Statistics statistics = new Statistics();
    statistics.setStatsLevel(StatsLevel.ALL);
    dbOptions.setStatistics(statistics);
    // for now, let's add all ticker stats as gauge metrics
    for (TickerType tickerType : TickerType.values()) {
      if (tickerType == TickerType.TICKER_ENUM_MAX) {
        continue;
      }

      newGauge(
          "kvstore_db." + tickerType.name().toLowerCase(),
          () -> statistics.getTickerCount(tickerType));
    }

    // Note that Statistics also contains various histogram metrics, but those cannot be easily
    // tracked through our metrics
  }

  public RocksDB openDB(
      final DBOptions dboptions,
      final String path,
      final List<ColumnFamilyDescriptor> columnNames,
      List<ColumnFamilyHandle> familyHandles)
      throws RocksDBException {
    boolean printLockMessage = true;

    while (true) {
      try {
        if (!noDBLogMessages) {
          return rocksDBOpenDelegate.open(dboptions, path, columnNames, familyHandles);
        }
        try (Logger logger =
            new Logger(dboptions) {
              // Create new logger that ignores all messages.
              @Override
              protected void log(org.rocksdb.InfoLogLevel infoLogLevel, String logMsg) {
                // Ignore all messages.
              }
            }) {
          dboptions.setLogger(logger);
          return rocksDBOpenDelegate.open(dboptions, path, columnNames, familyHandles);
        }
      } catch (RocksDBException e) {
        if (!(e.getStatus().getCode() == Status.Code.IOError
            || e.getStatus().getState().contains("While lock")
            || e.getStatus().getState().contains("lock hold by"))) {
          throw e;
        }

        // Don't retry if request came from a CLI command (noDBOpenRetry = true)
        if (noDBOpenRetry) {
          LOGGER.error(
              "Lock file to RocksDB is currently held by another process.  Stop other process before retrying.");
          System.out.println(
              "Lock file to RocksDB is currently held by another process.  Stop other process before retrying.");
          throw e;
        }

        if (printLockMessage) {
          LOGGER.info(
              "Lock file to RocksDB is currently held by another process. Will wait until lock is freed.");
          System.out.println(
              "Lock file to RocksDB is currently held by another process. Will wait until lock is freed.");
          printLockMessage = false;
        }
      }

      // Add some wait until the next attempt
      try {
        TimeUnit.MILLISECONDS.sleep(ROCKSDB_OPEN_SLEEP_MILLIS);
      } catch (InterruptedException e) {
        throw new RocksDBException(
            new Status(Status.Code.TryAgain, Status.SubCode.None, "While open db"));
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
    Preconditions.checkArgument(
        !DEFAULT.equals(name), "The store name 'default' is reserved and cannot be used.");
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
   * @param predicate predicate to apply
   * @return if replaying succeeded
   */
  boolean replayDelta(ReplayHandler replayHandler, Predicate<String> predicate) {
    if (inMemory) {
      return false;
    }

    final long lowest = metadataManager.getLowestTransactionNumber(predicate);
    if (lowest == -1L) {
      LOGGER.info("Could not deduce transaction number to replay from");
      return false;
    }

    LOGGER.info("Replaying updates from {}", lowest);

    replaySince(lowest, replayHandler);
    return true;
  }

  // We override the ReplayHandler to ensure that we resolve blob pointers stored in RocksDB.  This
  // won't get called
  // on other stores (like the in memory one) so its safe to require a RocksDBStore.
  private class ReplayHandlerWithPtrResolution implements ReplayHandler {
    private final ReplayHandler replayHandler;

    ReplayHandlerWithPtrResolution(ReplayHandler replayHandler) {
      super();
      this.replayHandler = replayHandler;
    }

    @Override
    public void put(String tableName, byte[] key, byte[] ptrOrValue) {
      ByteStore store = getStore(tableName);
      Preconditions.checkState(store instanceof RocksDBStore);
      try {
        final byte[] value = (((RocksDBStore) store).resolvePtrOrValue(ptrOrValue).getData());
        replayHandler.put(tableName, key, value);
      } catch (BlobNotFoundException e) {
        // Could not find the blob file when resolving a ptr.  This could be because the replayed
        // event's pointer is no
        // longer on disk since another event modified the entry (updated or deleted).  In this case
        // just skip replaying
        // the event.
        LOGGER.trace("Replaying could not resolve ptr.", e);
      }
    }

    @Override
    public void delete(String tableName, byte[] key) {
      replayHandler.delete(tableName, key);
    }
  }

  void replaySince(final long transactionNumber, ReplayHandler replayHandler) {
    final ReplayHandlerWithPtrResolution replayHandlerWrapper =
        new ReplayHandlerWithPtrResolution(replayHandler);

    try (ReplayHandlerAdapter handler =
            new ReplayHandlerAdapter(
                db.getDefaultColumnFamily().getID(), replayHandlerWrapper, handleIdToNameMap);
        TransactionLogIterator iterator = db.getUpdatesSince(transactionNumber)) {
      while (iterator.isValid()) {
        iterator.status();

        final TransactionLogIterator.BatchResult result =
            iterator.getBatch(); // requires isValid and status check
        LOGGER.debug(
            "Requested sequence number: {}, iterator sequence number: {}",
            transactionNumber,
            result.sequenceNumber());

        result.writeBatch().iterate(handler);

        if (!iterator.isValid()) {
          break;
        }
        iterator.next(); // requires isValid
      }

      for (String updatedStore : handler.getUpdatedStores()) {
        final long latestTransactionNumber = metadataManager.getLatestTransactionNumber();
        metadataManager.setLatestTransactionNumber(
            updatedStore, latestTransactionNumber, latestTransactionNumber);
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

    Preconditions.checkState(
        metadataManager != null, "#start was not invoked, so metadataManager is not available");
    return metadataManager;
  }

  @Override
  public void close() throws Exception {
    maps.invalidateAll();
    getMetadataManager().close();
    closeException.suppressingClose(defaultHandle);
    closeException.suppressingClose(db);
    closeException.close();
  }

  /**
   * Creates a new RocksDB checkpoint (snapshot).
   *
   * <p>The checkpoint is a copy of all SSTables and the current Manifest file stored in a different
   * filesystem path. It will also create a checkpoint of the blob stores associated to the RocksDB
   * data.
   *
   * <p>The destination directory path of the checkpoint is created into the <{@code
   * baseDirectory}>/checkpoints directory.
   *
   * <p>from the method call into {@link CheckpointInfo}. The caller can use the directory path.
   *
   * @param backupDir Directory path where the backup will be created.
   * @return a {@link CheckpointInfo} with information of the checkpoint.
   */
  public synchronized CheckpointInfo newCheckpoint(@Nonnull Path backupDir) {
    return BackupSupport.newCheckpoint(backupDir, baseDirectory, CATALOG_STORE_NAME, BLOB_PATH, db);
  }

  /** Implementation that manages metadata with {@link RocksDB}. */
  final class StoreMetadataManagerImpl implements StoreMetadataManager {
    private final Serializer<StoreMetadata, byte[]> valueSerializer =
        Serializer.of(StoreMetadata.getSchema());

    // in-memory holder of the latest transaction numbers. The second-to-last transaction number is
    // persisted
    private final ConcurrentMap<String, Long> latestTransactionNumbers = Maps.newConcurrentMap();

    private final ByteStore metadataStore;

    private volatile boolean allowUpdates = false;

    private StoreMetadataManagerImpl() {
      metadataStore =
          new RocksDBStore(
              DEFAULT,
              null /* dropping and creating default is not supported */,
              db.getDefaultColumnFamily(),
              db,
              stripeCount,
              dbReadOnly);
    }

    /**
     * Creates an entry in the metadata store if the table is not {@code preexisting} or if the
     * table was not previously added.
     *
     * @param storeName store name
     * @param preexisting preexisting
     */
    private void createEntry(String storeName, boolean preexisting) {
      LOGGER.trace(
          "Creating metastore entry for: '{}' and preexisting: '{}'", storeName, preexisting);
      final StoreMetadata storeMetadata = new StoreMetadata().setTableName(storeName);

      if (preexisting) {
        // this is a old table, and so if there is no history, flag it as -1

        final Optional<StoreMetadata> value = getValue(metadataStore.get(getKey(storeName)));
        if (!value.isPresent()) {
          storeMetadata.setLatestTransactionNumber(-1L);
          metadataStore.put(getKey(storeName), getValue(storeMetadata));
        } else {
          LOGGER.trace(
              "Metadata for {} already exists with transaction number {}",
              value.get().getTableName(),
              value.get().getLatestTransactionNumber());
        }
      } else {
        // this is a new table, and therefore the value should reflect the latest transaction number

        storeMetadata.setLatestTransactionNumber(getLatestTransactionNumber());
        assert !metadataStore.contains(getKey(storeName));

        metadataStore.put(getKey(storeName), getValue(storeMetadata));
      }
    }

    /** Block updates to the metadata store. */
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
      // replaying from the previous transaction number is sufficient (in fact, more than sufficient
      // sometimes).
      final long penultimateNumber = lastNumber.orElse(transactionNumber);

      setLatestTransactionNumber(storeName, transactionNumber, penultimateNumber);
    }

    @Override
    public void close() throws Exception {
      metadataStore.close();
    }

    private void setLatestTransactionNumber(
        String storeName, long transactionNumber, long penultimateNumber) {
      if (dbReadOnly) {
        throw new UnsupportedOperationException(
            "Not setting latest transaction number as the database is readonly");
      }
      final StoreMetadata storeMetadata =
          new StoreMetadata().setTableName(storeName).setLatestTransactionNumber(penultimateNumber);

      latestTransactionNumbers.put(storeName, transactionNumber);
      LOGGER.trace("Setting {} as transaction number for store '{}'", penultimateNumber, storeName);
      metadataStore.put(getKey(storeName), getValue(storeMetadata));
    }

    /**
     * Get the lowest transaction number across stores that satisfy the given predicate.
     *
     * @param predicate predicate to apply
     * @return lowest transaction number, or {@code -1} if lowest is not found
     */
    long getLowestTransactionNumber(Predicate<String> predicate) {
      final long[] lowest = {Long.MAX_VALUE};
      for (Document<byte[], byte[]> tuple : metadataStore.find()) {
        final Optional<StoreMetadata> value = getValue(tuple);
        value
            .filter(storeMetadata -> predicate.test(storeMetadata.getTableName()))
            .ifPresent(
                storeMetadata -> {
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

    private Optional<StoreMetadata> getValue(Document<byte[], byte[]> info) {
      return Optional.ofNullable(info).map(doc -> valueSerializer.revert(doc.getValue()));
    }
  }

  @FunctionalInterface
  private interface RocksDBOpenDelegate {
    RocksDB open(
        final DBOptions dboptions,
        final String path,
        final List<ColumnFamilyDescriptor> columnNames,
        final List<ColumnFamilyHandle> familyHandles)
        throws RocksDBException;
  }
}
