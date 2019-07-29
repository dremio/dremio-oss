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

import static com.dremio.datastore.MetricUtils.COLLECT_METRICS;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.io.IOUtils;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.codahale.metrics.MetricRegistry;
import com.dremio.common.AutoCloseables;
import com.dremio.common.DeferredException;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.datastore.MetricUtils.MetricSetBuilder;
import com.dremio.datastore.rocks.Rocks.BlobPointer;
import com.dremio.datastore.rocks.Rocks.BlobPointer.Codec;
import com.dremio.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;

/**
 * A kv implementation that uses a column family within rocksdb per named store
 * dataset. We use a single database to minimize seeks for WAL management as I
 * believe WAL is one per db in Rocks.
 *
 * Note that we use lock striping to minimize contention. Most operations are
 * shared operations (read, write). These have a shared read locks that is used
 * per stripe. However, there are some multiple operation items.  These do multiple
 * operations and need to ensure a consistent viewpoint of data. As such, we grab an
 * exclusive lock for the desired key range for the life of the set of operations. We
 * use the AutoCloseableLock pattern with try-with-resources to ensure that we avoid any
 * lock leaking.
 *
 * Since the RocksDB interface is native, we need to manage native memory
 * cautiously. To this end, we manage the range iterator through the use of a
 * ReferenceQueue and parallel Set. This allows us to automatically garbarge
 * collect no-longer used iterators. Additionally, the set allows us to cleanly
 * close out the RocksDB database through the close() method.
 */
class RocksDBStore implements ByteStore {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RocksDBStore.class);

  private static final boolean ITERATOR_METRICS = Boolean.getBoolean("dremio.kvstore.iterator_metrics");

  private static final String BLOB_SYS_PROP = "dremio.rocksdb.blob_filter_bytes";
  static final long BLOB_FILTER_DEFAULT = 1024*1024;
  private static final long BLOB_FILTER_MINIMUM = 1024;
  private static final String BLOB_MINIMUM_SYS_PROP = "dremio.rocksdb.minimum_blob_filter_bytes";
  //  0x06 and 0x07 are unused by proto so we use 0x07 to denote that the entry is a blob pointer
  static final byte BLOB_VALUE_PREFIX = 7;
  private static final String BLOB_PATH = "blob";
  static final long FILTER_SIZE_IN_BYTES;

  static {
    final long minimumSize = Long.getLong(BLOB_MINIMUM_SYS_PROP, BLOB_FILTER_MINIMUM);
    long filterSize = Long.getLong(BLOB_SYS_PROP, BLOB_FILTER_DEFAULT);
    if (filterSize < minimumSize) {
      filterSize = BLOB_FILTER_DEFAULT;
      logger.warn("Property {} was set to {} which is below the minimum of "
          + "{} bytes. Using default value of {}", BLOB_SYS_PROP, filterSize, minimumSize, BLOB_FILTER_DEFAULT);
    }
    FILTER_SIZE_IN_BYTES = filterSize;
  }

  private static final String METRICS_PREFIX = "kvstore.stores";
  private static final String[] METRIC_PROPERTIES = {
    // number of immutable memtables that have not yet been flushed
    "rocksdb.num-immutable-mem-table",
    // number of immutable memtables that have already been flushed
    "rocksdb.num-immutable-mem-table-flushed",
    // 1 if a memtable flush is pending; otherwise, returns 0
    "rocksdb.mem-table-flush-pending",
    // number of currently running flushes
    "rocksdb.num-running-flushes",
    // 1 if at least one compaction is pending; otherwise, returns 0
    "rocksdb.compaction-pending",
    // number of currently running compactions
    "rocksdb.num-running-compactions",
    // accumulated number of background errors
    "rocksdb.background-errors",
    // approximate size of active memtable (bytes)
    "rocksdb.cur-size-active-mem-table",
    // approximate size of active and unflushed immutable memtables (bytes)
    "rocksdb.cur-size-all-mem-tables",
    // approximate size of active, unflushed immutable, and pinned immutable memtables (bytes).
    "rocksdb.size-all-mem-tables",
    // total number of entries in the active memtable
    "rocksdb.num-entries-active-mem-table",
    // total number of entries in the unflushed immutable memtables
    "rocksdb.num-entries-imm-mem-tables",
    // total number of delete entries in the active memtable
    "rocksdb.num-deletes-active-mem-table",
    // total number of delete entries in the unflushed immutable memtables
    "rocksdb.num-deletes-imm-mem-tables",
    // estimated number of total keys in the active and unflushed immutable memtables and storage
    "rocksdb.estimate-num-keys",
    // estimated memory used for reading SST tables, excluding memory used in block cache (e.g., filter and index blocks)
    "rocksdb.estimate-table-readers-mem",
    // number of unreleased snapshots of the database
    "rocksdb.num-snapshots",
    // number of live versions. More live versions often mean more SST files are held from being deleted,
    // by iterators or unfinished compactions
    "rocksdb.num-live-versions",
    // an estimate of the amount of live data in bytes
    "rocksdb.estimate-live-data-size",
    // total size (bytes) of all SST files
    // WARNING: may slow down online queries if there are too many files
    "rocksdb.total-sst-files-size",
    // estimated total number of bytes compaction needs to rewrite to get all levels down to under target size.
    // Not valid for other compactions than level based
    "rocksdb.estimate-pending-compaction-bytes",
    // current actual delayed write rate. 0 means no delay
    "rocksdb.actual-delayed-write-rate",
    // 1 if write has been stopped
    "rocksdb.is-write-stopped"
  };

  private ColumnFamilyHandle handle;
  private final AutoCloseableLock[] sharedLocks;
  private final AutoCloseableLock[] exclusiveLocks;

  private final ColumnFamilyDescriptor family;
  private final RocksDB db;
  private final int parallel;
  private final String name;
  private final BlobManager blobManager;

  private final ReferenceQueue<FindByRangeIterator> iteratorQueue = new ReferenceQueue<>();
  private final Set<IteratorReference> iteratorSet = Sets.newConcurrentHashSet();

  private final AtomicLong openedIterators = new AtomicLong(0);
  private final AtomicLong gcIterators = new AtomicLong(0);
  private final AtomicLong closedIterators = new AtomicLong(0);

  private final AtomicBoolean closed = new AtomicBoolean(false);

  public RocksDBStore(String name, ColumnFamilyDescriptor family, ColumnFamilyHandle handle, RocksDB db, int stripes) {
    this(name, family, handle, db, stripes, INLINE_BLOB_MANAGER);
  }

  public RocksDBStore(String name, ColumnFamilyDescriptor family, ColumnFamilyHandle handle, RocksDB db, int stripes,
                      BlobManager blobManager) {
    super();
    this.family = family;
    this.name = name;
    this.db = db;
    this.parallel = stripes;
    this.handle = handle;
    this.sharedLocks = new AutoCloseableLock[stripes];
    this.exclusiveLocks = new AutoCloseableLock[stripes];
    this.blobManager = blobManager;

    for (int i = 0; i < stripes; i++) {
      ReadWriteLock core = new ReentrantReadWriteLock();
      sharedLocks[i] = new AutoCloseableLock(core.readLock());
      exclusiveLocks[i] = new AutoCloseableLock(core.writeLock());
    }

    if (COLLECT_METRICS) {
      registerMetrics();
    }
  }

  private void throwIfClosed() {
    if (closed.get()) {
      throw new DatastoreFatalException(String.format("'%s' store is already closed", name));
    }
  }

  private void registerMetrics() {
    final MetricSetBuilder builder = new MetricSetBuilder(MetricRegistry.name(METRICS_PREFIX, name));
    for (String property : METRIC_PROPERTIES) {
      builder.gauge(property, () -> {
        try {
          return db.getLongProperty(handle, property);
        } catch (RocksDBException e) {
          // throwing an exception would cause Dropwizard's metrics reporter to not report the remaining metrics
          logger.warn("failed to retrieve property '{}", property, e);
          return null;
        }
      });
    }
    Metrics.getInstance().registerAll(builder.build());
  }

  private void compact() throws RocksDBException {
    db.compactRange(handle);
  }

  private String stats() {
    try {
      StringBuilder sb = new StringBuilder();
      append(sb, "rocksdb.estimate-num-keys", "Estimated Number of Keys");
      append(sb, "rocksdb.estimate-live-data-size", "Estimated Live Data Size");
      append(sb, "rocksdb.total-sst-files-size", "Total SST files size");
      append(sb, "rocksdb.estimate-pending-compaction-bytes", "Pending Compaction Bytes");

      final BlobStats blobStats = blobManager.getStats();
      if (blobStats != null) {
        blobStats.append(sb);
      }
      return sb.toString();
    } catch(RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  private void append(StringBuilder sb, String propName, String propDisplayName) throws RocksDBException {
    sb.append("* ");
    sb.append(propDisplayName);
    sb.append(": ");
    sb.append(db.getLongProperty(handle, propName));
    sb.append("\n");
  }

  @Override
  public KVAdmin getAdmin() {
    return new RocksKVAdmin();
  }

  private class RocksKVAdmin extends KVAdmin {

    @Override
    public String getStats() {
      StringBuilder sb = new StringBuilder();
      sb.append(name);
      sb.append('\n');
      sb.append("\tbasic rocks store stats\n");
      sb.append(indent(2, stats()));
      sb.append('\n');
      return sb.toString();
    }

    @Override
    public void compactKeyValues() throws IOException {
      try {
        compact();
      } catch (RocksDBException ex) {
        throw new IOException(ex);
      }
    }

  }

  private AutoCloseableLock sharedLock(byte[] key) {
    Preconditions.checkNotNull(key);
    final int hash = Arrays.hashCode(key);
    AutoCloseableLock lock = sharedLocks[Math.abs(hash % parallel)];
    lock.open();
    return lock;
  }

  private AutoCloseableLock exclusiveLock(byte[] key) {
    Preconditions.checkNotNull(key);
    final int hash = Arrays.hashCode(key);
    AutoCloseableLock lock = exclusiveLocks[Math.abs(hash % parallel)];
    lock.open();
    return lock;
  }

  /**
   * Delete all values. Deletes only values inside the store, leaving behind any leftover blobs that have been placed
   * directly in the file system.
   */
  @Override
  @VisibleForTesting
  public void deleteAllValues() throws IOException {
    exclusively((deferred) -> {
      synchronized(this) {
        deleteAllIterators(deferred);

        try {
          db.dropColumnFamily(handle);
        } catch(RocksDBException ex) {
          deferred.addException(ex);
        }

        deferred.suppressingClose(handle);

        try {
          this.handle = db.createColumnFamily(family);
        } catch (Exception ex) {
          deferred.addException(ex);
        }
      }
    });
  }

  @Override
  public byte[] get(byte[] key) {
    try (AutoCloseableLock ac = sharedLock(key)) {
      throwIfClosed();
      return resolvePtrOrValue(db.get(handle, key));
    } catch (RocksDBException | BlobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  long openedIterators() {
    return openedIterators.get();
  }

  @VisibleForTesting
  long closedIterators() {
    return closedIterators.get();
  }

  @VisibleForTesting
  long gcIterators() {
    return gcIterators.get();
  }

  @VisibleForTesting
  int currentlyOpenIterators() {
    return iteratorSet.size();
  }

  @VisibleForTesting
  void cleanReferences() {
    IteratorReference ref;
    while ((ref = (IteratorReference) iteratorQueue.poll()) != null) {
      gcIterators.incrementAndGet();
      ref.close();
    }
  }

  private void deleteAllIterators(DeferredException ex) {
    // It is "safe" to iterate while adding/removing entries (also changes might not be visible)
    // It is not safe to have two threads iterating at the same time
    synchronized(iteratorSet) {
      for(IteratorReference ref : iteratorSet){
        ex.suppressingClose(ref);
      }
    }
  }

  private interface ExclusiveOperation {
    void execute(DeferredException e) throws RocksDBException;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    if (COLLECT_METRICS) {
      MetricUtils.removeAllMetricsThatStartWith(MetricRegistry.name(METRICS_PREFIX, name));
    }

    exclusively((deferred) -> {
      deleteAllIterators(deferred);
      try(FlushOptions options = new FlushOptions()){
        options.setWaitForFlush(true);
        db.flush(options, handle);
      } catch (RocksDBException ex) {
        deferred.addException(ex);
      }
      deferred.suppressingClose(handle);
    });
  }

  private void exclusively(ExclusiveOperation operation) throws IOException {
    // Attempt to acquire all exclusive locks to limit concurrent writes occurring.
    ArrayList<AutoCloseableLock> acquiredLocks = new ArrayList<>(exclusiveLocks.length);
    for (int i = 0; i < exclusiveLocks.length; i++) {
      try {
        // We cannot ensure that all write locks can be acquired, so a best attempt must be made.
        // If lock is still held after waiting 3 seconds, continue with the lock acquisition and close.
        // Note: The data from the concurrent write cannot be guaranteed to be persisted on restart.
        if (exclusiveLocks[i].tryOpen(3L, TimeUnit.SECONDS) != null) {
          acquiredLocks.add(exclusiveLocks[i]);
        }
      } catch (InterruptedException e) {
        // Do nothing.
      }
    }

    try(DeferredException deferred = new DeferredException()) {
      try {
        operation.execute(deferred);
      } catch(RocksDBException e) {
        deferred.addException(e);
      }
      deferred.suppressingClose(AutoCloseables.all(acquiredLocks));
    } catch (IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void put(byte[] key, byte[] value) {
    if (value == null) {
      throw new NullPointerException("null values are not allowed in kvstore");
    }

    try (AutoCloseableLock ac = sharedLock(key)) {
      throwIfClosed();

      final byte[] oldValueOrPtr = db.get(handle, key);

      try (BlobHolder blob = blobManager.filterPut(value)){
        final byte[] blobOrPtrVal = blob.ptrOrValue();
        db.put(handle, key, blobOrPtrVal);
        blobManager.deleteTranslation(oldValueOrPtr);
        blob.commit();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<byte[]> get(List<byte[]> keys) {
    final List<byte[]> values = new ArrayList<>();
    for (byte[] key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override
  public boolean validateAndPut(byte[] key, byte[] newValue, ByteValidator validator) {
    if (newValue == null) {
      throw new NullPointerException("null values are not allowed in kvstore");
    }

    try (AutoCloseableLock ac = exclusiveLock(key)) {
      throwIfClosed();
      final byte[] oldValueOrPtr = db.get(handle, key);
      final byte[] oldValue = resolvePtrOrValue(oldValueOrPtr);

      if (!validator.validate(oldValue)) {
        return false;
      }

      try (BlobHolder blob = blobManager.filterPut(newValue)) {
        db.put(handle, key, blob.ptrOrValue());
        blobManager.deleteTranslation(oldValueOrPtr);
        blob.commit();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return true;
    } catch (RocksDBException | BlobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean validateAndDelete(byte[] key, ByteValidator validator) {
    try (AutoCloseableLock ac = exclusiveLock(key)) {
      throwIfClosed();
      final byte[] oldValueOrPtr = db.get(handle, key);
      final byte[] oldValue = resolvePtrOrValue(oldValueOrPtr);
      if (!validator.validate(oldValue)) {
        return false;
      }
      db.delete(handle, key);
      blobManager.deleteTranslation(oldValueOrPtr);
      return true;
    } catch (RocksDBException | BlobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean contains(byte[] key) {
    try (AutoCloseableLock ac = sharedLock(key)) {
      throwIfClosed();
      return db.get(handle, key) != null;
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(byte[] key) {
    try (AutoCloseableLock ac = sharedLock(key)) {
      throwIfClosed();
      final byte[] oldValueOrPtr = db.get(handle, key);
      if (oldValueOrPtr == null) {
        return;
      }
      db.delete(handle, key);
      blobManager.deleteTranslation(oldValueOrPtr);
    } catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Iterable<Entry<byte[], byte[]>> find(com.dremio.datastore.KVStore.FindByRange<byte[]> find) {
    cleanReferences();
    return new RockIterable(find);
  }

  @Override
  public Iterable<Entry<byte[], byte[]>> find() {
    cleanReferences();
    return new RockIterable(null);
  }

  @Override
  public void delete(byte[] key, String previousVersion) {
    throw new UnsupportedOperationException("You must use a versioned store to delete by version.");
  }

  /**
   * Allows resolving a RocksDB value that may be a blob pointer.
   *
   * @param value
   * @return the actual value
   */
  byte[] resolvePtrOrValue(byte[] value) throws BlobNotFoundException {
    return blobManager.filterGet(value);
  }

  private class RockIterable implements Iterable<Map.Entry<byte[], byte[]>> {
    private final FindByRange<byte[]> range;

    public RockIterable(FindByRange<byte[]> range) {
      this.range = range;
    }

    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
      throwIfClosed(); // check not needed during iteration as the underlying iterator is closed when store is closed
      FindByRangeIterator iterator = new FindByRangeIterator(db, handle, range, blobManager);

      // Create a new reference which will self register
      @SuppressWarnings({ "unused", "resource" })
      final IteratorReference ref = new IteratorReference(iterator);
      return iterator;
    }
  }

  private class FindByRangeIterator implements Iterator<Map.Entry<byte[], byte[]>> {

    private final RocksIterator iter;
    private final byte[] end;
    private final boolean endInclusive;
    private final BlobManager blob;

    private final DescriptiveStatistics durations;
    private final DescriptiveStatistics valueSizes;
    private final Stopwatch stopwatch;
    private long cursorSetup;
    private boolean logged = false;

    private byte[] nextKey;
    private byte[] nextValue;

    public FindByRangeIterator(RocksDB db, ColumnFamilyHandle handle, FindByRange<byte[]> range, BlobManager blob) {
      this.iter = db.newIterator(handle);
      this.end = range == null ? null : range.getEnd();
      this.endInclusive = range == null ? false : range.isEndInclusive();
      this.blob = blob;

      durations = ITERATOR_METRICS ? new DescriptiveStatistics() : null;
      valueSizes = ITERATOR_METRICS ? new DescriptiveStatistics() : null;
      stopwatch = ITERATOR_METRICS ? Stopwatch.createStarted() : null;

      // position at beginning of cursor.
      if (range != null && range.getStart() != null) {
        iter.seek(range.getStart());
        if (iter.isValid() && !range.isStartInclusive() && Arrays.equals(iter.key(), range.getStart())) {
          seekNext();
        }
      } else {
        iter.seekToFirst();
      }
      if (ITERATOR_METRICS) {
        cursorSetup = stopwatch.elapsed(TimeUnit.MICROSECONDS);
      }

      populateNext();

    }

    private void seekNext() {
      if (ITERATOR_METRICS) {
        stopwatch.reset();
        stopwatch.start();
      }

      iter.next();

      if (ITERATOR_METRICS) {
        durations.addValue(stopwatch.elapsed(TimeUnit.MICROSECONDS));
      }
    }

    private void populateNext() {
      nextKey = null;
      nextValue = null;

      if (!iter.isValid()) {
        return;
      }

      byte[] key = iter.key();
      if (end != null) {
        int comparison = UnsignedBytes.lexicographicalComparator().compare(key, end);
        if ( !(comparison < 0 || (comparison == 0 && endInclusive))) {
          // hit end key.
          return;
        }
      }

      nextKey = key;
      nextValue = iter.value();

    }

    @Override
    public boolean hasNext() {
      final boolean hasNext = nextKey != null;

      if (ITERATOR_METRICS && !logged && !hasNext && durations.getN() > 0) {

        final double totalDuration = durations.getSum();
        logger.info("Duration statistics (in microseconds) while iterating over '{}':" +
                "\n{}\n95th: {}\n99th: {}\n99.5th: {}\n99.9th: {}\n99.99th: {}\ntotal: {} (or {} ms)\nsetup: {}",
            name, durations, durations.getPercentile(95), durations.getPercentile(99), durations.getPercentile(99.5),
            durations.getPercentile(99.9), durations.getPercentile(99.99), totalDuration,
            TimeUnit.MICROSECONDS.toMillis((long) totalDuration), cursorSetup);
        if (logger.isDebugEnabled()) {
          logger.debug("All durations: {}", durations.getValues());
        }

        logger.info("Size statistics (in bytes) while iterating over '{}':" +
                "\n{}\n95th: {}\n99th: {}\n99.5th: {}\n99.9th: {}\n99.99th: {}\ntotal: {}",
            name, valueSizes, valueSizes.getPercentile(95), valueSizes.getPercentile(99),
            valueSizes.getPercentile(99.5), valueSizes.getPercentile(99.9), valueSizes.getPercentile(99.99),
            valueSizes.getSum());
        if (logger.isDebugEnabled()) {
          logger.debug("All sizes: {}", valueSizes.getValues());
        }

        logged = true;
      }

      return hasNext;
    }

    @Override
    public Entry<byte[], byte[]> next() {
      Preconditions.checkArgument(nextKey != null, "Called next() when hasNext() is false.");
      Preconditions.checkArgument(nextValue != null, "Called next() when hasNext() is false.");

      RocksEntry entry = null;
      try {
        entry = new RocksEntry(nextKey, blob.filterGet(nextValue));
      } catch (BlobNotFoundException e) {
        throw new RuntimeException(e);
      }

      if (ITERATOR_METRICS) {
        valueSizes.addValue(entry.getValue().length);
      }
      seekNext();
      populateNext();
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  private class IteratorReference extends PhantomReference<FindByRangeIterator> implements AutoCloseable {
    private final RocksIterator iter;

    public IteratorReference(FindByRangeIterator referent) {
      super(referent, iteratorQueue);
      this.iter = referent.iter;
      iteratorSet.add(this);
      openedIterators.incrementAndGet();
    }

    @Override
    public void close() {
      iter.close();
      if (iteratorSet.remove(this)) {
        closedIterators.incrementAndGet();
      }
    }
  }

  private static class RocksEntry implements Map.Entry<byte[], byte[]> {
    private final byte[] key;
    private final byte[] value;

    public RocksEntry(byte[] key, byte[] value) {
      super();
      this.key = key;
      this.value = value;
    }

    @Override
    public byte[] getKey() {
      return key;
    }

    @Override
    public byte[] getValue() {
      return value;
    }

    @Override
    public byte[] setValue(byte[] value) {
      throw new UnsupportedOperationException();
    }

  }

  static {
    RocksDB.loadLibrary();
  }

  /**
   * Interface that supports external blob storage for RocksDB.
   */
  private interface BlobManager {
    /**
     * Calculates blob storage stats for the store.
     *
     * @return stats
     */
    BlobStats getStats();

    /**
     * Handles retrieving of the value from the blob storage if needed.
     *
     * @param valueFromRocks a value from RocksDB
     * @return
     */
    byte[] filterGet(byte[] valueFromRocks) throws BlobNotFoundException;

    /**
     * Handles storing of the value into the blob storage if needed.
     *
     * @param valueToFilter a kvstore value
     * @return
     */
    BlobHolder filterPut(byte[] valueToFilter);

    /**
     * Handles deleting the value from the blob storage if needed.
     *
     * @param valueToDelete
     */
    void deleteTranslation(byte[] valueToDelete);
  }

  private static final BlobManager INLINE_BLOB_MANAGER = new BlobManager() {
    @Override
    public BlobStats getStats() {
      return null;
    }

    @Override
    public byte[] filterGet(byte[] valueFromRocks) {
      return valueFromRocks;
    }

    @Override
    public BlobHolder filterPut(byte[] valueToFilter) {
      return new Inline(valueToFilter);
    }

    @Override
    public void deleteTranslation(byte[] valueToDelete) {
    }
  };

  /**
   * A filter on the underlying RocksDB that will automatically route files beyond a certain size to the local
   * filesystem instead of inside the rocks store.
   */
  static class RocksBlobManager implements BlobManager {

    private final Path base;
    private final long filterSize;

    public RocksBlobManager(String basePath, String name, long filterSize) {
      this.base = Paths.get(basePath, BLOB_PATH, name);
      try {
        Files.createDirectories(base);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      this.filterSize = filterSize;
    }

    /**
     * Determine whether the provided bytes are an inline value or an external blob value.
     * @param bytes The value to check
     * @return true if the value is inline (not a blob)
     */
    private static boolean isInline(byte[] bytes) {
      if (bytes == null || bytes.length == 0) {
        return true;
      }

      return !(bytes[0] == BLOB_VALUE_PREFIX);
    }

    private static Path path(Path base, byte[] bytes) {
      return base.resolve(Paths.get(ptr(bytes).getPath()));
    }

    @Override
    public BlobStats getStats() {
      try {
        final Iterator<Path> iter = Files.list(base).iterator();
        long count = 0;
        long size = 0;
        while (iter.hasNext()) {
          Path p = iter.next();
          count++;
          size += Files.size(p);
        }

        return new BlobStats(count, size);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Given a value from RocksDB that is a ptr rather than a value, get the ptr.
     * @param bytes Ptr bytes with prefix.
     * @return The value read from the file.
     */
    private static BlobPointer ptr(byte[] bytes) {
      try {
        // The offset accounts for BLOB_VALUE_PREFIX, which is one byte long
        final ByteArrayInputStream baos = new ByteArrayInputStream(bytes, 1, bytes.length);
        return BlobPointer.parseDelimitedFrom(baos);
      } catch (IOException e) {
        throw new RuntimeException("Failure parsing recorded translation.", e);
      }
    }

    @Override
    public byte[] filterGet(byte[] valueFromRocks) throws BlobNotFoundException {
      if (isInline(valueFromRocks)) {
        return valueFromRocks;
      }

      try {
        final BlobPointer ptr = ptr(valueFromRocks);
        final Path path = base.resolve(Paths.get(ptr.getPath()));
        switch (ptr.getCodec()) {
        case SNAPPY: {
          try (final SnappyInputStream snappyInputStream =
                 new SnappyInputStream(Files.newInputStream(path))) {
            return IOUtils.toByteArray(snappyInputStream);
          }
        }
        case UNCOMPRESSED:
          return Files.readAllBytes(path);
        case UNKNOWN:
        default:
          throw new IllegalArgumentException("Unknown codec: " + ptr.getCodec());
      }
      } catch (NoSuchFileException e) {
        throw new BlobNotFoundException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public BlobHolder filterPut(byte[] valueToFilter) {
      if (valueToFilter.length < filterSize) {
        return new Inline(valueToFilter);
      }
      final Path file = Paths.get(UUID.randomUUID().toString() + ".blob");
      final Path path = base.resolve(file);

      logger.debug("Value size {} bytes larger than limit {} bytes - storing actual data at {}",
        valueToFilter.length, filterSize, path);

      try (final SnappyOutputStream snappyOutputStream = new SnappyOutputStream(Files.newOutputStream(path))){
        snappyOutputStream.write(valueToFilter);
        return new Blob(file);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void deleteTranslation(byte[] valueToDelete) {
      if (isInline(valueToDelete)) {
        return;
      }

      final Path path = path(base, valueToDelete);
      try {
        Files.delete(path);
      } catch (IOException ex) {
        logger.warn("Failure while attempting to delete blob: {}", path, ex);
      }
    }
  }

  private static class BlobStats {

    private final long estBlobCount;
    private final long estBlobSize;

    public BlobStats(long estBlobCount, long estBlobSize) {
      super();
      this.estBlobCount = estBlobCount;
      this.estBlobSize = estBlobSize;
    }

    public void append(StringBuilder sb) {
      sb.append("* ");
      sb.append("Estimated Blob Count: ");
      sb.append(estBlobCount);
      sb.append("\n* Estimated Blob Bytes: ");
      sb.append(estBlobSize);
      sb.append("\n");
    }
  }

  private interface BlobHolder extends AutoCloseable {

    default void commit() {}

    byte[] ptrOrValue() throws IOException;

    @Override
    default void close() {}
  }

  private static class Blob implements BlobHolder {
    private final Path path;
    private boolean committed = false;

    public Blob(Path path) {
      this.path = Preconditions.checkNotNull(path);
    }

    @Override
    public void commit() {
      committed = true;
    }

    @Override
    public byte[] ptrOrValue() throws IOException {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      baos.write(BLOB_VALUE_PREFIX);
      BlobPointer.newBuilder()
        .setPath(path.toString())
        .setCodec(Codec.SNAPPY)
        .build().writeDelimitedTo(baos);

      return baos.toByteArray();
    }

    @Override
    public void close() {
      if (!committed) {
        try {
          Files.delete(path);
        } catch (IOException ex) {
          logger.warn("Failure while attempting to delete blob: {}", path, ex);
        }
      }
    }
  }

  private static class Inline implements BlobHolder {
    private final byte[] value;

    public Inline(byte[] value) {
      super();
      this.value = value;
    }

    @Override
    public byte[] ptrOrValue() {
      return value;
    }
  }

  /**
   * Exception for blob file not found
   */
  static class BlobNotFoundException extends FileNotFoundException {
    BlobNotFoundException(NoSuchFileException e) {
      super(e.getMessage());
    }
  }
}
