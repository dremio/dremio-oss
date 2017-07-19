/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.io.IOException;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.arrow.memory.util.AutoCloseableLock;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;

/**
 * A kv implementation that uses a column family within rocksdb per named store
 * dataset. We use a single database to minimize seeks for WAL management as I
 * believe WAL is one per db in Rocks.
 *
 * Note that we use lock striping to minimize contention. Most operations are
 * shared operations (read, write). These have a shared read locks that is used
 * per stripe. However, there are some multiple operation items, notably
 * checkAndPut and checkAndDelete. These do multiple operations and need to
 * ensure a consistent viewpoint of data. As such, we grab an exclusive lock for
 * the desired key range for the life of the set of operations. We use the
 * AutoCloseableLock pattern with try-with-resources to ensure that we avoid any
 * lock leaking.
 *
 * Since the RocksDB interface is native, we need to manage native memory
 * cautiously. To this end, we manage the range iterator through the use of a
 * ReferenceQueue and parallel Set. This allows us to automatically garbarge
 * collect no-longer used iterators. Additionally, the set allows us to cleanly
 * close out the RocksDB database through the close() method.
 */
class RocksDBStore implements ByteStore {

  private ColumnFamilyHandle handle;
  private final AutoCloseableLock[] sharedLocks;
  private final AutoCloseableLock[] exclusiveLocks;

  private final ColumnFamilyDescriptor family;
  private final RocksDB db;
  private final int parallel;

  private final ReferenceQueue<FindByRangeIterator> iteratorQueue = new ReferenceQueue<>();
  private final Set<IteratorReference> iteratorSet = Sets.newConcurrentHashSet();

  private final AtomicLong openedIterators = new AtomicLong(0);
  private final AtomicLong gcIterators = new AtomicLong(0);
  private final AtomicLong closedIterators = new AtomicLong(0);

  public RocksDBStore(ColumnFamilyDescriptor family, ColumnFamilyHandle handle, RocksDB db, int stripes) {
    super();
    this.family = family;
    this.db = db;
    this.parallel = stripes;
    this.handle = handle;
    this.sharedLocks = new AutoCloseableLock[stripes];
    this.exclusiveLocks = new AutoCloseableLock[stripes];

    for (int i = 0; i < stripes; i++) {
      ReadWriteLock core = new ReentrantReadWriteLock();
      sharedLocks[i] = new AutoCloseableLock(core.readLock());
      exclusiveLocks[i] = new AutoCloseableLock(core.writeLock());
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

  @Override
  @VisibleForTesting
  public void deleteAllValues() throws IOException {
    try{
      synchronized(this) {
        deleteAllIterators();
        db.dropColumnFamily(handle);
        handle.close();
        this.handle = db.createColumnFamily(family);
      }
    }catch(RocksDBException ex){
      throw new IOException(ex);
    }
  }

  @Override
  public byte[] get(byte[] key) {
    try (AutoCloseableLock ac = sharedLock(key)) {
      return db.get(handle, key);
    } catch (RocksDBException e) {
      throw wrap(e);
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

  private void deleteAllIterators() {
    // It is "safe" to iterate while adding/removing entries (also changes might not be visible)
    // It is not safe to have two threads iterating at the same time
    synchronized(iteratorSet) {
      for(IteratorReference ref : iteratorSet){
        ref.close();
      }
    }
  }

  @Override
  public void close() {
    deleteAllIterators();
    handle.close();
  }

  @Override
  public void put(byte[] key, byte[] value) {
    if (value == null) {
      throw new NullPointerException("null values are not allowed in kvstore");
    }

    try (AutoCloseableLock ac = sharedLock(key)) {
      db.put(handle, key, value);
    } catch (RocksDBException e) {
      throw wrap(e);
    }

  }

  @Override
  public List<byte[]> get(List<byte[]> keys) {
    List<byte[]> values = new ArrayList<>();
    for (byte[] key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override
  public boolean checkAndPut(byte[] key, byte[] expectedOldValue, byte[] newValue) {
    if (newValue == null) {
      throw new NullPointerException("null values are not allowed in kvstore");
    }

    try (AutoCloseableLock ac = exclusiveLock(key)) {
      byte[] oldValue = db.get(handle, key);
      if (!Arrays.equals(oldValue, expectedOldValue)) {
        return false;
      }
      db.put(handle, key, newValue);
      return true;
    } catch (RocksDBException e) {
      throw wrap(e);
    }
  }

  @Override
  public boolean contains(byte[] key) {
    try (AutoCloseableLock ac = sharedLock(key)) {
      return db.get(handle, key) != null;
    } catch (RocksDBException e) {
      throw wrap(e);
    }
  }

  @Override
  public void delete(byte[] key) {
    try (AutoCloseableLock ac = sharedLock(key)) {
      db.remove(handle, key);
    } catch (RocksDBException e) {
      throw wrap(e);
    }
  }

  @Override
  public boolean checkAndDelete(byte[] key, byte[] expectedOldValue) {
    try (AutoCloseableLock ac = exclusiveLock(key)) {
      byte[] oldValue = db.get(handle, key);
      if (!Arrays.equals(oldValue, expectedOldValue)) {
        return false;
      }
      db.remove(handle, key);
      return true;
    } catch (RocksDBException e) {
      throw wrap(e);
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
  public void delete(byte[] key, long previousVersion) {
    throw new UnsupportedOperationException("You must use a versioned store to delete by version.");
  }

  public RuntimeException wrap(RocksDBException e) {
    return Throwables.propagate(e);
  }

  private class RockIterable implements Iterable<Map.Entry<byte[], byte[]>> {
    private final FindByRange<byte[]> range;

    public RockIterable(FindByRange<byte[]> range) {
      this.range = range;
    }

    @Override
    public Iterator<Entry<byte[], byte[]>> iterator() {
      FindByRangeIterator iterator = new FindByRangeIterator(db, handle, range);

      // Create a new reference which will self register
      @SuppressWarnings("unused")
      final IteratorReference ref = new IteratorReference(iterator);
      return iterator;
    }
  }

  private static class FindByRangeIterator implements Iterator<Map.Entry<byte[], byte[]>> {

    private final RocksIterator iter;
    private final byte[] end;
    private final boolean endInclusive;

    private byte[] nextKey;
    private byte[] nextValue;

    public FindByRangeIterator(RocksDB db, ColumnFamilyHandle handle, FindByRange<byte[]> range) {
      this.iter = db.newIterator(handle);
      this.end = range == null ? null : range.getEnd();
      this.endInclusive = range == null ? false : range.isEndInclusive();

      // position at beginning of cursor.
      if (range != null && range.getStart() != null) {
        iter.seek(range.getStart());
        if (!range.isStartInclusive() && Arrays.equals(iter.key(), range.getStart())) {
          iter.next();
        }
      } else {
        iter.seekToFirst();
      }

      populateNext();

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
      return nextKey != null;
    }

    @Override
    public Entry<byte[], byte[]> next() {
      Preconditions.checkArgument(nextKey != null, "Called next() when hasNext() is false.");
      Preconditions.checkArgument(nextValue != null, "Called next() when hasNext() is false.");

      RocksEntry entry = new RocksEntry(nextKey, nextValue);
      iter.next();
      populateNext();
      return entry;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  private class IteratorReference extends PhantomReference<FindByRangeIterator> {
    private final RocksIterator iter;

    public IteratorReference(FindByRangeIterator referent) {
      super(referent, iteratorQueue);
      this.iter = referent.iter;
      iteratorSet.add(this);
      openedIterators.incrementAndGet();
    }

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

}
