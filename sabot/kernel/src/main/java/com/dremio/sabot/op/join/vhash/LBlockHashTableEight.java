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
package com.dremio.sabot.op.join.vhash;


import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.dremio.sabot.op.common.ht2.HashComputation;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.impl.hash.HashConfigWrapper;
import com.koloboke.collect.impl.hash.LHash.SeparateKVLongKeyMixing;
import com.koloboke.collect.impl.hash.LHashCapacities;

import io.netty.util.internal.PlatformDependent;

/**
 * A hash map of longs > ints
 */
public final class LBlockHashTableEight implements AutoCloseable {
  public static final int KEY_WIDTH = 8;
  public static final int ORDINAL_WIDTH = 4;
  public static final int BLOCK_WIDTH = KEY_WIDTH + ORDINAL_WIDTH;
  public static final int BITS_IN_CHUNK = 12;
  public static final int MAX_VALUES_PER_BATCH = 1 << BITS_IN_CHUNK;
  public static final int CHUNK_OFFSET_MASK = 0xFFFFFFFF >>> (32 - BITS_IN_CHUNK);
  public static final int POSITIVE_MASK = 0x7FFFFFFF;
  public static final int NO_MATCH = -1;
  // The ordinal written in FixedBlockVector, this value should be less than 0, now it's set to -2.
  private static final int NULL_ORDINAL_IN_FIXED_BLOCK_VECTOR = -2;
  // The key value written in FixedBlockVector, this value can be any value, now it's set to 0.
  private static final int NULL_KEY_VALUE = 0;

  private final HashConfigWrapper config;
  private final BufferAllocator allocator;

  private int capacityMask;
  private int capacity;
  private int maxSize;
  private int batches;
  private int currentOrdinal;
  /* Ordinal of null key, it's initialized as NO_MATCH, which means no null key in hash table.
   * After insertion of null key, its key value in FixedBlockVector is NULL_KEY_VALUE,
   * and its ordinal in FixedBlockVector is not the actual ordinal of null key,
   * but NULL_ORDINAL_IN_FIXED_BLOCK_VECTOR. During searching a key, the key is found
   * only if key value matched and ordinal is not NULL_ORDINAL_IN_FIXED_BLOCK_VECTOR.
   */
  private int nullKeyOrdinal = NO_MATCH;
  // The index of null key in FixedBlockVector, used to optimize rehash for null key
  private int nullKeyIndex = -1;

  private long freeValue = Long.MIN_VALUE + 474747l;

  private FixedBlockVector[] fixedBlocks = new FixedBlockVector[0];
  private long tableFixedAddresses[] = new long[0];

  private int rehashCount = 0;
  private final Stopwatch rehashTimer = Stopwatch.createUnstarted();
  private final Stopwatch initTimer = Stopwatch.createUnstarted();

  public LBlockHashTableEight(HashConfig config, BufferAllocator allocator, int initialSize) {
    this.allocator = allocator;
    this.config = new HashConfigWrapper(config);
    internalInit(LHashCapacities.capacity(this.config, initialSize, false));
  }

  /**
   * Insert key into the hash table
   * @param key 64 bit key
   * @param keyHash 32 bit hashvalue
   * @return ordinal of insertion point
   *
   * Hashing is now done externally to the hash table. With support
   * for spilling, the code that handles the input data (build or probe)
   * computes hash.
   */
  public int insert(long key, int keyHash) {
    if(key == freeValue){
      changeFree();
    }
    return getOrInsert(key, true, keyHash);
  }

  /* Get the ordinal of null key
   * It's NO_MATCH if no null key
   */
  public int getNull() {
    return nullKeyOrdinal;
  }

  /* remove null key from hash table,
   * it's used to optimize rehash for null key
   */
  private void removeNull() {
    if (nullKeyOrdinal != NO_MATCH) {
      long free = this.freeValue;
      int index = nullKeyIndex;
      int chunkIndex = index >>> BITS_IN_CHUNK;
      int valueIndex = (index & CHUNK_OFFSET_MASK);
      long addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);

      PlatformDependent.putLong(addr, free);

      nullKeyOrdinal = NO_MATCH;
      nullKeyIndex = -1;
    }
  }

  /* Insert null key into hash table, return the ordinal of null key
   * If null key is already inserted, just return the ordinal of null key.
   */
  public int insertNull() {
    return insertNull(NO_MATCH);
  }

  /* Insert null key into hash table, return the ordinal of null key
   * If null key is already inserted, just return the ordinal of null key.
   * If oldNullKeyOrdinal is not NO_MATCH, which means insertNull is called from rehash
   * to re-insert null key, then use oldNullKeyOrdinal as the ordinal key of null key.
   */
  public int insertNull(int oldNullKeyOrdinal) {
    if (nullKeyOrdinal != NO_MATCH) {
      return nullKeyOrdinal;
    }

    // Search an empty element in hash table
    long free = this.freeValue;

    final int capacityMask = this.capacityMask;
    int index = hash(NULL_KEY_VALUE) & capacityMask;
    int chunkIndex = index >>> BITS_IN_CHUNK;
    int valueIndex = (index & CHUNK_OFFSET_MASK);
    long addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);

    long cur;
    while ((cur = PlatformDependent.getLong(addr)) != free) {
      index = (index - 1) & capacityMask;
      chunkIndex = index >>> BITS_IN_CHUNK;
      valueIndex = (index & CHUNK_OFFSET_MASK);
      addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);
    }

    // set the key value of null key to NULL_KEY_VALUE.
    PlatformDependent.putLong(addr, NULL_KEY_VALUE);
    // set the ordinal of null key to NULL_ORDINAL_IN_FIXED_BLOCK_VECTOR.
    PlatformDependent.putInt(addr + KEY_WIDTH, NULL_ORDINAL_IN_FIXED_BLOCK_VECTOR);

    nullKeyIndex = index;
    if (oldNullKeyOrdinal == NO_MATCH) {
      nullKeyOrdinal = currentOrdinal;
      currentOrdinal++;

      // check if can resize.
      if (currentOrdinal > maxSize) {
        tryRehashForExpansion();
      }
    } else {
      nullKeyOrdinal = oldNullKeyOrdinal;
    }

    return nullKeyOrdinal;
  }

  /**
   * Lookup key in the hash table
   * @param key 64 bit key
   * @param keyHash 32 bit hashvalue
   * @return
   *
   * Hashing is now done externally to the hash table. With support
   * for spilling, the code that handles the input data (build or probe)
   * computes hash.
   */
  public int get(long key, int keyHash) {
    return getOrInsert(key, false, keyHash);
  }

  private final int getOrInsert(long key, boolean insertNew, int keyHash) {

    long free = this.freeValue;

    final int capacityMask = this.capacityMask;
    int index = keyHash & capacityMask;
    int chunkIndex = index >>> BITS_IN_CHUNK;
    int valueIndex = (index & CHUNK_OFFSET_MASK);
    long addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);
    long cur = PlatformDependent.getLong(addr);

    while (cur != free) {
      /* If the ordinal is less than 0, the key in hash table should be null key
       * and the inserting key is not found, otherwise the inserting key is found in hash table.
       */
      if ((cur == key) && (PlatformDependent.getInt(addr + KEY_WIDTH) >= 0)) {
        return PlatformDependent.getInt(addr + KEY_WIDTH);
      } else {
        index = (index - 1) & capacityMask;
        chunkIndex = index >>> BITS_IN_CHUNK;
        valueIndex = (index & CHUNK_OFFSET_MASK);
        addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);
        cur = PlatformDependent.getLong(addr);
      }
    }

    if(!insertNew){
      return -1;
    } else {

      int insertedOrdinal = currentOrdinal;
      // set the ordinal value for the insertion.
      PlatformDependent.putLong(addr, key);
      PlatformDependent.putInt(addr + KEY_WIDTH, insertedOrdinal);

      currentOrdinal++;

      // check if can resize.
      if (currentOrdinal > maxSize) {
        tryRehashForExpansion();
      }

      return insertedOrdinal;
    }
  }

  private long changeFree(){
    long oldFree = this.freeValue;
    long newFree = nextFree(oldFree);
    final long[] controlAddrs = this.tableFixedAddresses;
    for(int batch =0; batch < controlAddrs.length; batch++){
      long addr = controlAddrs[batch];
      final long max = addr + MAX_VALUES_PER_BATCH * BLOCK_WIDTH;
      for(long oldControlAddr = addr; oldControlAddr < max; oldControlAddr += BLOCK_WIDTH){
        if(PlatformDependent.getLong(oldControlAddr) == oldFree){
          PlatformDependent.putLong(oldControlAddr, newFree);
        }
      }
    }
    this.freeValue = newFree;
    return newFree;
  }

  private long nextFree(final long oldFree){
    Random random = ThreadLocalRandom.current();
    long newFree;

    /* Search for other free value in hash table
     * The new free value should not be in hash table, and it should not be NULL_KEY_VALUE
     */
    do {
      newFree = random.nextLong();
    } while (newFree == oldFree || newFree == NULL_KEY_VALUE
      || getOrInsert(newFree, false, (int)HashComputation.computeHash(newFree)) != NO_MATCH);
    return newFree;
  }

  private final void rehash(int newCapacity) {
    final long free = this.freeValue;
    // grab old references.

    final long[] oldFixedAddrs = this.tableFixedAddresses;

    final int oldNullKeyOrdinal = nullKeyOrdinal;
    if (oldNullKeyOrdinal != NO_MATCH) {
      removeNull();
    }

    try(RollbackCloseable closer = new RollbackCloseable()){ // Close old control blocks when done rehashing.
      for(FixedBlockVector cb : this.fixedBlocks){
        closer.add(cb);
      }

      internalInit(newCapacity);
      final int capacityMask = this.capacityMask;
      final long[] newFixedAddrs = this.tableFixedAddresses;

      for(int batch = 0; batch < oldFixedAddrs.length; batch++){
        long addr = oldFixedAddrs[batch];
        final long max = addr + (MAX_VALUES_PER_BATCH * BLOCK_WIDTH);
        for(long oldFixedAddr = addr; oldFixedAddr < max; oldFixedAddr += BLOCK_WIDTH){
          long key = PlatformDependent.getLong(oldFixedAddr);

          if(key != free){
            int newIndex = hash(key) & capacityMask; // get previously computed hash and slice it.
            long newFixedAddr = newFixedAddrs[newIndex >>> BITS_IN_CHUNK] + ((newIndex & CHUNK_OFFSET_MASK) * BLOCK_WIDTH);

            if (PlatformDependent.getLong(newFixedAddr) != free) {
              while (true) {
                newIndex = (newIndex - 1) & capacityMask;
                newFixedAddr = newFixedAddrs[newIndex >>> BITS_IN_CHUNK] + ((newIndex & CHUNK_OFFSET_MASK) * BLOCK_WIDTH);
                if (PlatformDependent.getLong(newFixedAddr) == free) {
                  break;
                }
              }
            }

            final int ordinalValue = PlatformDependent.getInt(oldFixedAddr + KEY_WIDTH);
            PlatformDependent.putLong(newFixedAddr, key);
            PlatformDependent.putInt(newFixedAddr + KEY_WIDTH, ordinalValue);

          }
        }
      }

    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    if (oldNullKeyOrdinal != NO_MATCH) {
      insertNull(oldNullKeyOrdinal);
    }
  }

  private static final int hash(long key){
    return SeparateKVLongKeyMixing.mix(key);
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public String toString() {
    return "BlockHashTable";
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj;
  }

  public int size(){
    return currentOrdinal;
  }

  public int blocks(){
    return (int) Math.ceil( currentOrdinal / (MAX_VALUES_PER_BATCH * 1.0d) );
  }

  public int capacity() {
    return capacity;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(ImmutableList.copyOf(fixedBlocks));
  }

  private boolean tryRehashForExpansion() {
    int newCapacity = LHashCapacities.capacity(config, capacity(), false);
    if (newCapacity > capacity()) {
      rehashTimer.start();
      rehash(newCapacity);
      rehashCount++;
      rehashTimer.stop();
      return true;
    } else {
      return false;
    }
  }

  public long getRehashTime(TimeUnit unit){
    return rehashTimer.elapsed(unit);
  }

  public int getRehashCount(){
    return rehashCount;
  }

  private void internalInit(final int capacity) {
    // capacity is power of two.
    Preconditions.checkArgument((capacity & (capacity - 1)) == 0);
    initTimer.start();
    this.capacity = capacity;
    this.maxSize = !LHashCapacities.isMaxCapacity(capacity, false) ? config.maxSize(capacity) : capacity - 1;
    this.batches = (int) Math.ceil( capacity / (MAX_VALUES_PER_BATCH * 1.0d) );
    final FixedBlockVector[] newFixedBlocks = new FixedBlockVector[batches];
    tableFixedAddresses = new long[batches];
    capacityMask = capacity - 1;
    try(RollbackCloseable rollbackable = new RollbackCloseable()) {

      final long free = this.freeValue;
      for(int i =0; i < batches; i++){
        FixedBlockVector vector = new FixedBlockVector(allocator, BLOCK_WIDTH);
        rollbackable.add(vector);
        vector.allocateNoClear(MAX_VALUES_PER_BATCH);
        newFixedBlocks[i] = vector;
        tableFixedAddresses[i] = newFixedBlocks[i].getMemoryAddress();

        final long addr = newFixedBlocks[i].getMemoryAddress();
        final long max = addr + MAX_VALUES_PER_BATCH * BLOCK_WIDTH;
        for(long l = addr; l < max; l+= LBlockHashTableEight.BLOCK_WIDTH){
          PlatformDependent.putLong(l, free);
          PlatformDependent.putInt(l + KEY_WIDTH, NO_MATCH);
        }
      }

      this.fixedBlocks = newFixedBlocks;
      rollbackable.commit();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    initTimer.stop();
  }

}
