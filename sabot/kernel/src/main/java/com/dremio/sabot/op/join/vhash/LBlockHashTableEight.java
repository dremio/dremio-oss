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
package com.dremio.sabot.op.join.vhash;



import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.sabot.op.common.ht2.FixedBlockVector;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
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

  private final HashConfigWrapper config;
  private final BufferAllocator allocator;

  private int capacityMask;
  private int capacity;
  private int maxSize;
  private int batches;
  private int currentOrdinal;
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

  public int insert(long key) {
    if(key == freeValue){
      changeFree();
    }
    return getOrInsert(key, true);
  }

  public int get(long key) {
    return getOrInsert(key, false);
  }

  private final int getOrInsert(long key, boolean insertNew) {

    long free = this.freeValue;

    int index;
    long cur;
    final int capacityMask = this.capacityMask;
    index = SeparateKVLongKeyMixing.mix(key) & capacityMask;
    int chunkIndex = index >>> BITS_IN_CHUNK;
    int valueIndex = (index & CHUNK_OFFSET_MASK);
    long addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);

    keyAbsent:
    if ((cur = PlatformDependent.getLong(addr)) != free) {
        if (cur == key) {
          return PlatformDependent.getInt(addr + KEY_WIDTH);
        } else {
          while (true) {
              index = (index - 1) & capacityMask;
              chunkIndex = index >>> BITS_IN_CHUNK;
              valueIndex = (index & CHUNK_OFFSET_MASK);
              addr = tableFixedAddresses[chunkIndex] + (valueIndex * BLOCK_WIDTH);

              if ((cur = PlatformDependent.getLong(addr)) == free) {
                break keyAbsent;
              } else if (cur == key) {
                return PlatformDependent.getInt(addr + KEY_WIDTH);
              }
            }
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

      do {
        newFree = random.nextLong();
      } while (newFree == oldFree || getOrInsert(newFree, false) != NO_MATCH);
    return newFree;
  }

  private final void rehash(int newCapacity) {
    final long free = this.freeValue;
    // grab old references.

    final long[] oldFixedAddrs = this.tableFixedAddresses;

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
    AutoCloseables.close(FluentIterable.of((AutoCloseable[]) fixedBlocks).toList());
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
