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
package com.dremio.sabot.op.common.ht2;

import static com.koloboke.collect.impl.hash.QHashCapacities.nearestGreaterCapacity;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Streams;
import com.google.common.primitives.Longs;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.impl.hash.HashConfigWrapper;
import com.koloboke.collect.impl.hash.QHashCapacities;

import io.netty.util.internal.PlatformDependent;

/**
 * A hash table of blocks. Table is broken into a fixed block and a variable block.
 *
 * Built from the following koloboke independent implementations and customized
 * for this purpose: UpdatableQHashObjSetGO < UpdatableObjQHashSetSO < UpdatableSeparateKVObjQHashGO < UpdatableSeparateKVObjQHashSO < UpdatableQHash
 */
public final class QBlockHashTable implements AutoCloseable {
  public static final int CONTROL_WIDTH = 8;
  public static final int BITS_IN_CHUNK = 12;
  public static final int VAR_OFFSET_SIZE = 4;
  public static final int VAR_LENGTH_SIZE = 4;
  public static final int MAX_VALUES_PER_BATCH = 1 << BITS_IN_CHUNK;
  public static final int CHUNK_OFFSET_MASK = 0xFFFFFFFF >>> (32 - BITS_IN_CHUNK);
  public static final int FREE = -1; // same for both int and long.
  public static final long LFREE = -1L; // same for both int and long.
  public static final int NEGATIZE = 0x80000000;
  public static final int POSITIVE_MASK = 0x7FFFFFFF;
  public static final int NO_MATCH = -1;

  private final HashConfigWrapper config;
  private final ResizeListener listener;

  private final PivotDef pivot;
  private final int defaultVariableLengthSize;
  private final BufferAllocator allocator;
  private final boolean fixedOnly;

  private int capacity;
  private int maxSize;
  private int batches;
  private int currentOrdinal;


  private ControlBlock[] controlBlocks;
  private FixedBlockVector[] fixedBlocks = new FixedBlockVector[0];
  private VariableBlockVector[] variableBlocks = new VariableBlockVector[0];
  private long[] tableControlAddresses = new long[0];
  private long[] tableFixedAddresses = new long[0];
  private long[] openVariableAddresses = new long[0]; // current pointer where we should add values.
  private long[] initVariableAddresses = new long[0];
  private long[] maxVariableAddresses = new long[0];

  private int rehashCount = 0;
  private Stopwatch rehashTimer = Stopwatch.createUnstarted();
  private Stopwatch initTimer = Stopwatch.createUnstarted();

  public QBlockHashTable(HashConfig config, PivotDef pivot, BufferAllocator allocator, int initialSize, int defaultVariableLengthSize, ResizeListener listener) {
    this.pivot = pivot;
    this.allocator = allocator;
    this.config = new HashConfigWrapper(config);
    this.defaultVariableLengthSize = defaultVariableLengthSize;
    this.fixedOnly = pivot.getVariableCount() == 0;
    this.listener = listener;
    internalInit(QHashCapacities.capacity(this.config, initialSize, false));
  }

  /**
   * Add or find a key. Returns the ordinal of the key in the table.
   * @param keyFixedVectorAddr
   * @param keyVarVectorAddr
   * @param keyIndex
   * @return ordinal of inserted key.
   */
  public final int add(final long keyFixedVectorAddr, final long keyVarVectorAddr, final int keyIndex) {
    return getOrInsert(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, true);
  }

  public final int find(final long keyFixedVectorAddr, final long keyVarVectorAddr, final int keyIndex) {
    return getOrInsert(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, false);
  }

  @SuppressWarnings("checkstyle:InnerAssignment") // complex legacy code
  private final int getOrInsert(final long keyFixedVectorAddr, final long keyVarVectorAddr, final int keyIndex, boolean insertNew) {
    final int blockWidth = pivot.getBlockWidth();
    final int capacity = this.capacity;
    final boolean fixedOnly = this.fixedOnly;
    final long keyFixedAddr = keyFixedVectorAddr + (blockWidth * keyIndex);

    final long[] tableControlAddresses = this.tableControlAddresses;
    final long[] tableFixedAddresses = this.tableFixedAddresses;
    final long[] initVariableAddresses = this.initVariableAddresses;

    final long keyVarAddr;
    final int keyVarLen;
    final int keyHash;
    final int dataWidth;

    if(fixedOnly){
      dataWidth = blockWidth;
      keyVarAddr = -1;
      keyVarLen = 0;
      keyHash = fixedKeyHashCode(keyFixedAddr, dataWidth);
    } else {
      dataWidth = blockWidth - VAR_OFFSET_SIZE;
      keyVarAddr = keyVarVectorAddr + PlatformDependent.getInt(keyFixedAddr + dataWidth);
      keyVarLen = PlatformDependent.getInt(keyVarAddr);
      keyHash = keyHashCode(keyFixedAddr, dataWidth, keyVarAddr, keyVarLen);
    }

    // start with a hash index.
    int controlIndex = keyHash % capacity;

    int controlChunkIndex = controlIndex >>> BITS_IN_CHUNK;
    long tableControlAddr = tableControlAddresses[controlChunkIndex] + ((controlIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);

    long control = PlatformDependent.getLong(tableControlAddr);

    keyAbsent: if (control != LFREE) {
      int dataChunkIndex;
      long tableDataAddr;

      int ordinal = (int) control;
      int tableHash = (int) (control >>> 32);

      if (keyHash == tableHash){
        // collision, check equality
        dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
        tableDataAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
        if(fixedKeyEquals(keyFixedAddr, tableDataAddr, dataWidth) && (fixedOnly || variableKeyEquals(keyVarAddr, initVariableAddresses[dataChunkIndex] + PlatformDependent.getInt(tableDataAddr + dataWidth), keyVarLen))){
          return ordinal;
        }
      }

      int backIndex = controlIndex, forwardIndex = controlIndex, step = 1;
      while (true) {
        // if we've rolled back to far, roll over to end of positions.
        if ((backIndex -= step) < 0) {
          backIndex += capacity;
        }

        // try to find with backIndex. //
        // calculate batch offset.
        controlChunkIndex = backIndex >>> BITS_IN_CHUNK;
        tableControlAddr = tableControlAddresses[controlChunkIndex] + ((backIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);
        control = PlatformDependent.getLong(tableControlAddr);
        tableHash = (int) (control >>> 32);
        if (control == LFREE) {
          break keyAbsent;
        } else if(keyHash == tableHash){
          ordinal = (int) control;
          dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
          tableDataAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
          if(fixedKeyEquals(keyFixedAddr, tableDataAddr, dataWidth) && (fixedOnly || variableKeyEquals(keyVarAddr, initVariableAddresses[dataChunkIndex] + PlatformDependent.getInt(tableDataAddr + dataWidth), keyVarLen))){
            // key is present
            return ordinal;
          }
        }

        int t;

        // roll over if negative
        if ((t = (forwardIndex += step) - capacity) >= 0) {
          forwardIndex = t;
        }

        // try to find with forwardIndex. //
        // calculate batch offset.
        controlChunkIndex = forwardIndex >>> BITS_IN_CHUNK;
        tableControlAddr = tableControlAddresses[controlChunkIndex] + ((forwardIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);
        control = PlatformDependent.getLong(tableControlAddr);
        tableHash = (int) (control >>> 32);

        if (control == LFREE) {
          break keyAbsent;
        } else if(keyHash == tableHash){
          ordinal = (int) control;
          dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
          tableDataAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
          if(fixedKeyEquals(keyFixedAddr, tableDataAddr, dataWidth) && (fixedOnly || variableKeyEquals(keyVarAddr, initVariableAddresses[dataChunkIndex] + PlatformDependent.getInt(tableDataAddr + dataWidth), keyVarLen))){
            // key is present
            return ordinal;
          }
        }
        step += 2;
      }
    }

    if(!insertNew){
      return -1;
    } else {
      // key is absent, let's insert.
      return insert(blockWidth, tableControlAddr, keyHash, dataWidth, keyFixedAddr, keyVarAddr, keyVarLen);
    }

  }

  private int insert(final long blockWidth, long tableControlAddr, final int keyHash, final int dataWidth, final long keyFixedAddr, final long keyVarAddr, final int keyVarLen){

    final int insertedOrdinal = currentOrdinal;

    // let's make sure we have space to write.
    if((insertedOrdinal & CHUNK_OFFSET_MASK) == 0){
      addDataBlocks();
    }

    // first we need to make sure we are up to date on the
    final int dataChunkIndex = insertedOrdinal >>> BITS_IN_CHUNK;
    final long tableDataAddr = tableFixedAddresses[dataChunkIndex] + ((insertedOrdinal & CHUNK_OFFSET_MASK) * blockWidth);

    // set the ordinal value for the insertion.
    PlatformDependent.putInt(tableControlAddr, insertedOrdinal);
    PlatformDependent.putInt(tableControlAddr + 4, keyHash);

    // set the body part of the fixed data in the table.
    Copier.copy(keyFixedAddr, tableDataAddr, dataWidth);

    if(!fixedOnly) {
      long tableVarAddr = openVariableAddresses[dataChunkIndex];
      final VariableBlockVector block = variableBlocks[dataChunkIndex];
      final int tableVarOffset = (int) (tableVarAddr - initVariableAddresses[dataChunkIndex]);

      // set the var offset in the fixed position at the variable info offset.
      PlatformDependent.putInt(tableDataAddr + dataWidth, tableVarOffset);

      // now do variable length value.
      // we'll set the variable value first so we can write the fixed data in a straight insertion.
      if(maxVariableAddresses[dataChunkIndex] < tableVarAddr + keyVarLen + 4){
        block.ensureAvailableDataSpace(tableVarOffset + keyVarLen + 4);
        tableVarAddr = block.getMemoryAddress() + tableVarOffset;
        this.initVariableAddresses[dataChunkIndex] = block.getMemoryAddress();
        this.openVariableAddresses[dataChunkIndex] = block.getMemoryAddress() + tableVarOffset;
        maxVariableAddresses[dataChunkIndex] = block.getMaxMemoryAddress();
      }

      // copy variable data.
      int size = keyVarLen + VAR_LENGTH_SIZE;
      Copier.copy(keyVarAddr, tableVarAddr, size);
      this.openVariableAddresses[dataChunkIndex] += size;
    }

    currentOrdinal++;

    // check if can resize.
    if (currentOrdinal > maxSize) {
      tryRehashForExpansion();
    }

    return insertedOrdinal;
  }

  private void addDataBlocks() {

    try(RollbackCloseable rollbackable = new RollbackCloseable()) {

      // make sure can fit the next batch.
      listener.addBatch();

      {
        FixedBlockVector newFixed = new FixedBlockVector(allocator, pivot.getBlockWidth());
        rollbackable.add(newFixed);
        newFixed.ensureAvailableBlocks(MAX_VALUES_PER_BATCH);
        fixedBlocks = ObjectArrays.concat(fixedBlocks, newFixed);
        tableFixedAddresses = Longs.concat(tableFixedAddresses, new long[]{newFixed.getMemoryAddress()});
      }

      {
        VariableBlockVector newVariable = new VariableBlockVector(allocator, pivot.getVariableCount());
        rollbackable.add(newVariable);
        newVariable.ensureAvailableDataSpace(pivot.getVariableCount() == 0 ? 0 : MAX_VALUES_PER_BATCH * defaultVariableLengthSize);
        variableBlocks = ObjectArrays.concat(variableBlocks, newVariable);
        initVariableAddresses = Longs.concat(initVariableAddresses, new long[]{newVariable.getMemoryAddress()});
        openVariableAddresses = Longs.concat(openVariableAddresses, new long[]{newVariable.getMemoryAddress()});
        maxVariableAddresses = Longs.concat(maxVariableAddresses, new long[]{newVariable.getMaxMemoryAddress()});
      }
      rollbackable.commit();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @SuppressWarnings("checkstyle:InnerAssignment") // complex legacy code
  private final void rehash(int newCapacity) {

    // grab old references.
    final ControlBlock[] oldControlBlocks = this.controlBlocks;
    final long[] oldControlAddrs = this.tableControlAddresses;

    try(RollbackCloseable closer = new RollbackCloseable()){ // Close old control blocks when done rehashing.
      for(ControlBlock cb : oldControlBlocks){
        closer.add(cb);
      }

      internalInit(newCapacity);

      final int capacity = this.capacity;
      final long[] controlAddrs = this.tableControlAddresses;

      // loop through backwards.

      for(int batch =0; batch < oldControlAddrs.length; batch++){
        long addr = oldControlAddrs[batch];
        final long max = addr + MAX_VALUES_PER_BATCH * CONTROL_WIDTH;
        for(long oldControlAddr = addr; oldControlAddr < max; oldControlAddr += CONTROL_WIDTH){
          long oldControl = PlatformDependent.getLong(oldControlAddr);

          if(oldControl != LFREE){

            int index = ((int) (oldControl >>> 32)) % capacity; // get previously computed hash and mod it.
            int newChunkIndex = index >>> BITS_IN_CHUNK;
            long controlAddr = controlAddrs[newChunkIndex] + ((index & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);

            if (PlatformDependent.getInt(controlAddr) != FREE) {
              int backIndex = index, forwardIndex = index, step = 1;
              while (true) {
                if ((backIndex -= step) < 0) {
                  backIndex += capacity;
                }

                newChunkIndex = backIndex >>> BITS_IN_CHUNK;
                controlAddr = controlAddrs[newChunkIndex] + ((backIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);

                if (PlatformDependent.getInt(controlAddr) == FREE) {
                  break;
                }
                int t;
                if ((t = (forwardIndex += step) - capacity) >= 0) {
                  forwardIndex = t;
                }

                newChunkIndex = forwardIndex >>> BITS_IN_CHUNK;
                controlAddr = controlAddrs[newChunkIndex] + ((forwardIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);

                if (PlatformDependent.getInt(controlAddr) == FREE) {
                  break;
                }
                step += 2;

              }
            }
            PlatformDependent.putLong(controlAddr, oldControl);
          }
        }
      }


    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private static final boolean fixedKeyEquals(
      final long keyDataAddr,
      final long tableDataAddr,
      final int dataWidth
      ) {
    return memEqual(keyDataAddr, tableDataAddr, dataWidth);
  }

  private static final boolean variableKeyEquals(
      final long keyVarAddr,
      final long tableVarAddr,
      final int keyVarLength
      ) {
    final int tableVarLength = PlatformDependent.getInt(tableVarAddr);
    return keyVarLength == tableVarLength && memEqual(keyVarAddr + VAR_LENGTH_SIZE, tableVarAddr + VAR_LENGTH_SIZE, keyVarLength);
  }

  private static final int fixedKeyHashCode(long keyDataAddr, int dataWidth){
    return mix(XXH64.xxHash6432(keyDataAddr, dataWidth, 0));
    //return mix(XXHashByteBuf.hashAddr(keyDataAddr, dataWidth, 0));
  }

  private static final int keyHashCode(long keyDataAddr, int dataWidth, final long keyVarAddr, int varDataLen){
    final long fixedValue = XXH64.xxHash64(keyDataAddr, dataWidth, 0);
    return mix(XXH64.xxHash6432(keyVarAddr + 4, varDataLen, fixedValue));

//    final int fixedValue = XXHashByteBuf.hashAddr(keyDataAddr, dataWidth, 0);
//    if(varDataLen == 0){
//      return fixedValue;
//    }
//    return mix(XXHashByteBuf.hashAddr(keyVarAddr + 4, varDataLen, fixedValue));
  }

  private static final boolean memEqual(final long laddr, final long raddr, int len) {
    int n = len;
    long lPos = laddr;
    long rPos = raddr;

    while (n > 7) {
      long leftLong = PlatformDependent.getLong(lPos);
      long rightLong = PlatformDependent.getLong(rPos);
      if (leftLong != rightLong) {
        return false;
      }
      lPos += 8;
      rPos += 8;
      n -= 8;
    }
    while (n > 3) {
      int leftInt = PlatformDependent.getInt(lPos);
      int rightInt = PlatformDependent.getInt(rPos);
      if (leftInt != rightInt) {
        return false;
      }
      lPos += 4;
      rPos += 4;
      n -= 4;
    }
    while (n-- != 0) {
      byte leftByte = PlatformDependent.getByte(lPos);
      byte rightByte = PlatformDependent.getByte(rPos);
      if (leftByte != rightByte) {
        return false;
      }
      lPos++;
      rPos++;
    }
    return true;
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

  /**
   * Taken directly from koloboke
   */
  private static int mix(int hash) {
    return (hash & 0x7FFFFFFF);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(
      Streams.concat(
        Arrays.stream(controlBlocks),
        Arrays.stream(fixedBlocks),
        Arrays.stream(variableBlocks)
      ).collect(ImmutableList.toImmutableList())
    );
  }

  private boolean tryRehashForExpansion() {
    int newCapacity = nearestGreaterCapacity(config.grow(capacity()), currentOrdinal, false);
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
    initTimer.start();
    this.capacity = capacity;
    this.maxSize = !QHashCapacities.isMaxCapacity(capacity, false) ? config.maxSize(capacity) : capacity - 1;
    this.batches = (int) Math.ceil( capacity / (MAX_VALUES_PER_BATCH * 1.0d) );
    final ControlBlock[] newControlBlocks = new ControlBlock[batches];
    tableControlAddresses = new long[batches];

    try(RollbackCloseable rollbackable = new RollbackCloseable()) {

      for(int i =0; i < batches; i++){
        newControlBlocks[i] = new ControlBlock(allocator, MAX_VALUES_PER_BATCH);
        rollbackable.add(newControlBlocks[i]);
        tableControlAddresses[i] = newControlBlocks[i].getMemoryAddress();

        final long addr = newControlBlocks[i].getMemoryAddress();
        final long max = addr + MAX_VALUES_PER_BATCH * CONTROL_WIDTH;
        for(long l = addr; l < max; l+= QBlockHashTable.CONTROL_WIDTH){
          PlatformDependent.putLong(l, QBlockHashTable.LFREE);
        }
      }

      this.controlBlocks = newControlBlocks;
      rollbackable.commit();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    initTimer.stop();
  }
}
