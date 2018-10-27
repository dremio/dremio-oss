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
package com.dremio.sabot.op.common.ht2;



import java.util.Formatter;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.ObjectArrays;
import com.google.common.primitives.Longs;
import com.koloboke.collect.hash.HashConfig;
import com.koloboke.collect.impl.hash.HashConfigWrapper;
import com.koloboke.collect.impl.hash.LHashCapacities;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * A hash table of blocks. Table is broken into a fixed block and a variable block.
 *
 * Built from the following koloboke independent implementations and customized
 * for this purpose: UpdatableQHashObjSetGO < UpdatableObjQHashSetSO < UpdatableSeparateKVObjQHashGO < UpdatableSeparateKVObjQHashSO < UpdatableQHash
 */
public final class LBlockHashTableNoSpill implements AutoCloseable {
  public static final int CONTROL_WIDTH = 8;
  public static final int BITS_IN_CHUNK = 12;
  public static final int VAR_OFFSET_SIZE = 4;
  public static final int VAR_LENGTH_SIZE = 4;
  public static final int MAX_VALUES_PER_BATCH = 1 << BITS_IN_CHUNK;
  public static final int CHUNK_OFFSET_MASK = 0xFFFFFFFF >>> (32 - BITS_IN_CHUNK);
  public static final int FREE = -1; // same for both int and long.
  public static final long LFREE = -1l; // same for both int and long.

  public static final int NEGATIZE = 0x80000000;
  public static final int POSITIVE_MASK = 0x7FFFFFFF;
  public static final int NO_MATCH = -1;

  private static final int RETRY_RETURN_CODE = -2;
  public static final int ORDINAL_SIZE = 4;


  private final HashConfigWrapper config;
  private final ResizeListener listener;

  private final PivotDef pivot;
  private final int defaultVariableLengthSize;
  private final BufferAllocator allocator;
  private final boolean fixedOnly;

  private int capacityMask;
  private int capacity;
  private int maxSize;
  private int batches;
  private int currentOrdinal;


  private ControlBlock[] controlBlocks;
  private FixedBlockVector[] fixedBlocks = new FixedBlockVector[0];
  private VariableBlockVector[] variableBlocks = new VariableBlockVector[0];
  private long tableControlAddresses[] = new long[0];
  private long tableFixedAddresses[] = new long[0];
  private long openVariableAddresses[] = new long[0]; // current pointer where we should add values.
  private long initVariableAddresses[] = new long[0];
  private long maxVariableAddresses[] = new long[0];

  private int rehashCount = 0;
  private Stopwatch rehashTimer = Stopwatch.createUnstarted();
  private Stopwatch initTimer = Stopwatch.createUnstarted();

  private ArrowBuf traceBuf;
  private long traceBufNext;

  public LBlockHashTableNoSpill(HashConfig config, PivotDef pivot, BufferAllocator allocator, int initialSize, int defaultVariableLengthSize, ResizeListener listener) {
    this.pivot = pivot;
    this.allocator = allocator;
    this.config = new HashConfigWrapper(config);
    this.defaultVariableLengthSize = defaultVariableLengthSize;
    this.fixedOnly = pivot.getVariableCount() == 0;
    this.listener = listener;
    internalInit(LHashCapacities.capacity(this.config, initialSize, false));
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
    int controlIndex = keyHash & capacityMask;

    int controlChunkIndex = controlIndex >>> BITS_IN_CHUNK;
    long tableControlAddr = tableControlAddresses[controlChunkIndex] + ((controlIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);
    long control = PlatformDependent.getLong(tableControlAddr);

    keyAbsent: if (control != LFREE) {
      int dataChunkIndex;
      long tableDataAddr;

      int ordinal = (int) control;
      if (keyHash == (int) (control >>> 32)){
        dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
        tableDataAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
        if(fixedKeyEquals(keyFixedAddr, tableDataAddr, dataWidth) && (fixedOnly || variableKeyEquals(keyVarAddr, initVariableAddresses[dataChunkIndex] + PlatformDependent.getInt(tableDataAddr + dataWidth), keyVarLen))){
          return ordinal;
        }
      }

      while (true) {
        controlIndex = (controlIndex - 1) & capacityMask;
        controlChunkIndex = controlIndex >>> BITS_IN_CHUNK;
        tableControlAddr = tableControlAddresses[controlChunkIndex] + ((controlIndex & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);
        control = PlatformDependent.getLong(tableControlAddr);

        if (control == LFREE) {
          break keyAbsent;
        } else if(keyHash == (int) (control >>> 32)){
          ordinal = (int) control;
          dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
          tableDataAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
          if(fixedKeyEquals(keyFixedAddr, tableDataAddr, dataWidth) && (fixedOnly || variableKeyEquals(keyVarAddr, initVariableAddresses[dataChunkIndex] + PlatformDependent.getInt(tableDataAddr + dataWidth), keyVarLen))){
            // key is present
            return ordinal;
          }
        }

      }
    }

    if(!insertNew){
      return -1;
    } else {
      // key is absent, let's insert.
      return insert(blockWidth, tableControlAddr, keyHash, dataWidth, keyFixedAddr, keyVarAddr, keyVarLen);
    }

  }

  // Get the length of the variable keys for the record specified by ordinal.
  public int getVarKeyLength(int ordinal) {
    if (fixedOnly) {
      return 0;
    } else {
      final int blockWidth = pivot.getBlockWidth();
      final int dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
      final long tableVarOffsetAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth) + blockWidth - VAR_OFFSET_SIZE;
      final int tableVarOffset = PlatformDependent.getInt(tableVarOffsetAddr);
      // VAR_LENGTH_SIZE is not added to varLen when pivot it in pivotVariableLengths method, so we need to add it here
      final int varLen = PlatformDependent.getInt(initVariableAddresses[dataChunkIndex] + tableVarOffset) + VAR_LENGTH_SIZE;
      return varLen;
    }
  }


  /* Copy the keys of the records specified in keyOffsetAddr to destination memory
   * keyOffsetAddr contains all the ordinals of keys
   * count is the number of keys
   * keyFixedAddr is the destination memory for fixed keys
   * keyVarAddr is the destination memory for variable keys
   */
  public void copyKeyToBuffer(long keyOffsetAddr, final int count, long keyFixedAddr, long keyVarAddr) {
    final long maxAddr = keyOffsetAddr + count * ORDINAL_SIZE;
    final int blockWidth = pivot.getBlockWidth();
    if (fixedOnly) {
      for (; keyOffsetAddr < maxAddr; keyOffsetAddr += ORDINAL_SIZE, keyFixedAddr += blockWidth) {
        // Copy the fixed key that is pivoted in Pivots.pivot
        final int ordinal = PlatformDependent.getInt(keyOffsetAddr);
        final int dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
        final long tableFixedAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
        Copier.copy(tableFixedAddr, keyFixedAddr, blockWidth);
      }
    } else {
      int varOffset = 0;
      for (; keyOffsetAddr < maxAddr; keyOffsetAddr += ORDINAL_SIZE, keyFixedAddr += blockWidth) {
        // Copy the fixed keys that is pivoted in Pivots.pivot
        final int ordinal = PlatformDependent.getInt(keyOffsetAddr);
        final int dataChunkIndex = ordinal >>> BITS_IN_CHUNK;
        final long tableFixedAddr = tableFixedAddresses[dataChunkIndex] + ((ordinal & CHUNK_OFFSET_MASK) * blockWidth);
        Copier.copy(tableFixedAddr, keyFixedAddr, blockWidth - VAR_OFFSET_SIZE);
        // Update the variable offset of the key
        PlatformDependent.putInt(keyFixedAddr + blockWidth - VAR_OFFSET_SIZE, varOffset);

        // Copy the variable keys that is pivoted in Pivots.pivot
        final long tableVarOffsetAddr = tableFixedAddr + blockWidth - VAR_OFFSET_SIZE;
        final int tableVarOffset = PlatformDependent.getInt(tableVarOffsetAddr);
        final int varLen = PlatformDependent.getInt(initVariableAddresses[dataChunkIndex] + tableVarOffset) + VAR_LENGTH_SIZE;
        Copier.copy(initVariableAddresses[dataChunkIndex] + tableVarOffset, keyVarAddr + varOffset, varLen);

        varOffset += varLen;
      }
    }
  }

  private int insert(final long blockWidth, long tableControlAddr, final int keyHash, final int dataWidth, final long keyFixedAddr, final long keyVarAddr, final int keyVarLen){

    long traceBufAddr = 0;
    if (traceBuf != null) {
      traceBufAddr = traceBuf.memoryAddress();
    }

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

  private void addDataBlocks(){

    // make sure can fit the next batch.
    listener.addBatch();

    try(RollbackCloseable rollbackable = new RollbackCloseable()) {

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

  private final void rehash(int newCapacity) {
    // grab old references.
    final ControlBlock[] oldControlBlocks = this.controlBlocks;
    final long[] oldControlAddrs = this.tableControlAddresses;

    try(RollbackCloseable closer = new RollbackCloseable()){ // Close old control blocks when done rehashing.
      for(ControlBlock cb : oldControlBlocks){
        closer.add(cb);
      }


      internalInit(newCapacity);

      final int capacityMask = this.capacityMask;
      final long[] controlAddrs = this.tableControlAddresses;

      // loop through backwards.

      for(int batch =0; batch < oldControlAddrs.length; batch++){
        long addr = oldControlAddrs[batch];
        final long max = addr + MAX_VALUES_PER_BATCH * CONTROL_WIDTH;
        for(long oldControlAddr = addr; oldControlAddr < max; oldControlAddr += CONTROL_WIDTH){
          long oldControl = PlatformDependent.getLong(oldControlAddr);

          if(oldControl != LFREE){
            int index = ((int) (oldControl >>> 32)) & capacityMask; // get previously computed hash and slice it.
            int newChunkIndex = index >>> BITS_IN_CHUNK;
            long controlAddr = controlAddrs[newChunkIndex] + ((index & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);

            if (PlatformDependent.getInt(controlAddr) != FREE) {
              while (true) {
                index = (index - 1) & capacityMask;
                newChunkIndex = index >>> BITS_IN_CHUNK;
                controlAddr = controlAddrs[newChunkIndex] + ((index & CHUNK_OFFSET_MASK) * CONTROL_WIDTH);
                if (PlatformDependent.getInt(controlAddr) == FREE) {
                  break;
                }
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

  public int hashCode() {
    return System.identityHashCode(this);
  }

  public String toString() {
    return "BlockHashTable";
  }

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
    AutoCloseables.close((Iterable<AutoCloseable>) Iterables.concat(FluentIterable.of(controlBlocks).toList(), FluentIterable.of(fixedBlocks).toList(), FluentIterable.of(variableBlocks).toList()));
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
    assert (capacity & (capacity - 1)) == 0;
    initTimer.start();
    this.capacity = capacity;
    this.maxSize = !LHashCapacities.isMaxCapacity(capacity, false) ? config.maxSize(capacity) : capacity - 1;
    this.batches = (int) Math.ceil( capacity / (MAX_VALUES_PER_BATCH * 1.0d) );
    final ControlBlock[] newControlBlocks = new ControlBlock[batches];
    tableControlAddresses = new long[batches];
    capacityMask = capacity - 1;
    try(RollbackCloseable rollbackable = new RollbackCloseable()) {

      for(int i =0; i < batches; i++){
        newControlBlocks[i] = new ControlBlock(allocator, MAX_VALUES_PER_BATCH);
        rollbackable.add(newControlBlocks[i]);
        tableControlAddresses[i] = newControlBlocks[i].getMemoryAddress();

        final long addr = newControlBlocks[i].getMemoryAddress();
        final long max = addr + MAX_VALUES_PER_BATCH * CONTROL_WIDTH;
        for(long l = addr; l < max; l+= LBlockHashTable.CONTROL_WIDTH){
          PlatformDependent.putLong(l, LBlockHashTable.LFREE);
        }
      }

      this.controlBlocks = newControlBlocks;
      rollbackable.commit();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    initTimer.stop();
  }

  public void unpivot(int batchIndex, int count){
    Unpivots.unpivot(pivot, fixedBlocks[batchIndex], variableBlocks[batchIndex], 0, count);
  }

  // Tracing support
  // When tracing is started, the hashtable allocates an ArrowBuf that will contain the following information:
  // 1. hashtable state before the insertion (recorded at {@link #traceStartInsert()})
  //       | int capacityMask | int capacity | int maxSize | int batches | int currentOrdinal | int rehashCount |
  //               4B                4B             4B            4B              4B                   4B
  //
  // 2. insertion record:
  //    - an int containing the count of insertions
  //       | int numInsertions |
  //               4B
  //    - an entry for each record being inserted into the hash table
  //       | match? |  batch# | ordinal-within-batch |      4 bytes
  //          1 bit    15 bits               16 bits
  // Please note that the batch# and ordinal-within-batch together form the hash table's currentOrdinal
  //
  // 3. hashtable state after the insertion
  //    (same structure as #1)

  /**
   * Start tracing the insert() operation of this table
   */
  public void traceStart(int numRecords) {
    int numEntries = 6 * 2 + 1 + numRecords;
    traceBuf = allocator.buffer(numEntries * 4);
    traceBufNext = traceBuf.memoryAddress();
  }

  /**
   * Stop tracing the insert(), and release any buffers that were allocated
   */
  public void traceEnd() {
    traceBuf.release();
    traceBuf = null;
    traceBufNext = 0;
  }

  public void traceOrdinals(final long indexes, final int numRecords) {
    if (traceBuf == null) {
      return;
    }
    PlatformDependent.copyMemory(indexes, traceBufNext, 4 * numRecords);
    traceBufNext += 4 * numRecords;
  }

  /**
   * Start of insertion. Record the state before insertion
   */
  public void traceInsertStart(int numRecords) {
    if (traceBuf == null) {
      return;
    }
    PlatformDependent.putInt(traceBufNext + 0 * 4, capacityMask);
    PlatformDependent.putInt(traceBufNext + 1 * 4, capacity);
    PlatformDependent.putInt(traceBufNext + 2 * 4, maxSize);
    PlatformDependent.putInt(traceBufNext + 3 * 4, batches);
    PlatformDependent.putInt(traceBufNext + 4 * 4, currentOrdinal);
    PlatformDependent.putInt(traceBufNext + 5 * 4, rehashCount);

    PlatformDependent.putInt(traceBufNext + 6 * 4, numRecords);
    traceBufNext += 7 * 4;
  }

  /**
   * End of insertion. Record the state after insertion
   */
  public void traceInsertEnd() {
    if (traceBuf == null) {
      return;
    }
    PlatformDependent.putInt(traceBufNext + 0 * 4, capacityMask);
    PlatformDependent.putInt(traceBufNext + 1 * 4, capacity);
    PlatformDependent.putInt(traceBufNext + 2 * 4, maxSize);
    PlatformDependent.putInt(traceBufNext + 3 * 4, batches);
    PlatformDependent.putInt(traceBufNext + 4 * 4, currentOrdinal);
    PlatformDependent.putInt(traceBufNext + 5 * 4, rehashCount);
    traceBufNext += 6 * 4;
  }

  /**
   * Report results from the tracing. Typically invoked when there was an error, since it generates a boatload of log messages
   */
  public String traceReport() {
    if (traceBuf == null) {
      return "";
    }

    long traceBufAddr = traceBuf.memoryAddress();
    int numEntries = PlatformDependent.getInt(traceBufAddr + 6 * 4);

    int reportSize = 17 * numEntries + 1024;  // it's really 16 bytes tops per entry, with a newline every 16 entries
    StringBuilder sb = new StringBuilder(reportSize);
    Formatter formatter = new Formatter(sb);
    int origOrdinal = PlatformDependent.getInt(traceBufAddr + 4 * 4) - 1;
    formatter.format("Pre-insert: capacity: %1$d, capacityMask: %2$#X, maxSize: %3$d, batches: %4$d (%4$#X), currentOrdinal: %5$d, rehashCount: %6$d %n",
                     PlatformDependent.getInt(traceBufAddr + 0 * 4),
                     PlatformDependent.getInt(traceBufAddr + 1 * 4),
                     PlatformDependent.getInt(traceBufAddr + 2 * 4),
                     PlatformDependent.getInt(traceBufAddr + 3 * 4),
                     PlatformDependent.getInt(traceBufAddr + 4 * 4),
                     PlatformDependent.getInt(traceBufAddr + 5 * 4));

    long traceBufCurr = traceBufAddr + 7 * 4;
    long traceBufLast = traceBufCurr + numEntries * 4;
    formatter.format("Number of entries: %1$d%n", numEntries);
    for (int i = 0; traceBufCurr < traceBufLast; traceBufCurr += 4, i++) {
      int traceValue = PlatformDependent.getInt(traceBufCurr);
      boolean isInsert = false;
      if (traceValue > origOrdinal) {
        isInsert = true;
        origOrdinal = traceValue;
      }
      formatter.format("%1$c(%2$d,%3$d)",
                       isInsert ? 'i' : 'm', (traceValue & 0xffff0000) >>> 16, (traceValue & 0x0000ffff));
      if ((i % 16) == 15) {
        formatter.format("%n");
      } else if (traceBufCurr < traceBufLast - 4) {
        formatter.format(", ");
      }
    }
    if ((numEntries % 16) != 0) {
      formatter.format("%n");
    }

    formatter.format("Post-insert: capacity: %1$d, capacityMask: %2$#X, maxSize: %3$d, batches: %4$d (%4$#X), currentOrdinal: %5$d, rehashCount: %6$d %n",
                     PlatformDependent.getInt(traceBufLast + 0 * 4),
                     PlatformDependent.getInt(traceBufLast + 1 * 4),
                     PlatformDependent.getInt(traceBufLast + 2 * 4),
                     PlatformDependent.getInt(traceBufLast + 3 * 4),
                     PlatformDependent.getInt(traceBufLast + 4 * 4),
                     PlatformDependent.getInt(traceBufLast + 5 * 4));
    return sb.toString();
  }

}
