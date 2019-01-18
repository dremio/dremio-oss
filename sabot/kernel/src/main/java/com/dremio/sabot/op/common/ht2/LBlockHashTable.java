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

import static java.util.Arrays.asList;
import static java.util.Arrays.copyOfRange;

import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.util.Numbers;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
public final class LBlockHashTable implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(LBlockHashTable.class);
  public static final int CONTROL_WIDTH = 8;
  public static final int VAR_OFFSET_SIZE = 4;
  public static final int VAR_LENGTH_SIZE = 4;
  public static final int FREE = -1; // same for both int and long.
  public static final long LFREE = -1l; // same for both int and long.

  private static final int RETRY_RETURN_CODE = -2;
  public static final int ORDINAL_SIZE = 4;

  private final HashConfigWrapper config;
  private final ResizeListener listener;

  private final PivotDef pivot;
  private final BufferAllocator allocator;
  private final boolean fixedOnly;
  private final boolean enforceVarWidthBufferLimit; // may not need this option once HashJoin spilling is implemented.

  private int capacity;
  private int maxSize;
  private int batches;

  private int currentOrdinal;
  /**
   * Number of gaps in ordinal space. We skip the ordinals left in current batch when the variable width buffer
   * reaches max limit
   */
  private int gaps;

  private final int variableBlockMaxLength;
  public final int MAX_VALUES_PER_BATCH;

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

  private boolean preallocatedSingleBatch;

  private long allocatedForFixedBlocks;
  private long allocatedForVarBlocks;
  private long unusedForFixedBlocks;
  private long unusedForVarBlocks;

  public LBlockHashTable(HashConfig config,
                         PivotDef pivot,
                         BufferAllocator allocator,
                         int initialSize,
                         int defaultVariableLengthSize,
                         boolean enforceVarWidthBufferLimit,
                         ResizeListener listener,
                         final int maxHashTableBatchSize) {
    this.pivot = pivot;
    this.allocator = allocator;
    this.config = new HashConfigWrapper(config);
    this.fixedOnly = pivot.getVariableCount() == 0;
    this.enforceVarWidthBufferLimit = enforceVarWidthBufferLimit;
    this.listener = listener;
    /* maximum records that can be stored in hashtable block/chunk */
    this.MAX_VALUES_PER_BATCH = maxHashTableBatchSize;
    this.variableBlockMaxLength = (pivot.getVariableCount() == 0) ? 0 : (MAX_VALUES_PER_BATCH * (((defaultVariableLengthSize + VAR_OFFSET_SIZE) * pivot.getVariableCount()) + VAR_LENGTH_SIZE));
    this.preallocatedSingleBatch = false;
    this.allocatedForFixedBlocks = 0;
    this.allocatedForVarBlocks = 0;
    this.unusedForFixedBlocks = 0;
    this.unusedForVarBlocks = 0;
    internalInit(LHashCapacities.capacity(this.config, initialSize, false));
    logger.debug("initialized hashtable, maxSize:{}, capacity:{}, batches:{}, maxVariableBlockLength:{}, maxValuesPerBatch:{}", maxSize, capacity, batches, variableBlockMaxLength, MAX_VALUES_PER_BATCH);
  }

  public int getMaxValuesPerBatch() {
    return MAX_VALUES_PER_BATCH;
  }

  public int getVariableBlockMaxLength() {
    return variableBlockMaxLength;
  }

  private boolean addNewBlock() {
    return currentOrdinal % MAX_VALUES_PER_BATCH == 0;
  }

  private int getChunkIndexForOrdinal(final int ordinal) {
    return Math.abs(ordinal / MAX_VALUES_PER_BATCH);
  }

  private int getOffsetInChunkForOrdinal(final int ordinal) {
    return Math.abs(ordinal % MAX_VALUES_PER_BATCH);
  }

  /**
   * Add or find a key. Returns the ordinal of the key in the table.
   * @param keyFixedVectorAddr starting address of fixed vector block
   * @param keyVarVectorAddr starting address of variable vector block
   * @param keyIndex record #
   * @param keyHash hashvalue (hashing is external to the hash table)
   * @return ordinal of inserted key.
   */
  public final int add(final long keyFixedVectorAddr, final long keyVarVectorAddr,
                       final int keyIndex, final int keyHash) {
    return getOrInsertWithRetry(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash, true);
  }

  public final int find(final long keyFixedVectorAddr, final long keyVarVectorAddr,
                        final int keyIndex, final int keyHash) {
    return getOrInsertWithRetry(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash, false);
  }

  private final int getOrInsertWithRetry(final long keyFixedVectorAddr, final long keyVarVectorAddr,
                                         final int keyIndex, final int keyHash, boolean insertNew) {
    int returnValue = getOrInsert(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash, insertNew);
    if (returnValue == RETRY_RETURN_CODE) {
      returnValue = getOrInsert(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash, insertNew);
    }
    return returnValue;
  }

  private final int getOrInsert(final long keyFixedVectorAddr, final long keyVarVectorAddr,
                                final int keyIndex, final int keyHash, boolean insertNew) {
    final int blockWidth = pivot.getBlockWidth();
    final boolean fixedOnly = this.fixedOnly;
    final long keyFixedAddr = keyFixedVectorAddr + (blockWidth * keyIndex);

    final long[] tableControlAddresses = this.tableControlAddresses;
    final long[] tableFixedAddresses = this.tableFixedAddresses;
    final long[] initVariableAddresses = this.initVariableAddresses;

    final long keyVarAddr;
    final int keyVarLen;
    final int dataWidth;

    if(fixedOnly){
      dataWidth = blockWidth;
      keyVarAddr = -1;
      keyVarLen = 0;
    } else {
      dataWidth = blockWidth - VAR_OFFSET_SIZE;
      keyVarAddr = keyVarVectorAddr + PlatformDependent.getInt(keyFixedAddr + dataWidth);
      keyVarLen = PlatformDependent.getInt(keyVarAddr);
    }

    // start with a hash index.
    int controlIndex = keyHash % capacity;

    int controlChunkIndex = getChunkIndexForOrdinal(controlIndex);
    int offsetInChunk = getOffsetInChunkForOrdinal(controlIndex);
    long tableControlAddr = tableControlAddresses[controlChunkIndex] + (offsetInChunk * CONTROL_WIDTH);
    long control = PlatformDependent.getLong(tableControlAddr);

    keyAbsent: if (control != LFREE) {
      int dataChunkIndex;
      long tableDataAddr;

      int ordinal = (int) control;
      if (keyHash == (int) (control >>> 32)){
        dataChunkIndex = getChunkIndexForOrdinal(ordinal);
        offsetInChunk = getOffsetInChunkForOrdinal(ordinal);
        tableDataAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * blockWidth);
        if(fixedKeyEquals(keyFixedAddr, tableDataAddr, dataWidth) && (fixedOnly || variableKeyEquals(keyVarAddr, initVariableAddresses[dataChunkIndex] + PlatformDependent.getInt(tableDataAddr + dataWidth), keyVarLen))){
          return ordinal;
        }
      }

      while (true) {
        controlIndex = (controlIndex - 1) % capacity;
        controlChunkIndex = getChunkIndexForOrdinal(controlIndex);
        offsetInChunk = getOffsetInChunkForOrdinal(controlIndex);
        tableControlAddr = tableControlAddresses[controlChunkIndex] + (offsetInChunk * CONTROL_WIDTH);
        control = PlatformDependent.getLong(tableControlAddr);

        if (control == LFREE) {
          break keyAbsent;
        } else if(keyHash == (int) (control >>> 32)){
          ordinal = (int) control;
          dataChunkIndex = getChunkIndexForOrdinal(ordinal);
          offsetInChunk = getOffsetInChunkForOrdinal(ordinal);
          tableDataAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * blockWidth);
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
      final int dataChunkIndex = getChunkIndexForOrdinal(ordinal);
      final int offsetInChunk = getOffsetInChunkForOrdinal(ordinal);
      final long tableVarOffsetAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * blockWidth) + blockWidth - VAR_OFFSET_SIZE;
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
        final int dataChunkIndex = getChunkIndexForOrdinal(ordinal);
        final int offsetInChunk = getOffsetInChunkForOrdinal(ordinal);
        final long tableFixedAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * blockWidth);
        Copier.copy(tableFixedAddr, keyFixedAddr, blockWidth);
      }
    } else {
      int varOffset = 0;
      for (; keyOffsetAddr < maxAddr; keyOffsetAddr += ORDINAL_SIZE, keyFixedAddr += blockWidth) {
        // Copy the fixed keys that is pivoted in Pivots.pivot
        final int ordinal = PlatformDependent.getInt(keyOffsetAddr);
        final int dataChunkIndex = getChunkIndexForOrdinal(ordinal);
        final int offsetInChunk = getOffsetInChunkForOrdinal(ordinal);
        final long tableFixedAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * blockWidth);
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

  public long getAllocatedForFixedBlocks() {
    return allocatedForFixedBlocks;
  }

  public long getUnusedForFixedBlocks() {
    return unusedForFixedBlocks;
  }

  public long getAllocatedForVarBlocks() {
    return allocatedForVarBlocks;
  }

  public long getUnusedForVarBlocks() {
    return unusedForVarBlocks;
  }

  /**
   * Helper method that moves to the next available valid ordinal.
   * @param keyVarLen
   * @return True if the table is resized.
   */
  private boolean moveToNextValidOrdinal(int keyVarLen) {
    if (currentOrdinal > maxSize) {
      // if we reached the capacity, expand it
      tryRehashForExpansion();
      return true;
    }

    /* we preallocate for single batch, so when currentOrdinal is 0
     * we don't have to add data blocks.
     */
    if(addNewBlock()){
      if (currentOrdinal > 0 || !preallocatedSingleBatch) {
        addDataBlocks();
      }
    }

    if (fixedOnly || !enforceVarWidthBufferLimit) {
      /* Important:
       * the hash table is used by both hash agg and hash join. for the design
       * of hash agg spilling, we introduced max limit on var block vector
       * (skipping ordinals if necessary) and that is not the case with hash join
       * -- join still continues to expand the variable block vector as and when
       * data is inserted  into the hash table.
       * on the other hand, if we are working with fixed width keys only then
       * there is never a need to skip ordinals and this is true for both agg and join
       */
      return false;
    }

    // Check if we can fit variable component in available space in current chunk.
    final int currentChunkIndex = getChunkIndexForOrdinal(currentOrdinal);
    final long tableVarAddr = openVariableAddresses[currentChunkIndex];
    final long tableMaxVarAddr = maxVariableAddresses[currentChunkIndex];
    if (tableMaxVarAddr - tableVarAddr >= keyVarLen + VAR_LENGTH_SIZE) {
      // there is enough space
      return false;
    }

    /* bump these stats iff we are going to skip ordinals */
    final int curFixedBlockWritePos = fixedBlocks[currentChunkIndex].getBufferLength();
    final int curVarBlockWritePos = variableBlocks[currentChunkIndex].getBufferLength();
    unusedForFixedBlocks += fixedBlocks[currentChunkIndex].getCapacity() - curFixedBlockWritePos;
    unusedForVarBlocks += variableBlocks[currentChunkIndex].getCapacity() - curVarBlockWritePos;

    // Not enough space, move to next chunk (may require expanding the hash table)
    int newOrdinal = (currentChunkIndex + 1) * MAX_VALUES_PER_BATCH;
    gaps += newOrdinal - currentOrdinal;
    currentOrdinal = newOrdinal;

    boolean retryStatus = false;
    if (currentOrdinal > maxSize) {
      tryRehashForExpansion();
      retryStatus = true;
    }

    if (!retryStatus) {
      addDataBlocks();
    }

    return retryStatus;
  }

  private int insert(final long blockWidth, long tableControlAddr, final int keyHash, final int dataWidth, final long keyFixedAddr, final long keyVarAddr, final int keyVarLen){
    if (moveToNextValidOrdinal(keyVarLen)) {
      // If the table is resized, we need to start search from beginning as in the new table this entry is mapped to
      // different control address which is determined by the caller of this method.
      return RETRY_RETURN_CODE;
    }
    final int insertedOrdinal = currentOrdinal;

    long traceBufAddr = 0;
    if (traceBuf != null) {
      traceBufAddr = traceBuf.memoryAddress();
    }

    // first we need to make sure we are up to date on the
    final int dataChunkIndex = getChunkIndexForOrdinal(insertedOrdinal);
    final int offsetInChunk = getOffsetInChunkForOrdinal(insertedOrdinal);
    final long tableDataAddr = tableFixedAddresses[dataChunkIndex] + (offsetInChunk * blockWidth);

    // set the ordinal value for the insertion.
    PlatformDependent.putInt(tableControlAddr, insertedOrdinal);
    PlatformDependent.putInt(tableControlAddr + 4, keyHash);

    // set the body part of the fixed data in the table.
    Copier.copy(keyFixedAddr, tableDataAddr, dataWidth);
    final ArrowBuf fixedBlockBuffer = fixedBlocks[dataChunkIndex].getUnderlying();
    fixedBlockBuffer.writerIndex(fixedBlockBuffer.writerIndex() + dataWidth);

    if(!fixedOnly) {
      long tableVarAddr = openVariableAddresses[dataChunkIndex];
      final VariableBlockVector block = variableBlocks[dataChunkIndex];
      final int tableVarOffset = (int) (tableVarAddr - initVariableAddresses[dataChunkIndex]);

      // set the var offset in the fixed position at the variable info offset.
      PlatformDependent.putInt(tableDataAddr + dataWidth, tableVarOffset);
      fixedBlockBuffer.writerIndex(fixedBlockBuffer.writerIndex() + VAR_OFFSET_SIZE);

      /* we should be considering to expand the buffer only if we have not enforced the constraint
       * on max size of variable block vector. if we are working (true by default) under the
       * max size constraint then moveToNextValidOrdinal() above would have taken
       * care of checking whether the max size is exhausted and if a new block needs
       * to be allocated. accordingly gaps would have been created in ordinals
       */
      if (!enforceVarWidthBufferLimit && maxVariableAddresses[dataChunkIndex] < tableVarAddr + keyVarLen + VAR_LENGTH_SIZE) {
        block.ensureAvailableDataSpace(tableVarOffset + keyVarLen + VAR_LENGTH_SIZE);
        tableVarAddr = block.getMemoryAddress() + tableVarOffset;
        this.initVariableAddresses[dataChunkIndex] = block.getMemoryAddress();
        this.openVariableAddresses[dataChunkIndex] = block.getMemoryAddress() + tableVarOffset;
        maxVariableAddresses[dataChunkIndex] = block.getMaxMemoryAddress();
      }

      // copy variable data.
      int size = keyVarLen + VAR_LENGTH_SIZE;
      Copier.copy(keyVarAddr, tableVarAddr, size);
      this.openVariableAddresses[dataChunkIndex] += size;
      final ArrowBuf variableBlockBuffer = variableBlocks[dataChunkIndex].getUnderlying();
      variableBlockBuffer.writerIndex(variableBlockBuffer.writerIndex() + size);
    }

    currentOrdinal++;

    return insertedOrdinal;
  }

  /**
   * Get number of records in hashtable batch. We just need to look at fixed block
   * buffer to count the number of records since both fixed block and variable block
   * buffers run paralelly (not in terms of length) but in terms of rows (pivoted records)
   * they store.
   *
   * A row in fixed block buffer is equal to block width <validity, all fixed columns, var offset)
   * and upon insertion of every new entry into hash table, we bump the writer index
   * in both blocks. So readableBytes in buffer along with block width gives the exact
   * count of records inserted in a particular hash table batch.
   *
   * @param batchIndex hash table batch/block/chunk index
   * @return number of records in batch
   */
  public int getRecordsInBatch(final int batchIndex) {
    Preconditions.checkArgument(batchIndex < blocks(), "Error: invalid batch index");
    final int records = (fixedBlocks[batchIndex].getUnderlying().readableBytes())/pivot.getBlockWidth();
    Preconditions.checkArgument(records <= MAX_VALUES_PER_BATCH, "Error: detected invalid number of records in batch");
    return records;
  }

  /**
   * Get underlying buffers storing hash table data for fixed width key column(s)
   * Note that we don't account for control block buffers here since
   * the main purpose of this method is to get the data block buffers
   * for serializing primarily.
   *
   * @return list of ArrowBufs for hash table's fixed data blocks.
   */
  public List<ArrowBuf> getFixedBlockBuffers() {
    final int blocks = blocks();
    final List<ArrowBuf> blockBuffers = new ArrayList<>(blocks);
    for (int i = 0; i < blocks; i++) {
      final ArrowBuf buffer = fixedBlocks[i].getUnderlying();
      blockBuffers.add(buffer);
    }
    return blockBuffers;
  }

  /**
   * Get underlying buffers storing hash table data for variable width key columns.
   * Note that we don't account for control block buffers here since
   * the main purpose of this method is to get the data block buffers
   * for serializing primarily.
   *
   * @return list of ArrowBufs for hash table's variable data blocks.
   */
  public List<ArrowBuf> getVariableBlockBuffers() {
    final int blocks = blocks();
    final List<ArrowBuf> blockBuffers = new ArrayList<>(blocks);
    for (int i = 0; i < blocks; i++) {
      final ArrowBuf buffer = variableBlocks[i].getUnderlying();
      blockBuffers.add(buffer);
    }
    return blockBuffers;
  }

  /**
   * Get the size of hash table structure in bytes. This peeks
   * at ArrowBuf for each internal structure (fixed blocks, variable blocks,
   * control blocks) and gets the total size of buffers in bytes.
   *
   * @return hash table size (in bytes).
   */
  public long getSizeInBytes() {
    if (size() == 0) {
      /* hash table is empty if currentOrdinal is 0. as long as there is at least
       * one record inserted into the hashtable, currentOrdinal is >= 1
       */
      return 0;
    }
    final long sizeOfKeyBlocks = getKeyBlockSizeInBytes();
    final long sizeOfControlBlock = CONTROL_WIDTH * MAX_VALUES_PER_BATCH * blocks();
    return sizeOfKeyBlocks + sizeOfControlBlock;
  }

  /**
   * Compute the size of fixed vector block and variable block size (in bytes)
   * for all batches of data inserted into the hash table.
   *
   * @return total size (in bytes) of fixed and variable blocks in the hash table.
   */
  private long getKeyBlockSizeInBytes() {
    final int blocks = blocks();
    long totalFixedBlockSize = 0;
    long totalVariableBlockSize = 0;
    for (int i = 0; i < blocks; i++) {
      totalFixedBlockSize += fixedBlocks[i].getUnderlying().readableBytes();
      totalVariableBlockSize += variableBlocks[i].getUnderlying().readableBytes();
    }
    logger.debug("Hash table blocks: {}, total size of fixed blocks: {}, total size of var blocks: {}", blocks, totalFixedBlockSize, totalVariableBlockSize);
    return totalFixedBlockSize + totalVariableBlockSize;
  }

  /**
   * Add a new data block (batch) to the hashtable (and accumulator). Memory
   * allocation is needed for the following things:
   *
   * (1) Add new {@link FixedBlockVector} to array of fixed blocks.
   * (2) Add new {@link VariableBlockVector} to array of variable blocks.
   * (3) Add new {@link org.apache.arrow.vector.FieldVector} as a new target vector
   *     in {@link com.dremio.sabot.op.aggregate.vectorized.BaseSingleAccumulator}
   *     to store computed values. This is done for _each_ accumulator inside
   *     {@link com.dremio.sabot.op.aggregate.vectorized.AccumulatorSet}.
   *
   * All of the above operations have to be done in a single transaction
   * as one atomic unit of work. This allows us to handle OutOfMemory situations
   * without creating any inconsistent state of data structures.
   */
  private void addDataBlocks(){
    final long currentAllocatedMemory = allocator.getAllocatedMemory();
    FixedBlockVector[] oldFixedBlocks = fixedBlocks;
    long[] oldTableFixedAddresses = tableFixedAddresses;
    try(RollbackCloseable rollbackable = new RollbackCloseable()) {
      FixedBlockVector newFixed;
      VariableBlockVector newVariable;
      {
        /* add new target accumulator vector to each accumulator.
         * since this is an array based allocation, we can fail in the middle
         * later on we revert on each accumulator and that might be a NO-OP
         * for some accumulators
         */
        listener.addBatch();
      }

      /* if the above operation was successful and we fail anywhere in the following
       * operations then we need to revert memory allocation on accumulator (all accumulators
       * in NestedAccumulator)
       */
      {
        newFixed = new FixedBlockVector(allocator, pivot.getBlockWidth());
        /* no need to rollback explicitly */
        rollbackable.add(newFixed);
        newFixed.ensureAvailableBlocks(MAX_VALUES_PER_BATCH);
        /* if we fail while allocating memory in above step, the state of fixed block array is still
         * consistent so we don't have to revert anything.
         */
        fixedBlocks = ObjectArrays.concat(fixedBlocks, newFixed);
        tableFixedAddresses = Longs.concat(tableFixedAddresses, new long[]{newFixed.getMemoryAddress()});
      }

      {
        newVariable = new VariableBlockVector(allocator, pivot.getVariableCount());
        /* no need to rollback explicitly */
        rollbackable.add(newVariable);
        newVariable.ensureAvailableDataSpace(variableBlockMaxLength);
        /* if we fail while allocating memory in above step, the state of variable block array is still consistent */
        variableBlocks = ObjectArrays.concat(variableBlocks, newVariable);
        initVariableAddresses = Longs.concat(initVariableAddresses, new long[]{newVariable.getMemoryAddress()});
        openVariableAddresses = Longs.concat(openVariableAddresses, new long[]{newVariable.getMemoryAddress()});
        maxVariableAddresses = Longs.concat(maxVariableAddresses, new long[]{newVariable.getMaxMemoryAddress()});
      }

      listener.commitResize();
      rollbackable.commit();
      /* bump these stats only after all new allocations have been successful as otherwise we would revert everything */
      allocatedForFixedBlocks += newFixed.getCapacity();
      allocatedForVarBlocks += newVariable.getCapacity();
    } catch (Exception e) {
      logger.debug("ERROR: failed to add data blocks, exception: ", e);
      /* explicitly rollback resizing operations on NestedAccumulator */
      listener.revertResize();
      fixedBlocks = oldFixedBlocks;
      tableFixedAddresses = oldTableFixedAddresses;
      /* do sanity checking on the state of all data structures after
       * memory allocation failed and we rollbacked. this helps in proactively detecting
       * potential IndexOutOfBoundsException and seg faults due to inconsistent state across
       * data structures.
       */
      Preconditions.checkArgument(fixedBlocks.length == variableBlocks.length,
                                  "Error: detected inconsistent state in hashtable after memory allocation failed");
      listener.verifyBatchCount(fixedBlocks.length);
      /* at this point we are as good as no memory allocation was ever attempted */
      Preconditions.checkArgument(allocator.getAllocatedMemory() == currentAllocatedMemory,
                                  "Error: detected inconsistent state of allocated memory");
      /* VectorizedHashAggOperator inserts data into hashtable and will handle (if OOM) this exception */
      throw Throwables.propagate(e);
    }
  }

  private final void rehash(int newCapacity) {
    // grab old references.
    final ControlBlock[] oldControlBlocks = this.controlBlocks;
    final long[] oldControlAddrs = this.tableControlAddresses;

    try {
      /* this is the only step that allocates memory during rehash, if the method fails the state is unchanged */
      internalInit(newCapacity);

      final long[] controlAddrs = this.tableControlAddresses;

      // loop through backwards.

      for(int batch =0; batch < oldControlAddrs.length; batch++){
        long addr = oldControlAddrs[batch];
        final long max = addr + MAX_VALUES_PER_BATCH * CONTROL_WIDTH;
        for(long oldControlAddr = addr; oldControlAddr < max; oldControlAddr += CONTROL_WIDTH){
          long oldControl = PlatformDependent.getLong(oldControlAddr);

          if(oldControl != LFREE){
            int index = ((int) (oldControl >>> 32)) % capacity; // get previously computed hash and slice it.
            int newChunkIndex = getChunkIndexForOrdinal(index);
            int offetInChunk = getOffsetInChunkForOrdinal(index);
            long controlAddr = controlAddrs[newChunkIndex] + (offetInChunk * CONTROL_WIDTH);

            if (PlatformDependent.getInt(controlAddr) != FREE) {
              while (true) {
                index = (index - 1) % capacity;
                newChunkIndex = getChunkIndexForOrdinal(index);
                offetInChunk = getOffsetInChunkForOrdinal(index);
                controlAddr = controlAddrs[newChunkIndex] + (offetInChunk * CONTROL_WIDTH);
                if (PlatformDependent.getInt(controlAddr) == FREE) {
                  break;
                }
              }
            }
            PlatformDependent.putLong(controlAddr, oldControl);
          }
        }
      }

      // Release existing control blocks only after rehashing is successful.
      AutoCloseables.close(asList(oldControlBlocks));
    } catch (Exception e) {
      logger.debug("ERROR: failed to rehash, exception: ", e);
      /* VectorizedHashAggOperator inserts data into hashtable  and will handle (if OOM) this exception */
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

  public int relativeSize() {
    return currentOrdinal - gaps;
  }

  public int blocks(){
    return (int) Math.ceil( currentOrdinal / (MAX_VALUES_PER_BATCH * 1.0d) );
  }

  public int capacity() {
    return capacity;
  }

  @VisibleForTesting
  public int gaps() {
    return gaps;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close((Iterable<AutoCloseable>) Iterables.concat(FluentIterable.of(controlBlocks).toList(), FluentIterable.of(fixedBlocks).toList(), FluentIterable.of(variableBlocks).toList()));
  }

  private void tryRehashForExpansion() {
    int newCapacity = LHashCapacities.capacity(config, capacity(), false);
    if (newCapacity > capacity()) {
      try {
        rehashTimer.start();
        rehash(newCapacity);
        rehashCount++;
      } finally {
        rehashTimer.stop();
      }
    } else {
      throw new HashTableMaxCapacityReachedException(capacity());
    }
  }

  public long getRehashTime(TimeUnit unit){
    return rehashTimer.elapsed(unit);
  }

  public int getRehashCount(){
    return rehashCount;
  }

  private void internalInit(int capacity) {
    /* capacity is power of 2 */
    assert (capacity & (capacity - 1)) == 0;
    initTimer.start();
    /* tentative new state */
    capacity = Math.max(Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH), capacity);
    final int newCapacity = capacity;
    final int newMaxSize = !LHashCapacities.isMaxCapacity(capacity, false) ? config.maxSize(capacity) : capacity - 1;
    final int newBatches = (int) Math.ceil( capacity / (MAX_VALUES_PER_BATCH * 1.0d) );
    /* new memory allocation */
    final ControlBlock[] newControlBlocks = new ControlBlock[newBatches];
    final long[] newTableControlAddresses = new long[newBatches];
    try(RollbackCloseable rollbackable = new RollbackCloseable()) {
      /* if we fail while allocating a ControlBlock,
       * RollbackCloseable will take care of releasing memory allocated so far.
       * secondly, control block array, address array and corresponding
       * state is anyway unchanged since their state is updated only
       * after allocation for all control blocks is successful.
       */
      for(int i =0; i < newBatches; i++){
        newControlBlocks[i] = new ControlBlock(allocator, MAX_VALUES_PER_BATCH);
        rollbackable.add(newControlBlocks[i]);
        newTableControlAddresses[i] = newControlBlocks[i].getMemoryAddress();
        initControlBlock(newTableControlAddresses[i]);
      }

      /* memory allocation successful so update ControlBlock arrays and state */
      this.controlBlocks = newControlBlocks;
      this.tableControlAddresses = newTableControlAddresses;
      this.capacity = newCapacity;
      this.batches = newBatches;
      this.maxSize = newMaxSize;
      rollbackable.commit();
    } catch (Exception e) {
      /* will be propagated back to the operator */
      throw Throwables.propagate(e);
    } finally {
      initTimer.stop();
    }
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
    PlatformDependent.putInt(traceBufNext + 0 * 4, capacity);
    PlatformDependent.putInt(traceBufNext + 1 * 4, maxSize);
    PlatformDependent.putInt(traceBufNext + 2 * 4, batches);
    PlatformDependent.putInt(traceBufNext + 3 * 4, currentOrdinal);
    PlatformDependent.putInt(traceBufNext + 4 * 4, rehashCount);
    PlatformDependent.putInt(traceBufNext + 5 * 4, numRecords);
    traceBufNext += 6 * 4;
  }

  /**
   * End of insertion. Record the state after insertion
   */
  public void traceInsertEnd() {
    if (traceBuf == null) {
      return;
    }
    PlatformDependent.putInt(traceBufNext + 0 * 4, capacity);
    PlatformDependent.putInt(traceBufNext + 1 * 4, maxSize);
    PlatformDependent.putInt(traceBufNext + 2 * 4, batches);
    PlatformDependent.putInt(traceBufNext + 3 * 4, currentOrdinal);
    PlatformDependent.putInt(traceBufNext + 4 * 4, rehashCount);
    traceBufNext += 5 * 4;
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
    formatter.format("Pre-insert: capacity: %1$d, maxSize: %2$d, batches: %3$d (%3$#X), currentOrdinal: %4$d, rehashCount: %5$d %n",
                     PlatformDependent.getInt(traceBufAddr + 0 * 4),
                     PlatformDependent.getInt(traceBufAddr + 1 * 4),
                     PlatformDependent.getInt(traceBufAddr + 2 * 4),
                     PlatformDependent.getInt(traceBufAddr + 3 * 4),
                     PlatformDependent.getInt(traceBufAddr + 4 * 4));

    long traceBufCurr = traceBufAddr + 6 * 4;
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

    formatter.format("Post-insert: capacity: %1$d, maxSize: %2$d, batches: %3$d (%3$#X), currentOrdinal: %4$d, rehashCount: %5$d %n",
                     PlatformDependent.getInt(traceBufLast + 0 * 4),
                     PlatformDependent.getInt(traceBufLast + 1 * 4),
                     PlatformDependent.getInt(traceBufLast + 2 * 4),
                     PlatformDependent.getInt(traceBufLast + 3 * 4),
                     PlatformDependent.getInt(traceBufLast + 4 * 4));
    return sb.toString();
  }

  /**
   * Resets the HashTable to minimum size which has capacity to contain {@link #MAX_VALUES_PER_BATCH}.
   */
  public void resetToMinimumSize() throws Exception {
    if (capacity() <= MAX_VALUES_PER_BATCH) {
      /* if there is only 1 batch, we don't need to shrink hashtable
       * just reset the state for first batch
       */
      resetToMinimumSizeHelper();
      return;
    }

    final List<AutoCloseable> toRelease = Lists.newArrayList();

    // Release all except the first entry
    toRelease.addAll(asList(copyOfRange(controlBlocks, 1, controlBlocks.length)));
    toRelease.addAll(asList(copyOfRange(fixedBlocks, 1, fixedBlocks.length)));
    toRelease.addAll(asList(copyOfRange(variableBlocks, 1, variableBlocks.length)));
    AutoCloseables.close(toRelease);

    controlBlocks = copyOfRange(controlBlocks, 0, 1);
    fixedBlocks = copyOfRange(fixedBlocks, 0, 1);
    variableBlocks = copyOfRange(variableBlocks, 0, 1);

    tableControlAddresses = copyOfRange(tableControlAddresses, 0, 1);
    tableFixedAddresses = copyOfRange(tableFixedAddresses, 0, 1);
    initVariableAddresses = copyOfRange(initVariableAddresses, 0, 1);
    openVariableAddresses = copyOfRange(openVariableAddresses, 0, 1);
    maxVariableAddresses = copyOfRange(maxVariableAddresses, 0, 1);

    resetToMinimumSizeHelper();
  }

  private void resetToMinimumSizeHelper() throws Exception {
    controlBlocks[0].reset();
    fixedBlocks[0].reset();
    variableBlocks[0].reset();
    currentOrdinal = 0;
    gaps = 0;
    capacity = MAX_VALUES_PER_BATCH;
    maxSize = !LHashCapacities.isMaxCapacity(capacity, false) ? config.maxSize(capacity) : capacity - 1;
    batches = 1;
    openVariableAddresses[0] = initVariableAddresses[0];

    initControlBlock(tableControlAddresses[0]);

    listener.resetToMinimumSize();
  }

  private void initControlBlock(final long controlBlockAddr) {
    final long addr = controlBlockAddr;
    final long max = addr + MAX_VALUES_PER_BATCH * CONTROL_WIDTH;
    for (long l = addr; l < max; l += LBlockHashTable.CONTROL_WIDTH) {
      PlatformDependent.putLong(l, LBlockHashTable.LFREE);
    }
  }

  /**
   * Preallocate memory for storing a single batch of data in hashtable (and accumulators).
   */
  public void preallocateSingleBatch() {
    Preconditions.checkArgument(fixedBlocks.length == 0, "Error: expecting 0 batches in hashtable");
    Preconditions.checkArgument(variableBlocks.length == 0, "Error: expecting 0 batches in hashtable");
    Preconditions.checkArgument(size() == 0, "Error: Expecting empty hashtable");
    addDataBlocks();
    Preconditions.checkArgument(fixedBlocks.length == 1, "Error: expecting space for single batch for fixed block");
    Preconditions.checkArgument(variableBlocks.length == 1, "Error: expecting space for single batch for variable block");
    Preconditions.checkArgument(size() == 0, "Error: Expecting empty hashtable");
    preallocatedSingleBatch = true;
  }

  public int getCurrentNumberOfBlocks() {
    Preconditions.checkArgument(fixedBlocks.length == variableBlocks.length, "Error: detected inconsistent number of blocks");
    return fixedBlocks.length;
  }
}
