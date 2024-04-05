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
package com.dremio.sabot.op.aggregate.vectorized;

import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.op.common.ht2.LBlockHashTable;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.util.LargeMemoryUtil;
import org.apache.arrow.vector.types.Types;

/** Works like a SelectionVector/Iterator over HashAggPartition buffers */
public class HashAggPartitionWritableBatch {
  private final LBlockHashTable hashTable;
  private final List<ArrowBuf> fixedBlockBuffers;
  private final List<ArrowBuf> variableBlockBuffers;
  private final Accumulator[] accumulators;
  private final ArrowBuf[] buffers;
  private int currentBatchIndex;
  private final int blockWidth;
  private final int numWritableBuffers;
  private final int maxValuesPerBatch;

  /* metadata length could possibly be optimized to smaller value later */
  static final byte BATCH_HEADER_LENGTH = 12;
  static final byte FIXED_BUFFER_LENGTH_OFFSET = 0;
  static final byte VARIABLE_BUFFER_LENGTH_OFFSET = 4;
  static final byte NUM_ACCUMULATORS_OFFSET = 8;

  /* for the fixed width accumulators each
   * corresponding accumulator column vector that stores computed
   * values has 2 buffers -- validity, data
   */
  private static final int NUM_BUFFERS_PER_FIXED_ACCUMULATOR_PERBATCH = 2;

  /** Varlen and HLL accumulators has validity, offset & data */
  private static final int NUM_BUFFERS_PER_VARLEN_ACCUMULATOR_PERBATCH = 3;

  /**
   * LISTAGG accumulators has validity & offset and 3 buffers for underlying
   * BaseVariableWidthVector.
   */
  private static final int NUM_BUFFERS_PER_LISTAGG_ACCUMULATOR_PERBATCH = 5;

  /* per data batch inserted into hash table, we have 2 buffers
   * that store hash table data.
   * one buffer that stores the data from all the fixed width group by
   * key columns and other buffer that stores data from variable
   * width key columns.
   */
  private static final int NUM_HASHTABLE_BUFFERS_PERBATCH = 2;

  /**
   * Returns the actual number of Arrow buffers required to spill an accumulator set.
   *
   * @return
   */
  private final int GetNumWritableBuffers() {
    int fixedWidthAccum = 0;
    int varLenAccum = 0;
    int listaggAccum = 0;
    for (int i = 0; i < accumulators.length; ++i) {
      /*
       * For ListAgg accumulators, the output type is either BaseVariableWidthVector or
       * ListVector however irrespective of it, the intermediate format that is written
       * to spill disk, is ListVector.
       */
      if (accumulators[i].getType() == AccumulatorBuilder.AccumulatorType.LISTAGG
          || accumulators[i].getType() == AccumulatorBuilder.AccumulatorType.LOCAL_LISTAGG
          || accumulators[i].getType() == AccumulatorBuilder.AccumulatorType.LISTAGG_MERGE) {
        ++listaggAccum;
      } else {
        final Types.MinorType type = accumulators[i].getOutput().getMinorType();
        if (type == Types.MinorType.VARCHAR || type == Types.MinorType.VARBINARY) {
          ++varLenAccum;
        } else {
          ++fixedWidthAccum;
        }
      }
    }

    int totalBuffers = NUM_HASHTABLE_BUFFERS_PERBATCH;
    totalBuffers += (fixedWidthAccum * NUM_BUFFERS_PER_FIXED_ACCUMULATOR_PERBATCH);
    totalBuffers += (varLenAccum * NUM_BUFFERS_PER_VARLEN_ACCUMULATOR_PERBATCH);
    totalBuffers += (listaggAccum * NUM_BUFFERS_PER_LISTAGG_ACCUMULATOR_PERBATCH);

    return totalBuffers;
  }

  public HashAggPartitionWritableBatch(
      final LBlockHashTable hashTable,
      final List<ArrowBuf> fixedBlockBuffers,
      final List<ArrowBuf> variableBlockBuffers,
      final int blockWidth,
      final AccumulatorSet accumulator,
      final int maxValuesPerBatch,
      final int currentBatchIndex) {
    this.hashTable = hashTable;
    this.accumulators = accumulator.getChildren();
    checkAccumulatorsBatchSize(fixedBlockBuffers.size());
    this.fixedBlockBuffers = fixedBlockBuffers;
    this.variableBlockBuffers = variableBlockBuffers;
    this.numWritableBuffers = GetNumWritableBuffers();
    this.buffers = new ArrowBuf[numWritableBuffers];
    this.blockWidth = blockWidth;
    this.currentBatchIndex = currentBatchIndex;
    this.maxValuesPerBatch = maxValuesPerBatch;
  }

  /**
   * Verify that the number of batches in each Accumulator is equal to the number of batches in the
   * hash table.
   */
  private void checkAccumulatorsBatchSize(int batchCount) {
    for (int i = 0; i < accumulators.length; i++) {
      accumulators[i].verifyBatchCount(batchCount);
    }
  }

  /*
   * This method is invoked by the caller (VectorizedHashAggPartitionSerializable) in
   * a loop. The caller does the I/O and invokes this function to get a set of
   * buffers (along with corresponding metadata) that are then written to disk by the
   * caller. These buffers make up a chunk of data that is spilled.
   * If no more data is available, this function returns NULL.
   *
   * For example, say there are 4 GROUP BY key columns; 2 fixed width CF1, CF2
   * and 2 variable width CV1, CV2. The hash table has row-wise (aka pivoted)
   * representation of keys. The fixed width key columns are together in fixed block
   * vector and variable width key columns are together in variable block vector.
   * Further, the hash table is segmented into multiple batches. This implies that
   * there are as many fixed and variable block vectors as there are batches of data
   * inserted into the hash table. Similarly, each accumulator will have as many
   * internal accumulator vectors as there are batches of data.
   *
   * For this example, assume hash table has 2 batches.
   *
   * So the hash table has following:
   *
   * fixed block vector 1 (containing row-wise representation of keys from CF1, CF2 for batch 0)
   * fixed block vector 2 (containing row-wise representation of keys from CF1, CF2 for batch 1)
   * variable block vector 1 (containing row-wise representation of keys from CV1, CV2 for batch 0)
   * variable block vector 2 (containing row-wise representation of keys from CV1, CV2 for batch 1)
   *
   * The ArrowBuf for fixed block vector 1 will look like:
   * CF1_KEY1 CF2_KEY1 var_offset CF1_KEY2 CF2_KEY2 var_offset ........... 4096 KEYS
   *
   * The ArrowBuf for variable block vector 1 will look like
   * total_var_length var_length1 CV1_KEY1 var_length2 CV2_KEY1 total_var_length var_length1 CV1_KEY2 var_length2 ........... 4096 KEYS
   *
   * The ArrowBufs for the second batch will also have the same structure and may have
   * 4096 or less records.
   *
   * We then go over each accumulator and get the buffers from the accumulator vector
   * for the corresponding batch.
   *
   * We can have 1 or more accumulators and they are all encapsulated under NestedAccumulator.
   * Each accumulator with a fixed width vector (INT, BIGINT, FLOAT, FLOAT4, DECIMAL etc) will
   * have 2 buffers -- validity buffer and data buffer. min/max on a varlen and HLL accumulators
   * have 3 buffers -- validity buffer, offset buffer and data buffer listagg accumulator
   * have 5 buffers -- validity buffer, offset buffer for ListVector and 3 buffers for the dataVector.
   * Secondly, each type of accumulator will internally have as many accumulators as there
   * are batches of data inserted into the hash table.
   *
   * So for the particular example, a single accumulator will have 2 internal vectors
   * (one for batch 0 and second for batch 1). Assume the example has 2 types of
   * accumulators (SUM and MIN). So per batch of data, the total buffers is computed as:
   *
   * 1. fixed block buffer from hash table
   * 2. var block buffer from hash table
   * 3. validity buffer for accumulator vector of type SUM
   * 4. data buffer for accumulator vector of type SUM.
   * 5. validity buffer for accumulator vector of type MIN
   * 6. data buffer for accumulator vector of type MIN.
   *
   * So total 6 buffers per batch.
   *
   * Finally, we form a set of buffers (from hash table and accumulator) along with corresponding
   * metadata and this is then spilled by the caller.
   */
  public HashAggPartitionBatchDefinition getNextWritableBatch() {
    if (currentBatchIndex == fixedBlockBuffers.size()) {
      /* we have gone through all the batches of data inserted into the hash table and
       * therefore there are no more chunks of data to be spilled.
       */
      return null;
    }

    /* get the fixed block buffer for the current batch we are looking at */
    final ArrowBuf fixedBlockBuffer = fixedBlockBuffers.get(currentBatchIndex);
    /* get the variable block buffer for the current batch we are looking at */
    final ArrowBuf variableBlockBuffer = variableBlockBuffers.get(currentBatchIndex);

    /* get total length of buffers, this is why readerIndex and writerIndex are appropriately
     * set by LBlockHashTable when it writes to these buffers during insertion.
     */
    final int fixedBufferLength =
        LargeMemoryUtil.checkedCastToInt(fixedBlockBuffer.readableBytes());
    final int variableBufferLength =
        LargeMemoryUtil.checkedCastToInt(variableBlockBuffer.readableBytes());
    final int numRecordsInChunk = fixedBufferLength / blockWidth;
    Preconditions.checkArgument(
        numRecordsInChunk <= maxValuesPerBatch,
        "Error: detected invalid number of records in batch");

    buffers[0] = fixedBlockBuffer;
    buffers[1] = variableBlockBuffer;

    final List<UserBitShared.SerializedField> metadata = new ArrayList<>(accumulators.length);
    final byte[] accumulatorTypes = new byte[accumulators.length];

    int k = 2;
    for (int i = 0; i < accumulators.length; i++) {
      final Accumulator accumulator = accumulators[i];

      /*
       * Variable length accumulators assume getBuffers() followed by getSerializedField().
       * If the order has changed or any additional changes in between (which is highly unlikely),
       * adjust the functions in variable length accumulators as well.
       */
      List<ArrowBuf> accumulatorBuffers =
          accumulator.getBuffers(currentBatchIndex, numRecordsInChunk);
      metadata.add(accumulator.getSerializedField(currentBatchIndex, numRecordsInChunk));
      accumulatorTypes[i] = (byte) accumulator.getType().ordinal();

      for (int j = 0; j < accumulatorBuffers.size(); j++) {
        buffers[k] = accumulatorBuffers.get(j);
        k++;
      }
    }

    final UserBitShared.RecordBatchDef accumulatorBatchDef =
        UserBitShared.RecordBatchDef.newBuilder()
            .addAllField(metadata)
            .setRecordCount(numRecordsInChunk)
            .setCarriesTwoByteSelectionVector(false)
            .build();

    final int batchIdx = currentBatchIndex++;

    return new HashAggPartitionBatchDefinition(
        fixedBufferLength,
        variableBufferLength,
        accumulators.length,
        accumulatorTypes,
        accumulatorBatchDef,
        batchIdx);
  }

  public ArrowBuf[] getBuffers() {
    return buffers;
  }

  public static class HashAggPartitionBatchDefinition {
    final int fixedBufferLength;
    final int variableBufferLength;
    final int numAccumulators;
    final byte[] accumulatorTypes;
    final UserBitShared.RecordBatchDef accumulatorBatchDef;
    final int batchIdx;

    public HashAggPartitionBatchDefinition(
        int fixedBufferLength,
        int variableBufferLength,
        int numAccumulators,
        byte[] accumulatorTypes,
        UserBitShared.RecordBatchDef accumulatorBatchDef,
        final int batchIdx) {
      this.fixedBufferLength = fixedBufferLength;
      this.variableBufferLength = variableBufferLength;
      this.numAccumulators = numAccumulators;
      this.accumulatorTypes = accumulatorTypes;
      this.accumulatorBatchDef = accumulatorBatchDef;
      this.batchIdx = batchIdx;
    }

    public int getCurrentBatchIndex() {
      return batchIdx;
    }
  }
}
