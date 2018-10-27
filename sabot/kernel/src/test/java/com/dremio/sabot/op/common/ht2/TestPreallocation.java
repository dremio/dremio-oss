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

import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.SumAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.MaxAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorSet;
import com.dremio.sabot.op.aggregate.vectorized.Accumulator;
import com.koloboke.collect.hash.HashConfig;

import org.junit.Test;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.SimpleBigIntVector;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestPreallocation {
  private static int MAX_VALUES_PER_BATCH = 0;

  @Test
  public void testInsertIntoPreallocatedHashTable() throws Exception {
    /* try with HT batch size as power of 2 */
    MAX_VALUES_PER_BATCH = 4096;
    testPreallocationHelper();

    MAX_VALUES_PER_BATCH = 2048;
    testPreallocationHelper();

    MAX_VALUES_PER_BATCH = 1024;
    testPreallocationHelper();

    /* try with HT batch size as arbitrary non power of 2 */
    MAX_VALUES_PER_BATCH = 940;
    testPreallocationHelper();

    MAX_VALUES_PER_BATCH = 1017;
    testPreallocationHelper();
  }

  private void testPreallocationHelper() throws Exception {
    /* GROUP BY key columns */
    String[] col1arr = {
      "hello", "my", "hello", "hello",
      null, null, "hello", null,
      "hello", "my", "my", "hello"
    };

    String[] col2arr = {
      "every", "every", "every", "none",
      null, null, "every", null,
      "none", "every", "every", "every"
    };

    Integer[] col3arr = {
      1, 1, 1, 1,
      1, 1, 2, 2,
      1, 1, null, 1};

    /* Measure columns */
    Integer[] aggcol1 = {100000, 160000, 200000, 300000, 120000, 50000, 80000, 140000, 90000, 100000, 110000, null};
    final Long[] expectedSum = {300000L, 260000L, 390000L, 170000L, 80000L, 140000L, 110000L};
    final Integer[] expectedMax = {200000, 160000, 300000, 120000, 80000, 140000, 110000};

    /* Expected ordinals after insertion into hash table */
    final int[] expectedOrdinals = {0, 1, 0, 2, 3, 3, 4, 5, 2, 1, 6, 0};

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer();) {

      /* GROUP BY key columns */
      VarCharVector col1 = new VarCharVector("col1", allocator);
      TestVarBinaryPivot.populate(col1, col1arr);
      c.add(col1);

      VarCharVector col2 = new VarCharVector("col2", allocator);
      TestVarBinaryPivot.populate(col2, col2arr);
      c.add(col2);

      IntVector col3 = new IntVector("col3", allocator);
      populateInt(col3, col3arr);
      c.add(col3);

      /* Measure column 1 */
      IntVector m1 = new IntVector("m1", allocator);
      populateInt(m1, aggcol1);
      c.add(m1);

      final int records = c.setAllCount(col1arr.length);

      /* create pivot definition */
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
        new FieldVectorPair(col1, col1),
        new FieldVectorPair(col2, col2),
        new FieldVectorPair(col3, col3)
      );

      try(final AccumulatorSet accumulator = createAccumulator(m1, allocator);
          final LBlockHashTable hashTable = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, true, accumulator, MAX_VALUES_PER_BATCH)) {
        final Accumulator[] accumulators = accumulator.getChildren();
        assertEquals(2, accumulators.length);

        /* check state before preallocation */
        assertEquals(0, ((SumAccumulators.IntSumAccumulator)(accumulators[0])).getBatchCount());
        assertEquals(0, ((MaxAccumulators.IntMaxAccumulator)(accumulators[1])).getBatchCount());
        assertEquals(0, hashTable.getCurrentNumberOfBlocks());

        /* preallocate space for single batch before inserting anything */
        hashTable.preallocateSingleBatch();

        /* check state after preallocation */
        assertEquals(1, ((SumAccumulators.IntSumAccumulator)(accumulators[0])).getBatchCount());
        assertEquals(1, ((MaxAccumulators.IntMaxAccumulator)(accumulators[1])).getBatchCount());
        assertEquals(1, hashTable.getCurrentNumberOfBlocks());
        /* hashtable size and blocks should not be affected due to preallocation since they are based on currentOrdinal */
        assertEquals(0, hashTable.size());
        assertEquals(0, hashTable.blocks());
        assertEquals(0, hashTable.getFixedBlockBuffers().size());
        assertEquals(0, hashTable.getFixedBlockBuffers().size());

        /* insert and accumulate */
        insertAndAccumulateForAllPartitions(allocator, records, pivot, hashTable, accumulator, expectedOrdinals);

        /* check state after insertion -- all should go into pre-allocated batch */
        assertEquals(7, hashTable.size());
        assertEquals(1, hashTable.blocks());
        assertEquals(1, hashTable.getFixedBlockBuffers().size());
        assertEquals(1, hashTable.getFixedBlockBuffers().size());
        assertEquals(7, hashTable.getRecordsInBatch(0));

        final FieldVector sumOutput = ((SumAccumulators.IntSumAccumulator)(accumulators[0])).getAccumulatorVector(0);
        final FieldVector maxOutput = ((MaxAccumulators.IntMaxAccumulator)(accumulators[1])).getAccumulatorVector(0);

        for (int i = 0; i < hashTable.getRecordsInBatch(0); i++) {
          assertEquals(expectedSum[i], sumOutput.getObject(i));
          assertEquals(expectedMax[i], maxOutput.getObject(i));
        }
      }
    }
  }

  private AccumulatorSet createAccumulator(IntVector in1,
                                              final BufferAllocator allocator) {
    /* SUM Accumulator */
    BigIntVector in1SumOutputVector = new BigIntVector("int-sum", allocator);
    final SumAccumulators.IntSumAccumulator in1SumAccum =
      new SumAccumulators.IntSumAccumulator(in1, in1SumOutputVector, in1SumOutputVector, MAX_VALUES_PER_BATCH, allocator);

    /* Min Accumulator */
    IntVector in1MaxOutputVector = new IntVector("int-max", allocator);
    final MaxAccumulators.IntMaxAccumulator in1MaxAccum =
      new MaxAccumulators.IntMaxAccumulator(in1, in1MaxOutputVector, in1MaxOutputVector, MAX_VALUES_PER_BATCH, allocator);

    return new AccumulatorSet(4*1024, 64*1024, allocator, in1SumAccum, in1MaxAccum);
  }

  private void populateInt(IntVector vector, Integer[] data) {
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < data.length; i++){
      Integer val = data[i];
      if(val != null){
        vector.setSafe(i, val);
      } else {
        vector.setSafe(i, 0, r.nextInt());
      }
    }
    vector.setValueCount(data.length);
  }

  private void insertAndAccumulateForAllPartitions(final BufferAllocator allocator,
                                                   final int records,
                                                   final PivotDef pivot,
                                                   final LBlockHashTable hashTable,
                                                   final AccumulatorSet accumulator,
                                                   final int[] expectedOrdinals) {
    try (final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
         final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());
         final ArrowBuf offsets = allocator.buffer(records * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
         final SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {

      /* pivot the data into temporary space */
      Pivots.pivot(pivot, records, fbv, var);

      final long keyFixedVectorAddr = fbv.getMemoryAddress();
      final long keyVarVectorAddr = var.getMemoryAddress();
      int[] actualOrdinals = new int[expectedOrdinals.length];
      final boolean fixedOnly = pivot.getVariableCount() == 0;

      /* compute hash on the pivoted data */
      hashValues.allocateNew(records);
      final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
        pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
      HashComputation.computeHash(blockChunk);

      /* insert */
      long offsetAddr = offsets.memoryAddress();
      for (int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH) {
        final int keyHash = (int)hashValues.get(keyIndex);
        actualOrdinals[keyIndex] = hashTable.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
        PlatformDependent.putByte(offsetAddr, (byte)0);
        PlatformDependent.putInt(offsetAddr + VectorizedHashAggOperator.HTORDINAL_OFFSET, actualOrdinals[keyIndex]);
        PlatformDependent.putInt(offsetAddr + VectorizedHashAggOperator.KEYINDEX_OFFSET, keyIndex);
      }

      assertArrayEquals(expectedOrdinals, actualOrdinals);

      /* accumulate */
      accumulator.accumulate(offsets.memoryAddress(), records);
    }
  }
}
