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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SimpleBigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.work.AttemptId;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorSet;
import com.dremio.sabot.op.aggregate.vectorized.CountColumnAccumulator;
import com.dremio.sabot.op.aggregate.vectorized.CountOneAccumulator;
import com.dremio.sabot.op.aggregate.vectorized.MaxAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.MinAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.PartitionToLoadSpilledData;
import com.dremio.sabot.op.aggregate.vectorized.SumAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartition;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSerializable;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSpillHandler;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSpillHandler.SpilledPartitionIterator;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;
import com.dremio.test.DremioTest;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.koloboke.collect.hash.HashConfig;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;


/**
 * Tests for {@link VectorizedHashAggPartitionSerializable} and
 * {@link VectorizedHashAggPartitionSpillHandler}
 *
 * These are sort of standalone tests for particular modules which
 * require us to mock certain things. We will also have separate tests
 * to test the operator lifeycle using BaseTestOperator framework
 * and there we won't have to mock certain data structures.
 */
public class TestVectorizedHashAggPartitionSerializable {
  private final List<Field> postSpillAccumulatorVectorFields = Lists.newArrayList();
  private int MAX_VALUES_PER_BATCH = 0;

  /**
   * Both fixed and variable width GROUP BY key columns.
   *
   * @throws Exception
   */
  @Test
  public void testPartitionRoundTrip1() throws Exception {
    /* try with HT batch size as power of 2 */
    MAX_VALUES_PER_BATCH = 4096;
    testPartitionRoundTrip1Helper();
    postSpillAccumulatorVectorFields.clear();

    MAX_VALUES_PER_BATCH = 2048;
    testPartitionRoundTrip1Helper();
    postSpillAccumulatorVectorFields.clear();

    MAX_VALUES_PER_BATCH = 1024;
    testPartitionRoundTrip1Helper();
    postSpillAccumulatorVectorFields.clear();

    /* try with HT batch size as arbitrary non power of 2 */
    MAX_VALUES_PER_BATCH = 940;
    testPartitionRoundTrip1Helper();
    postSpillAccumulatorVectorFields.clear();

    MAX_VALUES_PER_BATCH = 1017;
    testPartitionRoundTrip1Helper();
  }

  private void testPartitionRoundTrip1Helper() throws Exception {

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

    /**
     *  all keys:
     *
     *  [hello, every, 1] -> expected ordinal 0
     *  [my, every, 1]    -> expected ordinal 1
     *  [hello, every, 1] -> expected ordinal 0
     *  [hello, none, 1]  -> expected ordinal 2
     *  [null, null, 1]   -> expected ordinal 3
     *  [null, null, 1]   -> expected ordinal 3
     *  [hello, every, 2] -> expected ordinal 4
     *  [null, null, 2]   -> expected ordinal 5
     *  [hello, none, 1]  -> expected ordinal 2
     *  [my, every, 1]    -> expected ordinal 1
     *  [my, every, null] -> expected ordinal 6
     *  [hello, every, 1] -> expected ordinal 0
     *
     *  keys in HT and corresponding computed accumulators:
     *
     *  [hello, every, 1] -> 100000 + 200000 + null(0) = 300000
     *  [my, every, 1]    -> 160000 + 100000 = 260000
     *  [hello, none, 1]  -> 300000 + 90000 = 390000
     *  [null, null, 1]   -> 120000 + 50000 = 170000
     *  [hello, every, 2] -> 80000
     *  [null, null, 2]   -> 140000
     *  [my, every, null] -> 110000
     */

    /* Expected ordinals after insertion into hash table */
    final int[] expectedOrdinals = {0, 1, 0, 2, 3, 3, 4, 5, 2, 1, 6, 0};

    /* Measure columns -- compute SUM, MIN, MAX, COUNT on each column, COUNT(1) on first column */
    Integer[] aggcol1 = {100000, 160000, 200000, 300000, 120000, 50000, 80000, 140000, 90000, 100000, 110000, null};
    final Long[] expectedSum1 = {300000L, 260000L, 390000L, 170000L, 80000L, 140000L, 110000L};
    final Integer[] expectedMax1 = {200000, 160000, 300000, 120000, 80000, 140000, 110000};
    final Integer[] expectedMin1 = {100000, 100000, 90000, 50000, 80000, 140000, 110000};

    Long[] aggcol2 = {100000000L, 160000000L, 250000000L, 300000000L, 120000000L, 50000000L, 80000000L, 140000000L, 90000000L, 100000000L, 110000000L, null};
    final Long[] expectedSum2 = {350000000L, 260000000L, 390000000L, 170000000L, 80000000L, 140000000L, 110000000L};
    final Long[] expectedMax2 = {250000000L, 160000000L, 300000000L, 120000000L, 80000000L, 140000000L, 110000000L};
    final Long[] expectedMin2 = {100000000L, 100000000L, 90000000L, 50000000L, 80000000L, 140000000L, 110000000L};

    Float[] aggcol3 = {20.5f, 10.75f, 5.2f, 3.44f, 12.25f, 50.25f, 80.75f, 14.15f, 90.25f, 10.25f, 11.25f, null};
    final Double[] expectedSum3 = {25.7d, 21.0d, 93.69d, 62.50d, 80.75d, 14.15d, 11.25d};
    final Float[] expectedMax3 = {20.5f, 10.75f, 90.25f, 50.25f, 80.75f, 14.15f, 11.25f};
    final Float[] expectedMin3 = {5.2f, 10.25f, 3.44f, 12.25f, 80.75f, 14.15f, 11.25f};

    Double[] aggcol4 = {100.375, 160.1245, 250.6232, 23.4265, 120.12345, 50.3452, 80.1254, 140.2567, 90.2345, 100.2345, 110.7523, null};
    final Double[] expectedSum4 = {350.9982, 260.359, 113.661, 170.46865, 80.1254, 140.2567, 110.7523};
    final Double[] expectedMax4 = {250.6232, 160.1245, 90.2345, 120.12345, 80.1254, 140.2567, 110.7523};
    final Double[] expectedMin4 = {100.375, 100.2345, 23.4265, 50.3452, 80.1254, 140.2567, 110.7523};

    final SumOutputHolder expectedSumOutput = new SumOutputHolder(expectedSum1, expectedSum2, expectedSum3, expectedSum4);
    final MaxOutputHolder expectedMaxOutput = new MaxOutputHolder(expectedMax1, expectedMax2, expectedMax3, expectedMax4);
    final MinOutputHolder expectedMinOutput = new MinOutputHolder(expectedMin1, expectedMin2, expectedMin3, expectedMin4);

    /*
     * key [hello, every, 1] is occurring 3 times but the last occurrence
     * has null value in the column to be aggregated.
     * So count will not consider the null value, count(1) will account for it.
     */
    final long[] expectedCount = {2, 2, 2, 2, 1, 1, 1};
    final long[] expectedCount1 = {3, 2, 2, 2, 1, 1, 1};

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

      /* Measure column 2 */
      BigIntVector m2 = new BigIntVector("m2", allocator);
      populateBigInt(m2, aggcol2);
      c.add(m2);

      /* Measure column 3 */
      Float4Vector m3 = new Float4Vector("m3", allocator);
      populateFloat(m3, aggcol3);
      c.add(m3);

       /* Measure column 4 */
      Float8Vector m4 = new Float8Vector("m4", allocator);
      populateDouble(m4, aggcol4);
      c.add(m4);

      final int records = c.setAllCount(col1arr.length);

      /* create pivot definition */
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
        new FieldVectorPair(col1, col1),
        new FieldVectorPair(col2, col2),
        new FieldVectorPair(col3, col3)
      );

      final AccumulatorSet accumulator = createAccumulator(m1, m2, m3, m4, allocator);
      testReadWriteHelper(pivot, allocator, records,
        expectedOrdinals, accumulator, expectedSumOutput,
        expectedMaxOutput, expectedMinOutput, expectedCount,
        expectedCount1, false, expectedCount.length);
    }
  }

  /**
   * Fixed width GROUP BY key columns along with several
   * null values in aggregation columns.
   *
   * @throws Exception
   */
  @Test
  public void testPartitionRoundTrip2() throws Exception {
    /* try with HT batch size as power of 2 */
    MAX_VALUES_PER_BATCH = 4096;
    testPartitionRoundTrip2Helper();
    postSpillAccumulatorVectorFields.clear();

    MAX_VALUES_PER_BATCH = 2048;
    testPartitionRoundTrip2Helper();
    postSpillAccumulatorVectorFields.clear();

    MAX_VALUES_PER_BATCH = 1024;
    testPartitionRoundTrip2Helper();
    postSpillAccumulatorVectorFields.clear();

    /* try with HT batch size as arbitrary non power of 2 */
    MAX_VALUES_PER_BATCH = 940;
    testPartitionRoundTrip2Helper();
    postSpillAccumulatorVectorFields.clear();

    MAX_VALUES_PER_BATCH = 1017;
    testPartitionRoundTrip2Helper();
  }

  private void testPartitionRoundTrip2Helper() throws Exception {

    /* GROUP BY key columns */
    Long[] col1arr = {1000L, 1L, 1000L, 1L, 1L, 1L, 1L, 2L, 2L, 2L, null, 1L};
    Long[] col2arr = {1L, 1L, 1L, 1L, 2L, null, null, 2L, 1L, 2L, null, 2L};
    Integer[] col3arr = {1, 1, 1, 1, 3, 1, null, 2, 1, 2, null, 3};

    /**
     *  all keys:
     *
     *  [1000, 1, 1]       -> expected ordinal 0
     *  [1, 1, 1]          -> expected ordinal 1
     *  [1000, 1, 1]       -> expected ordinal 0
     *  [1, 1, 1]          -> expected ordinal 1
     *  [1, 2, 3]          -> expected ordinal 2
     *  [1, null, 1]       -> expected ordinal 3
     *  [1, null, null]    -> expected ordinal 4
     *  [2, 2, 2]          -> expected ordinal 5
     *  [2, 1, 1]          -> expected ordinal 6
     *  [2, 2, 2]          -> expected ordinal 5
     *  [null, null, null] -> expected ordinal 7
     *  [1, 2, 3]          -> expected ordinal 2
     *
     *  keys in HT and corresponding computed accumulators:
     *
     *  [1000, 1, 1]        -> 10 + null(0) = 10
     *  [1, 1, 1]           -> 16 + null(0) = 16
     *  [1, 2, 3]           -> 12 + null(0) = 12
     *  [1, null, 1]        -> null(0)
     *  [1, null, null]     -> 80
     *  [2, 2, 2]           -> null(0) + 90 = 90
     *  [2, 1, 1]           -> 14
     *  [null, null, null]  -> 10
     */

    /* expected ordinals after insertion into hash table */
    final int expectedOrdinals[] = {0, 1, 0, 1, 2, 3, 4, 5, 6, 5, 7, 2};

    /* Measure columns -- compute SUM, MIN, MAX, COUNT on each column, COUNT(1) on first column */
    Integer[] aggcol1 = {10, 16, null, null, 12, null, 80, null, 14, 90, 10, null};
    final Long[] expectedSum1 = {10L, 16L, 12L, null, 80L, 90L, 14L, 10L};
    final Integer[] expectedMax1 = {10, 16, 12, null, 80, 90, 14, 10};
    final Integer[] expectedMin1 = {10, 16, 12, null, 80, 90, 14, 10};

    Long[] aggcol2 = {100L, null, null, 350L, 120L, null, 800L, null, 900L, 125L, 110L, null};
    final Long[] expectedSum2 = {100L, 350L, 120L, null, 800L, 125L, 900L, 110L};
    final Long[] expectedMax2 = {100L, 350L, 120L, null, 800L, 125L, 900L, 110L};
    final Long[] expectedMin2 = {100L, 350L, 120L, null, 800L, 125L, 900L, 110L};

    Float[] aggcol3 = {20.5f, null, null, 3.44f, 12.25f, null, 80.75f, null, 90.25f, 10.25f, 11.25f, null};
    final Double[] expectedSum3 = {20.5d, 3.44d, 12.25d, null, 80.75d, 10.25d, 90.25d, 11.25d};
    final Float[] expectedMax3 = {20.5f, 3.44f, 12.25f, null, 80.75f, 10.25f, 90.25f, 11.25f};
    final Float[] expectedMin3 = {20.5f, 3.44f, 12.25f, null, 80.75f, 10.25f, 90.25f, 11.25f};

    Double[] aggcol4 = {null, 100.2375, 23.4265, null, 120.12345, null, 80.1254, null, 90.2345, 100.2345, 110.7523, null};
    final Double[] expectedSum4 = {23.4265, 100.2375, 120.12345, null, 80.1254, 100.2345, 90.2345, 110.7523};
    final Double[] expectedMax4 = {23.4265, 100.2375, 120.12345, null, 80.1254, 100.2345, 90.2345, 110.7523};
    final Double[] expectedMin4 = {23.4265, 100.2375, 120.12345, null, 80.1254, 100.2345, 90.2345, 110.7523};

    final SumOutputHolder expectedSumOutput = new SumOutputHolder(expectedSum1, expectedSum2, expectedSum3, expectedSum4);
    final MaxOutputHolder expectedMaxOutput = new MaxOutputHolder(expectedMax1, expectedMax2, expectedMax3, expectedMax4);
    final MinOutputHolder expectedMinOutput = new MinOutputHolder(expectedMin1, expectedMin2, expectedMin3, expectedMin4);

    final long[] expectedCount = {1, 1, 1, 0, 1, 1, 1, 1};
    final long[] expectedCount1 = {2, 2, 2, 1, 1, 2, 1, 1};

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer();) {

      /* GROUP BY key columns */
      BigIntVector col1 = new BigIntVector("col1", allocator);
      populateBigInt(col1, col1arr);
      c.add(col1);

      BigIntVector col2 = new BigIntVector("col2", allocator);
      populateBigInt(col2, col2arr);
      c.add(col2);

      IntVector col3 = new IntVector("col3", allocator);
      populateInt(col3, col3arr);
      c.add(col3);

      /* Measure column 1 */
      IntVector m1 = new IntVector("m1", allocator);
      populateInt(m1, aggcol1);
      c.add(m1);

      /* Measure column 2 */
      BigIntVector m2 = new BigIntVector("m2", allocator);
      populateBigInt(m2, aggcol2);
      c.add(m2);

      /* Measure column 3 */
      Float4Vector m3 = new Float4Vector("m3", allocator);
      populateFloat(m3, aggcol3);
      c.add(m3);

       /* Measure column 4 */
      Float8Vector m4 = new Float8Vector("m4", allocator);
      populateDouble(m4, aggcol4);
      c.add(m4);

      final int records = c.setAllCount(col1arr.length);

      /* create pivot definition */
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
        new FieldVectorPair(col1, col1),
        new FieldVectorPair(col2, col2),
        new FieldVectorPair(col3, col3)
      );

      /* test */
      final AccumulatorSet accumulator = createAccumulator(m1, m2, m3, m4, allocator);
      testReadWriteHelper(pivot, allocator, records,
        expectedOrdinals, accumulator, expectedSumOutput,
        expectedMaxOutput, expectedMinOutput, expectedCount,
        expectedCount1, true, expectedCount.length);
    }
  }

  /* helper function for tests */
  private void testReadWriteHelper(final PivotDef pivot, final BufferAllocator allocator,
                                   final int records, final int[] expectedOrdinals,
                                   final AccumulatorSet accumulator, final SumOutputHolder sum,
                                   final MaxOutputHolder max, final MinOutputHolder min,
                                   final long[] counts, final long[] counts1, boolean nullsInAccumulator,
                                   final int numCollapsedRecords) throws Exception {

    final SabotConfig sabotConfig = DremioTest.DEFAULT_SABOT_CONFIG;

    final ExecProtos.FragmentHandle fragmentHandle = ExecProtos.FragmentHandle.newBuilder()
      .setQueryId(new AttemptId().toQueryId())
      .setMinorFragmentId(0)
      .setMajorFragmentId(0)
      .build();

    final int estimatedVariableWidthKeySize = 15;
    final int fixedWidthDataRowSize = pivot.getBlockWidth();
    final int variableWidthDataRowSize = ((estimatedVariableWidthKeySize + LBlockHashTable.VAR_LENGTH_SIZE) * pivot.getVariableCount()) + LBlockHashTable.VAR_LENGTH_SIZE;
    final int fixedBufferSize = fixedWidthDataRowSize * MAX_VALUES_PER_BATCH;
    final int variableBlockSize = variableWidthDataRowSize * MAX_VALUES_PER_BATCH;

    try (
      final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
      final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());
      final PartitionToLoadSpilledData partitionToLoadSpilledData = new PartitionToLoadSpilledData(allocator, fixedBufferSize, variableBlockSize, postSpillAccumulatorVectorFields, MAX_VALUES_PER_BATCH)) {

      /* pivot the data into temporary space */
      Pivots.pivot(pivot, records, fbv, var);

      /* mock a single partition */
      final VectorizedHashAggPartition[] hashAggPartitions = new VectorizedHashAggPartition[1];

      try (ArrowBuf offsets = allocator.buffer(records * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
           SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)){

        Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

        final FileSystem fs = FileSystem.get(conf);
        final File tempDir = Files.createTempDir();
        final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
        tempDir.deleteOnExit();
        final SpillDirectory spillDirectory = new SpillDirectory(path, fs);

        SpillService spillService = mock(SpillService.class);
        doAnswer(new Answer<SpillDirectory>() {
          @Override
          public SpillDirectory answer(InvocationOnMock invocationOnMock) throws Throwable {
            return spillDirectory;
          }
        }).when(spillService).getSpillSubdir(any(String.class));

        LBlockHashTable sourceHashTable = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, true, accumulator, MAX_VALUES_PER_BATCH);
        VectorizedHashAggPartition hashAggPartition =  new VectorizedHashAggPartition(accumulator, sourceHashTable, pivot.getBlockWidth(), "P0");
        final VectorizedHashAggPartitionSpillHandler partitionSpillHandler = new VectorizedHashAggPartitionSpillHandler(hashAggPartitions, fragmentHandle, null, sabotConfig, 1, partitionToLoadSpilledData, spillService, true);
        hashAggPartitions[0] = hashAggPartition;

        final long keyFixedVectorAddr = fbv.getMemoryAddress();
        final long keyVarVectorAddr = var.getMemoryAddress();
        int[] actualOrdinals = new int[expectedOrdinals.length];
        final boolean fixedOnly = pivot.getVariableCount() == 0;

        /* compute hash on the pivoted data */
        hashValues.allocateNew(records);
        final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
          pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
        HashComputation.computeHash(blockChunk);

        /* insert pivoted data into the hash table */
        long offsetAddr = offsets.memoryAddress();
        final int hashPartitionIndex = 0;
        for (int keyIndex = 0; keyIndex < records; keyIndex++, offsetAddr += VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH) {
          final int keyHash = (int)hashValues.get(keyIndex);
          actualOrdinals[keyIndex] = sourceHashTable.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
          PlatformDependent.putByte(offsetAddr, (byte)hashPartitionIndex);
          PlatformDependent.putInt(offsetAddr + VectorizedHashAggOperator.HTORDINAL_OFFSET, actualOrdinals[keyIndex]);
          PlatformDependent.putInt(offsetAddr + VectorizedHashAggOperator.KEYINDEX_OFFSET, keyIndex);
        }

        /* accumulate */
        accumulator.accumulate(offsets.memoryAddress(), records);

        /* check hash table ordinals */
        assertArrayEquals(expectedOrdinals, actualOrdinals);
        final List<ArrowBuf> sourceFixedBuffers = sourceHashTable.getFixedBlockBuffers();
        final List<ArrowBuf> sourceVarBuffers = sourceHashTable.getVariableBlockBuffers();
        assertEquals(1, sourceFixedBuffers.size());
        assertEquals(1, sourceVarBuffers.size());

        /* once we spill a partition, we reset it to minimum size and that forces
         * a reset on the state of buffers -- reader/writer index back to 0 and
         * fill memory with 0. But in order to compare exact buffer contents for
         * testing, we save state about buffers to compare later after reading spilled
         * data.
         */
        final int fixedWidthPivotedDataLength = sourceFixedBuffers.get(0).readableBytes();
        final int varWidthPivotedDataLength = sourceVarBuffers.get(0).readableBytes();
        try(final ArrowBuf sourceFixedBuffer = allocator.buffer(fixedWidthPivotedDataLength);
            final ArrowBuf sourceVarBuffer = allocator.buffer(varWidthPivotedDataLength)) {
          PlatformDependent.copyMemory(sourceFixedBuffers.get(0).memoryAddress(), sourceFixedBuffer.memoryAddress(), fixedWidthPivotedDataLength);
          PlatformDependent.copyMemory(sourceVarBuffers.get(0).memoryAddress(), sourceVarBuffer.memoryAddress(), varWidthPivotedDataLength);

          /* spill the partition */
          VectorizedHashAggPartition victimPartition = partitionSpillHandler.chooseVictimPartition();
          if (victimPartition == null) {
            victimPartition = hashAggPartition;
          }
          partitionSpillHandler.spillPartition(victimPartition);

          /* NO-OP since partition was empty after spilling */
          partitionSpillHandler.spillAnyInMemoryDataForSpilledPartitions();

          /* read back spilled data */
          try (final SpilledPartitionIterator partitionIterator = partitionSpillHandler.getActiveSpilledPartition()) {
            int numRecordsInBatch = 0;
            int iteration = 0;
            while ((numRecordsInBatch = partitionIterator.getNextBatch()) > 0) {
              assertEquals(numRecordsInBatch, numCollapsedRecords);
              final ArrowBuf fixedWidthPivotedData = partitionToLoadSpilledData.getFixedKeyColPivotedData();
              final ArrowBuf variableWidthPivotedData = partitionToLoadSpilledData.getVariableKeyColPivotedData();
              final FieldVector[] accumulatorVectors = partitionToLoadSpilledData.getPostSpillAccumulatorVectors();

              /* compare the pivoted hash table buffer data length read from spilled batch to
               * original state that was spilled.
               */
              assertEquals(fixedWidthPivotedDataLength, fixedWidthPivotedData.readableBytes());
              assertEquals(varWidthPivotedDataLength, variableWidthPivotedData.readableBytes());

              /* compare bytes of fixed width pivoted hash table data loaded from disk to the in-memory
               * contents that were spilled.
               */
              long addr1 = sourceFixedBuffer.memoryAddress();
              long addr2 = fixedWidthPivotedData.memoryAddress();
              assertTrue(memcmp(addr1, addr2, sourceFixedBuffers.get(0).readableBytes()));

              /* compare bytes of variable width pivoted hash table data loaded from disk to the in-memory
               * contents that were spilled.
               */
              addr1 = sourceVarBuffer.memoryAddress();
              addr2 = variableWidthPivotedData.memoryAddress();
              assertTrue(memcmp(addr1, addr2, sourceFixedBuffers.get(0).readableBytes()));

              /* check accumulator data */
              verifyAccumulators(accumulatorVectors, sum, max, min, counts, counts1, nullsInAccumulator, numRecordsInBatch);
              iteration++;
            }
            assertEquals(1, iteration);
          }
        } finally {
          partitionSpillHandler.close();
          hashAggPartition.close();
          fs.close();
        }
      }
    }
  }

  /**
   * Check the initial accumulators (the ones built when partition was populated in-memory).
   * Compare the values of the accumulators to expected computed values.
   *
   * Then check the new accumulators (the ones built after de-serializing the data from disk).
   * Compare the values of these accumulators to the original accumulators -- the old accumulator
   * vector with computed values that was spilled now becomes the input vector after reading
   * data back from disk.
   *
   * @param deserializedAccumulatorVectors
   * @param sum output holder for SUM on all measure columns
   * @param max output holder for MAX on all measure columns
   * @param min output holder for MIN on all measure columns
   */
  private void verifyAccumulators(final FieldVector[] deserializedAccumulatorVectors,
                                  final SumOutputHolder sum, final MaxOutputHolder max, final MinOutputHolder min,
                                  final long[] counts, final long[] counts1,
                                  final boolean nullsInAccumulator, final int valueCount) {
    /*
     * 4 measure columns and we compute SUM, MIN, MAX, COUNT for each
     * along with COUNT1 just for 1st column
     */
    assertEquals(17, deserializedAccumulatorVectors.length);

    /* measure column 1 - SUM, MIN, MAX, COUNT, COUNT1 */
    BigIntVector ac1New = (BigIntVector)deserializedAccumulatorVectors[0];
    IntVector ac2New = (IntVector)deserializedAccumulatorVectors[1];
    IntVector ac3New = (IntVector)deserializedAccumulatorVectors[2];
    BigIntVector ac4New = (BigIntVector)deserializedAccumulatorVectors[3];
    BigIntVector ac5New = (BigIntVector)deserializedAccumulatorVectors[4];

    /* measure column 2 - SUM, MIN, MAX, COUNT */
    BigIntVector ac6New = (BigIntVector)deserializedAccumulatorVectors[5];
    BigIntVector ac7New = (BigIntVector)deserializedAccumulatorVectors[6];
    BigIntVector ac8New = (BigIntVector)deserializedAccumulatorVectors[7];
    BigIntVector ac9New = (BigIntVector)deserializedAccumulatorVectors[8];

    /* measure column 3 - SUM, MIN, MAX, COUNT */
    Float8Vector ac10New = (Float8Vector)deserializedAccumulatorVectors[9];
    Float4Vector ac11New = (Float4Vector)deserializedAccumulatorVectors[10];
    Float4Vector ac12New = (Float4Vector)deserializedAccumulatorVectors[11];
    BigIntVector ac13New = (BigIntVector)deserializedAccumulatorVectors[12];

    /* measure column 4 - SUM, MIN, MAX, COUNT */
    Float8Vector ac14New = (Float8Vector)deserializedAccumulatorVectors[13];
    Float8Vector ac15New = (Float8Vector)deserializedAccumulatorVectors[14];
    Float8Vector ac16New = (Float8Vector)deserializedAccumulatorVectors[15];
    BigIntVector ac17New = (BigIntVector)deserializedAccumulatorVectors[16];

    assertEquals(valueCount, ac1New.getValueCount());
    assertEquals(valueCount, ac2New.getValueCount());
    assertEquals(valueCount, ac3New.getValueCount());
    assertEquals(valueCount, ac4New.getValueCount());
    assertEquals(valueCount, ac5New.getValueCount());

    assertEquals(valueCount, ac6New.getValueCount());
    assertEquals(valueCount, ac7New.getValueCount());
    assertEquals(valueCount, ac8New.getValueCount());
    assertEquals(valueCount, ac9New.getValueCount());

    assertEquals(valueCount, ac10New.getValueCount());
    assertEquals(valueCount, ac11New.getValueCount());
    assertEquals(valueCount, ac12New.getValueCount());
    assertEquals(valueCount, ac13New.getValueCount());

    assertEquals(valueCount, ac14New.getValueCount());
    assertEquals(valueCount, ac15New.getValueCount());
    assertEquals(valueCount, ac16New.getValueCount());
    assertEquals(valueCount, ac17New.getValueCount());

    for (int i = 0; i < sum.intOut.length; i++) {
      /* VERIFY ACCUMULATORS REBUILT OFF DISK */
      assertEquals(sum.intOut[i], ac1New.getObject(i));
      assertEquals(max.intOut[i], ac2New.getObject(i));
      assertEquals(min.intOut[i], ac3New.getObject(i));
      assertEquals(counts[i], ac4New.get(i));
      assertEquals(counts1[i], ac5New.get(i));

      assertEquals(sum.bigintOut[i],  ac6New.getObject(i));
      assertEquals(max.bigintOut[i], ac7New.getObject(i));
      assertEquals(min.bigintOut[i], ac8New.getObject(i));
      assertEquals(counts[i], ac9New.get(i));

      if (i == 3 && nullsInAccumulator) {
        assertTrue(ac10New.isNull(i));
        assertTrue(ac14New.isNull(i));
      } else {
        assertEquals(sum.floatOut[i], ac10New.getObject(i), 0.1D);
        assertEquals(sum.doubleOut[i], ac14New.getObject(i), 0.1D);
      }

      assertEquals(max.floatOut[i], ac11New.getObject(i));
      assertEquals(min.floatOut[i], ac12New.getObject(i));
      assertEquals(counts[i], ac13New.get(i));

      assertEquals(max.doubleOut[i], ac15New.getObject(i));
      assertEquals(min.doubleOut[i], ac16New.getObject(i));
      assertEquals(counts[i], ac17New.get(i));
    }
  }

  /* JAVA equivalent of C/C++ memcmp */
  private boolean memcmp(final long addr1, final long addr2,
                         int length) {
    long leftAddr = addr1;
    long rightAddr = addr2;

    while (length > 7) {
      if (PlatformDependent.getLong(leftAddr) != PlatformDependent.getLong(rightAddr)) {
        return false;
      }

      leftAddr += 8;
      rightAddr += 8;
      length -= 8;
    }

    while (length != 0) {
      if (PlatformDependent.getByte(leftAddr) != PlatformDependent.getByte(rightAddr)) {
        return false;
      }

      leftAddr++;
      rightAddr++;
      length--;
    }

    return true;
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

  private void populateBigInt(BigIntVector vector, Long[] data) {
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < data.length; i++){
      Long val = data[i];
      if(val != null){
        vector.setSafe(i, val);
      } else {
        vector.setSafe(i, 0, r.nextLong());
      }
    }
    vector.setValueCount(data.length);
  }

  private void populateFloat(Float4Vector vector, Float[] data) {
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < data.length; i++){
      Float val = data[i];
      if(val != null){
        vector.setSafe(i, val);
      } else {
        vector.setSafe(i, 0, r.nextFloat());
      }
    }
    vector.setValueCount(data.length);
  }

  private void populateDouble(Float8Vector vector, Double[] data) {
    vector.allocateNew();
    Random r = new Random();
    for(int i =0; i < data.length; i++){
      Double val = data[i];
      if(val != null){
        vector.setSafe(i, val);
      } else {
        vector.setSafe(i, 0, r.nextDouble());
      }
    }
    vector.setValueCount(data.length);
  }

  /**
   * we have 4 measure columns and want to compute SUM, MIN, MAX, COUNT for
   * each column and COUNT1 just for first column. Accordingly create the
   * necessary accumulators in this function.
   *
   * @param in1 input vector for first measure column
   * @param in2 input vector for second measure column
   * @param in3 input vector for third measure column
   * @param in4 input vector for fourth measure column
   * @param allocator
   *
   * @return NestedAccumulator
   */
  private AccumulatorSet createAccumulator(IntVector in1, BigIntVector in2,
                                              Float4Vector in3, Float8Vector in4,
                                              final BufferAllocator allocator) {
    /* INT */
    BigIntVector in1SumOutputVector = new BigIntVector("int-sum", allocator);
    final SumAccumulators.IntSumAccumulator in1SumAccum =
      new SumAccumulators.IntSumAccumulator(in1, in1SumOutputVector, in1SumOutputVector,
                                            MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in1SumOutputVector.getField());

    IntVector in1MaxOutputVector = new IntVector("int-max", allocator);
    final MaxAccumulators.IntMaxAccumulator in1MaxAccum =
      new MaxAccumulators.IntMaxAccumulator(in1, in1MaxOutputVector, in1MaxOutputVector,
                                            MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in1MaxOutputVector.getField());

    IntVector in1MinOutputVector = new IntVector("int-min", allocator);
    final MinAccumulators.IntMinAccumulator in1MinAccum =
      new MinAccumulators.IntMinAccumulator(in1, in1MinOutputVector, in1MinOutputVector,
                                            MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in1MinOutputVector.getField());

    BigIntVector in1Count = new BigIntVector("int-count", allocator);
    final CountColumnAccumulator in1countAccum =
      new CountColumnAccumulator(in1, in1Count, in1Count,
                                 MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in1Count.getField());

    BigIntVector in1Count1 = new BigIntVector("int-count1", allocator);
    final CountOneAccumulator in1count1Accum =
      new CountOneAccumulator(null, in1Count1, in1Count1,
                              MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in1Count1.getField());

    /* BIGINT */
    BigIntVector in2SumOutputVector = new BigIntVector("bigint-sum", allocator);
    final SumAccumulators.BigIntSumAccumulator in2SumAccum =
      new SumAccumulators.BigIntSumAccumulator(in2, in2SumOutputVector, in2SumOutputVector,
                                               MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in2SumOutputVector.getField());

    BigIntVector in2MaxOutputVector = new BigIntVector("bigint-max", allocator);
    final MaxAccumulators.BigIntMaxAccumulator in2MaxAccum =
      new MaxAccumulators.BigIntMaxAccumulator(in2, in2MaxOutputVector, in2MaxOutputVector,
                                               MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in2MaxOutputVector.getField());

    BigIntVector in2MinOutputVector = new BigIntVector("bigint-min", allocator);
    final MinAccumulators.BigIntMinAccumulator in2MinAccum =
      new MinAccumulators.BigIntMinAccumulator(in2, in2MinOutputVector, in2MinOutputVector,
                                               MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in2MinOutputVector.getField());

    BigIntVector in2Count = new BigIntVector("bigint-count", allocator);
    final CountColumnAccumulator in2countAccum =
      new CountColumnAccumulator(in2, in2Count, in2Count,
                                 MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in2Count.getField());

    /* FLOAT */
    Float8Vector in3SumOutputVector = new Float8Vector("float-sum", allocator);
    final SumAccumulators.FloatSumAccumulator in3SumAccum =
      new SumAccumulators.FloatSumAccumulator(in3, in3SumOutputVector, in3SumOutputVector,
                                              MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in3SumOutputVector.getField());

    Float4Vector in3MaxOutputVector = new Float4Vector("float-max", allocator);
    final MaxAccumulators.FloatMaxAccumulator in3MaxAccum =
      new MaxAccumulators.FloatMaxAccumulator(in3, in3MaxOutputVector, in3MaxOutputVector,
                                              MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in3MaxOutputVector.getField());

    Float4Vector in3MinOutputVector = new Float4Vector("float-min", allocator);
    final MinAccumulators.FloatMinAccumulator in3MinAccum =
      new MinAccumulators.FloatMinAccumulator(in3, in3MinOutputVector, in3MinOutputVector,
                                              MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in3MinOutputVector.getField());

    BigIntVector in3Count = new BigIntVector("float-count", allocator);
    final CountColumnAccumulator in3countAccum =
      new CountColumnAccumulator(in3, in3Count, in3Count,
                                 MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in3Count.getField());

    /* DOUBLE */
    Float8Vector in4SumOutputVector = new Float8Vector("double-sum", allocator);
    final SumAccumulators.DoubleSumAccumulator in4SumAccum =
      new SumAccumulators.DoubleSumAccumulator(in4, in4SumOutputVector, in4SumOutputVector,
                                               MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in4SumOutputVector.getField());

    Float8Vector in4MaxOutputVector = new Float8Vector("double-max", allocator);
    final MaxAccumulators.DoubleMaxAccumulator in4MaxAccum =
      new MaxAccumulators.DoubleMaxAccumulator(in4, in4MaxOutputVector, in4MaxOutputVector,
                                               MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in4MaxOutputVector.getField());

    Float8Vector in4MinOutputVector = new Float8Vector("double-min", allocator);
    final MinAccumulators.DoubleMinAccumulator in4MinAccum =
      new MinAccumulators.DoubleMinAccumulator(in4, in4MinOutputVector, in4MinOutputVector,
                                               MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in4MinOutputVector.getField());

    BigIntVector in4Count = new BigIntVector("double-count", allocator);
    final CountColumnAccumulator in4countAccum =
      new CountColumnAccumulator(in4, in4Count, in4Count,
                                 MAX_VALUES_PER_BATCH, allocator);
    postSpillAccumulatorVectorFields.add(in4Count.getField());

    return new AccumulatorSet(4*1024, 128*1024, allocator,
      in1SumAccum, in1MaxAccum, in1MinAccum, in1countAccum, in1count1Accum,
      in2SumAccum, in2MaxAccum, in2MinAccum, in2countAccum,
      in3SumAccum, in3MaxAccum, in3MinAccum, in3countAccum,
      in4SumAccum, in4MaxAccum, in4MinAccum, in4countAccum);
  }

  private static class OutputHolder {

    final Long[] bigintOut;
    final Double[] doubleOut;

    OutputHolder(final Long[] bigintOut, final Double[] doubleOut) {
      this.bigintOut = bigintOut;
      this.doubleOut = doubleOut;
    }
  }

  private static class SumOutputHolder extends OutputHolder {
    final Long[] intOut;
    final Double[] floatOut;
    SumOutputHolder(final Long[] intOut, final Long[] bigintOut,
                    final Double[] floatOut, final Double[] doubleOut) {
      super(bigintOut, doubleOut);
      this.intOut = intOut;
      this.floatOut = floatOut;
    }
  }

  private static class MaxOutputHolder extends OutputHolder {
    final Integer[] intOut;
    final Float[] floatOut;
    MaxOutputHolder(final Integer[] intOut, final Long[] bigintOut,
                    final Float[] floatOut, final Double[] doubleOut) {
      super(bigintOut, doubleOut);
      this.intOut = intOut;
      this.floatOut = floatOut;
    }
  }

  private static class MinOutputHolder extends OutputHolder {
    final Integer[] intOut;
    final Float[] floatOut;
    MinOutputHolder(final Integer[] intOut, final Long[] bigintOut,
                    final Float[] floatOut, final Double[] doubleOut) {
      super(bigintOut, doubleOut);
      this.intOut = intOut;
      this.floatOut = floatOut;
    }
  }
}
