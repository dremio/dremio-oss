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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.record.VectorContainer;
import com.dremio.options.OptionManager;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorBuilder;
import com.dremio.sabot.op.aggregate.vectorized.AccumulatorSet;
import com.dremio.sabot.op.aggregate.vectorized.CountColumnAccumulator;
import com.dremio.sabot.op.aggregate.vectorized.CountOneAccumulator;
import com.dremio.sabot.op.aggregate.vectorized.MaxAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.MinAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.PartitionToLoadSpilledData;
import com.dremio.sabot.op.aggregate.vectorized.SumAccumulators;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggDiskPartition;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartition;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSpillHandler;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggPartitionSpillHandler.SpilledPartitionIterator;
import com.dremio.service.spill.SpillDirectory;
import com.dremio.service.spill.SpillService;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.koloboke.collect.hash.HashConfig;
import io.netty.util.internal.PlatformDependent;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SimpleBigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Tests for {@link VectorizedHashAggPartitionSpillHandler}
 *
 * <p>These are sort of standalone tests for particular modules which require us to mock certain
 * things. We will also have separate tests to test the operator lifeycle using BaseTestOperator
 * framework and there we won't have to mock certain data structures. This will be done as part of
 * code changes in DX-10239 which integrates {@link VectorizedHashAggOperator} with already
 * implemented spilling infrastructure.
 */
public class TestVectorizedHashAggPartitionSpillHandler extends DremioTest {
  private final List<Field> postSpillAccumulatorVectorFields = Lists.newArrayList();
  private final byte[] accumulatorTypes = new byte[7];
  private final List<FieldVector> varlenAccumVectorFields = Lists.newArrayList();
  private int MAX_VALUES_PER_BATCH = 0;

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Test
  public void testPartitionSpillHandler() throws Exception {
    /* try with HT batch size as power of 2 */
    MAX_VALUES_PER_BATCH = 4096;
    testPartitionSpillHandlerHelper();
    postSpillAccumulatorVectorFields.clear();
    varlenAccumVectorFields.clear();

    MAX_VALUES_PER_BATCH = 2048;
    testPartitionSpillHandlerHelper();
    postSpillAccumulatorVectorFields.clear();
    varlenAccumVectorFields.clear();

    MAX_VALUES_PER_BATCH = 1024;
    testPartitionSpillHandlerHelper();
    postSpillAccumulatorVectorFields.clear();
    varlenAccumVectorFields.clear();

    /* try with HT batch size as non power of 2 */
    MAX_VALUES_PER_BATCH = 990;
    testPartitionSpillHandlerHelper();
    postSpillAccumulatorVectorFields.clear();
    varlenAccumVectorFields.clear();

    MAX_VALUES_PER_BATCH = 976;
    testPartitionSpillHandlerHelper();
    postSpillAccumulatorVectorFields.clear();
    varlenAccumVectorFields.clear();
  }

  private void testPartitionSpillHandlerHelper() throws Exception {

    /* GROUP BY key columns */
    String[] col1arr = {
      "hello", "my", "hello", "hello", null, null, "hello", null, "hello", "my", "my", "hello"
    };

    String[] col2arr = {
      "every", "every", "every", "none", null, null, "every", null, "none", "every", "every",
      "every"
    };

    Integer[] col3arr = {
      1, 1, 1, 1,
      1, 1, 2, 2,
      1, 1, null, 1
    };

    /**
     * all keys:
     *
     * <p>[hello, every, 1] -> expected ordinal 0 [my, every, 1] -> expected ordinal 1 [hello,
     * every, 1] -> expected ordinal 0 [hello, none, 1] -> expected ordinal 2 [null, null, 1] ->
     * expected ordinal 3 [null, null, 1] -> expected ordinal 3 [hello, every, 2] -> expected
     * ordinal 4 [null, null, 2] -> expected ordinal 5 [hello, none, 1] -> expected ordinal 2 [my,
     * every, 1] -> expected ordinal 1 [my, every, null] -> expected ordinal 6 [hello, every, 1] ->
     * expected ordinal 0
     *
     * <p>keys in HT and corresponding computed accumulators:
     *
     * <p>[hello, every, 1] -> 100000 + 200000 + null(0) = 300000 [my, every, 1] -> 160000 + 100000
     * = 260000 [hello, none, 1] -> 300000 + 90000 = 390000 [null, null, 1] -> 120000 + 50000 =
     * 170000 [hello, every, 2] -> 80000 [null, null, 2] -> 140000 [my, every, null] -> 110000
     */

    /* Expected ordinals after insertion into hash table */
    final int[] expectedOrdinals = {0, 1, 0, 2, 3, 3, 4, 5, 2, 1, 6, 0};

    final String[] aggVarLenMin1 = {
      "zymotechnical" /*0*/,
      "zymophosphate" /*1*/,
      "zygozoospore" /*0*/,
      "zygosporangium" /*2*/,
      "zygosporange" /*3*/,
      "zygopleural" /*3*/,
      "abarticulation" /*4*/,
      "abbotnullius" /*5*/,
      "abarticulation" /*2*/,
      "abandonment" /*1*/,
      "A" /*6*/,
      "abarticulation" /*0*/
    };

    final String[] aggVarLenMax1 = {
      "aymotechnical" /*0*/,
      "aymophosphate" /*1*/,
      "bygozoospore" /*0*/,
      "aygosporangium" /*2*/,
      "aygosporange" /*3*/,
      "zygopleural" /*3*/,
      "abarticulation" /*4*/,
      "abbotnullius" /*5*/,
      "zbarticulation" /*2*/,
      "zbandonment" /*1*/,
      "A" /*6*/,
      "zbarticulation" /*0*/
    };

    /* Measure columns -- compute SUM, MIN, MAX, COUNT, COUNT(1) */
    Integer[] aggcol1 = {
      100000, 160000, 200000, 300000, 120000, 50000, 80000, 140000, 90000, 100000, 110000, null
    };

    final String[] expectedMin5 = {
      "abarticulation" /*0*/,
      "abandonment" /*1*/,
      "abarticulation" /*2*/,
      "zygopleural" /*3*/,
      "abarticulation" /*4*/,
      "abbotnullius" /*5*/,
      "A" /*6*/
    };

    final String[] expectedMax5 = {
      "zbarticulation" /*0*/,
      "zbandonment" /*1*/,
      "zbarticulation" /*2*/,
      "zygopleural" /*3*/,
      "abarticulation" /*4*/,
      "abbotnullius" /*5*/,
      "A" /*6*/
    };

    /*
     * key [hello, every, 1] is occurring 3 times but the last occurrence
     * has null value in the column to be aggregated.
     * So count will not consider the null value, count(1) will account for it.
     */
    final long[] expectedCount = {2, 2, 2, 2, 1, 1, 1};
    final long[] expectedCount1 = {3, 2, 2, 2, 1, 1, 1};

    try (final BufferAllocator allocator =
            allocatorRule.newAllocator(
                "test-vectorized-hashagg-partition-spill-handler", 0, Long.MAX_VALUE);
        final VectorContainer c = new VectorContainer(); ) {

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

      /* Measure column 2  - min on varlength column */
      VarCharVector m2 = new VarCharVector("m2", allocator);
      populateStrings(m2, aggVarLenMin1);
      c.add(m2);

      /* Measure column 3 - max on varlength column */
      VarCharVector m3 = new VarCharVector("m3", allocator);
      populateStrings(m3, aggVarLenMax1);
      c.add(m3);

      final int records = c.setAllCount(col1arr.length);

      /* create pivot definition */
      final PivotDef pivot =
          PivotBuilder.getBlockDefinition(
              new FieldVectorPair(col1, col1),
              new FieldVectorPair(col2, col2),
              new FieldVectorPair(col3, col3));

      testHelper(
          pivot,
          allocator,
          records,
          expectedOrdinals,
          m1,
          m2,
          m3,
          expectedCount,
          expectedCount1,
          expectedCount.length);
    }
  }

  private void insertAndAccumulateForAllPartitions(
      final BufferAllocator allocator,
      final int records,
      final PivotDef pivot,
      final VectorizedHashAggPartition[] partitions,
      final int[] expectedOrdinals) {
    try (final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector var =
            new VariableBlockVector(allocator, pivot.getVariableCount());
        final ArrowBuf offsets =
            allocator.buffer(records * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
        final SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {

      /* pivot the data into temporary space */
      Pivots.pivot(pivot, records, fbv, var);

      final long keyFixedVectorAddr = fbv.getMemoryAddress();
      final long keyVarVectorAddr = var.getMemoryAddress();
      int[] actualOrdinals = new int[expectedOrdinals.length];
      final boolean fixedOnly = pivot.getVariableCount() == 0;

      /* compute hash on the pivoted data */
      hashValues.allocateNew(records);
      final BlockChunk blockChunk =
          new BlockChunk(
              keyFixedVectorAddr,
              keyVarVectorAddr,
              var.getCapacity(),
              fixedOnly,
              pivot.getBlockWidth(),
              records,
              hashValues.getBufferAddress(),
              0);
      HashComputation.computeHash(blockChunk);

      /* since we are mocking partitions to test spill handler,
       * just insert same data into all partitions
       */
      int hashPartitionIndex = 0;
      for (int i = 0; i < partitions.length; i++) {

        /* insert */
        long offsetAddr = offsets.memoryAddress();
        LBlockHashTable hashTable = null;
        for (int keyIndex = 0;
            keyIndex < records;
            keyIndex++, offsetAddr += VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH) {
          final int keyHash = (int) hashValues.get(keyIndex);
          hashTable = partitions[i].getHashTable();
          actualOrdinals[keyIndex] =
              hashTable.add(
                  keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash);
          PlatformDependent.putByte(offsetAddr, (byte) hashPartitionIndex);
          PlatformDependent.putInt(
              offsetAddr + VectorizedHashAggOperator.HTORDINAL_OFFSET, actualOrdinals[keyIndex]);
          PlatformDependent.putInt(
              offsetAddr + VectorizedHashAggOperator.KEYINDEX_OFFSET, keyIndex);
        }

        /* accumulate */
        partitions[i]
            .getAccumulator()
            .accumulate(
                offsets.memoryAddress(),
                records,
                partitions[0].getHashTable().getBitsInChunk(),
                partitions[0].getHashTable().getChunkOffsetMask());

        /* check hashtable */
        assertArrayEquals(expectedOrdinals, actualOrdinals);
        assertEquals(1, hashTable.getFixedBlockBuffers().size());
        assertEquals(1, hashTable.getFixedBlockBuffers().size());
      }
    }
  }

  /* helper function for tests */
  private void testHelper(
      final PivotDef pivot,
      final BufferAllocator allocator,
      final int records,
      final int[] expectedOrdinals,
      final IntVector accumulatorInput,
      final VarCharVector m2,
      final VarCharVector m3,
      final long[] counts,
      final long[] counts1,
      final int numCollapsedRecords)
      throws Exception {

    final SabotConfig sabotConfig = DremioTest.DEFAULT_SABOT_CONFIG;
    final ExecProtos.FragmentHandle fragmentHandle =
        ExecProtos.FragmentHandle.newBuilder()
            .setQueryId(new AttemptId().toQueryId())
            .setMinorFragmentId(0)
            .setMajorFragmentId(0)
            .build();

    /* mock 4 partitions */
    final int numPartitions = 4;
    final ArrowBuf combined =
        allocator.buffer(
            numPartitions
                * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH
                * MAX_VALUES_PER_BATCH);
    final VectorizedHashAggPartition[] partitions = new VectorizedHashAggPartition[numPartitions];
    // to track the temporary vectors. as a convenience for releasing them at once
    VarCharVector[] tempVectors = new VarCharVector[2];
    tempVectors[0] = new VarCharVector("varchar-min", allocator);
    tempVectors[0].allocateNew(1024 * 1024, records);
    tempVectors[1] = new VarCharVector("varchar-max", allocator);
    tempVectors[1].allocateNew(1024 * 1024, records);

    for (int i = 0; i < numPartitions; i++) {
      final AccumulatorSet accumulator =
          createAccumulator(accumulatorInput, m2, m3, tempVectors, allocator, (i == 0));
      LBlockHashTable sourceHashTable =
          new LBlockHashTable(
              HashConfig.getDefault(), pivot, allocator, 16000, 10, true, MAX_VALUES_PER_BATCH);
      sourceHashTable.registerResizeListener(accumulator);
      final ArrowBuf buffer =
          combined.slice(
              i * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH * MAX_VALUES_PER_BATCH,
              VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH * MAX_VALUES_PER_BATCH);
      VectorizedHashAggPartition hashAggPartition =
          new VectorizedHashAggPartition(
              accumulator,
              sourceHashTable,
              pivot.getBlockWidth(),
              "P" + String.valueOf(i),
              buffer,
              false);
      partitions[i] = hashAggPartition;
      varlenAccumVectorFields.add(tempVectors[0]);
      varlenAccumVectorFields.add(tempVectors[1]);
    }
    combined.close();

    /* we have just initialized partitions and everything should be in memory */
    verifyStateOfInMemoryPartitions(partitions);

    final int estimatedVariableWidthKeySize = 15;
    final int fixedWidthDataRowSize = pivot.getBlockWidth();
    final int variableWidthDataRowSize =
        ((estimatedVariableWidthKeySize + LBlockHashTable.VAR_LENGTH_SIZE)
                * pivot.getVariableCount())
            + LBlockHashTable.VAR_LENGTH_SIZE;
    final int fixedBufferSize = fixedWidthDataRowSize * MAX_VALUES_PER_BATCH;
    final int variableBlockSize = variableWidthDataRowSize * MAX_VALUES_PER_BATCH;

    try {
      FileSystem fs = null;
      VectorizedHashAggPartitionSpillHandler partitionSpillHandler = null;
      try (final PartitionToLoadSpilledData partitionToLoadSpilledData =
          new PartitionToLoadSpilledData(
              allocator,
              fixedBufferSize,
              variableBlockSize,
              postSpillAccumulatorVectorFields,
              accumulatorTypes,
              MAX_VALUES_PER_BATCH,
              (estimatedVariableWidthKeySize * MAX_VALUES_PER_BATCH))) {
        Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");

        fs = FileSystem.get(conf);
        final File tempDir = Files.createTempDir();
        final Path path = new Path(tempDir.getAbsolutePath(), "dremioSerializable");
        tempDir.deleteOnExit();
        final SpillDirectory spillDirectory = new SpillDirectory(path, fs);

        SpillService spillService = mock(SpillService.class);
        doAnswer(
                new Answer<SpillDirectory>() {
                  @Override
                  public SpillDirectory answer(InvocationOnMock invocationOnMock) throws Throwable {
                    return spillDirectory;
                  }
                })
            .when(spillService)
            .getSpillSubdir(any(String.class));

        OptionManager optionManager = mock(OptionManager.class);
        partitionSpillHandler =
            new VectorizedHashAggPartitionSpillHandler(
                partitions,
                fragmentHandle,
                optionManager,
                sabotConfig,
                1,
                partitionToLoadSpilledData,
                spillService,
                true,
                null);

        /* insert incoming into partitions */
        insertAndAccumulateForAllPartitions(
            allocator, records, pivot, partitions, expectedOrdinals);

        /* spill a partition */
        VectorizedHashAggPartition victimPartition1 = partitionSpillHandler.chooseVictimPartition();
        if (victimPartition1 == null) {
          victimPartition1 = partitions[0];
        }
        partitionSpillHandler.spillPartition(victimPartition1);

        /* spilled partition should have been added to currentSpilledPartitions list */
        assertEquals(1, partitionSpillHandler.getActiveSpilledPartitionCount());
        /* FIFO spill partition queue should still be empty as we haven't closed the iteration yet */
        assertTrue(partitionSpillHandler.isSpillQueueEmpty());
        /* since all partition sizes were equal, we should have spilled first partition */
        final VectorizedHashAggDiskPartition spilledPartition1 =
            partitionSpillHandler.getActiveSpilledPartitions().get(0);
        assertEquals(partitions[0].getIdentifier(), spilledPartition1.getIdentifier());
        assertEquals("P0", spilledPartition1.getIdentifier());
        /* partition should have been marked as spilled */
        assertTrue(partitions[0].isSpilled());
        /* the corresponding optional pointer to spilled partition should not be null */
        assertTrue(partitions[0].getSpillInfo() != null);
        assertTrue(partitions[0].getSpillInfo() == spilledPartition1);
        /* similarly the back-pointer to corresponding in-memory partition be non-null since we haven't closed the iteration yet */
        assertTrue(spilledPartition1.getInmemoryPartitionBackPointer() != null);
        assertTrue(spilledPartition1.getInmemoryPartitionBackPointer() == partitions[0]);
        /* after we spill a partition, we downsize it minimum pre-allocated memory, the size should
         * be reported as 0 since the partition is effectively empty without any data.
         */
        assertEquals(0, partitions[0].getSize());
        /* check spill file name */
        assertEquals("P0", spilledPartition1.getSpillFile().getPath().getName());

        /* spill another partition */
        VectorizedHashAggPartition victimPartition2 = partitionSpillHandler.chooseVictimPartition();
        if (victimPartition2 == null) {
          victimPartition2 = partitions[1];
        }
        partitionSpillHandler.spillPartition(victimPartition2);

        /* spilled partition should have been added to currentSpilledPartitions list */
        assertEquals(2, partitionSpillHandler.getActiveSpilledPartitionCount());
        /* FIFO spill partition queue should still be empty as we haven't closed the iteration yet */
        assertTrue(partitionSpillHandler.isSpillQueueEmpty());
        /* since all partition sizes were equal, we should have spilled second partition as first is already spilled */
        final VectorizedHashAggDiskPartition spilledPartition2 =
            partitionSpillHandler.getActiveSpilledPartitions().get(1);
        assertEquals(partitions[1].getIdentifier(), spilledPartition2.getIdentifier());
        assertEquals("P1", spilledPartition2.getIdentifier());
        /* partition should have been marked as spilled */
        assertTrue(partitions[1].isSpilled());
        /* the corresponding optional pointer to spilled partition should not be null */
        assertTrue(partitions[1].getSpillInfo() != null);
        assertTrue(partitions[1].getSpillInfo() == spilledPartition2);
        /* similarly the back-pointer to corresponding in-memory partition should be non-null since we haven't closed the iteration yet */
        assertTrue(spilledPartition2.getInmemoryPartitionBackPointer() != null);
        assertTrue(spilledPartition2.getInmemoryPartitionBackPointer() == partitions[1]);
        /* after we spill a partition, we downsize it minimum pre-allocated memory, the size should
         * be reported as 0 since the partition is effectively empty without any data.
         */
        assertEquals(0, partitions[1].getSize());
        /* check spill file name */
        assertEquals("P1", spilledPartition2.getSpillFile().getPath().getName());

        /* spill in-memory data for spilled partitions -- NO-OP since nothing was inserted into
         * partitions after the initial spill so they must be empty and serializable
         * code will handle if given empty partitions to spill
         */
        partitionSpillHandler.spillAnyInMemoryDataForSpilledPartitions();

        /* close the iteration */
        partitionSpillHandler.transitionPartitionState();

        /* number of spilled partitions in the current iteration should be 0 */
        assertEquals(0, partitionSpillHandler.getActiveSpilledPartitionCount());
        /* spilled partitions in the previous iteration should have been added to spill queue as "disk based" partitions */
        assertFalse(partitionSpillHandler.isSpillQueueEmpty());
        assertEquals(2, partitionSpillHandler.getSpilledPartitionCount());

        partitions[2].resetToMinimumSize();
        partitions[3].resetToMinimumSize();

        /* after state transition, there should be disjoint sets of partitions
         * representing our two distinct partition concepts "FULL MEMORY BASED"
         * and "FULL DISK BASED".
         */
        verifyStateOfInMemoryPartitions(partitions);
        verifyStateOfDiskBasedPartitions(partitionSpillHandler);

        consumeSpilledData(
            partitionSpillHandler, partitionToLoadSpilledData, numCollapsedRecords, "P0");
        consumeSpilledData(
            partitionSpillHandler, partitionToLoadSpilledData, numCollapsedRecords, "P1");

        AutoCloseables.close(varlenAccumVectorFields);
      } finally {
        partitionSpillHandler.close();
        AutoCloseables.close(partitions);
        fs.close();
      }
    } catch (Exception e) {
      throw e;
    }
  }

  private void verifyStateOfInMemoryPartitions(final VectorizedHashAggPartition[] partitions) {
    for (VectorizedHashAggPartition partition : partitions) {
      assertFalse(partition.isSpilled());
      assertNull(partition.getSpillInfo());
      assertEquals(0, partition.getSize());
    }
  }

  private void verifyStateOfDiskBasedPartitions(
      final VectorizedHashAggPartitionSpillHandler partitionSpillHandler) {
    final Queue<VectorizedHashAggDiskPartition> spilledPartitions =
        partitionSpillHandler.getSpilledPartitions();
    Iterator<VectorizedHashAggDiskPartition> iterator = spilledPartitions.iterator();
    while (iterator.hasNext()) {
      VectorizedHashAggDiskPartition diskPartition = iterator.next();
      assertNotNull(diskPartition.getSpillFile());
      assertTrue(diskPartition.getNumberOfBatches() > 0);
      assertNull(diskPartition.getInmemoryPartitionBackPointer());
    }
  }

  private void consumeSpilledData(
      final VectorizedHashAggPartitionSpillHandler partitionSpillHandler,
      final PartitionToLoadSpilledData partitionToLoadSpilledData,
      final int numCollapsedRecords,
      final String partitionIdentifier)
      throws Exception {
    final SpilledPartitionIterator partitionIterator =
        partitionSpillHandler.getNextSpilledPartitionToProcess();
    assertEquals(partitionIdentifier, partitionIterator.getIdentifier());
    int numRecordsInBatch;
    int numBatches = 0;
    while ((numRecordsInBatch = partitionIterator.getNextBatch()) > 0) {
      assertEquals(numRecordsInBatch, numCollapsedRecords);
      numBatches++;
      partitionToLoadSpilledData.reset();
    }
    assertEquals(1, numBatches);
    partitionSpillHandler.closeSpilledPartitionIterator();
  }

  private void populateInt(IntVector vector, Integer[] data) {
    vector.allocateNew();
    Random r = new Random();
    for (int i = 0; i < data.length; i++) {
      Integer val = data[i];
      if (val != null) {
        vector.setSafe(i, val);
      } else {
        vector.setSafe(i, 0, r.nextInt());
      }
    }
    vector.setValueCount(data.length);
  }

  private AccumulatorSet createAccumulator(
      IntVector in1,
      VarCharVector in2,
      VarCharVector in3,
      VarCharVector[] tempVectors,
      final BufferAllocator allocator,
      final boolean firstPartition) {
    /* SUM Accumulator */
    BigIntVector in1SumOutputVector = new BigIntVector("int-sum", allocator);
    final SumAccumulators.IntSumAccumulator in1SumAccum =
        new SumAccumulators.IntSumAccumulator(
            in1, in1SumOutputVector, in1SumOutputVector, MAX_VALUES_PER_BATCH, allocator);

    /* Min Accumulator */
    IntVector in1MaxOutputVector = new IntVector("int-max", allocator);
    final MaxAccumulators.IntMaxAccumulator in1MaxAccum =
        new MaxAccumulators.IntMaxAccumulator(
            in1, in1MaxOutputVector, in1MaxOutputVector, MAX_VALUES_PER_BATCH, allocator);

    /* Max accumulator */
    IntVector in1MinOutputVector = new IntVector("int-min", allocator);
    final MinAccumulators.IntMinAccumulator in1MinAccum =
        new MinAccumulators.IntMinAccumulator(
            in1, in1MinOutputVector, in1MinOutputVector, MAX_VALUES_PER_BATCH, allocator);

    /* Count accumulator */
    BigIntVector in1Count = new BigIntVector("int-count", allocator);
    final CountColumnAccumulator in1countAccum =
        new CountColumnAccumulator(in1, in1Count, in1Count, MAX_VALUES_PER_BATCH, allocator);

    /* Count(1) accumulator */
    BigIntVector in1Count1 = new BigIntVector("int-count1", allocator);
    final CountOneAccumulator in1count1Accum =
        new CountOneAccumulator(null, in1Count1, in1Count1, MAX_VALUES_PER_BATCH, allocator);

    VarCharVector v1 = new VarCharVector("varchar-min", allocator);
    final MinAccumulators.VarLenMinAccumulator in2MinAccum =
        new MinAccumulators.VarLenMinAccumulator(
            in2, v1, MAX_VALUES_PER_BATCH, allocator, 15, 256, 95, 0, tempVectors[0], null);

    VarCharVector v2 = new VarCharVector("varchar-max", allocator);
    final MaxAccumulators.VarLenMaxAccumulator in3MaxAccum =
        new MaxAccumulators.VarLenMaxAccumulator(
            in3, v2, MAX_VALUES_PER_BATCH, allocator, 15, 256, 95, 1, tempVectors[1], null);

    if (firstPartition) {
      postSpillAccumulatorVectorFields.add(in1SumOutputVector.getField());
      postSpillAccumulatorVectorFields.add(in1MaxOutputVector.getField());
      postSpillAccumulatorVectorFields.add(in1MinOutputVector.getField());
      postSpillAccumulatorVectorFields.add(in1Count.getField());
      postSpillAccumulatorVectorFields.add(in1Count1.getField());
      postSpillAccumulatorVectorFields.add(in2.getField());
      postSpillAccumulatorVectorFields.add(in3.getField());

      accumulatorTypes[0] = (byte) AccumulatorBuilder.AccumulatorType.SUM.ordinal();
      accumulatorTypes[1] = (byte) AccumulatorBuilder.AccumulatorType.MAX.ordinal();
      accumulatorTypes[2] = (byte) AccumulatorBuilder.AccumulatorType.MIN.ordinal();
      accumulatorTypes[3] = (byte) AccumulatorBuilder.AccumulatorType.COUNT.ordinal();
      accumulatorTypes[4] = (byte) AccumulatorBuilder.AccumulatorType.COUNT1.ordinal();
      accumulatorTypes[5] = (byte) AccumulatorBuilder.AccumulatorType.MIN.ordinal();
      accumulatorTypes[6] = (byte) AccumulatorBuilder.AccumulatorType.MAX.ordinal();
    }

    return new AccumulatorSet(
        4 * 1024,
        128 * 1024,
        allocator,
        in1SumAccum,
        in1MaxAccum,
        in1MinAccum,
        in1countAccum,
        in1count1Accum,
        in2MinAccum,
        in3MaxAccum);
  }

  private void populateStrings(VarCharVector vector, String[] data) {
    vector.allocateNew(1024 * 1024, data.length);
    vector.zeroVector();
    for (int i = 0; i < data.length; i++) {
      final String val = data[i];
      if (val != null) {
        vector.setSafe(i, val.getBytes());
      }
    }
    vector.setValueCount(data.length);
  }
}
