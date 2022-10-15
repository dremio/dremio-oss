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
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SimpleBigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.util.Numbers;
import com.dremio.exec.record.VectorContainer;
import com.dremio.test.AllocatorRule;
import com.dremio.test.DremioTest;
import com.koloboke.collect.hash.HashConfig;

public class TestHashTable2 extends DremioTest {

  private int MAX_VALUES_PER_BATCH = 0;

  @Rule
  public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Test
  public void testSimple() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    simpleHelper();
    MAX_VALUES_PER_BATCH = 2048;
    simpleHelper();
    MAX_VALUES_PER_BATCH = 1024;
    simpleHelper();
    MAX_VALUES_PER_BATCH = 990;
    simpleHelper();
    MAX_VALUES_PER_BATCH = 976;
    simpleHelper();
  }

  private void simpleHelper() throws Exception {

    String[] col1arr = {"hello", "my", "hello", "hello", null, null};
    String[] col2arr = {"every", "every", "every", "none", null, null};
    Integer[] col3arr = {1, 1, 1, 1, 1, 1};

    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-hash-table-2", 0, Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer()) {

      VarCharVector col1 = new VarCharVector("col1", allocator);
      TestVarBinaryPivot.populate(col1, col1arr);
      c.add(col1);
      VarCharVector col2 = new VarCharVector("col2", allocator);
      TestVarBinaryPivot.populate(col2, col2arr);
      c.add(col2);
      IntVector col3 = new IntVector("col3", allocator);
      TestIntPivot.populate(col3, col3arr);
      c.add(col3);
      final int records = c.setAllCount(col1arr.length);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
          );
      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000,
          10, true, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          int[] expectedOrdinals = {0, 1, 0, 2, 3, 3};
          int[] actualOrdinals = new int[expectedOrdinals.length];
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), fixedOnly,
            pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            actualOrdinals[keyIndex] = bht.add(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash);
          }
          assertArrayEquals("ordinals mismatch", expectedOrdinals, actualOrdinals);
          assertEquals("Absolute size mismatch", 4, bht.size());
          assertEquals("Relative size mismatch", 4, bht.relativeSize());
          assertEquals("Unexpected number of gaps", 0, bht.gaps());
        }
      }
    }
  }

  @Test
  public void testFixedOnly() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    fixedOnlyHelper();
    MAX_VALUES_PER_BATCH = 2048;
    fixedOnlyHelper();
    MAX_VALUES_PER_BATCH = 1024;
    fixedOnlyHelper();
    MAX_VALUES_PER_BATCH = 990;
    fixedOnlyHelper();
    MAX_VALUES_PER_BATCH = 976;
    fixedOnlyHelper();
  }

  private void fixedOnlyHelper() throws Exception {

    Integer[] col1Arr = {1, 2, 3, 3, 2, 1};
    Integer[] col2Arr = {100, 200, 300, 300, 200, 400};

    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-hash-table-2", 0, Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer()) {
      IntVector intcol1 = new IntVector("intcol1", allocator);
      TestIntPivot.populate(intcol1, col1Arr);
      IntVector intcol2 = new IntVector("intcol2", allocator);
      TestIntPivot.populate(intcol2, col2Arr);
      c.add(intcol1);
      c.add(intcol2);
      final int records = c.setAllCount(col1Arr.length);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
        new FieldVectorPair(intcol1, intcol1),
        new FieldVectorPair(intcol2, intcol2)
      );
      try (
        final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
        final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000,
          10, true, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          int[] expectedOrdinals = {0, 1, 2, 2, 1, 3};
          int[] actualOrdinals = new int[expectedOrdinals.length];
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), fixedOnly,
            pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            actualOrdinals[keyIndex] = bht.add(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash);
          }
          assertArrayEquals("ordinals mismatch", expectedOrdinals, actualOrdinals);
          assertEquals("Absolute size mismatch", 4, bht.size());
          assertEquals("Relative size mismatch", 4, bht.relativeSize());
          assertEquals("Unexpected number of gaps", 0, bht.gaps());
        }
      }
    }
  }

  @Test
  public void testEmptyValues() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    emptyValuesHelper();
    MAX_VALUES_PER_BATCH = 2048;
    emptyValuesHelper();
    MAX_VALUES_PER_BATCH = 1024;
    emptyValuesHelper();
    MAX_VALUES_PER_BATCH = 990;
    emptyValuesHelper();
    MAX_VALUES_PER_BATCH = 976;
    emptyValuesHelper();
  }

  private void emptyValuesHelper() throws Exception {

    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-hash-table-2", 0, Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer()) {

      VarCharVector col1 = new VarCharVector("col1", allocator);
      c.add(col1);
      VarCharVector col2 = new VarCharVector("col2", allocator);
      c.add(col2);
      IntVector col3 = new IntVector("col3", allocator);
      c.add(col3);
      c.allocateNew();
      final int records = c.setAllCount(2000);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
          );

      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000,
          10, true, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), fixedOnly,
            pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            assertEquals(0, bht.add(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash));
          }
        }
      }
    }
  }

  @Test
  public void testEnforceVarWidthBufferLimitsWithGaps() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    testGapsHelper(0);
    MAX_VALUES_PER_BATCH = 1024;
    testGapsHelper(2012);
    MAX_VALUES_PER_BATCH = 990;
    testGapsHelper(2012);
    MAX_VALUES_PER_BATCH = 976;
    testGapsHelper(2012);
  }

  private void testGapsHelper(final int expectedGaps) throws Exception {
    enforceVarWidthBufferLimitsHelper(45, 1000, 4, expectedGaps);
  }

  @Test
  public void testEnforceVarWidthBufferLimitsWithNoGaps() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    testNoGapsHelper();
    MAX_VALUES_PER_BATCH = 2048;
    testNoGapsHelper();
    MAX_VALUES_PER_BATCH = 1024;
    testNoGapsHelper();
    /* If MAX_VALUES_PER_BATCH is not 2^n, there will be gaps */
//    MAX_VALUES_PER_BATCH = 990;
//    testNoGapsHelper();
//    MAX_VALUES_PER_BATCH = 976;
//    testNoGapsHelper();
  }

  private void testNoGapsHelper() throws Exception {
    enforceVarWidthBufferLimitsHelper(9349, 7, 10, 0); // estimate more than actual
    enforceVarWidthBufferLimitsHelper(9349, 7, 7, 0); // exact estimate
  }

  private void enforceVarWidthBufferLimitsHelper(int inputLength, int varCharLength,
                                                 int estimatedVarFieldsLength, int expGaps) throws Exception {
    final Random random = new Random();
    final String[] col1Arr = new String[inputLength];
    final String[] col2Arr = new String[col1Arr.length];
    final Integer[] col3Arr = new Integer[col1Arr.length];
    for (int i = 0; i < col1Arr.length; i++) {
      if (i % 10 != 0) {
        col1Arr[i] = RandomStringUtils.randomAlphanumeric(varCharLength - 5) + String.format("%05d",i);
      }
      if (i % 7 != 0) {
        col2Arr[i] = RandomStringUtils.randomAlphanumeric(varCharLength - 5) + String.format("%05d", i);
      }
      if (i % 3 != 0) {
        col3Arr[i] = random.nextInt();
      }
    }

    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-hash-table-2", 0, Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer()) {

      VarCharVector col1 = new VarCharVector("col1", allocator);
      TestVarBinaryPivot.populate(col1, col1Arr);
      c.add(col1);
      VarCharVector col2 = new VarCharVector("col2", allocator);
      TestVarBinaryPivot.populate(col2, col2Arr);
      c.add(col2);
      IntVector col3 = new IntVector("col3", allocator);
      TestIntPivot.populate(col3, col3Arr);
      c.add(col3);
      final int records = c.setAllCount(col1Arr.length);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
      );
      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);
        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator,
            200, estimatedVarFieldsLength, true, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          int[] expectedOrdinals = new int[col1Arr.length];

          // populate ordinals based on the length of the var width fields
          long runningVarBufferLen = 0;
          int currentOrdinal = 0;
          int numChunks = 0;
          int nullOrdinal = -1;
          int numberNulls = 0;
          int maxVarBufferSize = Numbers.nextPowerOfTwo((((estimatedVarFieldsLength + LBlockHashTable.VAR_OFFSET_SIZE) * 2) + LBlockHashTable.VAR_LENGTH_SIZE) * Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH));
          for (int i = 0; i < expectedOrdinals.length; i++) {
            int newLen = LBlockHashTable.VAR_OFFSET_SIZE * 2 + LBlockHashTable.VAR_LENGTH_SIZE;
            if (col1Arr[i] != null) {
              newLen += col1Arr[i].getBytes(Charsets.UTF_8).length;
            }
            if (col2Arr[i] != null) {
              newLen += col2Arr[i].getBytes(Charsets.UTF_8).length;
            }

            if (col1Arr[i] == null && col2Arr[i] == null && col3Arr[i] == null && nullOrdinal != -1) {
              // all nulls are already inserted
              expectedOrdinals[i] = nullOrdinal;
              numberNulls++;
              continue;
            }

            if (runningVarBufferLen + newLen > maxVarBufferSize || (i - numberNulls) % MAX_VALUES_PER_BATCH == 0) {
              //gaps in ordinals since we can't fit the next varchar key in the current block
              currentOrdinal = numChunks * Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH);
              runningVarBufferLen = newLen; // reset buffer size
              numChunks ++;
            } else {
              runningVarBufferLen += newLen;
            }

            if (col1Arr[i] == null && col2Arr[i] == null && col3Arr[i] == null) {
              nullOrdinal = currentOrdinal++;
              expectedOrdinals[i] = nullOrdinal;
            } else {
              expectedOrdinals[i] = currentOrdinal++;

            }
          }

          int[] actualOrdinals = new int[expectedOrdinals.length];
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), fixedOnly,
              pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            actualOrdinals[keyIndex] = bht.add(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash);
          }
          assertArrayEquals("ordinals mismatch", expectedOrdinals, actualOrdinals);
          assertEquals("Absolute size mismatch", expectedOrdinals[expectedOrdinals.length - 1] + 1, bht.size() + bht.gaps());
          // we can't match the capacity exactly as the initialization of hash table capacity depends on heuristics
          // in LHashCapacities
          assertTrue("Unexpected capacity", numChunks * Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH) <= bht.capacity());
          assertEquals("Unexpected number of gaps", expGaps, bht.gaps());
        }
      }
    }
  }

  @Test
  public void testResetToMinimumSize() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    testResetToMinimumSizeHelper();
    MAX_VALUES_PER_BATCH = 2048;
    testResetToMinimumSizeHelper();
    MAX_VALUES_PER_BATCH = 1024;
    testResetToMinimumSizeHelper();
    MAX_VALUES_PER_BATCH = 990;
    testResetToMinimumSizeHelper();
    MAX_VALUES_PER_BATCH = 976;
    testResetToMinimumSizeHelper();
  }

  private void testResetToMinimumSizeHelper() throws Exception {
    final Random random = new Random();
    final String[] col1Arr = new String[2 * MAX_VALUES_PER_BATCH];
    final String[] col2Arr = new String[col1Arr.length];
    final Integer[] col3Arr = new Integer[col1Arr.length];
    for (int i = 0; i < col1Arr.length; i++) {
      col1Arr[i] = RandomStringUtils.randomAlphanumeric(5) + String.format("%05d",i);
      col2Arr[i] = RandomStringUtils.randomAlphanumeric(10) + String.format("%05d", i);
      col3Arr[i] = random.nextInt();
    }

    try (final BufferAllocator allocator = allocatorRule.newAllocator("test-hash-table-2", 0, Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer()) {

      VarCharVector col1 = new VarCharVector("col1", allocator);
      TestVarBinaryPivot.populate(col1, col1Arr);
      c.add(col1);
      VarCharVector col2 = new VarCharVector("col2", allocator);
      TestVarBinaryPivot.populate(col2, col2Arr);
      c.add(col2);
      IntVector col3 = new IntVector("col3", allocator);
      TestIntPivot.populate(col3, col3Arr);
      c.add(col3);
      final int records = c.setAllCount(col1Arr.length);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, col1),
          new FieldVectorPair(col2, col2),
          new FieldVectorPair(col3, col3)
      );
      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());) {

        Pivots.pivot(pivot, records, fbv, var);

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator,
            200, 20, true, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), false,
              pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            bht.add(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash);
          }

          assertTrue(bht.capacity() > Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH));

          // now reset HashTable to minimum size
          bht.resetToMinimumSize();
          assertEquals(Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH), bht.capacity());

          // insert the same records again and check the hashtable is expanded in capacity
          for (int keyIndex = 20; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            bht.add(keyFixedVectorAddr, keyVarVectorAddr, var.getCapacity(), keyIndex, keyHash);
          }
          assertTrue(bht.capacity() > Numbers.nextPowerOfTwo(MAX_VALUES_PER_BATCH));
        }
      }
    }
  }

  /**
   * there are 3 key columns <varkey> <varkey> <fixedkey>
   * each key is repeated once - k1,k1,k2,k2,k3,k3
   * @return
   */
  /*
   * XXX: Rework on the test as the original design changed quite a bit.
  @Test
  public void testSpliceBasic()
  {
    MAX_VALUES_PER_BATCH = 4096;
    final int numUniqueKeys = 3968;
    final int inputRecords = (2 * numUniqueKeys);
    try (final BufferAllocator allocator = allocatorRule.newAllocator("testSpliceBasic", 1024 * 1024, Long.MAX_VALUE);
        final VectorContainer c = new VectorContainer();
        VarCharVector col1 = new VarCharVector("col1", allocator);
        VarCharVector out1 = new VarCharVector("col1_output", allocator);
        VarCharVector col2 = new VarCharVector("col2", allocator);
        VarCharVector out2 = new VarCharVector("col2_output", allocator);
        IntVector col3 = new IntVector("col3", allocator);
        IntVector out3 = new IntVector("col3_output", allocator);
        // Measure column 1: sum of integers
        IntVector m1 = new IntVector("m1", allocator);
        // Measure column 2: min varchar accumulator
        VarCharVector m2 = new VarCharVector("m2", allocator);
        // Measure column 3: max varchar accumulator
        VarCharVector m3 = new VarCharVector("m3", allocator);
        ) {
      final String[] key1 = new String[inputRecords];
      final String[] key2 = new String[inputRecords];
      final Integer[] key3 = new Integer[inputRecords];
      final int v3 = 1;
      for (int i = 0; i < inputRecords; i += 2) {
        final String v1 = RandomStringUtils.randomAlphanumeric(2) + String.format("%05d", i);
        final String v2 = RandomStringUtils.randomAlphanumeric(2) + String.format("%05d", i);
        key1[i] = v1;
        key1[i + 1] = v1;

        key2[i] = v2;
        key2[i + 1] = v2;

        key3[i] = v3;
        key3[i + 1] = v3;
      }

      //populate keys
      TestVarBinaryPivot.populate(col1, key1);
      c.add(col1);
      TestVarBinaryPivot.populate(col2, key2);
      c.add(col2);
      TestIntPivot.populate(col3, key3);
      c.add(col3);

      // Measure column 1: sum accumulator
      for (int i = 0; i < inputRecords; i += 2) {
        m1.setSafe(i, 1);
        m1.setSafe(i + 1, 1);
      }
      c.add(m1);

      // Measure column 2: min varchar accumulator
      final String minKey = "aaaaa";
      final String maxKey = "zzzzz";
      for (int i = 0; i < inputRecords; i += 2) {
        m2.setSafe(i, maxKey.getBytes());
        m2.setSafe(i + 1, minKey.getBytes());
      }
      c.add(m2);

      // Measure column 3:max varchar accumulator
      for (int i = 0; i < inputRecords; i += 2) {
        m3.setSafe(i, minKey.getBytes());
        m3.setSafe(i + 1, maxKey.getBytes());
      }
      c.add(m3);

      final int records = c.setAllCount(inputRecords);
      final PivotDef pivot = PivotBuilder.getBlockDefinition(
          new FieldVectorPair(col1, out1),
          new FieldVectorPair(col2, out2),
          new FieldVectorPair(col3, out3)
          );

      VarCharVector[] tempVectors = new VarCharVector[2];
      final Accumulator[] accums = new Accumulator[3];
      try (
          final FixedBlockVector fbv = new FixedBlockVector(allocator, pivot.getBlockWidth());
          final VariableBlockVector var = new VariableBlockVector(allocator, pivot.getVariableCount());
          BigIntVector in1SumOutputVector = new BigIntVector("int-sum", allocator);
          VarCharVector v1 = new VarCharVector("varchar-min", allocator);
          VarCharVector v2 = new VarCharVector("varchar-max", allocator);
          ) {
        fbv.ensureAvailableBlocks(inputRecords);
        var.ensureAvailableDataSpace(MAX_VALUES_PER_BATCH * 100);

        //pivot the keys
        final int recordsPivoted = BoundedPivots.pivot(pivot, 0, inputRecords, fbv, var);
        assertEquals(recordsPivoted, inputRecords);

        tempVectors[0] = new VarCharVector("varchar-min", allocator);
        tempVectors[1] = new VarCharVector("varchar-max", allocator);

        final SumAccumulators.IntSumAccumulator in1SumAccum =
          new SumAccumulators.IntSumAccumulator(m1, in1SumOutputVector, in1SumOutputVector,
              MAX_VALUES_PER_BATCH, allocator);

        final MinAccumulators.VarLenMinAccumulator in2MinAccum =
          new MinAccumulators.VarLenMinAccumulator(m2, v1, v1, MAX_VALUES_PER_BATCH, allocator, MAX_VALUES_PER_BATCH * 15, tempVectors[0]);

        final MaxAccumulators.VarLenMaxAccumulator in3MaxAccum =
          new MaxAccumulators.VarLenMaxAccumulator(m3, v2, v2, MAX_VALUES_PER_BATCH, allocator, MAX_VALUES_PER_BATCH * 15, tempVectors[1]);

        accums[0] = in1SumAccum;
        accums[1] = in2MinAccum;
        accums[2] = in3MaxAccum;

        try (
            AccumulatorSet accumulatorSet = new AccumulatorSet(4 * 1024, 64 * 1024, allocator, accums);
            LBlockHashTable sourceHashTable = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, MAX_VALUES_PER_BATCH, 15, true, accumulatorSet, MAX_VALUES_PER_BATCH);
            ArrowBuf offsets = allocator.buffer(records * VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH);
            SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator);
            ) {

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(fbv.getMemoryAddress(), var.getMemoryAddress(), false,
              pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          HashSet<Integer> actualOrdinals = new HashSet<>();
          int lastOrdinal = 0;
          VectorizedHashAggPartition hashAggPartition = new VectorizedHashAggPartition
            (accumulatorSet, sourceHashTable, pivot.getBlockWidth(), "P0", offsets, true);

          // insert keys
          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int) hashValues.get(keyIndex);
            final int ordinal = sourceHashTable.add(fbv.getMemoryAddress(), var.getMemoryAddress(), keyIndex, keyHash);
            //System.out.println("key: " + key1[keyIndex] + " keyhash to add: " + keyHash + " keyidx: " + keyIndex + " returned ordinal: " + ordinal);
            actualOrdinals.add(ordinal);
            lastOrdinal = ordinal;
            hashAggPartition.appendRecord(ordinal, keyIndex);
          }

          assertEquals(numUniqueKeys, actualOrdinals.size());

          // accumulate and splice. Use some large size so the batch is split.
          sourceHashTable.splice(lastOrdinal + 1, 64 * 1024 * 1024, hashAggPartition,
            sourceHashTable.getBitsInChunk(), sourceHashTable.getChunkOffsetMask(), 0);

          //lookup keys after splice
          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final long keyHash = hashValues.get(keyIndex);
            final int ordinal = sourceHashTable.find(fbv.getMemoryAddress(), var.getMemoryAddress(), keyIndex, (int) keyHash);
            //System.out.println(">>> find keyhash: " + keyHash + " keyhashint: " + (int) keyHash + " keyidx: " + keyIndex + " ordinal: " + ordinal);
            assertNotEquals(-1, ordinal);
          }

          //unpivot the newly added batch
          sourceHashTable.unpivot(1, numUniqueKeys / 2);

          //verify that keys match
          int count = 0;
          int key = numUniqueKeys % 2 == 0 ? (inputRecords / 2) : numUniqueKeys + 1;
          for (; key < inputRecords; key += 2, ++count) {
            // expected and actual strings
            Assert.assertEquals(out1.getObject(count).toString(), col1.getObject(key).toString());
            Assert.assertEquals(out2.getObject(count).toString(), col2.getObject(key).toString());
            Assert.assertEquals(out3.getObject(count).toString(), col3.getObject(key).toString());
          }

          //verify that accumulation matches
          Accumulator[] accumulators = accumulatorSet.getChildren();

          accumulators[0].output(1, numUniqueKeys / 2);
          BigIntVector fv1 = (BigIntVector) accumulators[0].getOutput();
          for (int i = 0; i < numUniqueKeys / 2; ++i) {
            Assert.assertEquals(fv1.get(i), v3 * 2);
          }

          accumulators[1].output(1, numUniqueKeys / 2);
          BaseVariableWidthVector bv1 = (BaseVariableWidthVector) accumulators[1].getOutput();

          accumulators[2].output(1, numUniqueKeys / 2);
          BaseVariableWidthVector bv2 = (BaseVariableWidthVector) accumulators[2].getOutput();
          for (int i = 0; i < numUniqueKeys / 2; ++i) {
            Assert.assertEquals(bv1.getObject(i).toString(), minKey);
            Assert.assertEquals(bv2.getObject(i).toString(), maxKey);
          }
          AutoCloseables.close(Arrays.asList(hashAggPartition), Arrays.asList(accums), Arrays.asList(tempVectors));
        } catch (Exception e) {
        } //accumulatorset
      } //inner try, pivot
    } //outer level try
  }
   */
}
