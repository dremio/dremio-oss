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

import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_LENGTH_SIZE;
import static com.dremio.sabot.op.common.ht2.LBlockHashTable.VAR_OFFSET_SIZE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SimpleBigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import com.dremio.common.util.Numbers;
import com.dremio.exec.record.VectorContainer;
import com.koloboke.collect.hash.HashConfig;

public class TestHashTable2 {

  private int MAX_VALUES_PER_BATCH = 0;

  @Test
  public void simple() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    simpleHelper();
    MAX_VALUES_PER_BATCH = 2048;
    simpleHelper();
    MAX_VALUES_PER_BATCH = 1024;
    simpleHelper();
  }

  private void simpleHelper() throws Exception {

    String[] col1arr = {"hello", "my", "hello", "hello", null, null};
    String[] col2arr = {"every", "every", "every", "none", null, null};
    Integer[] col3arr = {1, 1, 1, 1, 1, 1};

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorContainer c = new VectorContainer();) {

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

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, true, ResizeListener.NO_OP, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          int[] expectedOrdinals = {0, 1, 0, 2, 3, 3};
          int[] actualOrdinals = new int[expectedOrdinals.length];
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
            pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            actualOrdinals[keyIndex] = bht.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
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
  public void fixedOnly() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    fixedOnlyHelper();
    MAX_VALUES_PER_BATCH = 2048;
    fixedOnlyHelper();
    MAX_VALUES_PER_BATCH = 1024;
    fixedOnlyHelper();
  }

  private void fixedOnlyHelper() throws Exception {

    Integer[] col1Arr = {1, 2, 3, 3, 2, 1};
    Integer[] col2Arr = {100, 200, 300, 300, 200, 400};

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer();) {
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

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, true, ResizeListener.NO_OP, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          int[] expectedOrdinals = {0, 1, 2, 2, 1, 3};
          int[] actualOrdinals = new int[expectedOrdinals.length];
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
            pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            actualOrdinals[keyIndex] = bht.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
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
  public void emptyValues() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    emptyValuesHelper();
    MAX_VALUES_PER_BATCH = 2048;
    emptyValuesHelper();
    MAX_VALUES_PER_BATCH = 1024;
    emptyValuesHelper();
  }

  private void emptyValuesHelper() throws Exception {

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        final VectorContainer c = new VectorContainer();) {

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

        try (LBlockHashTable bht = new LBlockHashTable(HashConfig.getDefault(), pivot, allocator, 16000, 10, true, ResizeListener.NO_OP, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();
          final boolean fixedOnly = pivot.getVariableCount() == 0;

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
            pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            assertEquals(0, bht.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash));
          }
        }
      }
    }
  }

  @Test
  public void enforceVarWidthBufferLimitsWithGaps() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    testGapsHelper(0);
    MAX_VALUES_PER_BATCH = 1024;
    testGapsHelper(2012);
  }

  private void testGapsHelper(final int expectedGaps) throws Exception {
    enforceVarWidthBufferLimitsHelper(45, 1000, 4, expectedGaps);
  }

  @Test
  public void enforceVarWidthBufferLimitsWithNoGaps() throws Exception {
    MAX_VALUES_PER_BATCH = 4096;
    testNoGapsHelper();
    MAX_VALUES_PER_BATCH = 2048;
    testNoGapsHelper();
    MAX_VALUES_PER_BATCH = 1024;
    testNoGapsHelper();
  }

  private void testNoGapsHelper() throws Exception {
    enforceVarWidthBufferLimitsHelper(9349, 7, 10, 0); // estimate more than actual
    enforceVarWidthBufferLimitsHelper(9349, 7, 7, 0); // exact estimate
  }

  private void enforceVarWidthBufferLimitsHelper(int inputLength, int varCharLength,
                                                 int estimatedVarFieldsLength, int expGaps)
      throws Exception {
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

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer();) {

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
            200, estimatedVarFieldsLength, true, ResizeListener.NO_OP, MAX_VALUES_PER_BATCH);
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
          int maxVarBufferSize = Numbers.nextPowerOfTwo((((estimatedVarFieldsLength + VAR_OFFSET_SIZE) * 2) + VAR_LENGTH_SIZE) * MAX_VALUES_PER_BATCH);
          for (int i = 0; i < expectedOrdinals.length; i++) {
            int newLen = VAR_OFFSET_SIZE * 2 + VAR_LENGTH_SIZE;
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
              currentOrdinal = numChunks * MAX_VALUES_PER_BATCH;
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
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, fixedOnly,
              pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            actualOrdinals[keyIndex] = bht.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
          }
          assertArrayEquals("ordinals mismatch", expectedOrdinals, actualOrdinals);
          assertEquals("Relative size mismatch", inputLength - numberNulls, bht.relativeSize());
          assertEquals("Absolute size mismatch", expectedOrdinals[expectedOrdinals.length -1] + 1, bht.size());
          // we can't match the capacity exactly as the initialization of hash table capacity depends on heuristics
          // in LHashCapacities
          assertTrue("Unexpected capacity", numChunks * MAX_VALUES_PER_BATCH <= bht.capacity());
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

    try (BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);
         final VectorContainer c = new VectorContainer();) {

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
            200, 20, true, ResizeListener.NO_OP, MAX_VALUES_PER_BATCH);
             SimpleBigIntVector hashValues = new SimpleBigIntVector("hashvalues", allocator)) {
          final long keyFixedVectorAddr = fbv.getMemoryAddress();
          final long keyVarVectorAddr = var.getMemoryAddress();

          hashValues.allocateNew(records);
          final BlockChunk blockChunk = new BlockChunk(keyFixedVectorAddr, keyVarVectorAddr, false,
              pivot.getBlockWidth(), records, hashValues.getBufferAddress(), 0);
          HashComputation.computeHash(blockChunk);

          for (int keyIndex = 0; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            bht.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
          }

          assertTrue(bht.capacity() > MAX_VALUES_PER_BATCH);

          // now reset HashTable to minimum size
          bht.resetToMinimumSize();
          assertEquals(MAX_VALUES_PER_BATCH, bht.capacity());

          // insert the same records again and check the hashtable is expanded in capacity
          for (int keyIndex = 20; keyIndex < records; keyIndex++) {
            final int keyHash = (int)hashValues.get(keyIndex);
            bht.add(keyFixedVectorAddr, keyVarVectorAddr, keyIndex, keyHash);
          }
          assertTrue(bht.capacity() > MAX_VALUES_PER_BATCH);
        }
      }
    }
  }
}
