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
package com.dremio.exec.store.parquet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecTest;
import com.dremio.exec.store.TestOutputMutator;
import com.dremio.test.AllocatorRule;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.arrow.vector.SimpleIntVector;
import org.apache.arrow.vector.ValueVector;
import org.junit.Rule;
import org.junit.Test;

/** Test class for {@link BigIntAutoIncrementer} */
public class TestBigIntAutoIncrementer extends ExecTest {
  private static final String ROW_INDEX_COLUMN_NAME = "R_O_W_I_N_D_E_X";

  @Rule public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

  @Test
  public void testBasicRowIndexGeneration() throws Exception {
    try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      final int batchSize = 10;
      final int count = 10;
      final long rowIndexBase = 0;
      TestOutputMutator outputMutator = new TestOutputMutator(allocator);
      closer.add(outputMutator);
      BigIntAutoIncrementer incrementer =
          new BigIntAutoIncrementer(ROW_INDEX_COLUMN_NAME, batchSize, null);
      closer.add(incrementer);

      incrementer.setRowIndexBase(rowIndexBase);
      incrementer.setup(outputMutator);
      incrementer.populate(count);

      ValueVector vector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      closer.add(vector);
      assertNotNull(vector);
      assertEquals(count, vector.getValueCount());

      final List<Integer> expectedResult = ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
      verifyResults(expectedResult, vector);
    }
  }

  @Test
  public void testPopulateMultipleTimes() throws Exception {
    try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      final int batchSize = 15;
      final int count = 5;
      final long rowIndexBase = 100;

      TestOutputMutator outputMutator = new TestOutputMutator(allocator);
      closer.add(outputMutator);
      BigIntAutoIncrementer incrementer =
          new BigIntAutoIncrementer(ROW_INDEX_COLUMN_NAME, batchSize, null);
      closer.add(incrementer);

      incrementer.setRowIndexBase(rowIndexBase);
      incrementer.setup(outputMutator);

      // First population
      incrementer.populate(count);
      ValueVector firstPopulationVector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      assertNotNull(firstPopulationVector);
      assertEquals(count, firstPopulationVector.getValueCount());
      List<Integer> expectedResultFirstPopulation = ImmutableList.of(100, 101, 102, 103, 104);
      verifyResults(expectedResultFirstPopulation, firstPopulationVector);
      closer.add(firstPopulationVector);

      // Second population
      incrementer.populate(count);
      ValueVector secondPopulationVector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      assertNotNull(secondPopulationVector);
      assertEquals(count, secondPopulationVector.getValueCount());
      List<Integer> expectedResultSecondPopulation = ImmutableList.of(105, 106, 107, 108, 109);
      verifyResults(expectedResultSecondPopulation, secondPopulationVector);
      closer.add(secondPopulationVector);

      // Third population
      incrementer.populate(count);
      ValueVector thirdPopulationVector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      assertNotNull(thirdPopulationVector);
      assertEquals(count, thirdPopulationVector.getValueCount());
      List<Integer> expectedResultThirdPopulation = ImmutableList.of(110, 111, 112, 113, 114);
      verifyResults(expectedResultThirdPopulation, thirdPopulationVector);
      closer.add(thirdPopulationVector);
    }
  }

  @Test
  public void testPopulateMultipleTimeWithDeltas() throws Exception {
    try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      final int batchSize = 15;
      final int count = 5;
      final long rowIndexBase = 10;

      SimpleIntVector deltas = new SimpleIntVector("deltas", allocator);
      closer.add(deltas);
      TestOutputMutator outputMutator = new TestOutputMutator(allocator);
      closer.add(outputMutator);
      BigIntAutoIncrementer incrementer =
          new BigIntAutoIncrementer(ROW_INDEX_COLUMN_NAME, batchSize, deltas);
      incrementer.setRowIndexBase(rowIndexBase);
      incrementer.setup(outputMutator);
      closer.add(incrementer);

      // First population
      initDeltas(count, deltas);
      deltas.set(0, 2);
      incrementer.populate(count);
      ValueVector firstPopulationVector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      assertNotNull(firstPopulationVector);
      assertEquals(count, firstPopulationVector.getValueCount());
      List<Integer> expectedResultFirstPopulation = ImmutableList.of(12, 13, 14, 15, 16);
      verifyResults(expectedResultFirstPopulation, firstPopulationVector);
      closer.add(firstPopulationVector);

      // Second population
      initDeltas(count, deltas);
      deltas.set(2, 4);
      incrementer.populate(count);
      ValueVector secondPopulationVector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      assertNotNull(secondPopulationVector);
      assertEquals(count, secondPopulationVector.getValueCount());
      List<Integer> expectedResultSecondPopulation = ImmutableList.of(17, 18, 23, 24, 25);
      verifyResults(expectedResultSecondPopulation, secondPopulationVector);
      closer.add(secondPopulationVector);

      // Third population
      initDeltas(count, deltas);
      deltas.set(4, 5);
      incrementer.populate(count);
      ValueVector thirdPopulationVector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      assertNotNull(thirdPopulationVector);
      assertEquals(count, thirdPopulationVector.getValueCount());
      List<Integer> expectedResultThirdPopulation = ImmutableList.of(26, 27, 28, 29, 35);
      verifyResults(expectedResultThirdPopulation, thirdPopulationVector);
      closer.add(thirdPopulationVector);
    }
  }

  @Test
  public void testPopulationCountExceedBatchSize() throws Exception {
    try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      final int batchSize = 10;
      final int count = 15;
      final long rowIndexBase = 0;
      TestOutputMutator outputMutator = new TestOutputMutator(allocator);
      closer.add(outputMutator);
      BigIntAutoIncrementer incrementer =
          new BigIntAutoIncrementer(ROW_INDEX_COLUMN_NAME, batchSize, null);
      closer.add(incrementer);

      incrementer.setRowIndexBase(rowIndexBase);
      incrementer.setup(outputMutator);
      incrementer.populate(count);

      ValueVector vector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      closer.add(vector);
      assertNotNull(vector);
      assertEquals(count, vector.getValueCount());

      final List<Integer> expectedResult =
          ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14);
      verifyResults(expectedResult, vector);
    }
  }

  @Test
  public void testPopulationCountExceedBatchSizeWithDeltas() throws Exception {
    try (AutoCloseables.RollbackCloseable closer = new AutoCloseables.RollbackCloseable()) {
      final int batchSize = 10;
      final int count = 15;
      final long rowIndexBase = 0;
      TestOutputMutator outputMutator = new TestOutputMutator(allocator);
      closer.add(outputMutator);
      SimpleIntVector deltas = new SimpleIntVector("deltas", allocator);
      closer.add(deltas);

      initDeltas(count, deltas);
      deltas.set(0, 2);

      BigIntAutoIncrementer incrementer =
          new BigIntAutoIncrementer(ROW_INDEX_COLUMN_NAME, batchSize, deltas);
      closer.add(incrementer);

      incrementer.setRowIndexBase(rowIndexBase);
      incrementer.setup(outputMutator);
      incrementer.populate(count);

      ValueVector vector = outputMutator.getVector(ROW_INDEX_COLUMN_NAME);
      closer.add(vector);
      assertNotNull(vector);
      assertEquals(count, vector.getValueCount());

      final List<Integer> expectedResult =
          ImmutableList.of(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16);
      verifyResults(expectedResult, vector);
    }
  }

  private void verifyResults(List<Integer> expectedResult, ValueVector actualVector) {
    assertEquals(expectedResult.size(), actualVector.getValueCount());
    for (int i = 0; i < actualVector.getValueCount(); i++) {
      assertEquals((int) expectedResult.get(i), ((Number) actualVector.getObject(i)).intValue());
    }
  }

  private void initDeltas(int valueCount, SimpleIntVector deltas) {
    // Allocate memory and set initial values: 0s
    deltas.allocateNew(valueCount);
    for (int i = 0; i < valueCount; i++) {
      deltas.set(i, 0);
    }
    deltas.setValueCount(valueCount);
  }
}
