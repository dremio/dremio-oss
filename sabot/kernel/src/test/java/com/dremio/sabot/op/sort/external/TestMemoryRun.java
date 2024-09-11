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
package com.dremio.sabot.op.sort.external;

import static com.dremio.sabot.CustomGenerator.ID;
import static java.util.Collections.singletonList;
import static org.apache.calcite.rel.RelFieldCollation.Direction.ASCENDING;
import static org.apache.calcite.rel.RelFieldCollation.NullDirection.FIRST;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.sabot.BaseTestOperator;
import com.dremio.sabot.CustomGenerator;
import com.dremio.sabot.exec.context.BufferManagerImpl;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestMemoryRun extends BaseTestOperator {

  private final ExternalSort externalSort =
      new ExternalSort(
          OpProps.prototype(),
          null,
          singletonList(ordering(ID.getName(), ASCENDING, FIRST)),
          false);

  private BufferAllocator allocator;
  private BufferManager bufferManager;
  private CustomGenerator generator;
  private ClassProducer producer;

  @Before
  public void prepare() {
    allocator = getTestAllocator().newChildAllocator("test-memory-run", 0, 1_000_000);
    bufferManager = new BufferManagerImpl(allocator);
    producer = testContext.newClassProducer(bufferManager);
    generator = new CustomGenerator(100_000, getTestAllocator());
  }

  @After
  public void cleanup() throws Exception {
    AutoCloseables.close(allocator, bufferManager, generator);
  }

  @Test
  public void testQuickSorterCloseToCopier() throws Exception {
    final VectorSortTracer tracer = new VectorSortTracer();
    try (MemoryRun memoryRun =
        new MemoryRun(
            externalSort.getOrderings(),
            producer,
            allocator,
            generator.getSchema(),
            tracer,
            2,
            false,
            8192,
            mock(ExecutionControls.class))) {
      int totalAdded = addBatches(memoryRun);
      validateCloseToCopier(memoryRun, 100, totalAdded);
    }
  }

  @Test
  public void testQuickSorterCloseToDisk() throws Exception {
    final VectorSortTracer tracer = new VectorSortTracer();
    try (MemoryRun memoryRun =
        new MemoryRun(
            externalSort.getOrderings(),
            producer,
            allocator,
            generator.getSchema(),
            tracer,
            2,
            false,
            8192,
            mock(ExecutionControls.class))) {
      int totalAdded = addBatches(memoryRun);
      validateCloseToDisk(memoryRun, totalAdded);
    }
  }

  @Test
  public void testSplayTreeCloseToCopier() throws Exception {
    final VectorSortTracer tracer = new VectorSortTracer();
    try (MemoryRun memoryRun =
        new MemoryRun(
            externalSort.getOrderings(),
            producer,
            allocator,
            generator.getSchema(),
            tracer,
            2,
            true,
            8192,
            mock(ExecutionControls.class))) {
      int totalAdded = addBatches(memoryRun);
      validateCloseToCopier(memoryRun, 100, totalAdded);
    }
  }

  @Test
  public void testSplayTreeCloseToDisk() throws Exception {
    final VectorSortTracer tracer = new VectorSortTracer();
    try (MemoryRun memoryRun =
        new MemoryRun(
            externalSort.getOrderings(),
            producer,
            allocator,
            generator.getSchema(),
            tracer,
            2,
            true,
            8192,
            mock(ExecutionControls.class))) {
      int totalAdded = addBatches(memoryRun);
      validateCloseToDisk(memoryRun, totalAdded);
    }
  }

  @Test
  public void testQuickSortStartMicroSpilling() throws Exception {
    testStartMicroSpilling(false);
  }

  @Test
  public void testSplaySortStartMicroSpilling() throws Exception {
    testStartMicroSpilling(true);
  }

  private void testStartMicroSpilling(boolean useSplaySort) throws Exception {
    final VectorSortTracer tracer = new VectorSortTracer();
    try (MemoryRun memoryRun =
        new MemoryRun(
            externalSort.getOrderings(),
            producer,
            allocator,
            generator.getSchema(),
            tracer,
            2,
            useSplaySort,
            8192,
            mock(ExecutionControls.class))) {
      final int totalAdded = addBatches(memoryRun);
      final DiskRunManager diskRunManager = mock(DiskRunManager.class);

      // the hypercontainer should be sorted before micro-spilling starts
      Answer<Void> spillAnswer =
          new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
              VectorContainer hyperBatch = invocation.getArgument(0, VectorContainer.class);
              validateHyperBatch(hyperBatch, totalAdded);
              return null;
            }
          };

      Mockito.doAnswer(spillAnswer)
          .when(diskRunManager)
          .startMicroSpilling(Mockito.any(VectorContainer.class));

      memoryRun.startMicroSpilling(diskRunManager);
    }
  }

  /**
   * Adds batches from generator to memory run until all batches are added or the memory run cannot
   * accept more batches in memory
   *
   * @param memoryRun memory run
   * @return total number of rows added
   */
  private int addBatches(MemoryRun memoryRun) throws Exception {
    int totalAdded = 0;
    while (true) {
      final int count = generator.next(4095);
      if (count == 0) {
        break;
      }

      if (!memoryRun.addBatch(generator.getOutput())) {
        break;
      }
      totalAdded += count;
    }
    return totalAdded;
  }

  /**
   * calls {@link MemoryRun#closeToCopier(VectorContainer, int)} and ensures all added records are
   * returned in their correct ordering
   */
  private void validateCloseToCopier(
      MemoryRun memoryRun, int targetRecordCount, int expectedRecordCount) throws Exception {
    final CustomGenerator.SortValidator validator = generator.getValidator(expectedRecordCount);

    try (VectorContainer output =
            VectorContainer.create(getTestAllocator(), generator.getSchema());
        MovingCopier copier = memoryRun.closeToCopier(output, targetRecordCount)) {
      int totalCopied = 0;
      int copied = copier.copy(targetRecordCount);
      while (copied > 0) {
        output.setAllCount(copied);

        validator.assertIsSorted(output, totalCopied);

        totalCopied += copied;
        copied = copier.copy(targetRecordCount);
      }

      assertEquals(expectedRecordCount, totalCopied);
    }
  }

  private void validateCloseToDisk(MemoryRun memoryRun, final int expectedRecordCount)
      throws Exception {
    // create a mock DiskRunManager and capture the spilled hyper batch
    DiskRunManager manager = mock(DiskRunManager.class);
    Answer<Void> spillAnswer =
        new Answer<Void>() {
          @Override
          public Void answer(InvocationOnMock invocation) throws Throwable {
            VectorContainer hyperBatch = invocation.getArgument(0, VectorContainer.class);
            validateHyperBatch(hyperBatch, expectedRecordCount);
            return null;
          }
        };

    Mockito.doAnswer(spillAnswer)
        .when(manager)
        .spill(Mockito.any(VectorContainer.class), Mockito.any(BufferAllocator.class));

    // closeToDisk expects the hyperBatch to be transferred to another allocator as it will be
    // closing the memoryRun
    // so make sure we either transfer the batch or validate it in the answer above
    memoryRun.closeToDisk(manager);
  }

  private void validateHyperBatch(VectorContainer hyperBatch, int expectedRecordCount)
      throws Exception {
    final CustomGenerator.SortValidator validator = generator.getValidator(expectedRecordCount);
    try (VectorContainer output =
            VectorContainer.create(getTestAllocator(), generator.getSchema());
        Copier copier = CopierOperator.getGenerated4Copier(producer, hyperBatch, output)) {
      final SelectionVector4 sv4 = hyperBatch.getSelectionVector4();
      assertEquals("Expected record count didn't match.", expectedRecordCount, sv4.getTotalCount());

      int totalCopied = 0;

      do {
        final int recordCount = sv4.getCount();
        if (recordCount == 0) {
          continue;
        }

        int localRecordCount = 0;
        while (localRecordCount < recordCount) {
          int copied = copier.copyRecords(localRecordCount, recordCount - localRecordCount);
          output.setAllCount(copied);

          validator.assertIsSorted(output, totalCopied);

          localRecordCount += copied;
          totalCopied += copied;
        }
      } while (sv4.next());

      assertEquals(expectedRecordCount, totalCopied);
    }
  }
}
