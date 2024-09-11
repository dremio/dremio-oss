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

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.common.logical.data.Order.Ordering;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Describes a set of ordered batches of data in memory. Sort each batch as it is inserted using the
 * Sorter. Sorter can be configured to use QuickSort (by default) or SplaySort.
 *
 * <p>Memory Guarantees Targeted: - Ensure that spilling can be done before accepting a new batch of
 * records. We do this by pre-reserving BATCH_SIZE_MULTIPLIER times the size of the largest batch
 * included in the memory run.
 *
 * <p>Can either be spilled or returned in SV4 sorted structure.
 */
class MemoryRun implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryRun.class);
  private static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(MemoryRun.class);

  @VisibleForTesting public static final String INJECTOR_OOM_ON_SORT = "injectOOMSort";
  public static final long INITIAL_COPY_ALLOCATOR_RESERVATION = 1 << 16;

  private final ClassProducer classProducer;
  private final Schema schema;
  private final BufferAllocator allocator;

  private Sorter sorter;
  private RecordBatchItem head;
  private RecordBatchItem tail;

  private long maxBatchSize;
  private int recordLength;
  private int size;

  private BufferAllocator copyTargetAllocator;
  private long copyTargetSize;
  private final VectorSortTracer tracer;
  private final int batchsizeMultiplier;
  private final int targetBatchSize;
  private final ExecutionControls executionControls;

  // holds the sorted records in memory just before spilling
  // private Sv4HyperContainer sv4HyperContainer = null;

  public MemoryRun(
      List<Ordering> sortOrderings,
      ClassProducer classProducer,
      BufferAllocator allocator,
      Schema schema,
      VectorSortTracer tracer,
      int batchsizeMultiplier,
      boolean useSplaySort,
      int targetBatchSize,
      ExecutionControls executionControls) {
    this.schema = schema;
    this.allocator = allocator;
    this.classProducer = classProducer;
    this.tracer = tracer;
    this.batchsizeMultiplier = batchsizeMultiplier;
    this.targetBatchSize = targetBatchSize;
    this.executionControls = executionControls;
    try {
      if (useSplaySort) {
        this.sorter = new SplaySorter(sortOrderings, classProducer, schema, allocator);
      } else {
        this.sorter = new QuickSorter(sortOrderings, classProducer, schema, allocator);
      }
    } catch (OutOfMemoryException ex) {
      this.sorter = null;
      logger.debug("Memory Run: failed to allocate memory for sorter");
      throw ex;
    }
    updateProtectedSize(INITIAL_COPY_ALLOCATOR_RESERVATION);
  }

  private boolean updateProtectedSize(long needed) {
    Preconditions.checkArgument(needed > 0);
    BufferAllocator oldAllocator = copyTargetAllocator;
    logger.debug(
        "Memory Run: attempting to update resserved memory for spill copy with new size as {}",
        needed);
    try {
      copyTargetAllocator = allocator.newChildAllocator("sort-copy-target", needed, Long.MAX_VALUE);
      copyTargetSize = needed;
      if (oldAllocator != null) {
        oldAllocator.close();
      }
    } catch (OutOfMemoryException ex) {
      tracer.reserveMemoryForSpillOOMEvent(needed, Long.MAX_VALUE, oldAllocator);
      logger.debug("Memory Run: failed to reserve memory for spill copy");
      return false;
    }

    return true;
  }

  static int nextPowerOfTwo(int val) {
    int highestBit = Integer.highestOneBit(val);
    if (highestBit == val) {
      return val;
    } else {
      return highestBit << 1;
    }
  }

  public boolean addBatch(VectorAccessible incoming) throws Exception {
    final int recordCount = incoming.getRecordCount();
    final long batchSize = new BatchStats().getSize(incoming, BatchStats.SizeType.WORSE_CASE);

    // If after adding incoming we end up with less than 20% of memory available for the sort
    // allocator we should
    // spill instead.
    double headroom = allocator.getHeadroom() - batchSize;
    double total = allocator.getAllocatedMemory() + headroom + batchSize;
    if (headroom / total < 0.2) {
      logger.debug(
          "Memory Run: less than 20% of memory available after adding, failed to add batch");
      return false;
    }

    // Make sure we have room for expanded MultiBatch global-sorter or SplayTree sorter before
    // adding a new
    // record batch.
    if (!sorter.expandMemoryIfNecessary(recordLength + recordCount)) {
      logger.debug("Memory Run: no room for expanding sorter, failed to add batch");
      return false;
    }

    // Make sure we are not already over max batches we are allowed to hold in memory.
    if (sorter.getHyperBatchSize() >= VectorSorter.MAX_BATCHES_PER_HYPERBATCH
        || size >= VectorSorter.MAX_BATCHES_PER_MEMORY_RUN) {
      logger.debug(
          "Memory Run: no room for storing new batch, current size = {}, failed to add batch",
          size);
      return false;
    }

    // Copy size is BATCH_SIZE_MULTIPLIER X worse case batch size since we have to be careful of
    // memory rounding.
    // Plus one sv4 for sort output vector, so we need number of records X 4bytes more. This has to
    // be reserved
    // here (but not allocated) so we can guarantee allocation later.
    final long needed =
        batchSize * batchsizeMultiplier + nextPowerOfTwo((recordLength + recordCount) * 4);
    if (copyTargetSize < needed) {
      logger.debug(
          "Memory Run: new size needed to reserve for spill {} current target copy size {}",
          needed,
          copyTargetSize);
      if (!updateProtectedSize(needed)) {
        logger.debug("Memory Run: failed to reserve space");
        return false;
      }
    }

    recordLength += recordCount;

    // We can safely transfer ownership of data into our allocator since this will always succeed
    // (even if we
    // become overlimit).
    final RecordBatchItem rbi = new RecordBatchItem(allocator, incoming);

    try (RollbackCloseable commitable = AutoCloseables.rollbackable(rbi.data)) {
      final boolean first = size == 0;
      if (first) {
        maxBatchSize = rbi.getMemorySize();
        head = rbi;
        tail = rbi;
        sorter.setup(rbi.data.getVectorAccessible());
      } else {
        maxBatchSize = Math.max(maxBatchSize, batchSize);
        tail.setNext(rbi);
        tail = rbi;
      }

      sorter.addBatch(rbi.data, copyTargetAllocator);
      size++;
      commitable.commit();
      return true;
    } catch (SchemaChangeException | ClassTransformationException | IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public int getNumberOfBatches() {
    return size;
  }

  public int getRecordLength() {
    return recordLength;
  }

  @Override
  public void close() throws Exception {
    final List<AutoCloseable> closeables = Lists.newArrayList();

    RecordBatchItem item = head;

    closeables.add(item);
    while (item != null && item.getNext() != null) {
      item = item.getNext();
      closeables.add(item);
    }

    closeables.add(sorter);
    closeables.add(copyTargetAllocator);
    AutoCloseables.close(closeables);

    copyTargetAllocator = null;
  }

  private class RecordBatchItem implements AutoCloseable {

    private RecordBatchItem next;
    private final RecordBatchData data;
    private final int recordCount;

    public RecordBatchItem(BufferAllocator allocator, VectorAccessible incoming) {
      final RecordBatchData data = new RecordBatchData(incoming, allocator);
      this.data = data;
      recordCount = data.getRecordCount();
    }

    public long getMemorySize() {
      return allocator.getAllocatedMemory();
    }

    public int getRecordCount() {
      return recordCount;
    }

    public void setNext(RecordBatchItem next) {
      this.next = next;
    }

    public RecordBatchItem getNext() {
      return next;
    }

    @Override
    public void close() throws Exception {
      data.close();
    }
  }

  private SelectionVector4 closeToContainer(VectorContainer container, int targetBatchSize) {
    injector.injectChecked(executionControls, INJECTOR_OOM_ON_SORT, OutOfMemoryException.class);

    SelectionVector4 sv4 = sorter.getFinalSort(copyTargetAllocator, targetBatchSize);
    for (VectorWrapper<?> w : sorter.getHyperBatch()) {
      container.add(w.getValueVectors());
    }
    sorter.getHyperBatch().noReleaseClear();

    container.buildSchema(SelectionVectorMode.FOUR_BYTE);
    return sv4;
  }

  public MovingCopier closeToCopier(VectorContainer output, int targetBatchSize) {
    if (size == 0) {
      return new EmptyCopier();
    }

    Sv4HyperContainer input = new Sv4HyperContainer(allocator, schema);
    // clear since we're going to add schema below.
    input.clear();
    SelectionVector4 sv4 = closeToContainer(input, targetBatchSize);
    input.setSelectionVector4(sv4);

    return new TreeCopier(input, output);
  }

  public void closeToDisk(DiskRunManager manager) throws Exception {
    Sv4HyperContainer container = new Sv4HyperContainer(allocator, schema);
    container.clear();
    SelectionVector4 sv4 = closeToContainer(container, this.targetBatchSize);
    container.setSelectionVector4(sv4);
    manager.spill(container, copyTargetAllocator);

    close();
  }

  public void startMicroSpilling(DiskRunManager diskRunManager) throws Exception {
    final Sv4HyperContainer sv4HyperContainer = new Sv4HyperContainer(allocator, schema);
    sv4HyperContainer.clear();
    final SelectionVector4 sv4 = closeToContainer(sv4HyperContainer, this.recordLength);
    sv4HyperContainer.setSelectionVector4(sv4);
    sv4HyperContainer.setRecordCount(sv4HyperContainer.getSelectionVector4().getTotalCount());
    diskRunManager.startMicroSpilling(sv4HyperContainer);
  }

  public boolean spillNextBatch(final DiskRunManager diskRunManager) throws Exception {
    final boolean done = diskRunManager.spillNextBatch(copyTargetAllocator);
    if (done) {
      close();
    }
    return done;
  }

  enum CopierState {
    INIT,
    ZERO,
    REMAINDER,
    DONE
  }

  class TreeCopier implements MovingCopier {

    private CopierState state = CopierState.INIT;
    private final Copier copier;
    private int nextPosition = 0;
    private final SelectionVector4 sv4;

    public TreeCopier(VectorAccessible input, VectorContainer output) {
      this.sv4 = input.getSelectionVector4();
      this.copier = CopierOperator.getGenerated4Copier(classProducer, input, output);
    }

    @Override
    public int copy(int targetRecordCount) {
      targetRecordCount = Math.min(sv4.getCount(), targetRecordCount);
      int startPosition = -1;

      switch (state) {
        case INIT:
          // sv4 starts in data available position, no need to call next first time through.
          startPosition = 0;
          break;

        case ZERO:
          boolean hasMore = sv4.next();
          targetRecordCount = Math.min(sv4.getCount(), targetRecordCount);
          if (!hasMore) {
            state = CopierState.DONE;
            return 0;
          }
          startPosition = 0;
          break;

        case REMAINDER:
          startPosition = nextPosition;
          break;

        case DONE:
          return 0;

        default:
          throw new IllegalStateException("unknown state" + state.name());
      }

      final int targetCopy = targetRecordCount - startPosition;
      assert startPosition >= 0;
      assert targetCopy >= 0 && targetCopy <= targetRecordCount;
      final int recordsCopied = copier.copyRecords(startPosition, targetCopy);
      if (recordsCopied < targetCopy) {
        nextPosition = startPosition + recordsCopied;
        state = CopierState.REMAINDER;
      } else {
        nextPosition = 0;
        state = CopierState.ZERO;
      }

      return recordsCopied;
    }

    @Override
    public void close() throws Exception {
      sv4.close();
    }
  }

  private class EmptyCopier implements MovingCopier {

    @Override
    public void close() {}

    @Override
    public int copy(int targetRecordCount) {
      return 0;
    }
  }
}
