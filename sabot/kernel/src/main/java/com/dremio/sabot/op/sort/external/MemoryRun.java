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
package com.dremio.sabot.op.sort.external;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.AutoCloseables;
import com.dremio.common.AutoCloseables.RollbackCloseable;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.expr.CodeGenerator;
import com.dremio.exec.physical.config.ExternalSort;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.op.copier.Copier;
import com.dremio.sabot.op.copier.CopierOperator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import io.netty.buffer.ArrowBuf;

/**
 * Describes a set of ordered batches of data. Sorts new data as it is inserted
 * using a SplayTree. Sort has two stages:
 *
 * - Sort each batch using an Sv2
 * - Insert each batch into a SplayTree as it arrives
 *
 * Memory Guarantees Targeted: ensures that spilling can be done before
 * accepting a new batch of records. Does this by pre-reserving
 * BATCH_SIZE_MULTIPLIER times the size of the largest batch included in the
 * memory run.
 *
 * Can either be spilled or returned in SV4 sorted structure.
 */
class MemoryRun implements AutoCloseable {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MemoryRun.class);

  private final ExternalSort sortConfig;
  private final ClassProducer classProducer;
  private final BufferAllocator allocator;

  private ArrowBuf splayTreeBuffer;
  private SingleBatchSorter localSorter;
  private SplaySorter treeManager;
  private RecordBatchItem head;
  private RecordBatchItem tail;
  private final Schema schema;

  private long maxBatchSize;
  private int minRecordCount = Character.MAX_VALUE;
  private int recordLength;
  private int size;

  private BufferAllocator copyTargetAllocator;
  private long copyTargetSize;
  private final ExternalSortTracer tracer;
  private final int batchsizeMultiplier;

  public MemoryRun(
      ExternalSort sortConfig,
      ClassProducer classProducer,
      BufferAllocator allocator,
      Schema schema,
      ExternalSortTracer tracer,
      int batchsizeMultiplier
      ) {
    this.schema = schema;
    this.sortConfig = sortConfig;
    this.allocator = allocator;
    this.classProducer = classProducer;
    this.splayTreeBuffer = allocator.buffer(4096 * SplayTree.NODE_SIZE);
    splayTreeBuffer.setZero(0, splayTreeBuffer.capacity());
    this.tracer = tracer;
    this.batchsizeMultiplier = batchsizeMultiplier;
    updateProtectedSize(1 << 16);
  }

  private boolean updateProtectedSize(long needed) {
    Preconditions.checkArgument(needed > 0);
    BufferAllocator oldAllocator = copyTargetAllocator;
    logger.debug("Memory Run: attempting to update resserved memory for spill copy with new size as {}", needed);
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
    final long batchSize = new BatchStats().getSize(incoming, BatchStats.SizeType.WORSE_CASE);

    // if after adding incoming we end up with less than 20% of memory available for the sort allocator we should spill instead
    double headroom = allocator.getHeadroom() - batchSize;
    double total = allocator.getAllocatedMemory() + headroom;

    if (headroom/total < 0.2) {
      logger.debug("Memory Run: less than 20% of memory available after adding, failed to add batch");
      return false;
    }

    // make sure we have room for an expanded tree before adding record batch.
    if (!expandTreeIfNecessary(incoming.getRecordCount())) {
      logger.debug("Memory Run: no room for expanding the tree, failed to add batch");
      return false;
    }

    if (treeManager != null && (treeManager.getHyperBatch().size() >= 65535 || size >= 32768)) {
      logger.debug("Memory Run: no room for storing new batch, failed to add batch");
      return false;
    }

    // copy size is BATCH_SIZE_MULTIPLIER worse case batch size since we have to
    // be careful of memory rounding. sv4 for sort output vector is the total
    // number of records x4 bytes per sv4 value. we need to reserve this here
    // (but not allocate so we always guarantee that we can allocate later.
    final long needed = batchSize * batchsizeMultiplier + nextPowerOfTwo((recordLength + incoming.getRecordCount()) * 4);
    if (copyTargetSize < needed) {
      logger.debug("Memory Run: new size needed to reserve for spill {} current target copy size {}", needed, copyTargetSize);
      if (!updateProtectedSize(needed)) {
        logger.debug("Memory Run: failed to reserve space");
        return false;
      }
    }

    final int recordCount = incoming.getRecordCount();
    if (recordCount < minRecordCount) {
      minRecordCount = recordCount;
    }

    // We can safely transfer ownership of data into our allocator since this will always succeed (even if we become overlimit).
    final RecordBatchItem rbi = new RecordBatchItem(allocator, incoming);

    try(RollbackCloseable commitable = AutoCloseables.rollbackable(rbi.data)){
      final boolean first = size == 0;
      if (first) {
        maxBatchSize = rbi.getMemorySize();
        head = tail = rbi;
        compileSortingClasses(rbi.data.getContainer());
      } else {
        maxBatchSize = Math.max(maxBatchSize, batchSize);
        tail.setNext(rbi);
        tail = rbi;
        treeManager.getHyperBatch().addBatch(rbi.data.getContainer());
      }

      sortBatchAndInsertInTree(rbi);
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

  private boolean expandTreeIfNecessary(int newRecords) {
    // 0 position is used for null in splay tree, see need to account for this
    final int requiredSize = (recordLength + newRecords + 1) * SplayTree.NODE_SIZE;

    while (splayTreeBuffer.capacity() < requiredSize) {

      try {
        final ArrowBuf oldSplayTree = splayTreeBuffer;
        this.splayTreeBuffer = allocator.buffer(splayTreeBuffer.capacity() * 2);
        splayTreeBuffer.setBytes(0, oldSplayTree, 0, oldSplayTree.capacity());
        splayTreeBuffer.setZero(oldSplayTree.capacity(), splayTreeBuffer.capacity() - oldSplayTree.capacity());
        if(treeManager != null){
          treeManager.setData(splayTreeBuffer);
        }
        oldSplayTree.close();
      } catch (OutOfMemoryException ex) {
        return false;
      }
    }
    recordLength += newRecords;
    return true;
  }

  private void sortBatchAndInsertInTree(RecordBatchItem item)
      throws SchemaChangeException {
    // next we'll generate a new sv2 for the local sort. We do this even if the
    // incoming batch has an sv2. This is because we need to treat that one as
    // immutable.
    //
    // Note that we shouldn't have an issue with allocation here since we'll use
    // the copyTargetAllocator.
    // This isn't yet used and is guaranteed to be larger than the size of this
    // ephemeral allocation.
    try (SelectionVector2 localSortVector = new SelectionVector2(copyTargetAllocator)) {
      final int recordCount = item.getRecordCount();
      localSortVector.allocateNew(recordCount);
      final SelectionVector2 incomingSv2 = item.data.getSv2();
      if (incomingSv2 != null) {
        // just copy the sv2.
        localSortVector.getBuffer(false).writeBytes(incomingSv2.getBuffer(false), 0, recordCount * 2);
      } else {
        for (int i = 0; i < recordCount; i++) {
          localSortVector.setIndex(i * 2, i);
        }
      }

      // quicksort for cache-local performance benefits (includes resetting vector references)
      localSorter.setup(classProducer.getFunctionContext(), localSortVector, item.data.getContainer());
      localSorter.sort(localSortVector);

      // now we need to insert the values into the splay tree.
      treeManager.add(localSortVector, item.data);

    }
  }

  private void compileSortingClasses(VectorAccessible batch)
      throws ClassTransformationException, SchemaChangeException, IOException {

    { // Local (single batch) sorter
      CodeGenerator<SingleBatchSorter> cg = classProducer.createGenerator(SingleBatchSorter.TEMPLATE_DEFINITION);
      ClassGenerator<SingleBatchSorter> g = cg.getRoot();
      ExternalSortOperator.generateComparisons(g, batch, sortConfig.getOrderings(), classProducer);
      this.localSorter = cg.getImplementationClass();
    }

    { // Tree
      CodeGenerator<SplaySorter> cg = classProducer.createGenerator(SplaySorter.TEMPLATE_DEFINITION);
      ClassGenerator<SplaySorter> g = cg.getRoot();
      final Sv4HyperContainer container = new Sv4HyperContainer(allocator, schema);
      ExternalSortOperator.generateComparisons(g, container, sortConfig.getOrderings(), classProducer);
      this.treeManager = cg.getImplementationClass();
      treeManager.init(classProducer.getFunctionContext(), container);
      treeManager.setData(splayTreeBuffer);
    }
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

    closeables.add(splayTreeBuffer);
    closeables.add(copyTargetAllocator);
    AutoCloseables.close(closeables);

    splayTreeBuffer = null;
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
    SelectionVector4 sv4 = treeManager.getFinalSort(copyTargetAllocator, targetBatchSize);
    for (VectorWrapper<?> w : treeManager.getHyperBatch()) {
      container.add(w.getValueVectors());
    }
    treeManager.getHyperBatch().noReleaseClear();
    container.buildSchema(SelectionVectorMode.FOUR_BYTE);
    return sv4;
  }

  public MovingCopier closeToCopier(VectorContainer output, int targetBatchSize) {
    if(size == 0){
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
    // passing minRecordCount to closeToContainer() will ensure we will spill batches of this size
    SelectionVector4 sv4 = closeToContainer(container, minRecordCount);
    container.setSelectionVector4(sv4);
    manager.spill(container, copyTargetAllocator);

    close();
  }

  enum CopierState {INIT, ZERO, REMAINDER, DONE}

  class TreeCopier implements MovingCopier {

    private CopierState state = CopierState.INIT;
    private final Copier copier;
    private int nextPosition = 0;
    private final SelectionVector4 sv4;

    public TreeCopier(VectorAccessible input, VectorContainer output) {
      this.sv4 = input.getSelectionVector4();
      this.copier = CopierOperator.getGenerated4Copier(
          classProducer,
          input,
          output);
    }


    @Override
    public int copy(int targetRecordCount) {
      targetRecordCount = Math.min(sv4.getCount(), targetRecordCount);
      int startPosition = -1;

      switch(state){

      case INIT:
        // sv4 starts in data available position, no need to call next first time through.
        startPosition = 0;
        break;

      case ZERO:
        boolean hasMore = sv4.next();
        targetRecordCount = Math.min(sv4.getCount(), targetRecordCount);
        if(!hasMore){
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
      if(recordsCopied < targetCopy){
        nextPosition = startPosition + recordsCopied;
        state = CopierState.REMAINDER;
      }else{
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
    public void close() {
    }

    @Override
    public int copy(int targetRecordCount) {
      return 0;
    }

  }
}
